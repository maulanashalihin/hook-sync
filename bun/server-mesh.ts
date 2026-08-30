// hook-sync Bun implementation — full mesh topology
// Bun.serve + bun:sqlite + crypto.randomUUID()
// Multi-peer: repeated --peer flags, per-peer watermark in _peer_state
//
// Usage:
//   bun server-mesh.ts --id nodeA --db a.db --listen :9001 \
//     --peer http://localhost:9002 \
//     --peer http://localhost:9003 \
//     --peer http://localhost:9004 \
//     --batch-ms 50

import { Database } from "bun:sqlite";

// --- Parse args ---
const args = process.argv.slice(2);
const getArg = (name: string, def: string) => {
	const i = args.indexOf(`--${name}`);
	return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
};

// Collect repeated --peer flags
const PEERS: string[] = [];
for (let i = 0; i < args.length; i++) {
	if (args[i] === "--peer" && i + 1 < args.length) {
		PEERS.push(args[i + 1]);
		i++;
	}
}

const ID = getArg("id", "");
const DB_PATH = getArg("db", "");
const LISTEN = getArg("listen", "");
const BATCH_MS = parseInt(getArg("batch-ms", "50"));

if (!ID || !DB_PATH || !LISTEN) {
	console.error(
		"usage: bun server-mesh.ts --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --peer http://localhost:9003",
	);
	process.exit(1);
}

// --- Database ---
const db = new Database(DB_PATH, { create: true });
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA synchronous = NORMAL");
db.exec("PRAGMA busy_timeout = 5000");

db.exec(`
	CREATE TABLE IF NOT EXISTS items (
		id TEXT PRIMARY KEY,
		name TEXT,
		value INTEGER,
		created_at INTEGER,
		updated_at INTEGER,
		node_id TEXT
	);

	CREATE TABLE IF NOT EXISTS _meta (
		key TEXT PRIMARY KEY,
		value INTEGER
	);
	INSERT OR IGNORE INTO _meta(key, value) VALUES('syncing', 0);

	CREATE TABLE IF NOT EXISTS _changes (
		change_id INTEGER PRIMARY KEY AUTOINCREMENT,
		op TEXT,
		row_id TEXT,
		row_data TEXT
	);

	CREATE TABLE IF NOT EXISTS _dead_letter (
		dead_id INTEGER PRIMARY KEY AUTOINCREMENT,
		op TEXT,
		row_id TEXT,
		row_data TEXT,
		failed_at INTEGER,
		retry_count INTEGER DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS _peer_state (
		peer_url TEXT PRIMARY KEY,
		last_acked INTEGER DEFAULT 0
	);
`);

// Init peer state rows for configured peers
const stmtInitPeer = db.prepare(
	"INSERT OR IGNORE INTO _peer_state(peer_url, last_acked) VALUES(?, 0)",
);
for (const peer of PEERS) {
	stmtInitPeer.run(peer);
}

// Triggers — only fire when not syncing
db.exec(`
	CREATE TRIGGER IF NOT EXISTS items_ai AFTER INSERT ON items
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(op, row_id, row_data) VALUES('INSERT', NEW.id,
			json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
				'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
	END;

	CREATE TRIGGER IF NOT EXISTS items_au AFTER UPDATE ON items
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(op, row_id, row_data) VALUES('UPDATE', NEW.id,
			json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
				'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
	END;

	CREATE TRIGGER IF NOT EXISTS items_ad AFTER DELETE ON items
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(op, row_id, row_data) VALUES('DELETE', OLD.id,
			json_object('id', OLD.id, 'name', OLD.name, 'value', OLD.value,
				'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'node_id', OLD.node_id));
	END;
`);

// --- Precompile statements ---
const stmtInsert = db.prepare(
	"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
);
const stmtUpdate = db.prepare(
	"UPDATE items SET name = ?, value = ?, updated_at = ? WHERE id = ?",
);
const stmtDelete = db.prepare("DELETE FROM items WHERE id = ?");
const stmtReplace = db.prepare(
	"INSERT OR REPLACE INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
);
const stmtGetUpdatedAt = db.prepare(
	"SELECT updated_at FROM items WHERE id = ?",
);
const stmtList = db.prepare(
	"SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100",
);
const stmtGet = db.prepare(
	"SELECT id, name, value, created_at, updated_at, node_id FROM items WHERE id = ?",
);
const stmtCount = db.prepare("SELECT COUNT(*) as count FROM items");
const stmtPendingChanges = db.prepare("SELECT COUNT(*) as count FROM _changes");
const stmtDeadLetterCount = db.prepare(
	"SELECT COUNT(*) as count FROM _dead_letter",
);

// Per-peer watermark statements
const stmtPeerChanges = db.prepare(
	"SELECT change_id, op, row_id, row_data FROM _changes WHERE change_id > ? ORDER BY change_id LIMIT 10000",
);
const stmtUpdatePeerAck = db.prepare(
	"UPDATE _peer_state SET last_acked = ? WHERE peer_url = ?",
);
const stmtPeerStates = db.prepare(
	"SELECT peer_url, last_acked FROM _peer_state",
);
const stmtMinAck = db.prepare(
	"SELECT MIN(last_acked) as min_ack FROM _peer_state",
);
const stmtDeleteChanges = db.prepare(
	"DELETE FROM _changes WHERE change_id <= ?",
);

const stmtSyncOn = db.prepare(
	"UPDATE _meta SET value = 1 WHERE key = 'syncing'",
);
const stmtSyncOff = db.prepare(
	"UPDATE _meta SET value = 0 WHERE key = 'syncing'",
);

const BACKOFF_MS = [50, 100, 200, 400, 800];

// --- Ship to a single peer (watermark-based: only send changes this peer hasn't ACKed) ---
async function shipToPeer(
	peerUrl: string,
	lastAcked: number,
): Promise<"acked" | "conn_error" | "ack_mismatch"> {
	const rows = stmtPeerChanges.all(lastAcked) as {
		change_id: number;
		op: string;
		row_id: string;
		row_data: string | null;
	}[];
	if (rows.length === 0) return "acked";

	const batchId = rows[rows.length - 1].change_id;
	const changes = rows.map((r) => ({
		op: r.op,
		table: "items",
		row: r.row_data ? JSON.parse(r.row_data) : null,
		old_id: r.op === "DELETE" ? r.row_id : null,
	}));

	let connError = false;
	for (let attempt = 0; attempt < BACKOFF_MS.length; attempt++) {
		try {
			const resp = await fetch(`${peerUrl}/sync`, {
				method: "POST",
				headers: { "Content-Type": "application/json", "X-Node-Id": ID },
				body: JSON.stringify({ batch_id: batchId, changes }),
			});
			if (resp.ok) {
				const body = (await resp.json()) as { applied: number; ack: number };
				if (body.ack === batchId) {
					stmtUpdatePeerAck.run(batchId, peerUrl);
					return "acked";
				}
				// ACK mismatch — protocol error, don't retry
				console.error(
					`[${ID}] peer ${peerUrl} ACK mismatch: got ${body.ack} want ${batchId}, skipping batch for this peer`,
				);
				stmtUpdatePeerAck.run(batchId, peerUrl);
				return "ack_mismatch";
			}
			connError = true;
		} catch {
			// network error — retry
			connError = true;
		}
		if (attempt < BACKOFF_MS.length - 1) await Bun.sleep(BACKOFF_MS[attempt]);
	}

	// Peer unreachable after 5 retries — keep changes, retry next cycle.
	// Changes stay in _changes until ALL peers ACK (watermark = min(last_acked)).
	console.error(
		`[${ID}] peer ${peerUrl} unreachable after 5 retries, will retry next cycle`,
	);
	return "conn_error";
}

// --- Cleanup: delete changes that ALL peers have ACKed ---
function cleanupChanges() {
	const { min_ack } = stmtMinAck.get() as { min_ack: number | null };
	if (min_ack && min_ack > 0) {
		stmtDeleteChanges.run(min_ack);
	}
}

// --- Ship loop: ship to all peers concurrently, then cleanup ---
let shipping = false;
setInterval(() => {
	if (shipping) return;
	if (PEERS.length === 0) return;

	shipping = true;
	(async () => {
		try {
			const peers = stmtPeerStates.all() as {
				peer_url: string;
				last_acked: number;
			}[];
			await Promise.all(peers.map((p) => shipToPeer(p.peer_url, p.last_acked)));
			cleanupChanges();
		} finally {
			shipping = false;
		}
	})();
}, BATCH_MS);

// --- Apply received changes (syncing flag prevents re-capture) ---
const applyChanges = db.transaction(
	(
		changes: {
			op: string;
			row: {
				id: string;
				name: string;
				value: number;
				created_at: number;
				updated_at: number;
				node_id: string;
			} | null;
			old_id: string | null;
		}[],
	): number => {
		stmtSyncOn.run();
		let applied = 0;
	try {
		for (const c of changes) {
			if (c.op === "INSERT" || c.op === "UPDATE") {
				if (!c.row) continue;
				const r = c.row;
				// Last-write-wins: skip if existing row is newer than incoming
				const existing = stmtGetUpdatedAt.get(r.id) as { updated_at: number } | null;
				if (existing && existing.updated_at > r.updated_at) {
					continue;
				}
				stmtReplace.run(
					r.id,
					r.name,
					r.value,
					r.created_at,
					r.updated_at,
					r.node_id,
				);
				applied++;
			} else if (c.op === "DELETE") {
				if (!c.old_id) continue;
				// Last-write-wins: skip delete if row was updated after deletion
				if (c.row) {
					const deleteUpdatedAt = c.row.updated_at;
					const existing = stmtGetUpdatedAt.get(c.old_id) as { updated_at: number } | null;
					if (existing && existing.updated_at > deleteUpdatedAt) {
						continue; // row was updated after delete, keep the update
					}
				}
				stmtDelete.run(c.old_id);
				applied++;
			}
		}
		} finally {
			stmtSyncOff.run();
		}
		return applied;
	},
);

// --- HTTP server (Bun.serve native) ---
const server = Bun.serve({
	port: parseInt(LISTEN.replace(":", "")),
	fetch(req) {
		const url = new URL(req.url);
		const method = req.method;
		const path = url.pathname;

		// POST /sync — { batch_id, changes } → { applied, ack }
		if (method === "POST" && path === "/sync") {
			return req
				.json()
				.then(
					(body: {
						batch_id: number;
						changes: Parameters<typeof applyChanges>[0];
					}) => {
						const applied = applyChanges(body.changes);
						return Response.json({ applied, ack: body.batch_id });
					},
				)
				.catch((e: unknown) =>
					Response.json({ error: String(e) }, { status: 400 }),
				);
		}

		// GET /api/items
		if (method === "GET" && path === "/api/items") {
			return Response.json(stmtList.all());
		}

		// GET /api/items/:id
		if (method === "GET" && path.startsWith("/api/items/")) {
			const id = path.slice("/api/items/".length);
			const row = stmtGet.get(id);
			return row
				? Response.json(row)
				: Response.json({ error: "not found" }, { status: 404 });
		}

		// POST /api/items
		if (method === "POST" && path === "/api/items") {
			return req
				.json()
				.then((body: { name: string; value: number }) => {
					const id = crypto.randomUUID();
					const now = Date.now();
					stmtInsert.run(id, body.name, body.value, now, now, ID);
					return Response.json({
						id,
						name: body.name,
						value: body.value,
						created_at: now,
						node_id: ID,
					});
				})
				.catch((e: unknown) =>
					Response.json({ error: String(e) }, { status: 500 }),
				);
		}

		// PUT /api/items/:id
		if (method === "PUT" && path.startsWith("/api/items/")) {
			const id = path.slice("/api/items/".length);
			return req
				.json()
				.then((body: { name: string; value: number }) => {
					const now = Date.now();
					stmtUpdate.run(body.name, body.value, now, id);
					return Response.json({
						id,
						name: body.name,
						value: body.value,
						updated_at: now,
					});
				})
				.catch((e: unknown) =>
					Response.json({ error: String(e) }, { status: 500 }),
				);
		}

		// DELETE /api/items/:id
		if (method === "DELETE" && path.startsWith("/api/items/")) {
			const id = path.slice("/api/items/".length);
			stmtDelete.run(id);
			return Response.json({ deleted: id });
		}

		// GET /health — includes per-peer watermark state
		if (method === "GET" && path === "/health") {
			const { count } = stmtCount.get() as { count: number };
			const { count: pendingChanges } = stmtPendingChanges.get() as {
				count: number;
			};
			const { count: deadLetter } = stmtDeadLetterCount.get() as {
				count: number;
			};
			const peers = stmtPeerStates.all() as {
				peer_url: string;
				last_acked: number;
			}[];
			return Response.json({
				ok: true,
				node_id: ID,
				item_count: count,
				pending_changes: pendingChanges,
				dead_letter: deadLetter,
				peers: peers.map((p) => ({
					peer_url: p.peer_url,
					last_acked: p.last_acked,
				})),
			});
		}

		return Response.json({ error: "not found" }, { status: 404 });
	},
});

console.log(
	`[${ID}] listening on ${LISTEN}, peers=[${PEERS.join(", ")}], batch=${BATCH_MS}ms`,
);
