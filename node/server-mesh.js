// hook-sync Node.js implementation — full mesh topology
// hyper-express + better-sqlite3 + crypto.randomUUID()
// Multi-peer: repeated --peer flags, per-peer watermark in _peer_state
//
// Usage:
//   node server-mesh.js --id nodeA --db a.db --listen :9001 \
//     --peer http://localhost:9002 \
//     --peer http://localhost:9003 \
//     --peer http://localhost:9004 \
//     --batch-ms 50

const HyperExpress = require("hyper-express");
const Database = require("better-sqlite3");
const crypto = require("crypto");

// --- Parse args ---
const args = process.argv.slice(2);
const getArg = (name, def) => {
	const i = args.indexOf(`--${name}`);
	return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
};

// Collect repeated --peer flags
const PEERS = [];
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
		"usage: node server-mesh.js --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --peer http://localhost:9003",
	);
	process.exit(1);
}

// --- Database ---
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("busy_timeout = 5000");

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
		INSERT INTO _changes(op, row_id, row_data) VALUES('DELETE', OLD.id, NULL);
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
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

// --- Ship to a single peer (watermark-based: only send changes this peer hasn't ACKed) ---
async function shipToPeer(peerUrl, lastAcked) {
	const rows = stmtPeerChanges.all(lastAcked);
	if (rows.length === 0) return true;

	const batchId = rows[rows.length - 1].change_id;
	const changes = rows.map((r) => ({
		op: r.op,
		table: "items",
		row: r.row_data ? JSON.parse(r.row_data) : null,
		old_id: r.op === "DELETE" ? r.row_id : null,
	}));

	for (let attempt = 0; attempt < BACKOFF_MS.length; attempt++) {
		try {
			const resp = await fetch(`${peerUrl}/sync`, {
				method: "POST",
				headers: { "Content-Type": "application/json", "X-Node-Id": ID },
				body: JSON.stringify({ batch_id: batchId, changes }),
			});
			if (resp.ok) {
				const body = await resp.json();
				if (body.ack === batchId) {
					stmtUpdatePeerAck.run(batchId, peerUrl);
					return true;
				}
			}
		} catch {
			// network error — retry
		}
		if (attempt < BACKOFF_MS.length - 1) await sleep(BACKOFF_MS[attempt]);
	}

	// Peer unreachable after 5 retries — don't dead-letter, retry next cycle.
	// Changes stay in _changes until ALL peers ACK (watermark = min(last_acked)).
	console.error(
		`[${ID}] peer ${peerUrl} unreachable after 5 retries, will retry next cycle`,
	);
	return false;
}

// --- Cleanup: delete changes that ALL peers have ACKed ---
function cleanupChanges() {
	const { min_ack } = stmtMinAck.get();
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
			const peers = stmtPeerStates.all();
			await Promise.all(peers.map((p) => shipToPeer(p.peer_url, p.last_acked)));
			cleanupChanges();
		} finally {
			shipping = false;
		}
	})();
}, BATCH_MS);

// --- Apply received changes (syncing flag prevents re-capture) ---
const applyChanges = db.transaction((changes) => {
	stmtSyncOn.run();
	let applied = 0;
	try {
		for (const c of changes) {
			if (c.op === "INSERT" || c.op === "UPDATE") {
				if (!c.row) continue;
				const r = c.row;
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
				stmtDelete.run(c.old_id);
				applied++;
			}
		}
	} finally {
		stmtSyncOff.run();
	}
	return applied;
});

// --- HTTP server (hyper-express / uWebSockets) ---
const app = new HyperExpress.Server();

// POST /sync — { batch_id, changes } → { applied, ack }
app.post("/sync", async (req, res) => {
	try {
		const body = await req.json();
		const applied = applyChanges(body.changes);
		res.json({ applied, ack: body.batch_id });
	} catch (e) {
		res.status(400).json({ error: e.message });
	}
});

// GET /api/items
app.get("/api/items", (req, res) => {
	res.json(stmtList.all());
});

// GET /api/items/:id
app.get("/api/items/:id", (req, res) => {
	const row = stmtGet.get(req.params.id);
	if (row) {
		res.json(row);
	} else {
		res.status(404).json({ error: "not found" });
	}
});

// POST /api/items
app.post("/api/items", async (req, res) => {
	try {
		const body = await req.json();
		const id = crypto.randomUUID();
		const now = Date.now();
		stmtInsert.run(id, body.name, body.value, now, now, ID);
		res.json({
			id,
			name: body.name,
			value: body.value,
			created_at: now,
			node_id: ID,
		});
	} catch (e) {
		res.status(500).json({ error: e.message });
	}
});

// PUT /api/items/:id
app.put("/api/items/:id", async (req, res) => {
	try {
		const body = await req.json();
		const now = Date.now();
		stmtUpdate.run(body.name, body.value, now, req.params.id);
		res.json({
			id: req.params.id,
			name: body.name,
			value: body.value,
			updated_at: now,
		});
	} catch (e) {
		res.status(500).json({ error: e.message });
	}
});

// DELETE /api/items/:id
app.delete("/api/items/:id", (req, res) => {
	stmtDelete.run(req.params.id);
	res.json({ deleted: req.params.id });
});

// GET /health — includes per-peer watermark state
app.get("/health", (req, res) => {
	const { count } = stmtCount.get();
	const { count: pendingChanges } = stmtPendingChanges.get();
	const { count: deadLetter } = stmtDeadLetterCount.get();
	const peers = stmtPeerStates.all();
	res.json({
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
});

app
	.listen(parseInt(LISTEN.replace(":", "")))
	.then(() => {
		console.log(
			`[${ID}] listening on ${LISTEN}, peers=[${PEERS.join(", ")}], batch=${BATCH_MS}ms`,
		);
	})
	.catch((e) => {
		console.error(`[${ID}] failed to listen:`, e);
		process.exit(1);
	});
