// hook-sync Bun implementation — pure Bun, zero Node.js APIs
// Bun.serve + bun:sqlite + crypto.randomUUID()
// Same wire protocol as Go and Node implementations
//
// Usage: bun server.ts --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50

import { Database } from "bun:sqlite";

// --- Parse args ---
const args = process.argv.slice(2);
const getArg = (name: string, def: string) => {
	const i = args.indexOf(`--${name}`);
	return i >= 0 ? args[i + 1] : def;
};

const ID = getArg("id", "");
const DB_PATH = getArg("db", "");
const LISTEN = getArg("listen", "");
const PEER_URL = getArg("peer", "");
const BATCH_MS = parseInt(getArg("batch-ms", "50"));

if (!ID || !DB_PATH || !LISTEN) {
	console.error("usage: bun server-native.ts --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50");
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
`);

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
const stmtInsert = db.prepare("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)");
const stmtUpdate = db.prepare("UPDATE items SET name = ?, value = ?, updated_at = ? WHERE id = ?");
const stmtDelete = db.prepare("DELETE FROM items WHERE id = ?");
const stmtReplace = db.prepare("INSERT OR REPLACE INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)");
const stmtList = db.prepare("SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100");
const stmtGet = db.prepare("SELECT id, name, value, created_at, updated_at, node_id FROM items WHERE id = ?");
const stmtCount = db.prepare("SELECT COUNT(*) as count FROM items");
const stmtChanges = db.prepare("SELECT change_id, op, row_id, row_data FROM _changes ORDER BY change_id LIMIT 100");
const stmtDeleteChanges = db.prepare("DELETE FROM _changes WHERE change_id <= ?");
const stmtSyncOn = db.prepare("UPDATE _meta SET value = 1 WHERE key = 'syncing'");
const stmtSyncOff = db.prepare("UPDATE _meta SET value = 0 WHERE key = 'syncing'");

// --- Batch ship ---
async function shipBatch(changes: unknown[]) {
	if (!PEER_URL || changes.length === 0) return;
	try {
		const resp = await fetch(`${PEER_URL}/sync`, {
			method: "POST",
			headers: { "Content-Type": "application/json", "X-Node-Id": ID },
			body: JSON.stringify(changes),
		});
		if (!resp.ok) console.error(`[${ID}] ship failed: ${resp.status}`);
	} catch (e) {
		console.error(`[${ID}] ship error:`, e);
	}
}

// Poll _changes every BATCH_MS
setInterval(() => {
	const rows = stmtChanges.all() as { change_id: number; op: string; row_id: string; row_data: string | null }[];
	if (rows.length === 0) return;

	const changes = rows.map((r) => ({
		op: r.op,
		table: "items",
		row: r.row_data ? JSON.parse(r.row_data) : null,
		old_id: r.op === "DELETE" ? r.row_id : null,
	}));

	stmtDeleteChanges.run(rows[rows.length - 1].change_id);
	shipBatch(changes);
}, BATCH_MS);


// --- Apply received changes (transaction prevents local writes from interleaving) ---
const applyChanges = db.transaction((changes: { op: string; row: { id: string; name: string; value: number; created_at: number; updated_at: number; node_id: string } | null; old_id: string | null }[]): number => {
	stmtSyncOn.run();
	let applied = 0;
	try {
		for (const c of changes) {
			if (c.op === "INSERT" || c.op === "UPDATE") {
				if (!c.row) continue;
				const r = c.row;
				stmtReplace.run(r.id, r.name, r.value, r.created_at, r.updated_at, r.node_id);
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

// --- HTTP server (Bun.serve native) ---
const server = Bun.serve({
	port: parseInt(LISTEN.replace(":", "")),
	fetch(req) {
		const url = new URL(req.url);
		const method = req.method;
		const path = url.pathname;

		// POST /sync
		if (method === "POST" && path === "/sync") {
			return req.json().then((changes: Parameters<typeof applyChanges>[0]) => {
				const applied = applyChanges(changes);
				return Response.json({ applied });
			}).catch((e: unknown) => Response.json({ error: String(e) }, { status: 400 }));
		}

		// GET /api/items
		if (method === "GET" && path === "/api/items") {
			return Response.json(stmtList.all());
		}

		// GET /api/items/:id
		if (method === "GET" && path.startsWith("/api/items/")) {
			const id = path.slice("/api/items/".length);
			const row = stmtGet.get(id);
			return row ? Response.json(row) : Response.json({ error: "not found" }, { status: 404 });
		}

		// POST /api/items
		if (method === "POST" && path === "/api/items") {
			return req.json().then((body: { name: string; value: number }) => {
				const id = crypto.randomUUID();
				const now = Date.now();
				stmtInsert.run(id, body.name, body.value, now, now, ID);
				return Response.json({ id, name: body.name, value: body.value, created_at: now, node_id: ID });
			}).catch((e: unknown) => Response.json({ error: String(e) }, { status: 500 }));
		}

		// PUT /api/items/:id
		if (method === "PUT" && path.startsWith("/api/items/")) {
			const id = path.slice("/api/items/".length);
			return req.json().then((body: { name: string; value: number }) => {
				const now = Date.now();
				stmtUpdate.run(body.name, body.value, now, id);
				return Response.json({ id, name: body.name, value: body.value, updated_at: now });
			}).catch((e: unknown) => Response.json({ error: String(e) }, { status: 500 }));
		}

		// DELETE /api/items/:id
		if (method === "DELETE" && path.startsWith("/api/items/")) {
			const id = path.slice("/api/items/".length);
			stmtDelete.run(id);
			return Response.json({ deleted: id });
		}

		// GET /health
		if (method === "GET" && path === "/health") {
			const { count } = stmtCount.get() as { count: number };
			return Response.json({ ok: true, node_id: ID, item_count: count });
		}

		return Response.json({ error: "not found" }, { status: 404 });
	},
});

console.log(`[${ID}] listening on ${LISTEN}, peer=${PEER_URL}, batch=${BATCH_MS}ms`);
