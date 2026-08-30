// hook-sync Node.js implementation
// Uses better-sqlite3 + hyper-express (uWebSockets backend)
// Same wire protocol as Go and Bun implementations
//
// Usage: node server.js --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50

const HyperExpress = require("hyper-express");
const Database = require("better-sqlite3");
const crypto = require("crypto");

// --- Parse args ---
const args = process.argv.slice(2);
const getArg = (name, def) => {
	const i = args.indexOf(`--${name}`);
	return i >= 0 ? args[i + 1] : def;
};

const ID = getArg("id", "");
const DB_PATH = getArg("db", "");
const LISTEN = getArg("listen", "");
const PEER_URL = getArg("peer", "");
const BATCH_MS = parseInt(getArg("batch-ms", "50"));

if (!ID || !DB_PATH || !LISTEN) {
	console.error("usage: node server.js --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50");
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

// --- Batch ship ---
async function shipBatch(changes) {
	if (!PEER_URL || changes.length === 0) return;
	try {
		const resp = await fetch(`${PEER_URL}/sync`, {
			method: "POST",
			headers: { "Content-Type": "application/json", "X-Node-Id": ID },
			body: JSON.stringify(changes),
		});
		if (!resp.ok) {
			console.error(`[${ID}] ship failed: ${resp.status}`);
		}
	} catch (e) {
		console.error(`[${ID}] ship error:`, e.message);
	}
}

// Poll _changes table every BATCH_MS, ship, delete shipped
setInterval(() => {
	const rows = db.prepare("SELECT change_id, op, row_id, row_data FROM _changes ORDER BY change_id LIMIT 100").all();
	if (rows.length === 0) return;

	const changes = rows.map((r) => ({
		op: r.op,
		table: "items",
		row: r.row_data ? JSON.parse(r.row_data) : null,
		old_id: r.op === "DELETE" ? r.row_id : null,
	}));

	const lastId = rows[rows.length - 1].change_id;
	db.prepare("DELETE FROM _changes WHERE change_id <= ?").run(lastId);

	shipBatch(changes);
}, BATCH_MS);

// --- Apply received changes ---
function applyChanges(changes) {
	db.prepare("UPDATE _meta SET value = 1 WHERE key = 'syncing'").run();
	let applied = 0;
	try {
		const insertReplace = db.prepare(
			"INSERT OR REPLACE INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)"
		);
		const deleteStmt = db.prepare("DELETE FROM items WHERE id = ?");

		for (const c of changes) {
			if (c.op === "INSERT" || c.op === "UPDATE") {
				if (!c.row) continue;
				const r = c.row;
				insertReplace.run(r.id, r.name, r.value, r.created_at, r.updated_at, r.node_id);
				applied++;
			} else if (c.op === "DELETE") {
				if (!c.old_id) continue;
				deleteStmt.run(c.old_id);
				applied++;
			}
		}
	} finally {
		db.prepare("UPDATE _meta SET value = 0 WHERE key = 'syncing'").run();
	}
	return applied;
}

// --- HTTP server (hyper-express / uWebSockets) ---
const app = new HyperExpress.Server();

// POST /sync
app.post("/sync", async (req, res) => {
	try {
		const changes = await req.json();
		const applied = applyChanges(changes);
		res.json({ applied });
	} catch (e) {
		res.status(400).json({ error: e.message });
	}
});

// GET /api/items
app.get("/api/items", (req, res) => {
	const rows = db.prepare(
		"SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100"
	).all();
	res.json(rows);
});

// GET /api/items/:id
app.get("/api/items/:id", (req, res) => {
	const row = db.prepare(
		"SELECT id, name, value, created_at, updated_at, node_id FROM items WHERE id = ?"
	).get(req.params.id);
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
		const id = crypto.randomUUID(); // UUIDv4 native (Node 19+)
		const now = Date.now();
		db.prepare(
			"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)"
		).run(id, body.name, body.value, now, now, ID);
		res.json({ id, name: body.name, value: body.value, created_at: now, node_id: ID });
	} catch (e) {
		res.status(500).json({ error: e.message });
	}
});

// PUT /api/items/:id
app.put("/api/items/:id", async (req, res) => {
	try {
		const body = await req.json();
		const now = Date.now();
		db.prepare("UPDATE items SET name = ?, value = ?, updated_at = ? WHERE id = ?").run(
			body.name, body.value, now, req.params.id
		);
		res.json({ id: req.params.id, name: body.name, value: body.value, updated_at: now });
	} catch (e) {
		res.status(500).json({ error: e.message });
	}
});

// DELETE /api/items/:id
app.delete("/api/items/:id", (req, res) => {
	db.prepare("DELETE FROM items WHERE id = ?").run(req.params.id);
	res.json({ deleted: req.params.id });
});

// GET /health
app.get("/health", (req, res) => {
	const { count } = db.prepare("SELECT COUNT(*) as count FROM items").get();
	res.json({ ok: true, node_id: ID, item_count: count });
});

app.listen(parseInt(LISTEN.replace(":", "")))
	.then(() => {
		console.log(`[${ID}] listening on ${LISTEN}, peer=${PEER_URL}, batch=${BATCH_MS}ms`);
	})
	.catch((e) => {
		console.error(`[${ID}] failed to listen:`, e);
		process.exit(1);
	});
