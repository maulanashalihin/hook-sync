// hook-sync Bun implementation
// Uses SQLite triggers for CDC (preupdate_hook not exposed in bun:sqlite)
// Same wire protocol as Go implementation — nodes can sync cross-language
//
// Usage: bun server.ts --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50

import { Database } from "bun:sqlite";
import { createServer } from "http";

// --- UUIDv7 (time-ordered, RFC 9562) ---
function uuidv7(): string {
	const ts = Date.now();
	const buf = new Uint8Array(16);
	crypto.getRandomValues(buf);

	// 48-bit timestamp (big-endian)
	buf[0] = (ts / 2 ** 40) & 0xff;
	buf[1] = (ts / 2 ** 32) & 0xff;
	buf[2] = (ts / 2 ** 24) & 0xff;
	buf[3] = (ts / 2 ** 16) & 0xff;
	buf[4] = (ts / 2 ** 8) & 0xff;
	buf[5] = ts & 0xff;

	// Version 7
	buf[6] = (buf[6] & 0x0f) | 0x70;
	// Variant 10xx
	buf[8] = (buf[8] & 0x3f) | 0x80;

	const hex = [...buf].map((b) => b.toString(16).padStart(2, "0"));
	return `${hex.slice(0, 4).join("")}-${hex.slice(4, 6).join("")}-${hex.slice(6, 8).join("")}-${hex.slice(8, 10).join("")}-${hex.slice(10, 16).join("")}`;
}

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
	console.error("usage: bun server.ts --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50");
	process.exit(1);
}

// --- Database ---
const db = new Database(DB_PATH, { create: true });
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA synchronous = NORMAL");
db.exec("PRAGMA busy_timeout = 5000");

// Schema
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

// Triggers — only fire when not syncing (prevents infinite loop)
// WHEN clause checks _meta.syncing flag
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
const COLS = ["id", "name", "value", "created_at", "updated_at", "node_id"];

async function shipBatch(changes: any[]) {
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
		console.error(`[${ID}] ship error:`, e);
	}
}

// Poll _changes table every BATCH_MS, ship, delete shipped
setInterval(() => {
	const rows = db.prepare("SELECT change_id, op, row_id, row_data FROM _changes ORDER BY change_id LIMIT 100").all() as any[];

	if (rows.length === 0) return;

	const changes = rows.map((r) => ({
		op: r.op,
		table: "items",
		row: r.row_data ? JSON.parse(r.row_data) : null,
		old_id: r.op === "DELETE" ? r.row_id : null,
	}));

	// Delete shipped rows
	const lastId = rows[rows.length - 1].change_id;
	db.prepare("DELETE FROM _changes WHERE change_id <= ?").run(lastId);

	// Ship (async, don't block)
	shipBatch(changes);
}, BATCH_MS);

// --- Apply received changes ---
function applyChanges(changes: any[]): number {
	// Set syncing flag — triggers won't fire
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
				insertReplace.run(
					r.id, r.name, r.value,
					r.created_at, r.updated_at, r.node_id
				);
				applied++;
			} else if (c.op === "DELETE") {
				if (!c.old_id) continue;
				deleteStmt.run(c.old_id);
				applied++;
			}
		}
	} finally {
		// Clear syncing flag
		db.prepare("UPDATE _meta SET value = 0 WHERE key = 'syncing'").run();
	}

	return applied;
}

// --- HTTP server ---
const server = createServer((req, res) => {
	const url = new URL(req.url!, `http://${req.headers.host}`);

	// POST /sync
	if (req.method === "POST" && url.pathname === "/sync") {
		let body = "";
		req.on("data", (chunk) => (body += chunk));
		req.on("end", () => {
			try {
				const changes = JSON.parse(body);
				const applied = applyChanges(changes);
				res.writeHead(200, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ applied }));
			} catch (e) {
				res.writeHead(400, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ error: String(e) }));
			}
		});
		return;
	}

	// GET /api/items
	if (req.method === "GET" && url.pathname === "/api/items") {
		const rows = db.prepare(
			"SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100"
		).all();
		res.writeHead(200, { "Content-Type": "application/json" });
		res.end(JSON.stringify(rows));
		return;
	}

	// GET /api/items/:id
	if (req.method === "GET" && url.pathname.startsWith("/api/items/")) {
		const id = url.pathname.slice("/api/items/".length);
		const row = db.prepare(
			"SELECT id, name, value, created_at, updated_at, node_id FROM items WHERE id = ?"
		).get(id);
		if (row) {
			res.writeHead(200, { "Content-Type": "application/json" });
			res.end(JSON.stringify(row));
		} else {
			res.writeHead(404, { "Content-Type": "application/json" });
			res.end(JSON.stringify({ error: "not found" }));
		}
		return;
	}

	// POST /api/items
	if (req.method === "POST" && url.pathname === "/api/items") {
		let body = "";
		req.on("data", (chunk) => (body += chunk));
		req.on("end", () => {
			try {
				const { name, value } = JSON.parse(body);
				const id = uuidv7();
				const now = Date.now();
				db.prepare(
					"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)"
				).run(id, name, value, now, now, ID);
				res.writeHead(200, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ id, name, value, created_at: now, node_id: ID }));
			} catch (e) {
				res.writeHead(500, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ error: String(e) }));
			}
		});
		return;
	}

	// PUT /api/items/:id
	if (req.method === "PUT" && url.pathname.startsWith("/api/items/")) {
		const id = url.pathname.slice("/api/items/".length);
		let body = "";
		req.on("data", (chunk) => (body += chunk));
		req.on("end", () => {
			try {
				const { name, value } = JSON.parse(body);
				const now = Date.now();
				db.prepare(
					"UPDATE items SET name = ?, value = ?, updated_at = ? WHERE id = ?"
				).run(name, value, now, id);
				res.writeHead(200, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ id, name, value, updated_at: now }));
			} catch (e) {
				res.writeHead(500, { "Content-Type": "application/json" });
				res.end(JSON.stringify({ error: String(e) }));
			}
		});
		return;
	}

	// DELETE /api/items/:id
	if (req.method === "DELETE" && url.pathname.startsWith("/api/items/")) {
		const id = url.pathname.slice("/api/items/".length);
		db.prepare("DELETE FROM items WHERE id = ?").run(id);
		res.writeHead(200, { "Content-Type": "application/json" });
		res.end(JSON.stringify({ deleted: id }));
		return;
	}

	// GET /health
	if (req.method === "GET" && url.pathname === "/health") {
		const { count } = db.prepare("SELECT COUNT(*) as count FROM items").get() as any;
		res.writeHead(200, { "Content-Type": "application/json" });
		res.end(JSON.stringify({ ok: true, node_id: ID, item_count: count }));
		return;
	}

	res.writeHead(404, { "Content-Type": "application/json" });
	res.end(JSON.stringify({ error: "not found" }));
});

server.listen(parseInt(LISTEN.replace(":", "")), () => {
	console.log(`[${ID}] listening on ${LISTEN}, peer=${PEER_URL}, batch=${BATCH_MS}ms`);
});
