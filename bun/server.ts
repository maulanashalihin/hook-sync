// hook-sync Bun wrapper — thin shell over js/ library
// Bun.serve + bun:sqlite + crypto.randomUUID()
//
// Usage: bun server.ts --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50

import { Database } from "bun:sqlite";
import { attach } from "../js/src/index.ts";
import type { Change } from "../js/src/index.ts";

// --- Parse args ---
const args = process.argv.slice(2);
const getArg = (name: string, def: string) => {
	const i = args.indexOf(`--${name}`);
	return i >= 0 && i + 1 < args.length ? args[i + 1] : def;
};

const ID = getArg("id", "");
const DB_PATH = getArg("db", "");
const LISTEN = getArg("listen", "");
const PEER_URL = getArg("peer", "");
const BATCH_MS = parseInt(getArg("batch-ms", "50"));

if (!ID || !DB_PATH || !LISTEN) {
	console.error("Usage: bun server.ts --id <id> --db <path> --listen <:port> [--peer <url>] [--batch-ms <ms>]");
	process.exit(1);
}

// --- Database ---
const db = new Database(DB_PATH, { create: true });
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA synchronous = NORMAL");
db.exec("PRAGMA busy_timeout = 5000");

// --- Data table (caller defines schema) ---
db.exec(`
	CREATE TABLE IF NOT EXISTS items(
		id TEXT PRIMARY KEY,
		name TEXT,
		value INTEGER,
		created_at INTEGER,
		updated_at INTEGER,
		node_id TEXT
	);
`);

// --- Attach sync library ---
const peers = PEER_URL ? [PEER_URL] : [];
const mgr = attach(db, { id: ID, peers, batchMs: BATCH_MS }, ["items"]);

// --- Precompile CRUD statements ---
const stmtInsert = db.prepare("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)");
const stmtUpdate = db.prepare("UPDATE items SET name = ?, value = ?, updated_at = ? WHERE id = ?");
const stmtDelete = db.prepare("DELETE FROM items WHERE id = ?");
const stmtList = db.prepare("SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100");
const stmtGet = db.prepare("SELECT id, name, value, created_at, updated_at, node_id FROM items WHERE id = ?");

// --- HTTP server (Bun.serve native) ---
const server = Bun.serve({
	port: parseInt(LISTEN.replace(":", "")),
	fetch(req) {
		const url = new URL(req.url);
		const method = req.method;
		const path = url.pathname;

		// POST /sync — { batch_id, changes } → { applied, ack }
		if (method === "POST" && path === "/sync") {
			return req.json().then((body: { batch_id: number; changes: Change[] }) => {
				const applied = mgr.applyChanges(body.changes);
				return Response.json({ applied, ack: body.batch_id });
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

		// POST /api/items/batch — create multiple items in one transaction
		if (method === "POST" && path === "/api/items/batch") {
			return req.json().then((items: { name: string; value: number }[]) => {
				const now = Date.now();
				const tx = db.transaction(() => {
					for (const item of items) {
						const id = crypto.randomUUID();
						stmtInsert.run(id, item.name, item.value, now, now, ID);
					}
				});
				tx();
				return Response.json({ created: items.length });
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
			return Response.json(mgr.health());
		}

		return Response.json({ error: "not found" }, { status: 404 });
	},
});

console.log(`[${ID}] listening on ${LISTEN}, peer=${PEER_URL}, batch=${BATCH_MS}ms`);
