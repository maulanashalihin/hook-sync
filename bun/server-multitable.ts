// hook-sync Bun multi-table implementation — items + categories
// Pure Bun: Bun.serve + bun:sqlite + crypto.randomUUID()
//
// Usage: bun server-multi.ts --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50

import { Database } from "bun:sqlite";

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
	console.error(
		"usage: bun server-multi.ts --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50",
	);
	process.exit(1);
}

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

	CREATE TABLE IF NOT EXISTS categories (
		id TEXT PRIMARY KEY,
		name TEXT,
		parent_id TEXT,
		created_at INTEGER,
		node_id TEXT
	);

	CREATE TABLE IF NOT EXISTS _meta (
		key TEXT PRIMARY KEY,
		value INTEGER
	);
	INSERT OR IGNORE INTO _meta(key, value) VALUES('syncing', 0);

	CREATE TABLE IF NOT EXISTS _changes (
		change_id INTEGER PRIMARY KEY AUTOINCREMENT,
		table_name TEXT,
		op TEXT,
		row_id TEXT,
		row_data TEXT
	);

	CREATE TABLE IF NOT EXISTS _dead_letter (
		dead_id INTEGER PRIMARY KEY AUTOINCREMENT,
		table_name TEXT,
		op TEXT,
		row_id TEXT,
		row_data TEXT,
		failed_at INTEGER,
		retry_count INTEGER DEFAULT 0
	);
`);

// Triggers for both tables
db.exec(`
	-- items triggers
	CREATE TRIGGER IF NOT EXISTS items_ai AFTER INSERT ON items
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('items', 'INSERT', NEW.id,
			json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
				'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
	END;

	CREATE TRIGGER IF NOT EXISTS items_au AFTER UPDATE ON items
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('items', 'UPDATE', NEW.id,
			json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
				'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
	END;

	CREATE TRIGGER IF NOT EXISTS items_ad AFTER DELETE ON items
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('items', 'DELETE', OLD.id, NULL);
	END;

	-- categories triggers
	CREATE TRIGGER IF NOT EXISTS cat_ai AFTER INSERT ON categories
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('categories', 'INSERT', NEW.id,
			json_object('id', NEW.id, 'name', NEW.name, 'parent_id', NEW.parent_id,
				'created_at', NEW.created_at, 'node_id', NEW.node_id));
	END;

	CREATE TRIGGER IF NOT EXISTS cat_au AFTER UPDATE ON categories
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('categories', 'UPDATE', NEW.id,
			json_object('id', NEW.id, 'name', NEW.name, 'parent_id', NEW.parent_id,
				'created_at', NEW.created_at, 'node_id', NEW.node_id));
	END;

	CREATE TRIGGER IF NOT EXISTS cat_ad AFTER DELETE ON categories
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('categories', 'DELETE', OLD.id, NULL);
	END;
`);

// --- Precompile statements ---
const stmtInsertItem = db.prepare(
	"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
);
const stmtReplaceItem = db.prepare(
	"INSERT OR REPLACE INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
);
const stmtDeleteItem = db.prepare("DELETE FROM items WHERE id = ?");
const stmtListItems = db.prepare(
	"SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100",
);

const stmtInsertCat = db.prepare(
	"INSERT INTO categories(id, name, parent_id, created_at, node_id) VALUES(?, ?, ?, ?, ?)",
);
const stmtReplaceCat = db.prepare(
	"INSERT OR REPLACE INTO categories(id, name, parent_id, created_at, node_id) VALUES(?, ?, ?, ?, ?)",
);
const stmtDeleteCat = db.prepare("DELETE FROM categories WHERE id = ?");
const stmtListCats = db.prepare(
	"SELECT id, name, parent_id, created_at, node_id FROM categories ORDER BY created_at DESC LIMIT 100",
);

const stmtChanges = db.prepare(
	"SELECT change_id, table_name, op, row_id, row_data FROM _changes ORDER BY change_id LIMIT 10000",
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
const stmtDeadLetter = db.prepare(
	"INSERT INTO _dead_letter(table_name, op, row_id, row_data, failed_at, retry_count) VALUES(?, ?, ?, ?, ?, ?)",
);
const stmtItemCount = db.prepare("SELECT COUNT(*) as count FROM items");
const stmtCatCount = db.prepare("SELECT COUNT(*) as count FROM categories");
const stmtPendingChanges = db.prepare("SELECT COUNT(*) as count FROM _changes");
const stmtDeadLetterCount = db.prepare(
	"SELECT COUNT(*) as count FROM _dead_letter",
);

// --- Batch ship (ACK-based) ---
async function shipBatch(
	batchId: number,
	changes: unknown[],
): Promise<boolean> {
	if (!PEER_URL || changes.length === 0) return true;
	try {
		const resp = await fetch(`${PEER_URL}/sync`, {
			method: "POST",
			headers: { "Content-Type": "application/json", "X-Node-Id": ID },
			body: JSON.stringify({ batch_id: batchId, changes }),
		});
		if (!resp.ok) return false;
		const body = (await resp.json()) as { applied: number; ack: number };
		return body.ack === batchId;
	} catch {
		return false;
	}
}

const BACKOFF_MS = [50, 100, 200, 400, 800];
let shipping = false;

setInterval(() => {
	if (shipping) return;
	const rows = stmtChanges.all() as {
		change_id: number;
		table_name: string;
		op: string;
		row_id: string;
		row_data: string | null;
	}[];
	if (rows.length === 0) return;

	const batchId = rows[rows.length - 1].change_id;
	const changes = rows.map((r) => ({
		op: r.op,
		table: r.table_name,
		row: r.row_data ? JSON.parse(r.row_data) : null,
		old_id: r.op === "DELETE" ? r.row_id : null,
	}));

	shipping = true;
	(async () => {
		try {
			for (let attempt = 0; attempt < BACKOFF_MS.length; attempt++) {
				const ok = await shipBatch(batchId, changes);
				if (ok) {
					stmtDeleteChanges.run(batchId);
					return;
				}
				if (attempt < BACKOFF_MS.length - 1) {
					const { promise, resolve } = Promise.withResolvers<void>();
					setTimeout(resolve, BACKOFF_MS[attempt]);
					await promise;
				}
			}
			const now = Date.now();
			for (const r of rows) {
				stmtDeadLetter.run(
					r.table_name,
					r.op,
					r.row_id,
					r.row_data,
					now,
					BACKOFF_MS.length,
				);
			}
			stmtDeleteChanges.run(batchId);
		} finally {
			shipping = false;
		}
	})();
}, BATCH_MS);

// --- Apply received changes (dispatch by table name) ---
const applyChanges = db.transaction(
	(
		changes: {
			op: string;
			table: string;
			row: Record<string, unknown> | null;
			old_id: string | null;
		}[],
	): number => {
		stmtSyncOn.run();
		let applied = 0;
		try {
			for (const c of changes) {
				if (c.table === "items") {
					if (c.op === "INSERT" || c.op === "UPDATE") {
						if (!c.row) continue;
						const r = c.row as {
							id: string;
							name: string;
							value: number;
							created_at: number;
							updated_at: number;
							node_id: string;
						};
						stmtReplaceItem.run(
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
						stmtDeleteItem.run(c.old_id);
						applied++;
					}
				} else if (c.table === "categories") {
					if (c.op === "INSERT" || c.op === "UPDATE") {
						if (!c.row) continue;
						const r = c.row as {
							id: string;
							name: string;
							parent_id: string | null;
							created_at: number;
							node_id: string;
						};
						stmtReplaceCat.run(
							r.id,
							r.name,
							r.parent_id,
							r.created_at,
							r.node_id,
						);
						applied++;
					} else if (c.op === "DELETE") {
						if (!c.old_id) continue;
						stmtDeleteCat.run(c.old_id);
						applied++;
					}
				}
			}
		} finally {
			stmtSyncOff.run();
		}
		return applied;
	},
);

// --- HTTP server ---
const server = Bun.serve({
	port: parseInt(LISTEN.replace(":", "")),
	fetch(req) {
		const url = new URL(req.url);
		const method = req.method;
		const path = url.pathname;

		// POST /sync
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
			return Response.json(stmtListItems.all());
		}

		// POST /api/items
		if (method === "POST" && path === "/api/items") {
			return req
				.json()
				.then((body: { name: string; value: number }) => {
					const id = crypto.randomUUID();
					const now = Date.now();
					stmtInsertItem.run(id, body.name, body.value, now, now, ID);
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

		// GET /api/categories
		if (method === "GET" && path === "/api/categories") {
			return Response.json(stmtListCats.all());
		}

		// POST /api/categories
		if (method === "POST" && path === "/api/categories") {
			return req
				.json()
				.then((body: { name: string; parent_id?: string }) => {
					const id = crypto.randomUUID();
					const now = Date.now();
					stmtInsertCat.run(id, body.name, body.parent_id ?? null, now, ID);
					return Response.json({
						id,
						name: body.name,
						parent_id: body.parent_id ?? null,
						created_at: now,
						node_id: ID,
					});
				})
				.catch((e: unknown) =>
					Response.json({ error: String(e) }, { status: 500 }),
				);
		}

		// GET /health
		if (method === "GET" && path === "/health") {
			const { count: items } = stmtItemCount.get() as { count: number };
			const { count: categories } = stmtCatCount.get() as { count: number };
			const { count: pendingChanges } = stmtPendingChanges.get() as {
				count: number;
			};
			const { count: deadLetter } = stmtDeadLetterCount.get() as {
				count: number;
			};
			return Response.json({
				ok: true,
				node_id: ID,
				items,
				categories,
				pending_changes: pendingChanges,
				dead_letter: deadLetter,
			});
		}

		return Response.json({ error: "not found" }, { status: 404 });
	},
});

console.log(
	`[${ID}] listening on ${LISTEN}, peer=${PEER_URL}, batch=${BATCH_MS}ms (multi-table)`,
);
