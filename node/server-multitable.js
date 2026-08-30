// hook-sync Node.js multi-table implementation — items + categories
// better-sqlite3 + hyper-express
//
// Usage: node server-multi.js --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50

const HyperExpress = require("hyper-express");
const Database = require("better-sqlite3");
const crypto = require("crypto");

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
	console.error(
		"usage: node server-multi.js --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --batch-ms 50",
	);
	process.exit(1);
}

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

	CREATE TABLE IF NOT EXISTS categories (
		id TEXT PRIMARY KEY,
		name TEXT,
		parent_id TEXT,
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
		INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('items', 'DELETE', OLD.id,
			json_object('id', OLD.id, 'name', OLD.name, 'value', OLD.value,
				'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'node_id', OLD.node_id));
	END;

	-- categories triggers
	CREATE TRIGGER IF NOT EXISTS cat_ai AFTER INSERT ON categories
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('categories', 'INSERT', NEW.id,
			json_object('id', NEW.id, 'name', NEW.name, 'parent_id', NEW.parent_id,
				'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
	END;

	CREATE TRIGGER IF NOT EXISTS cat_au AFTER UPDATE ON categories
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('categories', 'UPDATE', NEW.id,
			json_object('id', NEW.id, 'name', NEW.name, 'parent_id', NEW.parent_id,
				'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
	END;

	CREATE TRIGGER IF NOT EXISTS cat_ad AFTER DELETE ON categories
	WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
	BEGIN
		INSERT INTO _changes(table_name, op, row_id, row_data) VALUES('categories', 'DELETE', OLD.id,
			json_object('id', OLD.id, 'name', OLD.name, 'parent_id', OLD.parent_id,
				'created_at', OLD.created_at, 'updated_at', OLD.updated_at, 'node_id', OLD.node_id));
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
	"INSERT INTO categories(id, name, parent_id, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
);
const stmtReplaceCat = db.prepare(
	"INSERT OR REPLACE INTO categories(id, name, parent_id, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
);
const stmtDeleteCat = db.prepare("DELETE FROM categories WHERE id = ?");
const stmtListCats = db.prepare(
	"SELECT id, name, parent_id, created_at, updated_at, node_id FROM categories ORDER BY created_at DESC LIMIT 100",
);

const stmtGetItemUpdatedAt = db.prepare(
	"SELECT updated_at FROM items WHERE id = ?",
);
const stmtGetCatUpdatedAt = db.prepare(
	"SELECT updated_at FROM categories WHERE id = ?",
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

// --- Batch ship (ACK-based: returns "ack", "mismatch", or "conn_error") ---
async function shipBatch(batchId, changes) {
	if (!PEER_URL || changes.length === 0) return "ack";
	try {
		const resp = await fetch(`${PEER_URL}/sync`, {
			method: "POST",
			headers: { "Content-Type": "application/json", "X-Node-Id": ID },
			body: JSON.stringify({ batch_id: batchId, changes }),
		});
		if (!resp.ok) return "mismatch";
		const body = await resp.json();
		return body.ack === batchId ? "ack" : "mismatch";
	} catch {
		return "conn_error";
	}
}

const BACKOFF_MS = [50, 100, 200, 400, 800];
let shipping = false;

setInterval(() => {
	if (shipping) return;
	const rows = stmtChanges.all();
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
			let connError = false;
			for (let attempt = 0; attempt < BACKOFF_MS.length; attempt++) {
				const status = await shipBatch(batchId, changes);
				if (status === "ack") {
					stmtDeleteChanges.run(batchId);
					return;
				}
				if (status === "conn_error") {
					connError = true;
				} else {
					connError = false;
				}
				if (attempt < BACKOFF_MS.length - 1) {
					await new Promise((resolve) =>
						setTimeout(resolve, BACKOFF_MS[attempt]),
					);
				}
			}
			if (connError) {
				// Peer unreachable — keep changes in _changes, try again next tick
				console.error(`[${ID}] peer unreachable, keeping ${rows.length} changes for next tick`);
				return;
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
const applyChanges = db.transaction((changes) => {
	stmtSyncOn.run();
	let applied = 0;
	try {
		for (const c of changes) {
			if (c.table === "items") {
				if (c.op === "INSERT" || c.op === "UPDATE") {
					if (!c.row) continue;
					const r = c.row;
					// Last-write-wins: skip if existing row is newer than incoming
					const existing = stmtGetItemUpdatedAt.get(r.id);
					if (existing && existing.updated_at > r.updated_at) continue;
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
					// Last-write-wins: skip delete if row was updated after deletion
					if (c.row) {
						const existing = stmtGetItemUpdatedAt.get(c.old_id);
						if (existing && existing.updated_at > c.row.updated_at) continue;
					}
					stmtDeleteItem.run(c.old_id);
					applied++;
				}
			} else if (c.table === "categories") {
				if (c.op === "INSERT" || c.op === "UPDATE") {
					if (!c.row) continue;
					const r = c.row;
					// Last-write-wins: skip if existing row is newer than incoming
					const existing = stmtGetCatUpdatedAt.get(r.id);
					if (existing && existing.updated_at > r.updated_at) continue;
					stmtReplaceCat.run(
						r.id,
						r.name,
						r.parent_id,
						r.created_at,
						r.updated_at,
						r.node_id,
					);
					applied++;
				} else if (c.op === "DELETE") {
					if (!c.old_id) continue;
					// Last-write-wins: skip delete if row was updated after deletion
					if (c.row) {
						const existing = stmtGetCatUpdatedAt.get(c.old_id);
						if (existing && existing.updated_at > c.row.updated_at) continue;
					}
					stmtDeleteCat.run(c.old_id);
					applied++;
				}
			}
		}
	} finally {
		stmtSyncOff.run();
	}
	return applied;
});

// --- HTTP server ---
const app = new HyperExpress.Server();

app.post("/sync", async (req, res) => {
	try {
		const body = await req.json();
		const applied = applyChanges(body.changes);
		res.json({ applied, ack: body.batch_id });
	} catch (e) {
		res.status(400).json({ error: e.message });
	}
});

app.get("/api/items", (req, res) => {
	res.json(stmtListItems.all());
});

app.post("/api/items", async (req, res) => {
	try {
		const body = await req.json();
		const id = crypto.randomUUID();
		const now = Date.now();
		stmtInsertItem.run(id, body.name, body.value, now, now, ID);
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

app.get("/api/categories", (req, res) => {
	res.json(stmtListCats.all());
});

app.post("/api/categories", async (req, res) => {
	try {
		const body = await req.json();
		const id = crypto.randomUUID();
		const now = Date.now();
		stmtInsertCat.run(id, body.name, body.parent_id ?? null, now, now, ID);
		res.json({
			id,
			name: body.name,
			parent_id: body.parent_id ?? null,
			created_at: now,
			updated_at: now,
			node_id: ID,
		});
	} catch (e) {
		res.status(500).json({ error: e.message });
	}
});

app.get("/health", (req, res) => {
	const { count: items } = stmtItemCount.get();
	const { count: categories } = stmtCatCount.get();
	const { count: pendingChanges } = stmtPendingChanges.get();
	const { count: deadLetter } = stmtDeadLetterCount.get();
	res.json({
		ok: true,
		node_id: ID,
		items,
		categories,
		pending_changes: pendingChanges,
		dead_letter: deadLetter,
	});
});

app
	.listen(parseInt(LISTEN.replace(":", "")))
	.then(() => {
		console.log(
			`[${ID}] listening on ${LISTEN}, peer=${PEER_URL}, batch=${BATCH_MS}ms (multi-table)`,
		);
	})
	.catch((e) => {
		console.error(`[${ID}] failed to listen:`, e);
		process.exit(1);
	});
