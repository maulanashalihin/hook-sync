// hook-sync Node.js mesh wrapper — thin shell over js/ library
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
const { attach } = require("../js/src/index.ts");

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
	}
}

const ID = getArg("id", "");
const DB_PATH = getArg("db", "");
const LISTEN = getArg("listen", "");
const BATCH_MS = parseInt(getArg("batch-ms", "50"));

if (!ID || !DB_PATH || !LISTEN) {
	console.error("usage: node server-mesh.js --id node1 --db node1.db --listen :9001 --peer http://localhost:9002 --peer http://localhost:9003");
	process.exit(1);
}

// --- Database ---
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("synchronous = NORMAL");
db.pragma("busy_timeout = 5000");

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
const mgr = attach(db, { id: ID, peers: PEERS, batchMs: BATCH_MS }, ["items"]);

// --- Precompile CRUD statements ---
const stmtInsert = db.prepare("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)");
const stmtUpdate = db.prepare("UPDATE items SET name = ?, value = ?, updated_at = ? WHERE id = ?");
const stmtDelete = db.prepare("DELETE FROM items WHERE id = ?");
const stmtList = db.prepare("SELECT id, name, value, created_at, updated_at, node_id FROM items ORDER BY created_at DESC LIMIT 100");
const stmtGet = db.prepare("SELECT id, name, value, created_at, updated_at, node_id FROM items WHERE id = ?");

// --- HTTP server (hyper-express / uWebSockets) ---
const app = new HyperExpress.Server({ max_body_length: 100 * 1024 * 1024 });

// POST /sync — { batch_id, changes } → { applied, ack }
app.post("/sync", async (req, res) => {
	try {
		const body = await req.json();
		const applied = mgr.applyChanges(body.changes);
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
		stmtUpdate.run(body.name, body.value, now, req.params.id);
		res.json({ id: req.params.id, name: body.name, value: body.value, updated_at: now });
	} catch (e) {
		res.status(500).json({ error: e.message });
	}
});

// DELETE /api/items/:id
app.delete("/api/items/:id", (req, res) => {
	stmtDelete.run(req.params.id);
	res.json({ deleted: req.params.id });
});

// GET /health
app.get("/health", (req, res) => {
	res.json(mgr.health());
});

app.listen(parseInt(LISTEN.replace(":", "")))
	.then(() => {
		console.log(`[${ID}] listening on ${LISTEN}, peers=[${PEERS.join(", ")}], batch=${BATCH_MS}ms`);
	})
	.catch((e) => {
		console.error(`[${ID}] failed to listen:`, e);
		process.exit(1);
	});
