# hooksync.js

> SQLite replication library for Bun and Node.js. Trigger-based change capture, ACK-based sync, last-write-wins conflict resolution.

## Install

```bash
npm install hooksync.js
# or
bun add hooksync.js
```

## Quick Start

```ts
import { attach } from "hooksync.js";
import { Database } from "bun:sqlite"; // or: const Database = require("better-sqlite3");

const db = new Database("app.db");
db.exec("PRAGMA journal_mode = WAL");

// Your table — must have `id` (TEXT PRIMARY KEY) and `updated_at` (INTEGER) columns
db.exec(`
  CREATE TABLE IF NOT EXISTS items(
    id TEXT PRIMARY KEY,
    name TEXT,
    value INTEGER,
    created_at INTEGER,
    updated_at INTEGER
  );
`);

// Attach sync — creates _meta, _changes, _dead_letter, _peer_state tables
// and auto-generates triggers via schema introspection
const mgr = attach(db, {
  id: "node1",
  peers: ["http://localhost:9002"],
  batchMs: 50,
}, ["items"]);

// Writes to `items` now replicate automatically.
// Sync runs in the background — never blocks the write path.

// Receive changes from peers (wire to your HTTP server):
// POST /sync → mgr.applyChanges(body.changes)

// Health check:
// GET /health → mgr.health()

// Shutdown:
mgr.stop();
```

## How It Works

```
App write (native SQLite speed)
  → Trigger captures change to _changes table (same transaction)
  → Background timer: batch every 50ms
  → HTTP POST {batch_id, changes} to peer
  → Peer: INSERT OR REPLACE + timestamp check (last-write-wins)
  → Peer returns {applied, ack: batch_id}
  → Sender deletes from _changes only after ACK confirms
  → If peer is down: changes accumulate, retry on reconnect
  → If process crashes: changes survive in SQLite, resume on restart
```

**Write speed is identical with or without peers.** Sync runs in the background — it never blocks the write path.

## API

### `attach(db, config, tables) → Manager`

- **db** — SQLite database instance (`bun:sqlite` or `better-sqlite3`). Caller opens it.
- **config** — `{ id: string, peers: string[], batchMs?: number, batchSize?: number }`
- **tables** — array of table names to sync. Triggers auto-generated via `PRAGMA table_info`.

Returns a `Manager` with:

| Method | Description |
|---|---|
| `applyChanges(changes)` | Apply received changes (LWW conflict resolution). Returns count applied. |
| `health()` | Returns `{ ok, node_id, item_count, pending_changes, dead_letter, peers }`. |
| `stop()` | Stop the background ship loop. |

### Table Requirements

Every synced table **must** have:

- `id` — `TEXT PRIMARY KEY` (UUID, zero conflict)
- `updated_at` — `INTEGER` (millisecond timestamp, for last-write-wins)

### Wire Protocol

POST `/sync` with:

```json
{
  "batch_id": 42,
  "changes": [
    { "op": "INSERT", "table": "items", "row": { "id": "uuid", "name": "foo", "updated_at": 1690000000 }, "old_id": null },
    { "op": "DELETE", "table": "items", "row": null, "old_id": "uuid" }
  ]
}
```

Response:

```json
{ "applied": 2, "ack": 42 }
```

### HTTP Server Setup (Required)

The library does **not** include an HTTP server. You must wire one yourself and route `POST /sync` to `mgr.applyChanges()`. This is intentional — you may already have an HTTP server (Bun.serve, Express, Hono, HyperExpress, `http.createServer`, etc.).

**Bun:**

```ts
Bun.serve({
  port: 9001,
  fetch(req) {
    const url = new URL(req.url);

    // /sync — receive changes from peers
    if (req.method === "POST" && url.pathname === "/sync") {
      return req.json().then((body) => {
        const applied = mgr.applyChanges(body.changes);
        return Response.json({ applied, ack: body.batch_id });
      });
    }

    // /health
    if (req.method === "GET" && url.pathname === "/health") {
      return Response.json(mgr.health());
    }

    // ... your CRUD endpoints here
    return new Response("not found", { status: 404 });
  },
});
```

**Node (http.createServer):**

```js
const http = require("http");

const server = http.createServer(async (req, res) => {
  if (req.method === "POST" && req.url === "/sync") {
    const body = JSON.parse(await readBody(req));
    const applied = mgr.applyChanges(body.changes);
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ applied, ack: body.batch_id }));
    return;
  }

  if (req.method === "GET" && req.url === "/health") {
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(mgr.health()));
    return;
  }

  // ... your CRUD endpoints here
  res.writeHead(404);
  res.end("not found");
});

server.listen(9001);
```

**Node (Express / Hono / HyperExpress):**

```js
app.post("/sync", async (req, res) => {
  const applied = mgr.applyChanges(req.body.changes);
  res.json({ applied, ack: req.body.batch_id });
});

app.get("/health", (req, res) => {
  res.json(mgr.health());
});
```

The pattern is always the same: parse the JSON body, call `mgr.applyChanges(body.changes)`, return `{ applied, ack: body.batch_id }`.

## Multi-Peer (Full Mesh)

```ts
const mgr = attach(db, {
  id: "nodeA",
  peers: [
    "http://localhost:9002",
    "http://localhost:9003",
    "http://localhost:9004",
  ],
  batchMs: 50,
}, ["items"]);
```

Each peer has its own watermark (`_peer_state` table). Changes are deleted from `_changes` only after **all** peers have ACKed. Offline peers' changes accumulate until they reconnect.

## Hub Topology (Star, 8+ Nodes)

For 8+ nodes, full mesh generates too much traffic per node. Use a **dedicated hub** — a Go-only relay binary that forwards changes between edges. The hub is not part of this library; it's a separate process you run alongside your edge nodes.

```
edge1 ──POST /sync──→ hub ──POST /sync──→ edge2
edge3 ──POST /sync──→ hub ──POST /sync──→ edge4
```

### 1. Run the hub (Go binary, separate process)

```bash
# Build from the hook-sync repo
cd go && go build -o ../hook-sync-hub ./cmd/hub

./hook-sync-hub -id hub1 -listen :9010 -db hub1.pebble \
  -edge http://localhost:9001 \
  -edge http://localhost:9002 \
  -edge http://localhost:9003
```

The hub is a pure relay — no SQLite, no triggers, no `/api/items`. It stores a backup in Pebble KV and forwards changes to all edges. If the hub crashes, its durable forwarding queue survives and replays on restart.

### 2. Point your edge nodes to the hub

From the JS library's perspective, the hub is just a peer URL. No API changes:

```ts
const mgr = attach(db, {
  id: "edge1",
  peers: ["http://localhost:9010"],  // hub URL — same as any peer
  batchMs: 50,
}, ["items"]);
```

That's it. The edge ships changes to the hub, the hub ACKs immediately, then forwards to all other edges asynchronously. Edges don't know it's a hub — it's transparent.

### Multi-region (hub-to-hub)

For cross-region sync, run two hubs and peer them via `-edge` flag + `-url` flag (loop prevention):

```bash
# Region 1 hub
./hook-sync-hub -id hubA -listen :9100 -url http://localhost:9100 -db hubA.pebble \
  -edge http://localhost:9101 \
  -edge http://localhost:9200  # hub B (peer hub)

# Region 2 hub
./hook-sync-hub -id hubB -listen :9200 -url http://localhost:9200 -db hubB.pebble \
  -edge http://localhost:9201 \
  -edge http://localhost:9100  # hub A (peer hub)
```

Edges in each region point to their local hub only. Hubs relay cross-region via `X-Node-Url` header (prevents infinite loops). See [TOPOLOGY.md](https://github.com/maulanashalihin/hook-sync/blob/main/TOPOLOGY.md) for full details.

## SQLite Binding Compatibility

The library accepts a minimal `SqliteDatabase` interface — it never imports a binding:

```ts
interface SqliteDatabase {
  exec(sql: string): void;
  prepare(sql: string): SqliteStatement;
  transaction<T>(fn: T): T;
}
```

Both `bun:sqlite` and `better-sqlite3` satisfy this interface. Pass whichever you prefer.

## What This Library Does NOT Do

- **No HTTP server** — caller wires `Bun.serve()`, `http.createServer()`, or any framework
- **No hook capture mode** — neither `bun:sqlite` nor `better-sqlite3` has a preupdate hook API. Trigger-based only
- **No consensus** — no Raft, no coordinator, no leader election. Just triggers + HTTP + ACK

## License

MIT
