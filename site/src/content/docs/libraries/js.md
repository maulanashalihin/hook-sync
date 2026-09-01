---
title: JS Library (hooksync.js)
description: SQLite replication library for Bun and Node.js. Trigger-based capture, ACK-based sync, last-write-wins.
---

import { Tabs, TabItem, Aside } from '@astrojs/starlight/components';

# hooksync.js

> SQLite replication library for Bun and Node.js. One package, both runtimes.

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

// Your table — must have `id` (TEXT PRIMARY KEY) and `updated_at` (INTEGER)
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

// Shutdown:
mgr.stop();
```

## API

### `attach(db, config, tables) → Manager`

| Parameter | Type | Description |
|-----------|------|-------------|
| `db` | `SqliteDatabase` | SQLite instance (`bun:sqlite` or `better-sqlite3`). Caller opens it. |
| `config` | `Config` | `{ id: string, peers: string[], batchMs?: number, batchSize?: number }` |
| `tables` | `string[]` | Table names to sync. Triggers auto-generated via `PRAGMA table_info`. |

Returns a `Manager`:

| Method | Description |
|--------|-------------|
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

## HTTP Server Setup (Required)

<Aside type="warning" title="The library does NOT include an HTTP server">
You must wire one yourself and route `POST /sync` to `mgr.applyChanges()`. This is intentional — you may already have an HTTP server (Bun.serve, Express, Hono, HyperExpress, `http.createServer`, etc.).
</Aside>

<Tabs>
<TabItem label="Bun">

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

</TabItem>
<TabItem label="Node (http.createServer)">

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

</TabItem>
<TabItem label="Express / Hono / HyperExpress">

```js
app.post("/sync", async (req, res) => {
  const applied = mgr.applyChanges(req.body.changes);
  res.json({ applied, ack: req.body.batch_id });
});

app.get("/health", (req, res) => {
  res.json(mgr.health());
});
```

</TabItem>
</Tabs>

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

For 8+ nodes, use a **dedicated hub** — a Go-only relay binary. The hub is not part of this library; it's a separate process.

```bash
# Build from the hook-sync repo
cd go && go build -o ../hook-sync-hub ./cmd/hub

./hook-sync-hub -id hub1 -listen :9010 -db hub1.pebble \
  -edge http://localhost:9001 \
  -edge http://localhost:9002 \
  -edge http://localhost:9003
```

From the JS library's perspective, the hub is just a peer URL:

```ts
const mgr = attach(db, {
  id: "edge1",
  peers: ["http://localhost:9010"],  // hub URL — same as any peer
  batchMs: 50,
}, ["items"]);
```

See [Dedicated Hub](../topologies/hub/) for full details.

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
