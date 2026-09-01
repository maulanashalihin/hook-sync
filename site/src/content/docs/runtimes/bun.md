---
title: Bun
description: SQLite replication for Bun apps. hooksync.js library + bun:sqlite. Trigger-based capture, ACK-based sync.
---

import { Aside, Steps, Card, CardGrid } from '@astrojs/starlight/components';

# Bun

hook-sync works with Bun via the [`hooksync.js`](https://www.npmjs.com/package/hooksync.js) npm package and `bun:sqlite`. Trigger-based capture only (no preupdate_hook in `bun:sqlite`).

## Quick Start

<Steps>

1. **Install the library**

   ```bash
   bun add hooksync.js
   ```

2. **Create `server.ts`**

   ```ts
   import { attach } from "hooksync.js";
   import { Database } from "bun:sqlite";

   const db = new Database("app.db");
   db.exec("PRAGMA journal_mode = WAL");
   db.exec(`CREATE TABLE IF NOT EXISTS items(
    id TEXT PRIMARY KEY, name TEXT, value INTEGER,
    created_at INTEGER, updated_at INTEGER
   );`);

   const mgr = attach(db, {
     id: "node1",
     peers: ["http://localhost:9002"],
     batchMs: 50,
   }, ["items"]);

   Bun.serve({
     port: 9001,
     fetch(req) {
       const url = new URL(req.url);
       if (req.method === "POST" && url.pathname === "/sync") {
         return req.json().then((body) => {
           const applied = mgr.applyChanges(body.changes);
           return Response.json({ applied, ack: body.batch_id });
         });
       }
       if (req.method === "GET" && url.pathname === "/health") {
         return Response.json(mgr.health());
       }
       return new Response("not found", { status: 404 });
     },
   });
   ```

3. **Run two instances**

   ```bash
   bun server.ts   # node1 on :9001
   # In another terminal, change port to 9002 and peer to 9001
   bun server.ts
   ```

4. **Write to node A, verify on node B**

   ```bash
   curl -X POST http://localhost:9001/api/items \
     -H 'Content-Type: application/json' \
     -d '{"name":"hello","value":42}'

   curl http://localhost:9002/api/items
   # [{"id":"...","name":"hello","value":42,...}]
   ```

</Steps>

## API

### `attach(db, config, tables) → Manager`

| Parameter | Type | Description |
|-----------|------|-------------|
| `db` | `Database` | `bun:sqlite` instance. Caller opens it. |
| `config` | `Config` | `{ id: string, peers: string[], batchMs?: number, batchSize?: number }` |
| `tables` | `string[]` | Table names to sync. Triggers auto-generated via `PRAGMA table_info`. |

Returns a `Manager`:

| Method | Description |
|--------|-------------|
| `applyChanges(changes)` | Apply received changes (LWW conflict resolution). Returns count applied. |
| `health()` | Returns `{ ok, node_id, item_count, pending_changes, dead_letter, peers }`. |
| `stop()` | Stop the background ship loop. |

## HTTP Server (Required)

<Aside type="warning" title="The library does NOT include an HTTP server">
You must wire `Bun.serve()` yourself and route `POST /sync` to `mgr.applyChanges()`. This is intentional — you may already have an HTTP server.
</Aside>

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

The pattern: parse JSON body, call `mgr.applyChanges(body.changes)`, return `{ applied, ack: body.batch_id }`.

## Table Requirements

Every synced table **must** have:

- `id` — `TEXT PRIMARY KEY` (UUID, zero conflict)
- `updated_at` — `INTEGER` (millisecond timestamp, for last-write-wins)

```sql
CREATE TABLE items(
  id TEXT PRIMARY KEY,
  name TEXT,
  value INTEGER,
  created_at INTEGER,
  updated_at INTEGER
);
```

## What Happens Automatically

When you call `attach()`, the library:

1. Creates `_meta`, `_changes`, `_dead_letter`, `_peer_state` tables
2. Auto-generates INSERT/UPDATE/DELETE triggers via schema introspection
3. Starts a background ship loop (50ms default)
4. Handles ACK-based retry with exponential backoff
5. Manages per-peer watermarks for multi-peer topologies

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

Each peer has its own watermark. Changes deleted from `_changes` only after **all** peers ACK. See [Full Mesh](/topologies/full-mesh/).

## Hub Topology (8+ Nodes)

For 8+ nodes, use a dedicated hub — a Go-only relay binary. From the JS library's perspective, the hub is just a peer URL:

```ts
const mgr = attach(db, {
  id: "edge1",
  peers: ["http://localhost:9010"],  // hub URL — same as any peer
  batchMs: 50,
}, ["items"]);
```

See [Dedicated Hub](/topologies/hub/) for hub setup.

## UUIDv7 Recommended

UUIDv7 gives time-ordered IDs → sequential B-tree inserts. Bun: optimized hex-table impl (benchmark: `bun/bench-uuid.ts`).

```ts
function uuidv7(): string {
  const ts = Date.now();
  const buf = new Uint8Array(16);
  crypto.getRandomValues(buf);
  // ... see bun/bench-uuid.ts for full implementation
  return /* uuid string */;
}
```

## What This Library Does NOT Do

- **No HTTP server** — caller wires `Bun.serve()` or any framework
- **No hook capture mode** — `bun:sqlite` has no preupdate hook API. Trigger-based only
- **No consensus** — no Raft, no coordinator, no leader election. Just triggers + HTTP + ACK

<CardGrid stagger>
  <Card title="Point-to-Point" icon="seti:custom" href="/topologies/point-to-point/">
    2 nodes, active-active. Simplest setup.
  </Card>
  <Card title="Full Mesh" icon="seti:custom" href="/topologies/full-mesh/">
    3-7 nodes, all-to-all sync.
  </Card>
  <Card title="Dedicated Hub" icon="seti:custom" href="/topologies/hub/">
    8+ nodes. Go-only relay with Pebble KV.
  </Card>
  <Card title="Multi-Region" icon="seti:custom" href="/topologies/multi-region/">
    Hub-to-hub cross-region sync.
  </Card>
</CardGrid>
