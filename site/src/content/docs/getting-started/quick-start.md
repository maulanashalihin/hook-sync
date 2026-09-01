---
title: Quick Start
description: Get hook-sync running in 5 minutes with Go, Bun, or Node.
---

import { Tabs, TabItem, Steps } from '@astrojs/starlight/components';

# Quick Start

Get a two-node active-active SQLite replication cluster running in 5 minutes.

## Prerequisites

- **Go** 1.21+ (for Go runtime) — [go.dev/dl](https://go.dev/dl/)
- **Bun** 1.0+ (for Bun runtime) — [bun.sh](https://bun.sh/)
- **Node.js** 18+ (for Node runtime) — [nodejs.org](https://nodejs.org/)

Pick one runtime. All three sync to each other — you can mix and match.

<Tabs>
<TabItem label="Go">

<Steps>

1. **Build the binary**

   ```bash
   git clone https://github.com/maulanashalihin/hook-sync.git
   cd hook-sync/go
   go build -o ../hook-sync-go ./cmd/server
   ```

2. **Start node A (port 9001)**

   ```bash
   ./hook-sync-go -id node1 -db node1.db -listen :9001 -peer http://localhost:9002
   ```

3. **Start node B (port 9002)**

   ```bash
   ./hook-sync-go -id node2 -db node2.db -listen :9002 -peer http://localhost:9001
   ```

4. **Write to node A**

   ```bash
   curl -X POST http://localhost:9001/api/items \
     -H 'Content-Type: application/json' \
     -d '{"name":"hello","value":42}'
   ```

5. **Verify on node B**

   ```bash
   curl http://localhost:9002/api/items
   # [{"id":"...","name":"hello","value":42,...}]
   ```

</Steps>

</TabItem>
<TabItem label="Bun">

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
     created_at INTEGER, updated_at INTEGER, node_id TEXT
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
   ```

</Steps>

</TabItem>
<TabItem label="Node.js">

<Steps>

1. **Install the library**

   ```bash
   npm install hooksync.js better-sqlite3
   ```

2. **Create `server.js`**

   ```js
   const { attach } = require("hooksync.js");
   const Database = require("better-sqlite3");
   const http = require("http");

   const db = new Database("app.db");
   db.pragma("journal_mode = WAL");
   db.exec(`CREATE TABLE IF NOT EXISTS items(
     id TEXT PRIMARY KEY, name TEXT, value INTEGER,
     created_at INTEGER, updated_at INTEGER, node_id TEXT
   );`);

   const mgr = attach(db, {
     id: "node1",
     peers: ["http://localhost:9002"],
     batchMs: 50,
   }, ["items"]);

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
     res.writeHead(404);
     res.end("not found");
   });

   server.listen(9001);
   ```

3. **Run two instances**

   ```bash
   node server.js   # node1 on :9001
   # In another terminal, change port to 9002 and peer to 9001
   ```

</Steps>

</TabItem>
</Tabs>

## Table Requirements

Every synced table **must** have:

- `id` — `TEXT PRIMARY KEY` (UUID, zero conflict)
- `updated_at` — `INTEGER` (millisecond timestamp, for last-write-wins)
- `node_id` — `TEXT` (origin node, for debugging)

```sql
CREATE TABLE items(
  id TEXT PRIMARY KEY,
  name TEXT,
  value INTEGER,
  created_at INTEGER,
  updated_at INTEGER,
  node_id TEXT
);
```

## What Happens Automatically

When you call `attach()` (JS) or `trigger.Attach()` (Go), the library:

1. Creates `_meta`, `_changes`, `_dead_letter`, `_peer_state` tables
2. Auto-generates INSERT/UPDATE/DELETE triggers via schema introspection
3. Starts a background ship loop (50ms default)
4. Handles ACK-based retry with exponential backoff
5. Manages per-peer watermarks for multi-peer topologies

You only write CRUD endpoints + wire `POST /sync` to `mgr.applyChanges()`.
