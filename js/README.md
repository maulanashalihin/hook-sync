# @maulanashalihin/hook-sync

> SQLite replication library for Bun and Node.js. Trigger-based change capture, ACK-based sync, last-write-wins conflict resolution.

## Install

```bash
npm install @maulanashalihin/hook-sync
# or
bun add @maulanashalihin/hook-sync
```

## Quick Start

```ts
import { attach } from "@maulanashalihin/hook-sync";
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
    updated_at INTEGER,
    node_id TEXT
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

Caller wires the HTTP server (`Bun.serve`, `http.createServer`, Express, HyperExpress, etc.) and routes `/sync` to `mgr.applyChanges()`.

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
