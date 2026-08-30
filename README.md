# hook-sync

Zero-overhead SQLite replication via `sqlite3_preupdate_hook` + batched HTTP ship + UUID primary keys.

## Why

Two existing approaches to SQLite replication, both with tradeoffs:

| Approach | Write overhead | Sync reliability | Issue |
|---|---|---|---|
| WAL page shipping (walsync) | Zero | Broken | Burst writes produce WAL frames that don't match replica page layout |
| Trigger-based CDC (cr-sqlite) | 2.6x slower | Reliable | Every INSERT writes N extra rows to `__crsql_clocks` per changed column |

**hook-sync** is the third path: `sqlite3_preupdate_hook` gives row-level CDC with **zero write overhead** (no triggers, no extra tables) and **reliable row-level sync** (no page layout dependency).

## How It Works

```
App write (native SQLite speed)
  → sqlite3_preupdate_hook fires BEFORE change
  → Hook captures: op, table, full old/new row values
  → Push to in-memory channel (non-blocking)
  → Background goroutine: batch every 100ms
  → HTTP POST JSON to peer node
  → Peer: INSERT OR REPLACE (UUID PK = zero conflict)
```

### sqlite3_preupdate_hook

Unlike `sqlite3_update_hook` (which only gives rowid), `sqlite3_preupdate_hook` provides **full column values** via helper functions:

- `sqlite3_preupdate_old(D, N, P)` — old value of column N
- `sqlite3_preupdate_new(D, N, P)` — new value of column N
- `sqlite3_preupdate_count(D)` — number of columns

Hook fires before INSERT/UPDATE/DELETE on all tables (including `WITHOUT ROWID`). Built into SQLite core — requires `SQLITE_ENABLE_PREUPDATE_HOOK` compile flag.

### UUID primary keys

UUID PKs eliminate conflicts in multi-writer setups — no last-write-wins, no CRDT, no vector clocks. Each node generates independent UUIDs that never collide.

> **Performance note:** Random UUIDv4 as PK causes B-tree page splits (10-16x slower inserts). Use **UUIDv7** (time-ordered) for production to maintain sequential insert ordering. This prototype uses UUIDv4 for simplicity.

### Infinite loop prevention

A `syncing` flag guards `captureChange` — changes applied by `applyChanges` (received from peer) set the flag, so the hook doesn't re-capture and re-ship them.

## Stack

- **Go** + [mattn/go-sqlite3](https://github.com/mattn/go-sqlite3) (preupdate hook via `sqlite_preupdate_hook` build tag)
- [Fiber](https://gofiber.io) for HTTP (sync transport + REST API)
- [google/uuid](https://github.com/google/uuid) for UUID generation

## Build

```bash
go build -tags sqlite_preupdate_hook -o hook-sync .
```

The `sqlite_preupdate_hook` build tag is required — it enables `SQLITE_ENABLE_PREUPDATE_HOOK` in the C compilation.

## Run

Start two nodes that sync to each other:

```bash
# Terminal 1 — node1
./hook-sync -id node1 -db node1.db -listen :9001 -peer http://localhost:9002

# Terminal 2 — node2
./hook-sync -id node2 -db node2.db -listen :9002 -peer http://localhost:9001
```

## API

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/items` | Create item (local write, triggers sync) |
| `GET` | `/api/items` | List items (latest 100) |
| `GET` | `/api/items/:id` | Get single item |
| `PUT` | `/api/items/:id` | Update item (local write, triggers sync) |
| `DELETE` | `/api/items/:id` | Delete item (local write, triggers sync) |
| `POST` | `/sync` | Receive change batch from peer (internal) |
| `GET` | `/health` | Health check + item count |

## Test Sync

```bash
# Write to node1
curl -X POST http://localhost:9001/api/items \
  -H 'Content-Type: application/json' \
  -d '{"name":"hello","value":42}'

# Verify on node2 (within ~100ms)
curl http://localhost:9002/api/items
```

## Verified

Bidirectional sync verified locally with 2 nodes:

- INSERT node1 → node2 ✅
- UPDATE node2 → node1 ✅
- DELETE node1 → node2 ✅
- No infinite loop ✅
- All field types correct (TEXT, INTEGER) ✅

## Limitations (prototype)

- **Single table** (`items`) — generalization needed for production
- **Hardcoded columns** — column names mapped by index in hook callback
- **No persistence** — in-memory channel only; changes lost on crash before ship
- **No retry** — failed HTTP ship drops changes (no dead-letter queue)
- **Point-to-point** — no topology management (star, mesh, etc.)
- **UUIDv4** — should use UUIDv7 for production (sequential insert performance)
- **Single connection** — `SetMaxOpenConns(1)` required so hook captures all writes

## Comparison

| | walsync | cr-sqlite | hook-sync |
|---|---|---|---|
| Write overhead | Zero | 2.6x (trigger) | Zero |
| Sync reliability | Broken (page layout) | Reliable (row-level) | Reliable (row-level) |
| Multi-writer | No | Yes (CRDT) | Yes (UUID PK) |
| Sync delay | N/A | ~165ms | ~100ms (batch) |
| Conflict resolution | N/A | Last-write-wins | None needed (UUID) |
| Binding support | N/A | SQLite extension | Go (mattn), needs custom for JS/Python |

## License

MIT
