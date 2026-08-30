# hook-sync

Multi-language SQLite replication prototype. Change capture + batched HTTP sync + UUID primary keys. Go uses `sqlite3_preupdate_hook` (zero overhead); Bun/Node use SQLite triggers (1 extra INSERT per change).

## Why

Existing trigger-based CDC (cr-sqlite) adds 2.6x write overhead — every INSERT writes N extra rows to `__crsql_clocks` per changed column.

**hook-sync** gives row-level CDC with **zero or minimal write overhead** and **reliable row-level sync** (no page layout dependency). Each language uses the most efficient capture mechanism available.

## Multi-Language Architecture

```
hook-sync/
├── PROTOCOL.md           # Shared wire protocol (Change format, /sync endpoint)
├── go/                   # Go implementation — sqlite3_preupdate_hook (zero overhead)
│   ├── main.go
│   └── bench/            # Go benchmarks (UUIDv4 vs v7, direct SQLite throughput)
├── bun/                  # Bun implementation — SQLite triggers (1 extra INSERT/change)
│   └── server.ts
├── node/                 # Node.js implementation — better-sqlite3 + hyper-express
│   └── server.js
├── bench-hsync.js        # HTTP benchmark client (language-agnostic)
├── bench-interval.js     # Batch interval optimization
└── BENCHMARK-REPORT.md   # Full benchmark report
```

All implementations speak the same wire protocol — **Go, Node, and Bun nodes can sync to each other**.

| Language | Capture mechanism | Overhead | Binding |
|----------|------------------|----------|---------|
| Go | `sqlite3_preupdate_hook` | Zero (in-memory callback) | mattn/go-sqlite3 |
| Node | SQLite triggers + `_changes` table | 1 extra INSERT per change | better-sqlite3 |
| Bun | SQLite triggers + `_changes` table | 1 extra INSERT per change | bun:sqlite (built-in) |

## How It Works

```
App write (native SQLite speed)
  → Change captured (preupdate_hook OR trigger)
  → Push to in-memory channel / _changes table
  → Background timer: batch every 50ms (default)
  → HTTP POST JSON to peer node
  → Peer: INSERT OR REPLACE (UUID PK = zero conflict)
```

Sync runs in the background — it does not block the write path. The client gets its response as soon as SQLite write + capture completes, before sync happens.

### Change Capture

**Go (preupdate_hook):** `sqlite3_preupdate_hook` fires BEFORE INSERT/UPDATE/DELETE with full old/new row values. Zero overhead — in-memory callback, no extra DB writes. Requires `SQLITE_ENABLE_PREUPDATE_HOOK` compile flag.

**Bun/Node (triggers):** AFTER INSERT/UPDATE/DELETE triggers write to `_changes` table. Background timer polls every 50ms, ships, deletes shipped rows. 1 extra INSERT per change (less overhead than cr-sqlite's N rows per column). Triggers use `WHEN (SELECT syncing FROM _meta) = 0` clause to prevent infinite loop.

### UUID primary keys (mandatory)

UUID PKs are **required** for hook-sync. Multi-writer without UUID = data loss.

**Why integer auto-increment breaks:**

```
Node A: INSERT → rowid 1, 2, 3
Node B: INSERT → rowid 1, 2, 3  ← same IDs!
```

Sync to peer → `INSERT OR REPLACE` → rowid 1 from Node B overwrites rowid 1 from Node A. **Data lost silently.**

**UUID solves this:**

```
Node A: INSERT → id 01a051a7-7749-...
Node B: INSERT → id 01a051a7-78c1-...  ← different, no collision
```

Every node generates UUIDs independently — no coordinator needed, no collision possible. `INSERT OR REPLACE` is safe. No CRDT, no last-write-wins, no vector clocks.

**Alternatives considered:**

| Approach | Problem |
|----------|---------|
| Composite PK `(node_id, seq)` | `node_id` can change, seq restarts on DB corruption |
| Range assignment (A: 1-1000, B: 1001-2000) | Needs coordinator, not scalable |
| Snowflake ID | Needs machine ID assignment, clock sync |

UUID is simplest: each node generates its own, never collides.

**UUIDv7 vs UUIDv4 — each language uses the fastest for its runtime:**

| Language | UUID | Why |
|----------|------|-----|
| Go | UUIDv7 | B-tree is bottleneck → time-ordered = sequential insert, 2.1x faster at 100K writes |
| Bun | UUIDv4 | `crypto.randomUUID()` native C++ (31M gen QPS) → generation is bottleneck, 1.5x faster than JS UUIDv7 |
| Node | UUIDv4 | `crypto.randomUUID()` native (Node 19+) → same rationale as Bun |

UUIDv4 random inserts cause B-tree page splits at scale (QPS drops 50% at 100K writes in Go). UUIDv7 is time-ordered — append-like, no page splits. But in Bun/Node, native UUIDv4 generation is so fast that B-tree overhead is negligible, and `crypto.randomUUID()` beats JS UUIDv7 by 18x.

### Idle = idle

No writes = no traffic. The batch timer fires 20x/sec (50ms) but is a no-op when there are no changes. Zero HTTP requests, zero network traffic, zero CPU overhead when idle.

## Build & Run

### Go

```bash
cd go
go build -tags sqlite_preupdate_hook -o ../hook-sync-go .

# Start node
./hook-sync-go -id node1 -db node1.db -listen :9001 -peer http://localhost:9002 -batch-ms 50
```

### Bun

```bash
cd bun

# Start node
bun run server.ts --id node2 --db node2.db --listen :9002 --peer http://localhost:9001 --batch-ms 50
```

### Node.js

```bash
cd node
npm install

# Start node
node server.js --id node3 --db node3.db --listen :9003 --peer http://localhost:9001 --batch-ms 50
```

### Cross-language interop

```bash
# Terminal 1 — Go node
./hook-sync-go -id go1 -db go1.db -listen :9001 -peer http://localhost:9002

# Terminal 2 — Bun node
bun run bun/server.ts --id bun1 --db bun1.db --listen :9002 --peer http://localhost:9001
```

Both nodes sync bidirectionally — Go captures via preupdate_hook, Bun captures via triggers, same wire protocol.

## API

See [PROTOCOL.md](PROTOCOL.md) for full spec. All implementations expose:

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/items` | Create item (local write, triggers sync) |
| `GET` | `/api/items` | List items (latest 100) |
| `GET` | `/api/items/:id` | Get single item |
| `PUT` | `/api/items/:id` | Update item (local write, triggers sync) |
| `DELETE` | `/api/items/:id` | Delete item (local write, triggers sync) |
| `POST` | `/sync` | Receive change batch from peer (internal) |
| `GET` | `/health` | Health check + item count |

## Verified

- Go ↔ Go bidirectional sync: INSERT/UPDATE/DELETE ✅
- Bun ↔ Go bidirectional sync: INSERT/UPDATE/DELETE ✅
- Node ↔ Bun bidirectional sync: INSERT/UPDATE/DELETE ✅
- UUIDv7 primary keys (time-ordered, zero conflict) ✅
- No infinite loop (syncing flag / trigger WHEN clause) ✅
- Idle = zero traffic ✅
- Sync does not block write path (single vs with-peer: 0-6% difference = noise) ✅

## Limitations (prototype)

- **Single table** (`items`) — generalization needed for production
- **Hardcoded columns** — column names mapped by index in hook/trigger
- **No persistence** — in-memory channel / _changes table; changes lost on crash before ship
- **No retry** — failed HTTP ship drops changes (no dead-letter queue)
- **Point-to-point** — no topology management (star, mesh, etc.)
- **Single connection (Go)** — `SetMaxOpenConns(1)` required so hook captures all writes
- **Bun/Node trigger overhead** — 1 extra INSERT per change (vs zero in Go)

## Benchmark Results

All benchmarks run on Mac M4, 2 nodes on localhost, 50ms batch interval. Each runtime tested independently — no cross-runtime traffic during testing.

Full report: [BENCHMARK-REPORT.md](BENCHMARK-REPORT.md)

### What is reliable

These numbers are consistent across multiple runs:

**Direct SQLite (10K writes, no HTTP, capture mechanism active):**

| Runtime | Mode | QPS | Capture | Hooks/Triggers fired |
|---------|------|----:|---------|---------------------:|
| Go | Sequential | 255K | preupdate_hook | 10,000 ✅ |
| Go | Transaction | 373K | preupdate_hook | 10,000 ✅ |
| Node | Sequential | 307K | triggers | 10,000 ✅ |
| Node | Transaction | 354K | triggers | 10,000 ✅ |
| Bun | Sequential | 339K | triggers | 10,000 ✅ |
| Bun | Transaction | **394K** | triggers | 10,000 ✅ |

Bun:sqlite binding is fastest in raw SQLite — even with trigger overhead (1 extra INSERT per change), it beats Go's preupdate_hook (zero overhead) in transaction mode.

**Sync does not block writes (200 concurrent HTTP writes, 5 runs, median):**

| Runtime | Single (no peer) | With peer | Difference |
|---------|---:|---:|---:|
| Go | 12,668 QPS | 13,436 QPS | +6% (noise) |
| Bun | 8,099 QPS | 8,578 QPS | +6% (noise) |
| Node | 14,764 QPS | 14,701 QPS | -0.4% (noise) |

Sync runs in the background. Write path = SQLite INSERT + capture only. Sync = separate timer + HTTP POST. Having a peer does not slow down writes.

**Sync delay = batch interval (tunable):**

| Interval | Sync p50 | Burst sync (100 writes) | Use case |
|----------|---------:|------------------------:|----------|
| **10ms** | **12ms** | 20ms | Local/LAN |
| 50ms (default) | 52ms | 25ms | Remote/WAN |
| 100ms | 100ms | 22ms | Conservative |

Burst sync is constant ~20-25ms regardless of interval — batch threshold (100 changes) triggers immediate ship.

### What is NOT reliable

**HTTP throughput benchmark (localhost, 100-200 concurrent requests):**

⚠️ **Variance 10x across runs.** Do not use these numbers to compare runtimes.

Example: Go with-peer across 5 runs — 3,464 / 10,454 / 13,436 / 22,715 / 23,412 QPS. Same code, same machine, same conditions. The difference between min and max is 7x.

This is a characteristic of localhost HTTP benchmarking with small request counts — not a real performance difference between runtimes. To get reliable HTTP comparison, need:
- Remote server (not localhost — eliminates loopback artifacts)
- 1000+ requests per run
- Multiple runs with variance reported
- No other processes running

### What has NOT been tested

- **2 real servers** — all benchmarks on localhost (0ms RTT). Real deployment adds 35-40ms+ RTT.
- **Crash recovery** — changes in _changes table / in-memory channel are lost on crash before ship.
- **Multi-table** — all benchmarks use single `items` table.
- **High write volume sustained** — benchmarks use 100-200 request bursts, not sustained load.

## Comparison

| | Go | Node | Bun |
|---|---|---|---|
| Capture mechanism | preupdate_hook (zero overhead) | SQLite triggers (1 INSERT/change) | SQLite triggers (1 INSERT/change) |
| Direct SQLite (transaction) | 373K QPS | 354K QPS | **394K QPS** |
| Sync blocks writes? | No | No | No |
| Sync delay | ~51ms | ~49ms | ~52ms |
| UUID | UUIDv7 (B-tree optimal) | UUIDv4 (native gen optimal) | UUIDv4 (native gen optimal) |
| Conflict resolution | None needed (UUID PK) | None needed (UUID PK) | None needed (UUID PK) |
| SQLite binding | mattn/go-sqlite3 | better-sqlite3 | bun:sqlite (built-in) |
| HTTP server | Fiber (fasthttp) | hyper-express (uWebSockets) | Bun.serve (native) |
| Cross-language sync | Yes (same protocol) | Yes (same protocol) | Yes (same protocol) |

## License

MIT
