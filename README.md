# hook-sync

Zero-overhead SQLite replication via change capture + batched HTTP ship + UUIDv7 primary keys.

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
  → Peer: INSERT OR REPLACE (UUIDv7 PK = zero conflict)
```

### Change Capture

**Go (preupdate_hook):** `sqlite3_preupdate_hook` fires BEFORE INSERT/UPDATE/DELETE with full old/new row values. Zero overhead — in-memory callback, no extra DB writes. Requires `SQLITE_ENABLE_PREUPDATE_HOOK` compile flag.

**Bun (triggers):** AFTER INSERT/UPDATE/DELETE triggers write to `_changes` table. Background timer polls every 50ms, ships, deletes shipped rows. 1 extra INSERT per change (less overhead than cr-sqlite's N rows per column). Triggers use `WHEN (SELECT syncing FROM _meta) = 0` clause to prevent infinite loop.

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

UUIDv4 random inserts cause B-tree page splits at scale (QPS drops 50% at 100K writes in Go). UUIDv7 is time-ordered — append-like, no page splits. But in Bun, `bun:sqlite` is fast enough that B-tree overhead is negligible, and native UUIDv4 generation crushes JS UUIDv7 by 18x.

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

## Limitations (prototype)

- **Single table** (`items`) — generalization needed for production
- **Hardcoded columns** — column names mapped by index in hook/trigger
- **No persistence** — in-memory channel / _changes table; changes lost on crash before ship
- **No retry** — failed HTTP ship drops changes (no dead-letter queue)
- **Point-to-point** — no topology management (star, mesh, etc.)
- **Single connection (Go)** — `SetMaxOpenConns(1)` required so hook captures all writes
- **Bun trigger overhead** — 1 extra INSERT per change (vs zero in Go)

## Benchmark Results

All benchmarks run on Mac M4, 2 nodes on localhost, 50ms batch interval. Each runtime tested independently — same machine, same conditions, no cross-runtime traffic during testing.

### HTTP Benchmark (100 concurrent requests, 2 nodes same runtime)

| Metric | Go (preupdate_hook + UUIDv7) | Node (triggers + UUIDv4) | Bun (triggers + UUIDv4) |
|--------|----------------------------:|------------------------:|-----------------------:|
| Write throughput | **7,678 QPS** | 1,717 QPS | 1,401 QPS |
| Dual-node round-robin | **24,309 QPS** | 17,517 QPS | 14,360 QPS |
| Write latency p50 | **0.07ms** | 0.11ms | 0.10ms |
| Read latency p50 | 0.30ms | 0.16ms | **0.14ms** |
| Sync delay p50 (A→B) | 51ms | 52ms | 52ms |
| Sync delay p50 (B→A) | 51ms | 52ms | 52ms |
| Integrity | 340 ✅ | 340 ✅ | 340 ✅ |

Go leads both single-node and dual-node writes (zero-overhead hook + Fiber/fasthttp). Bun wins read latency. Sync delay identical across all (~50ms = batch interval). Note: dual-node results have high variance (8K–25K QPS across runs) due to system-level factors.

### Direct SQLite Benchmark (10K writes, no HTTP, capture mechanism active)

| Runtime | Mode | QPS | Capture | Hooks/Triggers fired |
|---------|------|----:|---------|---------------------:|
| Go | Sequential | 255,441 | preupdate_hook | 10,000 ✅ |
| Go | Transaction | **373,457** | preupdate_hook | 10,000 ✅ |
| Node | Sequential | 306,871 | triggers | 10,000 ✅ |
| Node | Transaction | 354,159 | triggers | 10,000 ✅ |
| Bun | Sequential | 339,064 | triggers | 10,000 ✅ |
| Bun | Transaction | **394,475** | triggers | 10,000 ✅ |

Bun's bun:sqlite is fastest in raw SQLite — even with trigger overhead (1 extra INSERT per change), it beats Go's preupdate_hook (zero overhead) in transaction mode. Go's mattn/go-sqlite3 binding has more overhead per call. Node's better-sqlite3 sits between.

### UUIDv4 vs UUIDv7

Each language uses the fastest UUID for its runtime:

| Language | UUID | Why |
|----------|------|-----|
| Go | UUIDv7 | B-tree is bottleneck → sequential insert wins, 2.1x faster at 100K writes |
| Bun | UUIDv4 | `crypto.randomUUID()` native C++ (31M gen QPS) → generation is bottleneck, 1.5x faster than JS UUIDv7 |
| Node | UUIDv4 | `crypto.randomUUID()` native (Node 19+) → same rationale as Bun |

### Batch Interval Optimization (Go)

| Interval | Sync p50 | Burst sync (100 writes) | Use case |
|----------|---------:|------------------------:|----------|
| **10ms** | **12ms** | 20ms | Local/LAN |
| 50ms (default) | 52ms | 25ms | Remote/WAN |
| 100ms | 100ms | 22ms | Conservative |

Burst sync is constant ~20-25ms regardless of interval — batch threshold (100 changes) triggers immediate ship.

## Comparison

| | Go | Node | Bun |
|---|---|---|---|
| Capture mechanism | preupdate_hook (zero overhead) | SQLite triggers (1 INSERT/change) | SQLite triggers (1 INSERT/change) |
| Write throughput (HTTP) | 7,678 QPS | 1,717 QPS | 1,401 QPS |
| Direct SQLite (transaction) | 373,457 QPS | 354,159 QPS | **394,475 QPS** |
| Dual-node round-robin | **24,309 QPS** | 17,517 QPS | 14,360 QPS |
| Sync delay | ~51ms | ~52ms | ~52ms |
| UUID | UUIDv7 (B-tree optimal) | UUIDv4 (native gen optimal) | UUIDv4 (native gen optimal) |
| Conflict resolution | None needed (UUID PK) | None needed (UUID PK) | None needed (UUID PK) |
| SQLite binding | mattn/go-sqlite3 | better-sqlite3 | bun:sqlite (built-in) |
| HTTP server | Fiber (fasthttp) | hyper-express (uWebSockets) | Node http.createServer |
| Cross-language sync | Yes (same protocol) | Yes (same protocol) | Yes (same protocol) |



## License

MIT
