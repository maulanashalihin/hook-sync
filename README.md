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
├── bench-hsync.js        # HTTP benchmark client (language-agnostic)
├── bench-interval.js     # Batch interval optimization
└── BENCHMARK-REPORT.md   # Full benchmark report
```

Both implementations speak the same wire protocol — **Bun and Go nodes can sync to each other**.

| Language | Capture mechanism | Overhead | Binding |
|----------|------------------|----------|---------|
| Go | `sqlite3_preupdate_hook` | Zero (in-memory callback) | mattn/go-sqlite3 |
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

### UUIDv7 primary keys

UUIDv7 PKs eliminate conflicts in multi-writer setups — no last-write-wins, no CRDT, no vector clocks. Each node generates independent UUIDs that never collide.

UUIDv7 is time-ordered (RFC 9562), so inserts are sequential in the B-tree — append-like, no page splits. Benchmarked vs UUIDv4:

| Writes | UUIDv4 QPS | UUIDv7 QPS | UUIDv7 advantage |
|-------:|-----------:|-----------:|----------------:|
| 1,000 | 29,992 | 38,792 | 1.3x |
| 100,000 | 19,365 | 39,850 | **2.1x** |

UUIDv4 random inserts cause B-tree page splits at scale — QPS drops 50% at 100K writes. UUIDv7 stays stable.

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

All benchmarks run on Mac M4, 2 nodes on localhost, 50ms batch interval.

### Bun vs Go — Write Throughput (100 concurrent)

| Metric | Go (preupdate_hook + UUIDv7) | Bun (triggers + UUIDv4) |
|--------|----------------------------:|-----------------------:|
| Write throughput | **10,272 QPS** | 1,250 QPS |
| Dual-node round-robin | **9,321 QPS** | — |
| Write latency p50 | **0.08ms** | 0.10ms |
| Read latency p50 | 0.23ms | **0.17ms** |
| Sync delay p50 | 51ms | 52ms |
| Integrity | 540 ✅ | 540 ✅ |

Go is 8.2x faster on writes (zero-overhead hook vs trigger). Read latency Bun wins (0.17ms vs 0.23ms). Sync delay identical (~50ms = batch interval).

### Direct Go Benchmark (pure SQLite, no HTTP)

| Mode | Writes | QPS | Hooks fired |
|------|-------:|----:|------------:|
| Sequential | 10,000 | 36,932 | 10,000 ✅ |
| Transaction | 10,000 | **379,404** | 10,000 ✅ |

379K QPS confirms **zero overhead** from preupdate_hook — hook fires on every write with no measurable cost.

### UUIDv4 vs UUIDv7

Each language uses the fastest UUID for its runtime:

| Language | UUID | Why | DB QPS (100K transaction) |
|----------|------|-----|--------------------------:|
| Go | UUIDv7 | B-tree is bottleneck → sequential insert wins | 378,207 |
| Bun | UUIDv4 | `crypto.randomUUID()` native C++ (31M gen QPS) → generation is bottleneck | 1,113,119 |

Go: UUIDv7 2.1x faster at 100K writes (B-tree page splits with random v4).
Bun: UUIDv4 1.5x faster (native generation crushes JS UUIDv7 by 18x; bun:sqlite too fast for B-tree to matter).

### Batch Interval Optimization

| Interval | Sync p50 | Burst sync (100 writes) | Use case |
|----------|---------:|------------------------:|----------|
| **10ms** | **12ms** | 20ms | Local/LAN |
| 50ms (default) | 52ms | 25ms | Remote/WAN |
| 100ms | 100ms | 22ms | Conservative |

Burst sync is constant ~20-25ms regardless of interval — batch threshold (100 changes) triggers immediate ship.

## Comparison

| | hook-sync (Go) | hook-sync (Bun) |
|---|---|---|
| Capture mechanism | preupdate_hook (zero overhead) | SQLite triggers (1 INSERT/change) |
| Write throughput | 10,272 QPS | 1,250 QPS |
| Sync delay | ~52ms | ~52ms |
| UUID | UUIDv7 (B-tree optimal) | UUIDv4 (native gen optimal) |
| Conflict resolution | None needed (UUID PK) | None needed (UUID PK) |
| Binding | mattn/go-sqlite3 | bun:sqlite (built-in) |
| Cross-language sync | Yes (same protocol) | Yes (same protocol) |


## License

MIT
