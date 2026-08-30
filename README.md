# hook-sync

Multi-language SQLite replication prototype. Change capture via SQLite triggers + `_changes` table, ACK-based batched HTTP sync, UUID primary keys. Go, Bun, and Node implementations speak the same wire protocol and sync to each other.

## How It Works

```
App write (native SQLite speed)
  → Trigger captures change to _changes table (same transaction)
  → Background timer: batch every 50ms (default)
  → HTTP POST {batch_id, changes} to peer
  → Peer: INSERT OR REPLACE (UUID PK = zero conflict)
  → Peer returns {applied, ack: batch_id}
  → Sender deletes from _changes only after ACK confirms
```

Sync runs in the background — it does not block the write path. The client gets its response as soon as SQLite write + capture completes.

### ACK-based reliability

Changes are persisted in `_changes` at write time (same transaction via triggers). If the process crashes, un-shipped changes survive and resume on restart.

Ship failures retry with exponential backoff (50/100/200/400/800ms, 5 attempts). After 5 failures, changes move to `_dead_letter` table for manual review.

`INSERT OR REPLACE` with UUID PK makes re-sends idempotent — shipping the same batch 10 times produces the same result, no duplicates.

### UUID primary keys (mandatory)

Multi-writer without UUID = data loss. Integer auto-increment collides across nodes (both get rowid 1, 2, 3 → `INSERT OR REPLACE` overwrites silently). UUID gives every node independent IDs — no coordinator, no collision, no CRDT.

| Language | UUID | Why |
|----------|------|-----|
| Go | UUIDv7 | B-tree is bottleneck → time-ordered = sequential insert |
| Bun | UUIDv4 | `crypto.randomUUID()` native → generation is bottleneck |
| Node | UUIDv4 | `crypto.randomUUID()` native → same as Bun |

## Architecture

```
hook-sync/
├── PROTOCOL.md               # Wire protocol spec
├── go/                       # Go implementation (Fiber + mattn/go-sqlite3)
│   ├── main.go               #   single-table
│   ├── multi/main.go         #   multi-table (items + categories)
│   └── bench/                #   direct SQLite benchmarks
├── bun/                      # Bun implementation (Bun.serve + bun:sqlite)
│   ├── server.ts             #   single-table
│   └── server-multi.ts       #   multi-table
├── node/                     # Node.js implementation (hyper-express + better-sqlite3)
│   ├── server.js             #   single-table
│   └── server-multi.js       #   multi-table
├── bench-dual-ack.sh         # Dual-writer benchmark (all 3 runtimes)
├── bench-hsync.js            # HTTP benchmark client
├── bench-interval.js         # Batch interval optimization
├── bench-trigger-overhead.ts # Trigger overhead measurement
└── BENCHMARK-REPORT.md       # Full benchmark report
```

| Language | Capture | Binding | HTTP server |
|----------|---------|---------|-------------|
| Go | SQLite triggers + `_changes` | mattn/go-sqlite3 | Fiber (fasthttp) |
| Bun | SQLite triggers + `_changes` | bun:sqlite (built-in) | Bun.serve (native) |
| Node | SQLite triggers + `_changes` | better-sqlite3 | hyper-express (uWebSockets) |

All implementations use the same capture mechanism (triggers + `_changes` table) and the same wire protocol. Nodes in different languages sync to each other bidirectionally.

## Build & Run

### Go

```bash
cd go
go build -o ../hook-sync-go .

./hook-sync-go -id node1 -db node1.db -listen :9001 -peer http://localhost:9002 -batch-ms 50
```

### Bun

```bash
bun run bun/server.ts --id node2 --db node2.db --listen :9002 --peer http://localhost:9001 --batch-ms 50
```

### Node.js

```bash
cd node && npm install

node server.js --id node3 --db ../node3.db --listen :9003 --peer http://localhost:9001 --batch-ms 50
```

### Cross-language interop

```bash
# Terminal 1 — Go
./hook-sync-go -id go1 -db go1.db -listen :9001 -peer http://localhost:9002

# Terminal 2 — Bun
bun run bun/server.ts --id bun1 --db bun1.db --listen :9002 --peer http://localhost:9001
```

Both nodes sync bidirectionally — same wire protocol, same ACK-based reliability.

## API

See [PROTOCOL.md](PROTOCOL.md) for full spec.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/items` | Create item (local write, triggers sync) |
| `GET` | `/api/items` | List items (latest 100) |
| `GET` | `/api/items/:id` | Get single item |
| `PUT` | `/api/items/:id` | Update item |
| `DELETE` | `/api/items/:id` | Delete item |
| `POST` | `/sync` | Receive change batch with ACK (internal) |
| `GET` | `/health` | Health + item count + pending changes + dead letter count |

## Benchmark Results

All benchmarks on Mac M4. Each runtime tested independently — no cross-runtime traffic during testing. Full report: [BENCHMARK-REPORT.md](BENCHMARK-REPORT.md).

### Dual-writer throughput (ACK-based, 10 runs × 100 req per node, localhost)

| Runtime | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 15,116 | 3,698 | 18,366 | 10/10 PASS |
| Node | 11,496 | 5,251 | 14,416 | 10/10 PASS |
| Bun | 4,904 | 1,947 | 14,888 | 10/10 PASS |

200 concurrent writes per run (100 to each node). Integrity: both nodes have equal item count, 0 pending changes, 0 dead letter after each run.

Localhost HTTP variance is high (3-8x across runs). Median is the reliable metric, not min/max.

### Direct SQLite (10K writes, no HTTP)

| Runtime | Sequential | Transaction |
|---------|--------:|--------:|
| Go | 255K QPS | 373K QPS |
| Node | 307K QPS | 354K QPS |
| Bun | 339K QPS | **394K QPS** |

bun:sqlite is fastest in raw SQLite — even with trigger overhead (1 extra INSERT per change), it beats Go and Node in transaction mode.

### Sync does not block writes

Having a peer does not slow down writes. Sync runs in a separate timer + HTTP POST. Write path = SQLite INSERT + trigger capture only.

### Real network (2 VPS, Go, pre-fix protocol)

| Metric | Single server | Dual server | Difference |
|--------|--------:|--------:|--------:|
| Write latency p50 | 35.21ms | 35.09ms | -0.3% (noise) |
| Sync delay p50 | — | 135ms | — |
| Integrity | — | 750 = 750 ✅ | — |

Setup: 2 VPS (OVH + underconst), 4.5ms RTT between servers, 38-40ms RTT from Mac. Dual server = replica gratis — write speed identical, pay sync delay, get live replica.

### Crash recovery

Tested: kill node mid-write → changes survive in `_changes` table → restart node → sync resumes → both nodes reach equal count. PASS.

### Dead letter queue

Tested: peer unreachable → 5 retries with backoff → changes moved to `_dead_letter` → pending cleared. PASS.

## Verified

- Go ↔ Go, Go ↔ Bun, Go ↔ Node, Bun ↔ Node bidirectional sync ✅
- Multi-table sync (items + categories) across all runtime pairs ✅
- ACK-based delivery: no data loss on ship failure ✅
- Crash recovery: changes survive in `_changes`, sync resumes on restart ✅
- Dead letter: 5 retries → `_dead_letter` table ✅
- Integrity: 10/10 runs PASS (2000 items per node, 0 pending, 0 dead letter) ✅
- Sync does not block write path ✅
- Idle = zero traffic (timer no-op on empty `_changes`) ✅

## Limitations (prototype)

- **Multi-table requires manual setup** — adding a table means writing triggers + updating applyChanges dispatch (see `go/multi/`, `bun/server-multi.ts`, `node/server-multi.js` for 2-table example)
- **Hardcoded columns** — column names mapped by index in triggers
- **Point-to-point** — no topology management (star, mesh, etc.)
- **Localhost benchmark variance** — HTTP throughput varies 3-8x on localhost; use real network for reliable comparison

## License

MIT
