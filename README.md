# hook-sync

Multi-language SQLite replication. Change capture via SQLite triggers + `_changes` table, ACK-based batched HTTP sync, UUID primary keys. Go, Bun, and Node implementations speak the same wire protocol and sync to each other. Supports point-to-point (2 nodes), full mesh (3-7 nodes), and dedicated hub / star (8+ nodes) topologies. Cross-server benchmark: 3.8x faster than Postgres at batch 10K, 100K items converge in 2s with zero data loss.

## How It Works

```
App write (native SQLite speed)
  → Trigger captures change to _changes table (same transaction)
  → Background timer: batch every 50ms (default)
  → Drain mode: ships until _changes is empty within each tick
  → HTTP POST {batch_id, changes} to peer
  → Peer: INSERT OR REPLACE (UUID PK = zero conflict)
  → Peer returns {applied, ack: batch_id}
  → Sender deletes from _changes only after ACK confirms
```

In full mesh mode, the sender ships to all peers concurrently. Each peer has its own watermark (`_peer_state` table) — changes are deleted from `_changes` only after ALL peers have ACKed. Offline peers' changes accumulate until they reconnect.

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
├── TOPOLOGY.md               # Topology recommendations (point-to-point, full mesh, hub)
├── go/                       # Go implementation (Fiber + mattn/go-sqlite3)
│   ├── main.go               #   single-table, point-to-point
│   ├── mesh/main.go          #   full mesh (multi-peer, per-peer watermark)
│   ├── hub/main.go          #   dedicated hub (Pebble KV, star topology relay)
│   ├── multitable/main.go    #   multi-table (items + categories)
│   └── bench/                #   direct SQLite benchmarks
├── bun/                      # Bun implementation (Bun.serve + bun:sqlite)
│   ├── server.ts             #   single-table, point-to-point
│   ├── server-mesh.ts        #   full mesh (multi-peer, per-peer watermark)
│   └── server-multitable.ts  #   multi-table
├── node/                     # Node.js implementation (hyper-express + better-sqlite3)
│   ├── server.js             #   single-table, point-to-point
│   ├── server-mesh.js        #   full mesh (multi-peer, per-peer watermark)
│   └── server-multitable.js  #   multi-table
├── bench-dual-ack.sh         # Dual-writer benchmark, point-to-point (all 3 runtimes)
├── bench-fullmesh.sh         # Full mesh benchmark, 4 nodes all-to-all (all 3 runtimes)
├── bench-hub.sh             # Dedicated hub benchmark, 1 hub + 3 edges (all 3 runtimes)
├── hook-sync-go              # Go binary (point-to-point, single-table)
├── hook-sync-mesh-go         # Go binary (full mesh, multi-peer)
├── hook-sync-hub             # Go binary (dedicated hub, Pebble KV)
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

./hook-sync-go -id node1 -db node1.db -listen :9001 -peer http://localhost:9002 -batch-ms 50 -batch-size 10000
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

### Full mesh (multi-peer)

Full mesh topology: every node ships changes to all other nodes directly. Uses per-peer watermarks (`_peer_state` table) — changes are deleted from `_changes` only after ALL peers have ACKed. Offline peers' changes accumulate until they reconnect.

```bash
# Go — build mesh binary
cd go && go build -o ../hook-sync-mesh-go ./mesh

# 4-node full mesh (repeat --peer for each neighbor)
./hook-sync-mesh-go -id nodeA -db a.db -listen :9001 \
  -peer http://localhost:9002 \
  -peer http://localhost:9003 \
  -peer http://localhost:9004

# Bun
bun run bun/server-mesh.ts --id nodeB --db b.db --listen :9002 \
  --peer http://localhost:9001 \
  --peer http://localhost:9003 \
  --peer http://localhost:9004

# Node
node node/server-mesh.js --id nodeC --db c.db --listen :9003 \
  --peer http://localhost:9001 \
  --peer http://localhost:9002 \
  --peer http://localhost:9004
```

Cross-runtime mesh works — Go, Bun, and Node nodes sync to each other in the same mesh. See [TOPOLOGY.md](TOPOLOGY.md) for scaling limits and hub topology design.

Benchmark: `bash bench-fullmesh.sh` — 4 nodes, all-to-all, all 3 runtimes.

### Dedicated hub (star topology, 8+ nodes)

Dedicated hub for star topology. Hub is **Go-only** (Pebble KV store, write-optimized LSM). No SQLite, no triggers, no `/api/items` — pure relay + backup. All data enters via `/sync`. Pebble stores backup (`data:{id}`) and durable forwarding queue (`fwd:{n}`). Hub ACKs edge immediately, forwards to other edges asynchronously. If hub crashes after ACK, forwarding queue survives in Pebble → replay on restart.

Edge nodes use the existing `server-mesh.*` scripts — hub is just a peer via `--peer http://localhost:9010`. No edge script changes needed.

```bash
# Build hub binary
cd go && go build -o ../hook-sync-hub ./hub

# Hub (1 process, Go-only)
./hook-sync-hub -id hub1 -listen :9010 -db hub1.pebble \
  -edge http://localhost:9001 \
  -edge http://localhost:9002 \
  -edge http://localhost:9003 \
  -edge http://localhost:9004

# Edges (existing mesh scripts, peer to hub only)
./hook-sync-mesh-go -id edge1 -db e1.db -listen :9001 -peer http://localhost:9010 -batch-ms 50
bun run bun/server-mesh.ts --id edge2 --db e2.db --listen :9002 --peer http://localhost:9010 --batch-ms 50
node node/server-mesh.js --id edge3 --db e3.db --listen :9003 --peer http://localhost:9010 --batch-ms 50
```

## API

See [PROTOCOL.md](PROTOCOL.md) for full spec.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/items` | Create item (local write, triggers sync) |
| `GET` | `/api/items` | List items (latest 100) |
| `GET` | `/api/items/:id` | Get single item |
| `PUT` | `/api/items/:id` | Update item |
| `DELETE` | `/api/items/:id` | Delete item |
| `POST` | `/api/items/batch` | Create multiple items in one transaction (batch write) |
| `POST` | `/sync` | Receive change batch with ACK (internal) |
| `GET` | `/health` | Health + item count + pending changes + dead letter count + per-peer watermarks (mesh) |

**Hub API** (dedicated hub only — no `/api/items` endpoints):

| Method | Path | Description |
|---|---|---|
| `POST` | `/sync` | Receive change batch, ACK immediately, forward to other edges |
| `GET` | `/health` | Hub status: backup item count, pending forwards, edge list |

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

### Full mesh throughput (4 nodes all-to-all, 5 runs × 50 req per node, localhost)

| Runtime | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 14,853 | 6,108 | 23,570 | 5/5 PASS |
| Node | 14,414 | 3,165 | 18,487 | 5/5 PASS |
| Bun | 13,175 | 1,872 | 18,887 | 5/5 PASS |

200 concurrent writes per run (50 to each of 4 nodes). Integrity: all 4 nodes have equal item count (1000 per node), 0 pending changes, 0 dead letter after each run. Cross-runtime mesh (Go+Bun+Node+Go) also verified — all nodes converge.

Benchmark script: `bash bench-fullmesh.sh`

### Dedicated hub throughput (1 Go hub + 3 edges, star, 5 runs × 50 req per edge, localhost)

| Runtime (edges) | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 16,268 | 5,706 | 17,516 | 5/5 PASS |
| Bun | 19,750 | 12,931 | 27,683 | 5/5 PASS |
| Node | 13,193 | 148 | 21,660 | 5/5 PASS |

150 concurrent writes per run (50 to each of 3 edges). Integrity: all 3 edges have equal item count (750 per edge), hub backup count matches edges, 0 pending changes, 0 pending forwards, 0 dead letter after each run. Hub is always Go (Pebble KV).

Benchmark script: `bash bench-hub.sh`

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

### Cross-server vs Postgres (100K writes, real network, fair durability)

2 VPS (OVH Canada + 1TIM Asia, ~2-4ms RTT). Same Go HTTP client, concurrency 10. Both with active replication. Both fast durability (SQLite `synchronous=NORMAL` vs Postgres `synchronous_commit=off`).

| Mode | hook-sync (SQLite) | Postgres | Advantage |
|------|--------:|--------:|--------:|
| Single write (1 req = 1 INSERT) | 6,065 QPS | 6,238 QPS | tie (HTTP dominates) |
| Batch 100 (1 req = 100 INSERTs) | 27,366 QPS | 22,703 QPS | **hook-sync +20.5%** |
| Batch 1,000 | 31,429 QPS | 23,682 QPS | **hook-sync +32.7%** |
| Batch 10,000 | 31,558 QPS | 8,278 QPS | **hook-sync 3.8x** |

At equal durability, raw write throughput is tied in single-write mode (HTTP overhead dominates). As batch size grows, SQLite advantage emerges — hook-sync plateaus at ~31K QPS (HTTP ceiling), Postgres degrades at batch 10K (WAL buffer contention).

| Metric | hook-sync | Postgres |
|--------|----------|----------|
| Sync overhead | ~0% (background goroutine) | WAL sender overhead |
| Replica converge (100K items) | 2s (batch 10K + drain mode) | 3s (WAL streaming) |
| Multi-writer | Yes (UUID PK, idempotent) | No (primary-only) |
| Sync engine runtime | Go, Bun, Node (pick any) | Server-internal (C, fixed) |
| Topology | Point-to-point, full mesh, hub | Primary-replica only |
| Operational complexity | Single binary + SQLite file | Postgres cluster + replication config |

Full report: [BENCHMARK-REPORT.md](BENCHMARK-REPORT.md)

## Verified

- Go ↔ Go, Go ↔ Bun, Go ↔ Node, Bun ↔ Node bidirectional sync ✅
- Multi-table sync (items + categories) across all runtime pairs ✅
- ACK-based delivery: no data loss on ship failure ✅
- Crash recovery: changes survive in `_changes`, sync resumes on restart ✅
- Dead letter: 5 retries → `_dead_letter` table ✅
- Integrity: 10/10 runs PASS (2000 items per node, 0 pending, 0 dead letter) ✅
- Sync does not block write path ✅
- Idle = zero traffic (timer no-op on empty `_changes`) ✅
- Full mesh: 4 nodes all-to-all, per-peer watermark, all 3 runtimes ✅ (5/5 integrity PASS, 1000 items/node)
- Cross-runtime mesh: Go + Bun + Node + Go in same mesh ✅
- Per-peer watermark: offline peers' changes accumulate, no data loss on reconnect ✅
- Dedicated hub (Pebble KV): Go hub + 3 Go edges, star topology, multi-writer converge ✅
- Hub crash recovery: kill hub mid-traffic → Pebble fwd queue survives → restart → all edges converge ✅
- Cross-runtime star: Go hub + Go/Bun/Node edges, all converge, 0 pending, 0 dead letter ✅
- Cross-server vs Postgres: 100K writes, OVH Canada + 1TIM Asia, fair durability, 3.8x faster at batch 10K ✅
- Batch scaling: hook-sync plateaus at 31K QPS, Postgres degrades at batch 10K ✅
- Convergence: 100K items in 2s (batch-size 10000 + drain mode), zero data loss ✅
- Sync overhead: ~0% (with peer vs without peer = noise) ✅

## Limitations

- **No multi-region topology** — point-to-point, full mesh, and dedicated hub all work. Multi-region (hubs in full mesh) not yet built — needs origin tracking for loop prevention (see [TOPOLOGY.md](TOPOLOGY.md))
- **Multi-table requires manual setup** — adding a table means writing triggers + updating applyChanges dispatch (see `go/multitable/`, `bun/server-multitable.ts`, `node/server-multitable.js` for 2-table example)
- **Localhost benchmark variance** — HTTP throughput varies 3-8x on localhost; use real network for reliable comparison

## License

MIT
