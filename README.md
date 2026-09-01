# hook-sync

> SQLite replication that just works. Multi-server, multi-writer, multi-runtime. Zero data loss.

**Website**: [hook-sync.pages.dev](https://hook-sync.pages.dev) · **npm**: [hooksync.js](https://www.npmjs.com/package/hooksync.js) · **GitHub**: [maulanashalihin/hook-sync](https://github.com/maulanashalihin/hook-sync)

SQLite is the fastest SQL database in the world — zero config, single file, serverless. But it can't replicate. Until now.

hook-sync adds replication to SQLite via triggers + HTTP sync. No consensus algorithm. No Raft. No coordinator. Just triggers, ACK, and UUID. The result: **3.8x faster than Postgres at batch 10K**, with multi-writer active-active, crash recovery, and split-brain safety — all in a single binary.

```
Your app writes to SQLite (native speed)
  → Trigger captures change to _changes table (same transaction)
  → Background timer ships to peers via HTTP
  → Peer applies with timestamp conflict check
  → ACK confirms → delete from _changes
  → If peer is down: changes accumulate, retry on reconnect
  → If process crashes: changes survive in SQLite, resume on restart
```

**Write speed is identical with or without peers.** Sync runs in the background — it never blocks the write path. You get a live replica for free.

## Why hook-sync?

| Problem | Postgres | Litestream | rqlite | hook-sync |
|---------|----------|------------|--------|-----------|
| Replication | ✅ WAL streaming | ✅ WAL to S3 | ✅ Raft consensus | ✅ Trigger + ACK |
| Multi-writer | ❌ Primary-only | ❌ Read-only replica | ❌ Leader-only | ✅ All nodes write |
| Zero write penalty | ❌ WAL sender overhead | ✅ Async | ❌ Raft quorum per write | ✅ Async, 0% overhead |
| Cross-runtime | ❌ C server only | ❌ Go only | ❌ Go only | ✅ Go, Bun, Node interop |
| Split-brain safety | ✅ Sync replication | ❌ | ✅ Raft | ✅ Timestamp LWW |
| Crash recovery | ✅ WAL replay | ✅ S3 restore | ✅ Raft log | ✅ `_changes` replay |
| Setup complexity | Cluster + replication config | S3 bucket + config | 3-node Raft cluster | `./hook-sync-go -peer http://...` |
| Speed (batch 10K) | 8,278 QPS | N/A | N/A (Raft quorum) | **31,558 QPS** |

hook-sync occupies a gap no other project fills: **SQLite simplicity + multi-writer replication + cross-runtime interop**, without consensus overhead.

## How It Works

```
App write (native SQLite speed)
  → Trigger captures change to _changes table (same transaction)
  → Background timer: batch every 50ms (default)
  → Drain mode: ships until _changes is empty within each tick
  → HTTP POST {batch_id, changes} to peer
  → Peer: INSERT OR REPLACE (UUID PK = zero conflict)
         + timestamp check (last-write-wins for split-brain safety)
  → Peer returns {applied, ack: batch_id}
  → Sender deletes from _changes only after ACK confirms
```

Go also supports **hook capture mode** (`hook/` library, opt-in): instead of SQL triggers, a preupdate hook captures changes in-memory during the transaction, then a commit hook flushes them to Pebble as a single batch. This eliminates the `_changes` table write — 35% faster write throughput. Requires `sqlite_preupdate_hook` build tag (Go-only).

In full mesh mode, the sender ships to all peers concurrently. Each peer has its own watermark (`_peer_state` table) — changes are deleted from `_changes` only after ALL peers have ACKed. Offline peers' changes accumulate until they reconnect.

Sync runs in the background — it does not block the write path. The client gets its response as soon as SQLite write + capture completes.

## Topologies

Four topologies, all built and verified across Go, Bun, and Node:

| Topology | Nodes | Connections | SPOF | Best for |
|----------|------:|-------------|:----:|----------|
| Point-to-point | 2 | 1 | No | Simple active-active replica |
| Full mesh | 3-7 | N*(N-1)/2 | No | Multi-writer cluster, no coordinator |
| Dedicated hub (star) | 8+ | N-1 | Hub | Scale past mesh limit, regional relay |
| Multi-region (hub-to-hub) | 2+ regions | N-1 + hub peers | Hub | Cross-region sync |

### Point-to-point (2 nodes)

```
Node A ←→ Node B
```

Both nodes write, both nodes sync. Simplest setup — one `--peer` flag each.

### Full mesh (3-7 nodes)

```
    A ←→ B
    ↕ ╳ ↕
    C ←→ D
```

Every node ships to all peers directly. Per-peer watermark (`_peer_state` table) — changes deleted only after ALL peers ACK. Offline peers' changes accumulate until reconnect. Idempotent `INSERT OR REPLACE` makes duplicate delivery safe.

Scaling limit: `(N-1) × writes/sec` per node. Switch to hub when that exceeds ~10,000.

### Dedicated hub / star (8+ nodes)

```
edge1 ──→ hub ──→ edge2
edge3 ──→ hub ──→ edge4
```

Hub is a **protocol-level relay** — speaks only HTTP + JSON, language-agnostic. Edges can be Go, Bun, Node, Python, Rust, any language that implements the [wire protocol](PROTOCOL.md). Hub itself is Go-only (Pebble KV, LSM tree, write-optimized). No SQLite, no triggers — pure relay + backup. Hub ACKs immediately, forwards asynchronously. Durable forwarding queue survives hub crash. Edges use existing mesh scripts — hub is just a peer.
See [TOPOLOGY.md](TOPOLOGY.md) for scaling formulas, hub design details, and multi-region setup.

### Multi-region (hub-to-hub)

```
  Region 1                         Region 2
  edge1 ──→ hub A ←── edge2        edge3 ──→ hub B ←── edge4
              ←────────────────→
                 hub-to-hub
```

Each region has its own hub + edges. Hubs peer directly via `-edge` flag. Loop prevention via `X-Node-Url` header — hub skips forwarding back to sender's URL. No bridge nodes, no origin tracking, no protocol changes. Benchmark: `bash bench-multi-region.sh` — 5/5 PASS.

## Split-brain safety

When the network splits and both nodes accept writes independently, hook-sync uses **last-write-wins by timestamp** to resolve conflicts on reconnect:

| Scenario | During partition | On reconnect | Data loss? |
|----------|-----------------|-------------|-----------|
| INSERT (new rows) | Both create different UUIDs | Merge — both rows appear | ❌ None |
| UPDATE same row | Node A: value=100, Node B: value=200 | Converge to higher `updated_at` | Older update dropped |
| DELETE vs UPDATE | Node A deletes, Node B updates | UPDATE wins if newer than delete | Delete intent dropped |

Both nodes always converge to the same state. No divergence, no silent corruption. Tested across all 3 runtimes: `bash bench-splitbrain.sh` — 36/36 PASS (Go 12/12, Bun 12/12, Node 12/12).

### ACK-based reliability

Changes are persisted in `_changes` at write time (same transaction via triggers). If the process crashes, un-shipped changes survive and resume on restart.

Ship failures retry with exponential backoff (50/100/200/400/800ms, 5 attempts). **Connection errors (peer unreachable) never dead-letter** — changes stay in `_changes` and retry every tick until the peer reconnects. Dead letter is reserved for ACK mismatch (protocol error) only.

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
├── PROTOCOL.md               # Wire protocol spec (copy to any language)
├── TOPOLOGY.md               # Topology recommendations (point-to-point, full mesh, hub)
├── ROADMAP.md                # Product vision, gap analysis, 7-phase roadmap
├── go/                       # Go implementation (Fiber + mattn/go-sqlite3)
│   ├── hooksync/             #   shared core: types, wire protocol, table-agnostic LWW apply
│   ├── trigger/              #   trigger-based capture: Attach(db, config, tables) — auto-generates triggers
│   ├── hook/                 #   preupdate_hook capture: Open(path, config, tables) — Pebble-backed, 35% faster
│   ├── cmd/                  #   binary entrypoints (thin wrappers over libraries)
│   │   ├── server/main.go    #     trigger/ library, single-peer, --no-trigger baseline
│   │   ├── mesh/main.go      #     trigger/ library, multi-peer, per-peer watermark
│   │   ├── hookserver/main.go#     hook/ library, --in-memory flag for Pebble vs in-memory
│   │   ├── hub/main.go       #     dedicated hub (Pebble KV, pure relay — no SQLite)
│   │   └── multitable/main.go#     multi-table (items + categories)
│   ├── hookpebble/           #   legacy: preupdate_hook + Pebble (refactored to use hook/ library)
│   ├── hookmem/              #   research: preupdate_hook + in-memory (no persistence, benchmark baseline only)
│   └── bench/                #   direct SQLite benchmarks (trigger overhead, hook vs trigger)
├── js/                       # JS/TS library (npm: hooksync.js) — shared core for Bun and Node
│   ├── src/
│   │   ├── index.ts          #   re-exports attach(), types
│   │   ├── types.ts          #   Change, Config, SyncRequest/Response, SqliteDatabase interface
│   │   ├── apply.ts          #   table-agnostic LWW apply (dynamic SQL from Change.row keys)
│   │   ├── ship.ts           #   shipWithAck() via fetch()
│   │   └── trigger.ts        #   attach(db, config, tables) → Manager (triggers, ship loop, watermarks)
│   ├── package.json          #   npm package: hooksync.js
│   └── tsconfig.json
├── bun/                      # Bun wrapper (Bun.serve + bun:sqlite, imports js/ library)
│   ├── server.ts             #   single-table, point-to-point
│   ├── server-mesh.ts        #   full mesh (multi-peer, per-peer watermark)
│   └── server-multitable.ts  #   multi-table
├── node/                     # Node.js wrapper (hyper-express + better-sqlite3, imports js/ library)
│   ├── server.js             #   single-table, point-to-point
│   ├── server-mesh.js        #   full mesh (multi-peer, per-peer watermark)
│   └── server-multitable.js  #   multi-table
├── bench-dual-ack.sh         # Dual-writer benchmark, point-to-point (all 3 runtimes)
├── bench-fullmesh.sh         # Full mesh benchmark, 4 nodes all-to-all (all 3 runtimes)
├── bench-hub.sh              # Dedicated hub benchmark, 1 hub + 3 edges (all 3 runtimes)
├── bench-multi-region.sh     # Multi-region benchmark, 2 hubs hub-to-hub (convergence + persistence + loop check)
├── bench-splitbrain.sh       # Split-brain safety test (partition, conflict, reconnect)
├── bench-stress.sh           # Volume stress test, 10K/100K/500K items (convergence + persistence + consistency)
├── bench-all.sh              # Run ALL benchmarks in one command (all topologies, all runtimes)
└── BENCHMARK-REPORT.md       # Full benchmark report
```

| Language | Capture | Binding | HTTP server |
|----------|---------|---------|-------------|
| Go | `trigger/` (SQL triggers) or `hook/` (preupdate_hook + Pebble, 35% faster) | mattn/go-sqlite3 | Fiber (fasthttp) |
| Bun | `js/` library (SQL triggers + `_changes`) | bun:sqlite (built-in) | Bun.serve (native) |
| Node | `js/` library (SQL triggers + `_changes`) | better-sqlite3 | hyper-express (uWebSockets) |

Go offers two capture modes via importable libraries: `trigger/` (default, database-level, cross-runtime) and `hook/` (opt-in, connection-level, Go-only, preupdate_hook + Pebble batch). Both share the same wire protocol — trigger nodes sync to hook nodes. Bun and Node use trigger-based capture only.


## Go Libraries

The Go replication core is extracted into three importable packages. Pick capture mode at import time:

| Package | API | Capture | Build tag | Use when |
|---------|-----|--------|-----------|----------|
| `hooksync/` | `Change`, `Config`, `ShipWithAck()`, `ApplyChange()` | — (shared core) | none | Always (both modes depend on it) |
| `trigger/` | `Attach(db, config, tables)` | SQL triggers + `_changes` | none | Default — cross-runtime, database-level |
| `hook/` | `Open(path, config, tables)`, `OpenInMemory()` | preupdate_hook + Pebble | `sqlite_preupdate_hook` | Opt-in — 35% faster, Go-only, connection-level |

```go
import (
    "hook-sync/hooksync"
    "hook-sync/trigger"  // or "hook-sync/hook" for preupdate_hook mode
)

// trigger/ mode (default)
db, _ := sql.Open("sqlite3", "app.db")
mgr, _ := trigger.Attach(db, hooksync.Config{
    ID: "node1", Peers: []string{"http://peer:9002"},
}, []string{"items"})
defer mgr.Stop()

// hook/ mode (35% faster, requires build tag)
mgr, _ := hook.Open("app.db", hooksync.Config{
    ID: "node1", Peers: []string{"http://peer:9002"},
}, []string{"items"})
defer mgr.Stop()
```

Wire the HTTP server — Go's `trigger.Manager` implements `http.Handler`, so `/sync` is one line:

```go
http.Handle("/sync", mgr)  // mgr.ServeHTTP handles parse + apply + ACK
http.ListenAndServe(":9001", nil)
```

`ApplyChange()` is table-agnostic — column names come from `Change.Row` map, no hardcoded columns. `validTable()` validates table names (alphanumeric + underscore only) as defense-in-depth against SQL injection via table names (SQLite doesn't support parameterized table names).

## JS Library

The JS/TS replication core is published as [`hooksync.js`](https://www.npmjs.com/package/hooksync.js) on npm. One package works with both `bun:sqlite` and `better-sqlite3` — caller injects the database instance, library never imports a binding.

```bash
npm install hooksync.js
# or
bun add hooksync.js
```

```ts
import { attach } from "hooksync.js";
import { Database } from "bun:sqlite"; // or: const Database = require("better-sqlite3");

const db = new Database("app.db");
db.exec("PRAGMA journal_mode = WAL");

// Your table — must have `id` (TEXT PRIMARY KEY) and `updated_at` (INTEGER) columns
db.exec(`CREATE TABLE IF NOT EXISTS items(
  id TEXT PRIMARY KEY, name TEXT, value INTEGER,
  created_at INTEGER, updated_at INTEGER, node_id TEXT
);`);

// Attach sync — creates _meta, _changes, _dead_letter, _peer_state tables
// and auto-generates triggers via PRAGMA table_info introspection
const mgr = attach(db, {
  id: "node1",
  peers: ["http://localhost:9002"],
  batchMs: 50,
}, ["items"]);

// Wire HTTP server — the library does NOT include one.
// You must route POST /sync to mgr.applyChanges():
//
//   Bun.serve, http.createServer, Express, Hono, HyperExpress, etc.
//   POST /sync → mgr.applyChanges(body.changes) → { applied, ack: body.batch_id }
//   GET /health → mgr.health()
//
// See js/README.md for full HTTP server setup examples (Bun + Node).

mgr.stop(); // shutdown
```

`attach()` returns a Manager with `applyChanges()`, `health()`, and `stop()`. The library handles trigger generation, ship loop with per-peer watermarks, dead letter queue, and LWW conflict resolution. Caller handles HTTP server + CRUD endpoints only.

No hook capture mode for JS — neither `bun:sqlite` nor `better-sqlite3` has a preupdate hook API. Trigger-based only.

## Build & Run

### Go

```bash
cd go
go build -o ../hook-sync-go ./cmd/server

./hook-sync-go -id node1 -db node1.db -listen :9001 -peer http://localhost:9002 -batch-ms 50 -batch-size 10000
```

Hook capture mode (preupdate_hook + Pebble, 35% faster — requires build tag):

```bash
cd go
go build -tags sqlite_preupdate_hook -o ../hook-sync-hookserver ./cmd/hookserver

./hook-sync-hookserver -id node1 -db node1.db -listen :9001 -peer http://localhost:9002 -batch-ms 50 -batch-size 10000

# In-memory mode (no Pebble, no persistence — research/benchmark only):
./hook-sync-hookserver -id node1 -db node1.db -listen :9001 -peer http://localhost:9002 -in-memory
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

Both nodes sync bidirectionally — same wire protocol, same ACK-based reliability, same split-brain safety.

### Full mesh (multi-peer)

Full mesh topology: every node ships changes to all other nodes directly. Uses per-peer watermarks (`_peer_state` table) — changes are deleted from `_changes` only after ALL peers have ACKed. Offline peers' changes accumulate until they reconnect.

```bash
# Go — build mesh binary
cd go && go build -o ../hook-sync-mesh-go ./cmd/mesh

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

Edge nodes use the existing `server-mesh.*` scripts or the `hooksync.js` library — hub is just a peer URL. No edge code changes needed.

```bash
# Build hub binary
cd go && go build -o ../hook-sync-hub ./cmd/hub

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

### Using hooksync.js library as edge with hub

If you're using the npm library directly (not the wrapper scripts), just point `peers` to the hub URL:

```ts
import { attach } from "hooksync.js";

const mgr = attach(db, {
  id: "edge1",
  peers: ["http://localhost:9010"],  // hub URL — same as any peer
  batchMs: 50,
}, ["items"]);
```

The hub is transparent to the edge — it's just a peer that happens to relay to other edges. See [TOPOLOGY.md](TOPOLOGY.md) for scaling formulas and multi-region setup.

## API

See [PROTOCOL.md](PROTOCOL.md) for full spec.

| Method | Path | Description |
|---|---|---|
| `POST` | `/api/items` | Create item (local write, triggers sync) |
| `POST` | `/api/items/batch` | Create multiple items in one transaction (batch write) |
| `GET` | `/api/items` | List items (latest 100) |
| `GET` | `/api/items/:id` | Get single item |
| `PUT` | `/api/items/:id` | Update item |
| `DELETE` | `/api/items/:id` | Delete item |
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

### Split-brain safety

Tested across all 3 runtimes: `bash bench-splitbrain.sh` — 36/36 PASS (Go 12/12, Bun 12/12, Node 12/12).

| Scenario | Result |
|----------|--------|
| INSERT during partition | ✅ Converge — UUID, no collision |
| UPDATE same row during partition | ✅ Converge — last-write-wins by timestamp |
| DELETE vs UPDATE during partition | ✅ Converge — UPDATE wins if newer |
| Connection failure (peer down) | ✅ Retry next tick — no dead letter, no data loss |
| Crash recovery | ✅ Changes survive in `_changes`, resume on restart |

### Volume stress (massive writes, convergence + persistence + consistency)

`bash bench-stress.sh` — writes 10K, 100K, 500K items via batch endpoint, then verifies convergence time, consistency (exact count, 0 pending, 0 dead letter), and persistence (kill + restart, data survives). 9/9 PASS.

| Volume | Runtime | Write time | Converge time | Consistency | Persistence |
|-------:|---------|----------:|--------------:|:-----------:|:-----------:|
| 10K | Go | 96ms | 1s | ✅ | ✅ |
| 10K | Bun | 58ms | 1s | ✅ | ✅ |
| 10K | Node | 50ms | 1s | ✅ | ✅ |
| 100K | Go | 891ms | 1s | ✅ | ✅ |
| 100K | Bun | 769ms | 1s | ✅ | ✅ |
| 100K | Node | 618ms | 1s | ✅ | ✅ |
| 500K | Go | 4.2s | 5s | ✅ | ✅ |
| 500K | Bun | 10.7s | 5s | ✅ | ✅ |
| 500K | Node | 10.2s | 7s | ✅ | ✅ |

### Cross-server vs Postgres (100K writes, real network, fair durability)

2 VPS (OVH Singapore + 1TIM Singapore, ~2.7ms RTT, 290 Mbps). Same Go HTTP client, concurrency 10. Both with active replication. Both fast durability (SQLite `synchronous=NORMAL` vs Postgres `synchronous_commit=off`).

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

### Run your own benchmarks

All benchmarks are shell scripts that start servers, run tests, and verify data integrity. Each runtime (Go, Bun, Node) is tested independently — servers are stopped and restarted between runtimes.

```bash
# Run ALL benchmarks in one command (all topologies, all runtimes)
bash bench-all.sh

# Or run individual benchmarks:
bash bench-dual-ack.sh       # Point-to-point: 2 nodes, dual-writer throughput
bash bench-fullmesh.sh       # Full mesh: 4 nodes, all-to-all sync
bash bench-hub.sh            # Dedicated hub: 1 Go hub + 3 edges (star)
bash bench-multi-region.sh   # Multi-region: 2 hubs hub-to-hub, convergence + persistence + loop check
bash bench-splitbrain.sh     # Split-brain safety: partition, conflict, reconnect
bash bench-stress.sh         # Volume stress: 10K/100K/500K items, convergence + persistence + consistency
bash bench-trigger.sh        # Trigger overhead via HTTP (baseline vs with triggers, Go)

# Test a single runtime for split-brain:
bash bench-splitbrain.sh go   # or: bun, node
```

Each benchmark:
- Starts servers, waits for readiness, runs N iterations
- Verifies data integrity after each iteration (item count, pending changes, dead letter)
- Reports QPS (min/median/max) and integrity PASS/FAIL
- Cleans up all processes and DB files between runtimes

**Prerequisites**: Go 1.21+, Bun, Node.js with `npm install` in `node/`.

## Verified

- Go ↔ Go, Go ↔ Bun, Go ↔ Node, Bun ↔ Node bidirectional sync ✅
- Multi-table sync (items + categories) across all runtime pairs ✅
- ACK-based delivery: no data loss on ship failure ✅
- Crash recovery: changes survive in `_changes`, sync resumes on restart ✅
- Dead letter: ACK mismatch → `_dead_letter` table ✅
- Integrity: 10/10 runs PASS (2000 items per node, 0 pending, 0 dead letter) ✅
- Sync does not block write path ✅
- Idle = zero traffic (timer no-op on empty `_changes`) ✅
- Full mesh: 4 nodes all-to-all, per-peer watermark, all 3 runtimes ✅ (5/5 integrity PASS, 1000 items/node)
- Cross-runtime mesh: Go + Bun + Node + Go in same mesh ✅
- Per-peer watermark: offline peers' changes accumulate, no data loss on reconnect ✅
- Dedicated hub (Pebble KV): Go hub + 3 Go edges, star topology, multi-writer converge ✅
- Hub crash recovery: kill hub mid-traffic → Pebble fwd queue survives → restart → all edges converge ✅
- Cross-runtime star: Go hub + Go/Bun/Node edges, all converge, 0 pending, 0 dead letter ✅
- Cross-server vs Postgres: 100K writes, OVH + 1TIM, fair durability, 3.8x faster at batch 10K ✅
- Batch scaling: hook-sync plateaus at 31K QPS, Postgres degrades at batch 10K ✅
- Convergence: 100K items in 2s (batch-size 10000 + drain mode), zero data loss ✅
- Sync overhead: ~0% (with peer vs without peer = noise) ✅
- Split-brain: INSERT/UPDATE/DELETE conflicts converge, 36/36 PASS across all 3 runtimes ✅
- Connection error retry: peer unreachable → no dead letter, retry next tick ✅
- Volume stress: 500K items, all 3 runtimes, convergence + persistence + consistency 9/9 PASS ✅
- Multi-region: hub-to-hub via `X-Node-Url` header, 5/5 PASS (convergence, bidirectional, persistence, hub down+reconnect, loop check) ✅

## Implement in Your Language

The protocol is language-agnostic. Read [PROTOCOL.md](PROTOCOL.md) — it's ~300 lines. You need:

1. SQLite database with `_changes`, `_meta`, `_dead_letter` tables
2. Triggers on your data tables that capture changes to `_changes`
3. Background ship loop: read `_changes`, POST to peer, delete on ACK
4. `/sync` endpoint: receive changes, apply with timestamp conflict check, return ACK
5. `syncing` flag to prevent infinite loop

No consensus. No Raft. No coordinator. Just triggers + HTTP + ACK.

Reference implementations: `go/cmd/server/main.go` (thin wrapper over `trigger/` library, ~240 lines), `go/cmd/hookserver/main.go` (thin wrapper over `hook/` library), `bun/server.ts` (thin wrapper over `js/` library, ~120 lines), `node/server.js` (thin wrapper over `js/` library, ~110 lines). All three sync to each other. Libraries: Go `hooksync/` (shared core), `trigger/` (trigger capture), `hook/` (preupdate_hook capture); JS `hooksync.js` (npm package, shared core for Bun + Node).

## Limitations

- **Multi-table setup** — Go libraries (`trigger/`, `hook/`) and JS library (`hooksync.js`) are table-agnostic: pass table names to `attach()`, triggers are auto-generated via schema introspection. Bun and Node wrappers still hardcode `items` table (see `go/cmd/multitable/`, `bun/server-multitable.ts`, `node/server-multitable.js` for 2-table example with manual trigger setup)
- **Localhost benchmark variance** — HTTP throughput varies 3-8x on localhost; use real network for reliable comparison
- **Last-write-wins, not CRDT** — split-brain conflicts resolve by timestamp. Older update is silently dropped. Fine for append-heavy workloads; for collaborative editing of shared rows, use cr-sqlite


## About

[hook-sync](https://github.com/maulanashalihin/hook-sync) is an open-source SQLite replication engine built by [Maulana Shalihin](https://github.com/maulanashalihin). It fills a gap no other project covers: SQLite simplicity + multi-writer replication + cross-runtime interop (Go, Bun, Node), without consensus overhead.

Full documentation, benchmarks, and topology guides at [hook-sync.pages.dev](https://hook-sync.pages.dev).

## License

MIT
