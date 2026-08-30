# Topology Recommendations

hook-sync supports **point-to-point** (2 nodes), **full mesh** (3-7 nodes), and **dedicated hub / star** (8+ nodes) topologies. All three are built and verified. This document covers what works today, scaling limits, and remaining work for multi-region.

## Current State

### Point-to-point (2 nodes)

Each node has one `--peer` URL. Both nodes ship and receive from each other.

```
Node A ←→ Node B
```

Tested and verified across all runtime pairs (Go, Bun, Node). Files: `go/main.go`, `bun/server.ts`, `node/server.js`.

### Full mesh (3-7 nodes)

Every node ships changes to all peers directly. Repeated `--peer` flag. Per-peer watermark in `_peer_state` table — changes deleted from `_changes` only after ALL peers ACK. Offline peers' changes accumulate until reconnection.

```
    A ←→ B
    ↕ ╳ ↕
    C ←→ D
```

Tested and verified: 4-node all-to-all, all 3 runtimes, cross-runtime mesh (Go+Bun+Node+Go). Files: `go/mesh/main.go`, `bun/server-mesh.ts`, `node/server-mesh.js`. Benchmark: `bash bench-fullmesh.sh`.

### Dedicated hub / star (8+ nodes)

Dedicated hub relays changes between edges. Hub is **Go-only** (Pebble KV store — write-optimized LSM, no SQLite, no triggers). Edges use existing `server-mesh.*` scripts with `--peer` pointing to hub. Hub ACKs immediately, forwards asynchronously. Durable forwarding queue in Pebble survives hub crash.

```
  edge1 ──POST /sync──→ hub ──POST /sync──→ edge2
         (changes)      │
                        ├──→ edge3
                        └ apply to Pebble (backup)
```

Tested and verified: Go hub + 3 Go edges (multi-writer converge), hub crash recovery (Pebble replay), cross-runtime star (Go hub + Go/Bun/Node edges). File: `go/hub/main.go`. Binary: `hook-sync-hub`. Benchmark: `bash bench-hub.sh`.

## Full Mesh Scaling Limits

**Why full mesh works with current protocol:** INSERT OR REPLACE with UUID PK is idempotent. If A ships to B and C, and B also ships to C, C receives the same change twice — safe, no duplicates. The `syncing` flag is not a problem because each node receives changes directly from the writer, not through intermediaries.

**How it works (built):** repeated `--peer` flag, per-peer watermark in `_peer_state` table. Each peer gets only changes with `change_id > its last_acked`. Changes deleted from `_changes` only when ALL peers have ACKed (`min(last_acked)`). Offline peers' changes accumulate until they reconnect — no dead-letter for transient failures.

```bash
./hook-sync-mesh-go -id nodeA -db a.db -listen :9001 \
  -peer http://localhost:9002 \
  -peer http://localhost:9003 \
  -peer http://localhost:9004
```

**Scaling formula:** the real bottleneck is not connection count, but **messages per second each node must handle**:

```
msgs/sec per node = (N - 1) × writes/sec per node
```

Each message ≈ 200 bytes JSON. Example with 1000 writes/sec per node:

| Nodes | Msgs/sec per node | Bandwidth/node | Verdict |
|-------|-------------------|---------------|---------|
| 2 | 1,000 | 0.2 MB/s | Fine |
| 5 | 4,000 | 0.8 MB/s | Fine |
| 8 | 7,000 | 1.4 MB/s | Fine for LAN |
| 15 | 14,000 | 2.8 MB/s | Pushing SQLite ingest |
| 20 | 19,000 | 3.8 MB/s | Too much |

**Cutoff depends on write rate, not node count alone:**

| Write rate per node | Full mesh OK up to | Bottleneck |
|--------------------|--------------------|------------|
| 100 writes/sec | 15-20 nodes | HTTP overhead negligible |
| 1,000 writes/sec | 8-12 nodes | SQLite ingest ~10K msg/sec |
| 10,000 writes/sec | 3-5 nodes | SQLite can't keep up |

Rule of thumb: **switch to dedicated hub when (N-1) × writes/sec exceeds ~10,000 per node.** That's where SQLite WAL write contention starts to degrade.

## When Full Mesh Doesn't Scale: Dedicated Hub

When full mesh hits the write-rate limit (see formula above), use a **dedicated hub** — a node that only relays changes, does not serve client requests.

```
  edge1 ──POST /sync──→ hub ──POST /sync──→ edge2
         (changes)      │
                        ├──→ edge3
                        ├──→ edge4
                        └ apply to Pebble KV (full backup)
```

### Why dedicated hub is simpler than dual-purpose hub

A dual-purpose hub (serves `/api/items` AND relays) has a problem: the `syncing` flag suppresses triggers during apply, which also blocks forwarding. Working around this is complex.

A dedicated hub has **no triggers at all**. All data enters via `/sync`, not local writes. No triggers = no `syncing` flag problem = forwarding just works.

| | Dual-purpose hub | Dedicated hub |
|---|---|---|
| Serves `/api/items`? | Yes | No |
| Has triggers? | Yes | No |
| `syncing` flag problem? | Yes — blocks forward | No — no triggers |
| Client traffic? | Yes — competes with relay | No — pure relay |
| What to build | Relay + work around syncing flag | Relay only |

### What the hub does (built)

1. **Receive `/sync`** — accept changes from any edge
2. **Apply to Pebble** — `Set("data:{id}", rowJSON)` for INSERT/UPDATE, `Delete` for DELETE (backup copy)
3. **Enqueue durable forwards** — `Set("fwd:{n}", {batchID, changes, edgeURL})` in Pebble before ACK
4. **ACK edge immediately** — edge deletes from its `_changes`
5. **Forward asynchronously** — try immediate forward, background sweep retries with backoff

All steps implemented in `go/hub/main.go`. Pebble chosen over bbolt/BadgerDB: LSM tree = write-optimized (hub workload is ~100% write ingest, no client reads).

### Hub failure

Hub is single point of failure. Mitigate: run hub ←→ hub-backup (point-to-point, already works). If hub dies, hub-backup takes over. Edges reconnect to hub-backup.

### Edge reconnection

If an edge goes down and comes back, it ships all pending `_changes` to hub. Hub forwards to other edges. No data loss — same ACK + retry mechanism as point-to-point.

### Hub forwarding queue (built with Pebble)

Hub ACKs edge immediately on receive, then forwards to other edges asynchronously. If hub crashes after ACK and before forward, changes survive in Pebble's durable forwarding queue → replay on restart.

Pebble stores forwards under key `fwd:{counter}` (one entry per edge per batch). On successful forward, the entry is deleted. On restart, `forwardSweep` picks up all pending `fwd:` entries and retries.

```
edge1 → hub receives
      → Set("data:{id}", rowJSON) in Pebble     ← backup
      → Set("fwd:{n}", {changes, edgeURL})       ← durable forward queue
      → ACK edge1 (edge deletes from its _changes)
      → forward to edge2/3/4
      → each edge ACKs → Delete("fwd:{n}") from Pebble

hub crash after ACK, before forward?
  → Pebble still has fwd:{n}
  → restart → forwardSweep replays → forward → done
```

**KV choice: Pebble** (cockroachdb/pebble). LSM tree = write-optimized. Hub workload is ~100% write ingest (no client reads, no `/api/items`). Pebble is Go-native, RocksDB-compatible, CockroachDB's production engine.

| KV | Type | Chosen? | Why |
|-----|------|---------|-----|
| Pebble | LSM tree | ✅ Yes | Write-optimized, Go-native, production-proven (CockroachDB) |
| bbolt | B-tree | No | Read-optimized, hub doesn't read |
| BadgerDB | LSM tree | No | Also write-optimized, but Pebble has better tooling + CockroachDB pedigree |

Verified: kill hub mid-traffic → write 5 items while hub down → restart hub → all edges converge to equal count, 0 pending, 0 dead letter.


## Multi-Region: Hub-to-Hub

Each region has its own hub + edges. Hubs peer directly via `-edge` flag. Loop prevention via `X-Node-Url` header — hub skips forwarding back to the sender's URL.

```
  Region 1                         Region 2
  edge1 ──→ hub A ←── edge2        edge3 ──→ hub B ←── edge4
              ←────────────────→
                 hub-to-hub
```

Flow: edge1 writes → hub A receives → forwards to edge2 + hub B (with `X-Node-Url: hubA`) → hub B sees `X-Node-Url: hubA`, skips forwarding back to hub A → forwards to edge3, edge4. No loop.

**How it works:** Hub sends `X-Node-Url` header (its own URL, set via `--url` flag) with every forward. Receiving hub reads `X-Node-Url` and skips the edge that matches — no forward back to sender. Edge nodes don't send `X-Node-Url` (empty header), so hubs forward to all edges normally.

```bash
# Region 1
./hook-sync-hub -id hubA -listen :9100 -url http://localhost:9100 -db hubA.pebble \
  -edge http://localhost:9101 \
  -edge http://localhost:9102 \
  -edge http://localhost:9200   # hub B (peer hub)

./hook-sync-mesh-go -id edge1 -db e1.db -listen :9101 -peer http://localhost:9100
./hook-sync-mesh-go -id edge2 -db e2.db -listen :9102 -peer http://localhost:9100

# Region 2
./hook-sync-hub -id hubB -listen :9200 -url http://localhost:9200 -db hubB.pebble \
  -edge http://localhost:9201 \
  -edge http://localhost:9202 \
  -edge http://localhost:9100   # hub A (peer hub)

./hook-sync-mesh-go -id edge3 -db e3.db -listen :9201 -peer http://localhost:9200
./hook-sync-mesh-go -id edge4 -db e4.db -listen :9202 -peer http://localhost:9200
```

**No bridge nodes needed.** Hubs peer directly. The `X-Node-Url` header is the only loop prevention mechanism — simple, no protocol changes, no origin tracking.

Tested: `bash bench-multi-region.sh` — 5/5 PASS (cross-region convergence, bidirectional, persistence, hub down + reconnect, loop check).


## Topology Comparison

| Topology | Connections | Hops | Redundancy | SPOF | Best for |
|----------|------------|------|------------|------|----------|
| Point-to-point | 1 | 1 | None | No | 2 nodes ✅ |
| Full mesh | N*(N-1)/2 | 1 | N-1 paths | No | 3-7 nodes ✅ |
| Star + relay | N-1 | 2 max | None | Hub | 8+ nodes ✅ |
| Multi-region (bridge) | N-1 + bridges | 3-4 max | Bridge | Hub | 2+ regions ✅ |
| Ring | N | N/2 avg | 1 path | No | Not recommended |
| Chain | N-1 | N max | None | No | Not recommended |

## Implementation Priority

1. ~~**Multi-peer support** (`--peer` repeated flag)~~ ✅ **Done** — full mesh built and verified across all 3 runtimes. Per-peer watermark in `_peer_state` table. Files: `go/mesh/`, `bun/server-mesh.ts`, `node/server-mesh.js`.
2. ~~**Relay mode**~~ ✅ **Done** — dedicated hub built with Pebble KV. Go-only (`go/hub/main.go`). Durable forwarding queue, crash recovery verified. Edges use existing `server-mesh.*` scripts.
3. **Watermark-based pull** — nodes ask "give me changes after X" instead of push. For unreliable networks / eventual consistency at scale. Highest complexity, defer until needed.

## Solved Problems

### _changes table management with multiple peers

**Solved with per-peer watermark.** Each peer has a `last_acked` entry in `_peer_state` table. Ship only sends changes with `change_id > peer's last_acked`. Delete from `_changes` only when ALL peers have ACKed (`min(last_acked)`). Offline peers' changes accumulate until they reconnect — no data loss, no dead-letter for transient failures.

```sql
CREATE TABLE _peer_state (
    peer_url TEXT PRIMARY KEY,
    last_acked INTEGER DEFAULT 0
);
```

Implemented in all 3 runtimes. Verified: 4-node mesh, 5/5 integrity PASS, 1000 items per node, 0 pending, 0 dead letter.

### Hub forwarding queue durability

**Solved with Pebble KV store.** Hub ACKs edge immediately, then forwards asynchronously. If hub crashes after ACK but before forward, the forwarding entry (`fwd:{n}`) survives in Pebble → replay on restart. No data loss.

Implemented in `go/hub/main.go`. Verified: kill hub mid-traffic → write 5 items → restart → all edges converge, 0 pending, 0 dead letter.

### Multi-region loop prevention

**Solved with `X-Node-Url` header.** Hubs peer directly — no bridge nodes needed. Hub sends `X-Node-Url` header (its own URL) with every forward. Receiving hub skips the edge matching sender's URL. No origin tracking, no protocol changes. See [Multi-Region: Hub-to-Hub](#multi-region-hub-to-hub) above.


## What NOT to Do

- **Don't build topology management into the protocol.** Keep protocol simple (ship changes, ACK). Topology is a node-config concern.
- **Don't add a coordinator.** UUID PKs eliminate the need for central ID assignment. Keep it decentralized.
- **Don't add conflict resolution.** INSERT OR REPLACE with UUID PK = no conflicts. No CRDT, no vector clocks.
- **Don't build ring or chain topologies.** They add latency and fragility with no benefit over full mesh (small N) or star (large N).
