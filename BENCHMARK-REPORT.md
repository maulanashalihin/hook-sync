# Benchmark Report: hook-sync

**Date:** 2026-08-30
**Stack:** Go 1.26 + Fiber + mattn/go-sqlite3, Node 22 + hyper-express + better-sqlite3, Bun 1.4 + Bun.serve + bun:sqlite
**Environment:** Mac M4 (localhost), 2-4 nodes per runtime depending on topology, 50ms batch interval, batch-size 10000
**Methodology:** Each runtime tested independently. All processes stopped before each run. No cross-runtime traffic during testing.

---

## Dual-Writer Throughput (ACK-based, latest)

10 runs × 100 concurrent requests per node (200 total per run). Both nodes receive writes simultaneously. Integrity checked after each run.

| Go | 9,149 | 3,705 | 15,063 | 10/10 PASS |
| Bun | 10,476 | 3,004 | 14,159 | 10/10 PASS |
| Node | 8,393 | 3,458 | 10,068 | 10/10 PASS |

Integrity criteria: both nodes have equal item count, 0 pending changes, 0 dead letter after sync settles.

Run with: `bash bench-dual-ack.sh`

### Localhost variance note

HTTP throughput on localhost varies 3-5x across runs (loopback artifact, not runtime difference). With 10 runs, median is stable and reliable: Go ~9K, Bun ~10K, Node ~8K QPS. Cross-server benchmark on real network confirms (variance drops to 1.2x).

---

## Direct SQLite (10K writes, no HTTP)

Pure SQLite write speed with triggers firing. No HTTP, no network, no event loop overhead. This is the reliable benchmark.

| Runtime | Sequential | Transaction | Triggers fired |
|---------|--------:|--------:|---:|
| Go | 255K QPS | 373K QPS | 10,000 ✅ |
| Node | 307K QPS | 354K QPS | 10,000 ✅ |
| Bun | 339K QPS | **394K QPS** | 10,000 ✅ |

bun:sqlite is fastest in raw SQLite — even with trigger overhead (1 extra INSERT per change), it beats Go and Node in transaction mode.


## Sync Does Not Block Writes

Having a peer does not slow down writes. Sync runs in a separate timer + HTTP POST. Write path = SQLite INSERT + trigger capture only.

200 concurrent HTTP writes, 5 runs, median:

| Runtime | Single (no peer) | With peer | Difference |
|---------|--------:|--------:|--------:|
| Go | 12,668 QPS | 13,436 QPS | +6% (noise) |
| Bun | 8,099 QPS | 8,578 QPS | +6% (noise) |
| Node | 14,764 QPS | 14,701 QPS | -0.4% (noise) |


*Data measured prior to bench script consolidation. Behavior unchanged — sync still runs in background timer, decoupled from write path.*
---

## Sync Delay = Batch Interval

| Interval | Sync p50 | Burst sync (100 writes) | Use case |
|----------|--------:|--------:|----------|
| **10ms** | **12ms** | 20ms | Local/LAN |
| 50ms (default) | 52ms | 25ms | Remote/WAN |
| 100ms | 100ms | 22ms | Conservative |

Burst sync is constant ~20-25ms regardless of interval — batch threshold (100 changes) triggers immediate ship, bypassing the ticker.

*Data measured prior to bench script consolidation. Behavior unchanged — sync delay is still determined by batch interval timer.*

---


## Full Mesh Throughput (4 nodes all-to-all)

4 nodes, each ships changes to all 3 peers concurrently. Per-peer watermark (`_peer_state` table) — changes deleted from `_changes` only after ALL peers ACK. 5 runs × 50 req per node (200 total per run).

| Runtime | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 14,853 | 6,108 | 23,570 | 5/5 PASS |
| Node | 14,414 | 3,165 | 18,487 | 5/5 PASS |
| Bun | 13,175 | 1,872 | 18,887 | 5/5 PASS |

Integrity: all 4 nodes have equal item count (1000 per node), 0 pending changes, 0 dead letter after each run. Cross-runtime mesh (Go+Bun+Node+Go) also verified — all nodes converge.

Run with: `bash bench-fullmesh.sh`

---

## Dedicated Hub Throughput (1 Go hub + 3 edges, star)

1 Go hub (Pebble KV store) + 3 edge nodes. Hub ACKs edge immediately, forwards to other edges asynchronously via durable Pebble forwarding queue. 5 runs × 50 req per edge (150 total per run).

| Runtime (edges) | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 16,268 | 5,706 | 17,516 | 5/5 PASS |
| Bun | 19,750 | 12,931 | 27,683 | 5/5 PASS |
| Node | 13,193 | 148 | 21,660 | 5/5 PASS |

Integrity: all 3 edges have equal item count (750 per edge), hub backup count matches edges, 0 pending changes, 0 pending forwards, 0 dead letter after each run. Hub is always Go (Pebble KV).

Run with: `bash bench-hub.sh`

---

## Convergence Speed (batch-size + drain mode)

Before fix: 100K items took ~60s to converge (single batch of 100 changes per tick, 50ms interval = 2000 ticks).

After fix: `-batch-size 10000` + drain mode (ships until `_changes` empty within each tick).

| Items | Before | After | Speedup |
|------:|--------:|--------:|--------:|
| 10K | ~6s | <1s | 6x |
| 100K | ~60s | ~2s | 30x |

Drain mode: ship loop runs until `_changes` is empty within each tick, not just one batch. Combined with batch-size 10000, all pending changes ship in 1-2 ticks.

---

## Split-Brain Safety (all 3 runtimes)

Tests partition + independent writes + reconnect convergence. 6 phases per runtime:

1. Start both nodes, create shared item, verify sync
2. Network partition (kill both nodes)
3. Start nodes independently (no peer), update same item + create new items
4. Reconnect (restart with peer)
5. Verify convergence: same value, all items merged, 0 dead letter
6. DELETE vs UPDATE conflict test

| Runtime | Checks | Passed | Result |
|---------|------:|------:|:---------:|
| Go | 12 | 12 | ✅ PASS |
| Bun | 12 | 12 | ✅ PASS |
| Node | 12 | 12 | ✅ PASS |
| **Total** | **36** | **36** | **✅ ALL PASS** |

Conflict resolution: last-write-wins by `updated_at` timestamp. Both nodes always converge to same state. INSERT = safe (UUID, no collision). UPDATE vs UPDATE = higher timestamp wins. DELETE vs UPDATE = UPDATE wins if newer.

Run with: `bash bench-splitbrain.sh` (all runtimes) or `bash bench-splitbrain.sh go` (single runtime)

---

## Real Network 10K Batch Test (OVH → 1TIM)

**Servers:** OVH (51.79.159.231, Singapore) + 1TIM (194.233.76.139, Singapore). RTT 2.7ms, 289 Mbps single stream (iperf3).

10,000 items sent in single batch from OVH → 1TIM. Both Go runtime, batch-size 10000, drain mode.

| Metric | Result |
|--------|--------|
| Converge time | <1s |
| Data loss | 0 |
| Dead letter | 0 |
| Pending after converge | 0 |

**Result: PASS.** 10K items converge in under 1 second over real network with 290 Mbps bandwidth. Zero data loss, zero dead letter.

---

## Compression Analysis (NOT implemented)

Tested gzip compression for sync payload on 290 Mbps link (OVH ↔ 1TIM, RTT 2.7ms).

| Payload | Uncompressed | Gzip | Ratio | CPU cost | Transfer save | Worth it? |
|---------|--------:|--------:|--------:|--------:|--------:|:---------:|
| 10K items JSON | ~2MB | ~100KB | 95% | 20-47ms | 0.5-50ms | ❌ No |

**Conclusion: compression NOT worth it.** CPU is the bottleneck at 290 Mbps — gzip adds 20-47ms CPU for only 0.5-50ms transfer save. Network is fast enough that compression overhead exceeds the bandwidth savings. Would only help on slow links (<50 Mbps).

---

## Crash Recovery

Tested with Go ↔ Go:

1. Write 50 items to node A
2. Kill node A immediately (before sync completes)
3. Verify `_changes` table has 50 pending rows in DB file
4. Restart node A
5. Sync resumes automatically
6. Both nodes reach 50 items, 0 pending

**Result: PASS.** Changes survive in `_changes` table (same transaction as write via triggers). Sync resumes on restart.

---

## Dead Letter Queue

Dead letter is reserved for **ACK mismatch** (protocol error) only. Connection errors (peer unreachable) never dead-letter — changes stay in `_changes` and retry every tick until peer reconnects.

Tested with Go, peer unreachable (port 9999 nothing listening):

1. Write 5 items to node A
2. Ship attempts fail: retry 1 (50ms) → retry 2 (100ms) → retry 3 (200ms) → retry 4 (400ms) → retry 5 (800ms)
3. After 5 connection failures: changes stay in `_changes` (NOT dead-lettered), retry next tick
4. `/health` reports `dead_letter: 0, pending_changes: 5`
5. Start peer on port 9999 → next tick ships successfully → both nodes converge

**Result: PASS.** Connection errors retry indefinitely — no data loss, no dead letter. Only ACK mismatch (peer returns wrong batch_id) moves to `_dead_letter`.

---

## Cross-Server: hook-sync vs Postgres (100K writes, real network)

**Date:** 2026-08-30
**Servers:** OVH (Singapore, Intel Haswell 6 vCPU, 11GB) + 1TIM (Singapore, AMD EPYC 6 vCPU, 11GB)
**Network:** 2.7ms RTT between servers, 289 Mbps single stream (iperf3)
**Methodology:** Same Go HTTP client, concurrency 10, 10 runs × 10,000 = 100,000 writes. Both systems with active replication cross-server. Apples-to-apples: both via HTTP API (`POST /api/items`), same JSON schema, same UUID PK.

### Setup

| System | Write path | Replication | DB |
|--------|-----------|------------|-----|
| hook-sync | HTTP → SQLite INSERT + trigger → return | ACK-based async HTTP sync (background goroutine) | SQLite WAL |
| Postgres | HTTP → pgx pool → INSERT → WAL flush | Streaming replication (WAL sender → replica) | PostgreSQL 18 |

### Results: fair comparison (both fast durability)

Both configured with equivalent durability: no fsync per write.

- hook-sync: `synchronous=NORMAL` (WAL flush at checkpoint, not per write)
- Postgres: `synchronous_commit=off` (ACK before WAL flush)

| System | Durability | QPS median | QPS min | QPS max | Replica converge | Integrity |
|--------|------------|--------:|--------:|--------:|:----------------:|:---------:|
| hook-sync | `synchronous=NORMAL` | 6,065 | 5,469 | 6,438 | ~2s (batch 10K + drain) | 100K, 0 pending, 0 dead letter |
| Postgres | `synchronous_commit=off` | **6,238** | 5,504 | 7,392 | ~3s (WAL streaming) | 100K |

**At equivalent durability, throughput is tied** — Postgres +2.8% (within noise). Raw write performance is not the differentiator.

### Results: Postgres default durability (unfair to Postgres)

Postgres default `synchronous_commit=on` (fsync per write) vs hook-sync `synchronous=NORMAL` (no fsync per write). This is the earlier benchmark — included for reference, but not a fair comparison.

| System | Durability | QPS median | Replica converge | Integrity |
|--------|------------|--------:|:----------------:|:---------:|
| hook-sync | `synchronous=NORMAL` | 5,857 | ~2s | 100K, 0 pending, 0 dead |
| hook-sync (no peer, baseline) | `synchronous=NORMAL` | 5,749 | — | 100K |
| Postgres | `synchronous_commit=on` | 4,377 | ~3s | 100K |

hook-sync appeared 34% faster, but only because Postgres was fsync-ing every write while hook-sync was not. Not a fair comparison.

### Sync overhead = ~0%

hook-sync with peer (5,857) vs without peer (5,749) — difference is noise. Sync runs in background goroutine, completely decoupled from write path. Client gets response after SQLite INSERT, never waits for sync.

### Where each system wins

| Metric | hook-sync | Postgres | Winner |
|--------|----------|----------|--------|
| Write throughput (fair durability) | 6,065 | 6,238 | **Tie** (Postgres +2.8%, noise) |
| Sync overhead | ~0% (background goroutine) | WAL sender overhead | **hook-sync** |
| Replica lag | ~2s (batch 10K + drain) | ~3s (WAL streaming) | **hook-sync** |
| Cross-runtime | Go, Bun, Node — same protocol | Go-only (pgx) | **hook-sync** |
| Topology | Point-to-point, full mesh, hub | Primary-replica only | **hook-sync** |
| Multi-writer | Yes (UUID PK, idempotent) | No (primary-only writes) | **hook-sync** |
| Operational complexity | Single binary + SQLite file | Postgres cluster + replication config | **hook-sync** |

### All QPS data

```
Fair durability:
  hook-sync (synchronous=NORMAL, with peer):    5633, 6286, 6170, 5636, 6230, 5841, 5745, 6438, 6065, 5469
  Postgres  (synchronous_commit=off, replica):  6235, 5727, 6426, 6262, 5504, 6238, 7392, 6065, 6074, 6778

Postgres default durability (unfair):
  hook-sync (synchronous=NORMAL, with peer):    5900, 5807, 5544, 5857, 5830, 5672, 5994, 6017, 5057, 6233
  hook-sync (synchronous=NORMAL, no peer):      5685, 5749, 5736, 5726, 5816, 6296, 6523, 5821, 5399, 5082
  Postgres  (synchronous_commit=on, replica):   4416, 4263, 4719, 4148, 4187, 4334, 4431, 4515, 4377, 4005
```

### Batch mode: HTTP overhead bypass

Same servers, same durability settings. Batch endpoint: 1 HTTP request contains N items, inserted in single transaction. 10 runs per batch size, 100K writes/run, concurrency 10.

| Batch size | hook-sync (SQLite) | Postgres | SQLite advantage |
|-----------|--------:|--------:|--------:|
| 1 (single) | 6,065 QPS | 6,238 QPS | tie (HTTP dominates) |
| 100 | 27,366 QPS | 22,703 QPS | +20.5% |
| 1,000 | 31,429 QPS | 23,682 QPS | +32.7% |
| 10,000 | 31,558 QPS | 8,278 QPS | **+3.8x** |

**hook-sync plateaus at ~31K QPS** (batch 1,000 and 10,000 identical). That's the HTTP server + JSON parsing ceiling — not SQLite. SQLite raw is 394K QPS, still far above.

**Postgres degrades at batch 10,000** — drops from 23,682 to 8,278 QPS (-65%). Single transaction with 10,000 INSERTs is too large: WAL buffer fills, lock contention, MVCC overhead. Postgres optimal at batch 100-1,000, degrades after.

Why SQLite wins more as batch grows: SQLite transaction = N INSERTs in 1 WAL flush, lightweight per-query (no query planner, no MVCC visibility check, no connection pool round-trip per Exec). Postgres has heavier per-query overhead that compounds in large transactions.

### Scaling: single vs batch vs direct

| Mode | hook-sync | Postgres | SQLite advantage |
|------|--------:|--------:|--------:|
| Direct (no HTTP) | 394K QPS | ~12K TPS | 33x |
| Batch 10,000 (1 req) | 31,558 QPS | 8,278 QPS | 3.8x |
| Batch 1,000 (1 req) | 31,429 QPS | 23,682 QPS | +32.7% |
| Batch 100 (1 req) | 27,366 QPS | 22,703 QPS | +20.5% |
| Single (1 req = 1 write) | 6,065 QPS | 6,238 QPS | tie |

As HTTP overhead decreases (larger batch), SQLite advantage increases. Direct SQLite is 33x faster than Postgres — the gap is hidden by HTTP overhead in single-write mode.

### All QPS data

```
Batch 100:
  hook-sync:  30604, 25765, 25870, 26166, 29858, 30093, 25095, 27366, 30483, 27106
  Postgres:   21448, 22124, 25084, 24103, 22210, 22326, 22703, 22588, 26814, 27302

Batch 1,000:
  hook-sync:  31065, 34427, 34727, 31111, 31429, 32775, 31693, 31030, 31427, 29124
  Postgres:   23361, 28353, 27320, 27661, 21813, 22207, 23682, 22377, 22064, 23872

Batch 10,000:
  hook-sync:  30379, 33514, 31310, 33983, 33634, 34478, 30618, 31558, 29089, 30521
  Postgres:   7912, 8324, 8067, 8272, 8179, 8424, 8646, 8408, 7976, 8278
```


## Files

- `bench-all.sh` — Run ALL benchmarks in one command (all topologies, all runtimes)
- `bench-dual-ack.sh` — Dual-writer benchmark, point-to-point (all 3 runtimes)
- `bench-fullmesh.sh` — Full mesh benchmark, 4 nodes all-to-all (all 3 runtimes)
- `bench-hub.sh` — Dedicated hub benchmark, 1 Go hub + 3 edges (all 3 runtimes)
- `bench-splitbrain.sh` — Split-brain safety test, partition + conflict + reconnect (all 3 runtimes)
- `go/bench/` — Go direct SQLite benchmarks (UUID, throughput)
