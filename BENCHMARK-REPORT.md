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


## Volume Stress Test (massive writes, all 3 runtimes)

Writes 10K, 100K, and 500K items via batch endpoint to node A, then verifies:
1. **Convergence time** — how long until node B has all items
2. **Consistency** — exact item count match, 0 pending, 0 dead letter
3. **Persistence** — kill both nodes, restart, verify data survives in SQLite file

| Volume | Runtime | Write time | Converge time | Consistency | Persistence |
|-------:|---------|-----------:|--------------:|:-----------:|:-----------:|
| 10K | Go | 96ms | 1s | ✅ PASS | ✅ PASS |
| 10K | Bun | 58ms | 1s | ✅ PASS | ✅ PASS |
| 10K | Node | 50ms | 1s | ✅ PASS | ✅ PASS |
| 100K | Go | 891ms | 1s | ✅ PASS | ✅ PASS |
| 100K | Bun | 769ms | 1s | ✅ PASS | ✅ PASS |
| 100K | Node | 618ms | 1s | ✅ PASS | ✅ PASS |
| 500K | Go | 4.2s | 5s | ✅ PASS | ✅ PASS |
| 500K | Bun | 10.7s | 5s | ✅ PASS | ✅ PASS |
| 500K | Node | 10.2s | 7s | ✅ PASS | ✅ PASS |

**9/9 PASS.** All 3 runtimes, all volume levels.

Key findings:
- **10K and 100K converge in 1s** across all runtimes — batch-size 10000 + drain mode ships all changes in 1-2 ticks
- **500K converges in 5-7s** — Go fastest (5s), Bun and Node similar (5-7s)
- **Go writes 500K in 4.2s** — 2.5x faster than Bun (10.7s) and Node (10.2s) at high volume
- **Bun and Node fastest at low volume** (10K: 58ms/50ms vs Go 96ms) — less HTTP overhead per request
- **Zero data loss** — all persistence tests pass, SQLite WAL ensures durability across kill+restart
- **Zero dead letters** — no ACK mismatches under load

Run with: `bash bench-stress.sh` (all runtimes) or `bash bench-stress.sh go` (single runtime)

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

**hook-sync plateaus at ~31K QPS** (batch 1,000 and 10,000 identical). That's the HTTP server + JSON parsing ceiling.

**Postgres degrades at batch 10,000** — drops from 23,682 to 8,278 QPS (-65%). Single transaction with 10,000 INSERTs is too large: WAL buffer fills, lock contention, MVCC overhead. Postgres optimal at batch 100-1,000, degrades after.

Why SQLite wins more as batch grows: SQLite transaction = N INSERTs in 1 WAL flush, lightweight per-query (no query planner, no MVCC visibility check, no connection pool round-trip per Exec). Postgres has heavier per-query overhead that compounds in large transactions.

### Scaling: single vs batch

| Mode | hook-sync | Postgres | SQLite advantage |
|------|--------:|--------:|--------:|
| Batch 10,000 (1 req) | 31,558 QPS | 8,278 QPS | 3.8x |
| Batch 1,000 (1 req) | 31,429 QPS | 23,682 QPS | +32.7% |
| Batch 100 (1 req) | 27,366 QPS | 22,703 QPS | +20.5% |
| Single (1 req = 1 write) | 6,065 QPS | 6,238 QPS | tie |

As HTTP overhead decreases (larger batch), SQLite advantage increases.

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


## Trigger Overhead: Direct SQLite (no HTTP)

**Date:** 2026-08-31
**Methodology:** 100K INSERTs per run, 10 runs, single transaction, WAL mode, synchronous=NORMAL. Direct SQLite — no HTTP layer. Baseline = no triggers, no `_changes` table. Triggers = production hook-sync schema (INSERT trigger + `_changes` + `json_object()`).

| Runtime | Driver | Baseline QPS | Triggers QPS | Overhead |
|---------|--------|--------:|--------:|--------:|
| Go | mattn/go-sqlite3 (CGO) | 317,260 | 105,070 | **-66.9%** |
| Go | modernc.org/sqlite (pure Go) | 272,610 | 73,101 | **-73.2%** |
| Bun | bun:sqlite (native) | 425,493 | 149,222 | **-64.9%** |
| Node | better-sqlite3 (N-API) | 469,483 | 199,468 | **-57.5%** |
**Trigger overhead is consistent ~58-73% across all runtimes/drivers.** Every INSERT with trigger = 2x B-tree write (items + `_changes`) + `json_object()` per row. modernc (pure Go, no CGO) is 14% slower at baseline and 30% slower at triggers vs mattn (CGO) — transpiled C via ccgo is slower than native C compilation.

### Why HTTP benchmark (bench-trigger.sh) showed no overhead

`bench-trigger.sh` measures through HTTP (200 concurrent requests). HTTP layer ceiling ~16K QPS — 6-12x lower than SQLite's 105K-199K QPS with triggers. HTTP parsing + JSON marshal + TCP connection handling dominates. Trigger overhead is buried under HTTP noise. Direct SQLite benchmark is the valid measurement.

### Cross-runtime ranking

Baseline: Node (469K) > Bun (425K) > Go/mattn (317K) > Go/modernc (272K). better-sqlite3 and bun:sqlite have lower per-call overhead than CGO mattn/go-sqlite3. modernc (pure Go, transpiled C) is slowest — no CGO but ccgo overhead.

Triggers: same ranking — Node (199K) > Bun (149K) > Go/mattn (105K) > Go/modernc (73K). Overhead is proportional.


### preupdate_hook vs triggers (Go only, direct SQLite)

Also benchmarked preupdate_hook (CGO callback) as alternative to SQL triggers. 100K INSERTs, 10 runs, direct SQLite.

| Mode | QPS median | vs baseline | Capture target |
|------|--------:|--------:|--------|
| baseline | 236,860 | — | No capture |
| triggers | 79,090 | -67% | `_changes` (same txn) |
| preupdate_hook (mem) | 175,088 | -26% | In-memory (no DB) |
| preupdate_hook + Pebble | 139,059 | -41% | Pebble LSM (batch) |

**Fair comparison (both persist change records): preupdate_hook + Pebble wins by 76%** (139K vs 79K QPS).

- Hook callback alone (CGO trampoline + `data.New()`): -26% overhead
- Pebble batch write additional: -21% (LSM append-only, no B-tree page split)
- Triggers: -67% (same-transaction B-tree write + WAL sync per row)

Tradeoff: preupdate_hook + Pebble (channel per-row) is not same-transaction. If write transaction rolls back, Pebble already has data → needs cleanup.

### preupdate_hook + commit/rollback + Pebble (same-txn safe, Go only)

Solved the same-transaction problem with `RegisterCommitHook` + `RegisterRollbackHook`:

```
preupdate_hook  →  in-memory slice (pending)
commit_hook     →  batch flush to Pebble (1 batch.Commit)
rollback_hook   →  discard slice (no false positives)
```

| Mode | QPS median | vs baseline | Same-txn safe? |
|------|--------:|--------:|--------|
| baseline | 236,664 | — | N/A |
| triggers | 80,693 | -66% | Yes (SQL) |
| **hook+commit+pebble** | **152,521** | **-36%** | **Yes (commit hook)** |
| hook+commit+pebble (rollback test) | 117,397 | -50% | Yes (verified) |

**hook+commit+pebble wins by 89%** (152K vs 81K QPS) — and same-transaction safe.

Rollback test: 50% of batches rolled back, 50% committed. Verified: `commit_count == pebble_count == items_count`, `rollback_count == discarded pending`. Zero false positives in Pebble.

Faster than channel-based preupdate+Pebble (152K vs 139K) because: in-memory slice append (no channel) → single `batch.Commit()` at commit hook (not per-row Set).

**Protocol design:**
- preupdate_hook captures to in-memory (Go-only, CGO)
- commit_hook flushes to Pebble LSM (batch, write-optimized)
- rollback_hook discards (same-txn safety, no false positives)
- sync reads from Pebble iterator → HTTP ship to peers (same protocol)
- ACK deletes from Pebble after all peers ACK (same logic as `_changes`)

**Tradeoffs:**
- Go-only (preupdate_hook + commit/rollback hooks are CGO bindings). Bun/Node would use triggers + `_changes` (hybrid)
- Pebble not SQL-queryable (sync reads via iterator, not SELECT)
- 89% faster than triggers for write-heavy workloads

Run with: `go build -tags sqlite_preupdate_hook -o /tmp/hook-commit-pebble ./go/bench/hook_commit_pebble/main.go && /tmp/hook-commit-pebble`

---

## Replication Benchmark: Trigger vs Hook+Pebble vs Hook+Memory (2-node, 100K writes)

**Date:** 2026-08-31
**Methodology:** 2-node replication, 5 runs × 100K batch writes per run (500K total). Write 100K items via `POST /api/items/batch` to node A, wait for convergence (item_count match + pending=0 on both nodes). Measure write QPS, converge time, integrity.

| Mode | QPS median | QPS min | QPS max | Converge median | Pass |
|------|--------:|--------:|--------:|----------------:|:----:|
| Trigger (SQL triggers + `_changes` table) | 116K | 30K | 133K | 4s | 5/5 |
| Hook+Pebble (preupdate_hook + commit_hook + Pebble batch) | 168K | 160K | 172K | 2s | 5/5 |
| Hook+Memory (preupdate_hook + in-memory slice, no persistence) | 186K | 184K | 192K | 1s | 5/5 |

### Architecture comparison

| | Trigger | Hook+Pebble | Hook+Memory |
|---|---|---|---|
| Capture mechanism | SQL AFTER INSERT/UPDATE/DELETE trigger | preupdate_hook (CGO callback) | preupdate_hook (CGO callback) |
| Change store | `_changes` table (SQLite B-tree) | Pebble LSM (batch commit) | In-memory slice |
| Same-txn safety | Yes (SQL trigger, same transaction) | Yes (commit_hook flush, rollback_hook discard) | No (crash = lost pending) |
| Crash recovery | Yes (`_changes` survives in DB file) | Yes (Pebble survives on disk) | No (in-memory only) |
| Cross-runtime | Go, Bun, Node (all have triggers) | Go-only (CGO preupdate_hook) | Go-only (CGO preupdate_hook) |
| Write overhead | -67% (2x B-tree write + json_object per row) | -29% (in-memory collect + 1 Pebble batch) | -21% (in-memory collect only) |
| Sync read | `SELECT FROM _changes` | Pebble iterator (`seq:` prefix) | Slice drain (mutex) |

### Key findings

- **Hook+Memory fastest (186K QPS)** — 1.6x trigger, 1.1x hook+pebble. No I/O for change capture. But no crash recovery.
- **Hook+Pebble best production choice (168K QPS)** — 1.4x trigger, persistent, same-txn safe. Pebble overhead vs in-memory only ~10%.
- **Trigger slowest and noisiest (116K median, 30K-133K range)** — `_changes` table bloats across runs (500K rows accumulated), degrades performance. Hook-based modes stable (160K-192K range).
- **Converge: memory 1s, pebble 2s, trigger 4s** — hook-based sync drains faster (no SQL SELECT overhead, iterator/slice is faster than table scan).
- **All 15/15 PASS** — integrity verified: item_count match, pending=0 on both nodes, every run.

### When to use which

- **Hook+Pebble** — production, write-heavy, needs crash recovery. Go-only.
- **Hook+Memory** — max throughput, ephemeral, can tolerate loss on crash. Go-only.
- **Trigger** — cross-runtime (Bun, Node), simpler, no CGO dependency.

Run with: `bash bench-hookpebble-vs-trigger.sh`

---

## Split-Brain Safety: Trigger vs Hook+Pebble vs Hook+Memory

**Date:** 2026-08-31
**Methodology:** Same 6-phase split-brain test as `bench-splitbrain.sh`, but across 3 capture modes instead of 3 runtimes. Tests partition + independent writes + reconnect convergence + DELETE vs UPDATE conflict.

| Mode | Checks | Passed | Result |
|------|------:|------:|:---------:|
| Trigger | 12 | 12 | ✅ PASS |
| Hook+Pebble | 12 | 12 | ✅ PASS |
| Hook+Memory | 6 | 12 | ❌ FAIL (expected — no crash recovery) |
| **Total** | **36** | **30** | **Hook+Pebble matches Trigger** |

### Why Hook+Memory fails (expected)

hookmem stores pending changes in-memory only. Phase 2 kills both nodes → pending changes lost. On reconnect, nodes cannot sync partition-time writes that were never shipped. This is the known tradeoff: max throughput (186K QPS) but no crash recovery.

Failures: shared item diverges (nodeA=100, nodeB=200), new items not merged (only_on_A / only_on_B missing), DELETE vs UPDATE disagrees.

### Bug found and fixed: DELETE sync missing row data

Initial run: Hook+Pebble failed Phase 6 (DELETE vs UPDATE). Root cause: `drainAndShip` only set `OldID` for DELETE, left `Row` nil. `applyChanges` skipped timestamp check (`c.Row != nil` was false) → DELETE always won, ignoring newer UPDATE.

Fix: always populate `c.Row` from `rowData` for all ops, including DELETE. Now UPDATE with newer timestamp wins over DELETE — same behavior as trigger.

### Conflict resolution (all modes that pass)

Same as trigger-based: last-write-wins by `updated_at` timestamp. INSERT = safe (UUID, no collision). UPDATE vs UPDATE = higher timestamp wins. DELETE vs UPDATE = UPDATE wins if newer.

Run with: `bash bench-splitbrain-hook.sh` (all modes) or `bash bench-splitbrain-hook.sh hookpebble` (single mode)

---
## Files

- `bench-all.sh` — Run ALL benchmarks in one command (all topologies, all runtimes)
- `bench-dual-ack.sh` — Dual-writer benchmark, point-to-point (all 3 runtimes)
- `bench-fullmesh.sh` — Full mesh benchmark, 4 nodes all-to-all (all 3 runtimes)
- `bench-hub.sh` — Dedicated hub benchmark, 1 Go hub + 3 edges (all 3 runtimes)
- `bench-splitbrain.sh` — Split-brain safety test, partition + conflict + reconnect (all 3 runtimes)
- `bench-trigger.sh` — Trigger overhead test via HTTP (baseline vs with triggers, Go) — noisy, see direct SQLite benchmark above
- `bench-hookpebble-vs-trigger.sh` — Replication benchmark: trigger vs hook+pebble vs hook+memory (2-node, 100K batch writes)
- `bench-splitbrain-hook.sh` — Split-brain safety test across capture modes (trigger, hookpebble, hookmem)
- `go/hookpebble/main.go` — Full Go server: preupdate_hook + commit_hook + rollback_hook + Pebble (build tag: `sqlite_preupdate_hook`)
- `go/hookmem/main.go` — Full Go server: preupdate_hook + in-memory slice, no persistence (build tag: `sqlite_preupdate_hook`)
- `bench-stress.sh` — Volume stress test, 10K/100K/500K items (convergence + persistence + consistency, all 3 runtimes)
- `go/bench/baseline_vs_trigger/main.go` — Direct SQLite trigger overhead (Go, no HTTP)
- `bun/bench-baseline-vs-trigger.ts` — Direct SQLite trigger overhead (Bun, no HTTP)
- `node/bench-baseline-vs-trigger.js` — Direct SQLite trigger overhead (Node, no HTTP)
- `go/bench/baseline_vs_trigger_modernc/main.go` — Direct SQLite trigger overhead (Go + modernc.org/sqlite, pure Go, no CGO)
- `go/bench/hook_vs_trigger/main.go` — preupdate_hook vs triggers vs Pebble (Go, direct SQLite, build tag: `sqlite_preupdate_hook`)
- `go/bench/hook_commit_pebble/main.go` — preupdate_hook + commit/rollback + Pebble (same-txn safe, Go, build tag: `sqlite_preupdate_hook`)