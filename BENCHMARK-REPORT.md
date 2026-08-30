# Benchmark Report: hook-sync

**Date:** 2026-08-30
**Stack:** Go 1.26 + Fiber + mattn/go-sqlite3, Node 22 + hyper-express + better-sqlite3, Bun 1.4 + Bun.serve + bun:sqlite
**Environment:** Mac M4, 2 nodes per runtime on localhost, 50ms batch interval
**Methodology:** Each runtime tested independently. All processes stopped before each run. No cross-runtime traffic during testing.

---

## Dual-Writer Throughput (ACK-based, latest)

10 runs × 100 concurrent requests per node (200 total per run). Both nodes receive writes simultaneously. Integrity checked after each run.

| Runtime | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 15,116 | 3,698 | 18,366 | 10/10 PASS |
| Node | 11,496 | 5,251 | 14,416 | 10/10 PASS |
| Bun | 4,904 | 1,947 | 14,888 | 10/10 PASS |

Integrity criteria: both nodes have equal item count, 0 pending changes, 0 dead letter after sync settles.

Run with: `bash bench-dual-ack.sh`

### Localhost variance warning

HTTP throughput on localhost varies 3-8x across runs with identical code and conditions. This is a characteristic of localhost HTTP benchmarking with small request counts — not a real performance difference between runtimes. Median is the reliable metric. For definitive runtime comparison, use real network (RTT dominates, variance drops to 1.12x).

---

## Direct SQLite (10K writes, no HTTP)

Pure SQLite write speed with triggers firing. No HTTP, no network, no event loop overhead. This is the reliable benchmark.

| Runtime | Sequential | Transaction | Triggers fired |
|---------|--------:|--------:|---:|
| Go | 255K QPS | 373K QPS | 10,000 ✅ |
| Node | 307K QPS | 354K QPS | 10,000 ✅ |
| Bun | 339K QPS | **394K QPS** | 10,000 ✅ |

bun:sqlite is fastest in raw SQLite — even with trigger overhead (1 extra INSERT per change), it beats Go and Node in transaction mode.

### Trigger overhead (measured separately, Go)

| Mode | Without triggers | With triggers | Overhead |
|------|--------:|--------:|--------:|
| Sequential | 466K QPS | 255K QPS | -45% |
| Transaction | 1,247K QPS | 373K QPS | -70% |

Trigger overhead is significant in direct SQLite. In HTTP benchmarks, HTTP overhead dominates — trigger effect is not measurable through HTTP.

---

## Sync Does Not Block Writes

Having a peer does not slow down writes. Sync runs in a separate timer + HTTP POST. Write path = SQLite INSERT + trigger capture only.

200 concurrent HTTP writes, 5 runs, median:

| Runtime | Single (no peer) | With peer | Difference |
|---------|--------:|--------:|--------:|
| Go | 12,668 QPS | 13,436 QPS | +6% (noise) |
| Bun | 8,099 QPS | 8,578 QPS | +6% (noise) |
| Node | 14,764 QPS | 14,701 QPS | -0.4% (noise) |

---

## Sync Delay = Batch Interval

| Interval | Sync p50 | Burst sync (100 writes) | Use case |
|----------|--------:|--------:|----------|
| **10ms** | **12ms** | 20ms | Local/LAN |
| 50ms (default) | 52ms | 25ms | Remote/WAN |
| 100ms | 100ms | 22ms | Conservative |

Burst sync is constant ~20-25ms regardless of interval — batch threshold (100 changes) triggers immediate ship, bypassing the ticker.

---

## Real Network (2 VPS, Go)

2 VPS (OVH + underconst), 4.5ms RTT between servers, 38-40ms RTT from Mac. 5 runs, median.

| Metric | Single server | Dual server | Difference |
|--------|--------:|--------:|--------:|
| Write latency p50 | 35.21ms | 35.09ms | -0.3% (noise) |
| Read latency p50 | 37.41ms | 37.27ms | -0.4% (noise) |
| Write throughput | 1,168 QPS | 1,140 QPS | -2.4% (noise) |
| Sync delay p50 (A→B) | — | 135ms | — |
| Integrity | — | 750 = 750 ✅ | — |

**Dual server = replica gratis.** Write speed identical to single server. Pay sync delay (135ms), get live replica. Real network variance: 1.12x (vs 10x on localhost) — RTT dominates, stabilizes benchmark.

Sync delay breakdown: batch 50ms + Mac→VPS-A 38ms (write) + VPS-A→VPS-B 4.5ms (sync) + Mac→VPS-B 40ms (poll) ≈ 132ms.

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

Tested with Go, peer unreachable (port 9999 nothing listening):

1. Write 5 items to node A
2. Ship attempts fail: retry 1 (50ms) → retry 2 (100ms) → retry 3 (200ms) → retry 4 (400ms) → retry 5 (800ms)
3. After 5 failures: 5 rows in `_dead_letter`, 0 in `_changes`
4. `/health` reports `dead_letter: 5, pending_changes: 0`

**Result: PASS.** Failed ships move to `_dead_letter` table for manual review. Pending changes cleared.

---

## What This Benchmark Does NOT Tell You

1. **HTTP server comparison is unreliable on localhost** — 3-8x variance. Need remote server + 1000+ requests for definitive comparison.
2. **No sustained load test** — benchmarks use 100-200 request bursts, not continuous load over minutes.
3. **Single table only** — all benchmarks use `items` table. Multi-table performance not tested.
4. **Bun/Node on real network** — real network test only done with Go. Bun/Node use same sync architecture, but not verified on remote servers.

---

## Files

- `bench-dual-ack.sh` — Dual-writer benchmark (all 3 runtimes, ACK-based)
- `bench-hsync.js` — HTTP benchmark client (latency, throughput, sync delay)
- `bench-interval.js` — Batch interval optimization
- `bench-trigger-overhead.ts` — Trigger overhead measurement
- `go/bench/` — Go direct SQLite benchmarks (UUID, throughput)
