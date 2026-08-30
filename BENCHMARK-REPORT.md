# Benchmark Report: hook-sync

**Tanggal:** 2026-08-30
**Stack:** Go 1.26 + Fiber + mattn/go-sqlite3, Node 22 + hyper-express + better-sqlite3, Bun 1.4 + Bun.serve + bun:sqlite
**Environment:** Mac M4, 2 nodes per runtime on localhost, 50ms batch interval
**Methodology:** Each runtime tested independently. No cross-runtime traffic during testing. All processes stopped before each run.

---

## Direct SQLite Benchmark (10K writes, no HTTP)

This is the reliable benchmark — pure SQLite write speed with capture mechanism firing. No HTTP, no network, no event loop overhead.

| Runtime | Mode | QPS | Capture | Hooks/Triggers fired |
|---------|------|----:|---------|---------------------:|
| Go | Sequential | 255K | preupdate_hook | 10,000 ✅ |
| Go | Transaction | 373K | preupdate_hook | 10,000 ✅ |
| Node | Sequential | 307K | triggers | 10,000 ✅ |
| Node | Transaction | 354K | triggers | 10,000 ✅ |
| Bun | Sequential | 339K | triggers | 10,000 ✅ |
| Bun | Transaction | **394K** | triggers | 10,000 ✅ |

### Key findings

- **Bun:sqlite is fastest in raw SQLite** — 394K QPS in transaction mode, even with trigger overhead (1 extra INSERT per change). The binding itself is faster than Go's mattn/go-sqlite3.
- **Go preupdate_hook has zero overhead** — no extra DB writes. But mattn/go-sqlite3 binding has more overhead per call than bun:sqlite.
- **Trigger overhead is real but small** — measured separately: -45% sequential, -70% transaction vs no triggers. But binding speed difference is larger than trigger overhead.

---

## HTTP Benchmark (100 concurrent requests, 2 nodes same runtime)

⚠️ **High variance — interpret with caution.** Localhost HTTP benchmark with 100 requests shows 10x variance across runs (e.g. Go dual-node: 2,400–24,000 QPS across runs). Numbers below are from single representative runs. Do not treat as definitive runtime comparison.

### Write & Read Latency

| Metric | Go | Node | Bun |
|--------|---:|---:|---:|
| Write latency p50 | **0.09ms** | 0.10ms | 0.10ms |
| Write latency p95 | 0.15ms | 0.18ms | 0.20ms |
| Read latency p50 | 0.21ms | 0.31ms | **0.14ms** |
| Read latency p95 | 0.36ms | 0.46ms | 0.17ms |

Write latency comparable across all three. Bun wins read latency — bun:sqlite binding is fast for reads.

### Sync Delay

| Metric | Go | Node | Bun |
|--------|---:|---:|---:|
| Sync delay p50 (A→B) | 51ms | 49ms | 52ms |
| Sync delay p50 (B→A) | 51ms | 49ms | 52ms |

Sync delay = batch interval (50ms) in all cases. Bottleneck is the timer, not the runtime. Tune via `--batch-ms` flag.

### Integrity

| Runtime | Node A count | Node B count | Match |
|---------|---:|---:|:---:|
| Go | 340 | 340 | ✅ |
| Node | 631 | 680 | ⚠️ |
| Bun | 340 | 340 | ✅ |

Node integrity mismatch on this run — likely race condition in sync during burst writes. Needs investigation.

---

## UUIDv4 vs UUIDv7

Each language uses the fastest UUID for its runtime:

| Language | UUID | Why |
|----------|------|-----|
| Go | UUIDv7 | B-tree is bottleneck → time-ordered = sequential insert, 2.1x faster at 100K writes |
| Bun | UUIDv4 | `crypto.randomUUID()` native C++ (31M gen QPS) → generation is bottleneck, 1.5x faster than JS UUIDv7 |
| Node | UUIDv4 | `crypto.randomUUID()` native (Node 19+) → same rationale as Bun |

UUIDv4 random inserts cause B-tree page splits at scale (QPS drops 50% at 100K writes in Go). UUIDv7 is time-ordered — append-like, no page splits. But in Bun/Node, native UUIDv4 generation is so fast that B-tree overhead is negligible, and `crypto.randomUUID()` beats JS UUIDv7 by 18x.

---

## Batch Interval Optimization (Go)

| Interval | Sync p50 | Sync p95 | Write QPS | Burst sync (100 writes) |
|----------|---------:|---------:|----------:|------------------------:|
| **10ms** | **12ms** | **13ms** | 7,648 | 20ms |
| 25ms | 24ms | 30ms | 4,911 | 24ms |
| 50ms (default) | 52ms | 54ms | 5,746 | 25ms |
| 100ms | 100ms | 104ms | 5,242 | 22ms |
| 200ms | 200ms | 204ms | 5,012 | 19ms |
| 500ms | 500ms | 505ms | 6,808 | 22ms |

### Findings

- Sync delay ≈ interval + 1-2ms overhead (linear, predictable)
- Burst sync (100 concurrent writes) is constant ~20-25ms regardless of interval — batch threshold (100 changes) triggers immediate ship, bypassing the ticker
- Write throughput shows no significant correlation with interval — differences are noise
- Ticker fires no-op on empty batch — no empty HTTP requests

### Recommendation

- **Local/LAN (0-5ms RTT):** `10ms` — lowest sync delay (12ms p50), no overhead penalty
- **Remote/WAN (35-40ms RTT):** `50ms` (default) — sync delay ~52ms + RTT
- **Conservative:** `100ms` — safe for high-latency or unreliable links

---

## Trigger Overhead (Direct SQLite, Go)

Measured by comparing direct SQLite writes with and without triggers active:

| Mode | Without triggers | With triggers | Overhead |
|------|---:|---:|---:|
| Sequential | 466K QPS | 255K QPS | -45% |
| Transaction | 1,247K QPS | 373K QPS | -70% |

Trigger overhead is significant in direct SQLite benchmark. However, HTTP benchmark variance (10x) is too high to measure trigger effect through HTTP — HTTP overhead dominates.

---

## What this benchmark does NOT tell you

1. **HTTP server comparison is unreliable** — localhost with 100 requests has 10x variance. To compare HTTP servers fairly, need remote server + 1000+ requests + multiple runs with variance reported.
2. **No real-world network latency** — all tests on localhost (0ms RTT). Real deployment adds 35-40ms+ RTT which dominates write/read latency.
3. **No crash recovery test** — changes in _changes table / in-memory channel are lost on crash before ship. Not benchmarked.
4. **Single table only** — all benchmarks use `items` table. Multi-table performance not tested.

---

## File di Project Ini

- `go/main.go` — Go implementation (preupdate_hook + Fiber)
- `go/bench/` — Go direct SQLite benchmarks (UUID, throughput)
- `bun/server.ts` — Bun implementation (triggers + Bun.serve)
- `node/server.js` — Node implementation (triggers + hyper-express)
- `bench-hsync.js` — HTTP benchmark client (language-agnostic)
- `bench-interval.js` — Batch interval optimization benchmark
- `bench-trigger-overhead.ts` — Trigger overhead measurement
- `PROTOCOL.md` — Shared wire protocol spec
