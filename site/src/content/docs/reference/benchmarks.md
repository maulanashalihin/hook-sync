---
title: Benchmarks
description: Performance benchmarks across Go, Bun, and Node — throughput, sync overhead, convergence, crash recovery, and Postgres comparison.
---

# Benchmarks

**Date:** 2026-08-30
**Stack:** Go 1.26 + Fiber + mattn/go-sqlite3, Node 22 + hyper-express + better-sqlite3, Bun 1.4 + Bun.serve + bun:sqlite
**Environment:** Mac M4 (localhost), 2-4 nodes per runtime, 50ms batch interval, batch-size 10000

## Dual-Writer Throughput (Point-to-Point)

10 runs × 100 concurrent requests per node (200 total per run). Both nodes receive writes simultaneously.

| Runtime | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 9,149 | 3,705 | 15,063 | 10/10 PASS |
| Bun | 10,476 | 3,004 | 14,159 | 10/10 PASS |
| Node | 8,393 | 3,458 | 10,068 | 10/10 PASS |

Integrity: both nodes have equal item count, 0 pending changes, 0 dead letter after sync settles.

## Sync Does Not Block Writes

Having a peer does not slow down writes. Sync runs in a separate timer + HTTP POST.

| Runtime | Single (no peer) | With peer | Difference |
|---------|--------:|--------:|--------:|
| Go | 12,668 QPS | 13,436 QPS | +6% (noise) |
| Bun | 8,099 QPS | 8,578 QPS | +6% (noise) |
| Node | 14,764 QPS | 14,701 QPS | -0.4% (noise) |

## Sync Delay = Batch Interval

| Interval | Sync p50 | Burst sync (100 writes) | Use case |
|----------|--------:|--------:|----------|
| **10ms** | **12ms** | 20ms | Local/LAN |
| 50ms (default) | 52ms | 25ms | Remote/WAN |
| 100ms | 100ms | 22ms | Conservative |

## Full Mesh (4 nodes all-to-all)

| Runtime | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 14,853 | 6,108 | 23,570 | 5/5 PASS |
| Node | 14,414 | 3,165 | 18,487 | 5/5 PASS |
| Bun | 13,175 | 1,872 | 18,887 | 5/5 PASS |

## Dedicated Hub (1 Go hub + 3 edges)

| Runtime (edges) | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 16,268 | 5,706 | 17,516 | 5/5 PASS |
| Bun | 19,750 | 12,931 | 27,683 | 5/5 PASS |
| Node | 13,193 | 148 | 21,660 | 5/5 PASS |

## Convergence Speed

| Items | Before (fixed LIMIT 100) | After (batch 10K + drain) | Speedup |
|------:|--------:|--------:|--------:|
| 10K | ~6s | <1s | 6x |
| 100K | ~60s | ~2s | 30x |

## Volume Stress Test

Writes 10K, 100K, and 500K items via batch endpoint, then verifies convergence, consistency, and persistence (kill + restart).

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

**9/9 PASS.** Zero data loss, zero dead letters across all volumes and runtimes.

## Split-Brain Safety

36 checks across all 3 runtimes — 36/36 PASS. See [Split-Brain Safety](../split-brain/) for details.

## Real Network Test (OVH → 1TIM)

10,000 items in single batch, real network (2.7ms RTT, 289 Mbps):

| Metric | Result |
|--------|--------|
| Converge time | <1s |
| Data loss | 0 |
| Dead letter | 0 |

## Crash Recovery

Write 50 items → kill node A → verify 50 pending in `_changes` → restart → sync resumes → both nodes converge. **PASS.**

## Dead Letter Queue

Connection errors retry indefinitely — no data loss, no dead letter. Only ACK mismatch (protocol error) moves to `_dead_letter`. **PASS.**

## hook-sync vs Postgres (100K writes, real network)

Both configured with equivalent durability (no fsync per write):

| System | Durability | QPS median | Replica converge | Integrity |
|--------|------------|--------:|:----------------:|:---------:|
| hook-sync | `synchronous=NORMAL` | 6,065 | ~2s (batch 10K + drain) | 100K, 0 pending, 0 dead letter |
| Postgres | `synchronous_commit=off` | 6,238 | ~3s (WAL streaming) | 100K |

**At equivalent durability, throughput is tied** — Postgres +2.8% (within noise).

### Batch Mode: HTTP Overhead Bypass

| Batch size | hook-sync (SQLite) | Postgres | SQLite advantage |
|-----------|--------:|--------:|--------:|
| 1 (single) | 6,065 QPS | 6,238 QPS | tie (HTTP dominates) |
| 100 | 27,366 QPS | 22,703 QPS | +20.5% |
| 1,000 | 31,429 QPS | 23,682 QPS | +32.7% |
| 10,000 | 31,558 QPS | 8,278 QPS | **+3.8x** |

At batch 10K, SQLite's single-transaction insert bypasses HTTP overhead per row. Postgres degrades because pgx pool handles rows individually.

### Where Each System Wins

| Metric | hook-sync | Postgres | Winner |
|--------|----------|----------|--------|
| Write throughput (fair durability) | 6,065 | 6,238 | **Tie** |
| Sync overhead | ~0% (background) | WAL sender overhead | **hook-sync** |
| Replica lag | ~2s | ~3s | **hook-sync** |
| Cross-runtime | Go, Bun, Node | Go-only (pgx) | **hook-sync** |
| Topology | P2P, mesh, hub | Primary-replica only | **hook-sync** |
| Multi-writer | Yes (UUID, idempotent) | No (primary-only) | **hook-sync** |
| Operational complexity | Single binary + SQLite file | Cluster + replication config | **hook-sync** |

## Compression Analysis (NOT implemented)

Tested gzip on 290 Mbps link: CPU cost (20-47ms) exceeds transfer save (0.5-50ms). **Compression NOT worth it** on fast links. Would only help on slow links (<50 Mbps).
