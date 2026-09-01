---
title: Dedicated Hub
description: Star topology with a Go-only relay hub for 8+ nodes. Pebble KV backup, durable forwarding queue, crash recovery.
---

import { Aside } from '@astrojs/starlight/components';

# Dedicated Hub (Star)

For 8+ nodes, full mesh generates too much traffic per node. Use a **dedicated hub** — a Go-only relay binary that forwards changes between edges.

```
edge1 ──POST /sync──→ hub ──POST /sync──→ edge2
edge3 ──POST /sync──→ hub ──POST /sync──→ edge4
```

## When to Use

- 8+ nodes (full mesh scaling limit exceeded)
- Regional relay (edges in different datacenters, hub in central location)
- Mixed runtime cluster (Go hub + Bun/Node edges)

## Why Dedicated Hub (Not Dual-Purpose)

A dual-purpose hub (serves `/api/items` AND relays) has a problem: the `syncing` flag suppresses triggers during apply, which also blocks forwarding. A dedicated hub has **no triggers at all** — all data enters via `/sync`, not local writes.

| | Dual-purpose hub | Dedicated hub |
|---|---|---|
| Serves `/api/items`? | Yes | No |
| Has triggers? | Yes | No |
| `syncing` flag problem? | Yes — blocks forward | No — no triggers |
| Client traffic? | Yes — competes with relay | No — pure relay |

### 0. Install the hub binary

No Go toolchain needed. Download pre-built binary from [GitHub Releases](https://github.com/maulanashalihin/hook-sync/releases):

```bash
# Linux amd64
curl -L https://github.com/maulanashalihin/hook-sync/releases/download/v0.1.0/hook-sync-hub-linux-amd64.tar.gz | tar xz
chmod +x hook-sync-hub-linux-amd64

# macOS Apple Silicon
curl -L https://github.com/maulanashalihin/hook-sync/releases/download/v0.1.0/hook-sync-hub-darwin-arm64.tar.gz | tar xz
chmod +x hook-sync-hub-darwin-arm64
```

Available: `linux-amd64`, `linux-arm64`, `darwin-amd64` (Intel), `darwin-arm64` (Apple Silicon).

### Run hub with Docker

```bash
docker build -t hook-sync-hub -f Dockerfile.hub .

docker run -d --name hub1 -p 9010:9010 \
  -v hub1-data:/data \
  hook-sync-hub \
  -id hub1 -listen :9010 -db /data/hub.pebble \
  -edge http://edge1:9001 \
  -edge http://edge2:9002 \
  -edge http://edge3:9003
```

Pebble data persists in the `hub1-data` volume. Hub is pure Go + Pebble — no CGO, image is ~15MB.

### 1. Run the hub (build from source)


```bash
cd go && go build -o ../hook-sync-hub ./cmd/hub

./hook-sync-hub -id hub1 -listen :9010 -db hub1.pebble \
  -edge http://localhost:9001 \
  -edge http://localhost:9002 \
  -edge http://localhost:9003
```

The hub is a pure relay — no SQLite, no triggers, no `/api/items`. It stores a backup in Pebble KV and forwards changes to all edges.

### 2. Point edge nodes to the hub

From any runtime, the hub is just a peer URL:

<Tabs>
<TabItem label="Go">

```bash
./hook-sync-mesh-go -id edge1 -db e1.db -listen :9001 \
  -peer http://localhost:9010
```

</TabItem>
<TabItem label="Bun / Node">

```ts
const mgr = attach(db, {
  id: "edge1",
  peers: ["http://localhost:9010"],  // hub URL
  batchMs: 50,
}, ["items"]);
```

</TabItem>
</Tabs>

Edges don't know it's a hub — it's transparent. No API changes.

## What the Hub Does

1. **Receive `/sync`** — accept changes from any edge
2. **Apply to Pebble** — `Set("data:{id}", rowJSON)` for INSERT/UPDATE, `Delete` for DELETE (backup copy)
3. **Enqueue durable forwards** — `Set("fwd:{n}", {batchID, changes, edgeURL})` in Pebble **before** ACK
4. **ACK edge immediately** — edge deletes from its `_changes`
5. **Forward asynchronously** — try immediate forward, background sweep retries with backoff

## Durable Forwarding Queue (Pebble)

Hub ACKs edge immediately on receive, then forwards to other edges asynchronously. If hub crashes after ACK but before forward, changes survive in Pebble's durable forwarding queue → replay on restart.

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

<Aside type="note" title="Why Pebble?">
Pebble (cockroachdb/pebble) is an LSM tree = write-optimized. Hub workload is ~100% write ingest (no client reads, no `/api/items`). Pebble is Go-native, RocksDB-compatible, CockroachDB's production engine.
</Aside>

## Crash Recovery

Verified: kill hub mid-traffic → write 5 items while hub down → restart hub → all edges converge to equal count, 0 pending, 0 dead letter.

## Hub Failure

Hub is a single point of failure. Mitigate: run hub ←→ hub-backup (point-to-point, already works). If hub dies, hub-backup takes over. Edges reconnect to hub-backup.

## Benchmark

1 Go hub + 3 edge nodes. 5 runs × 50 req per edge (150 total per run):

| Runtime (edges) | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 16,268 | 5,706 | 17,516 | 5/5 PASS |
| Bun | 19,750 | 12,931 | 27,683 | 5/5 PASS |
| Node | 13,193 | 148 | 21,660 | 5/5 PASS |

Integrity: all 3 edges have equal item count, hub backup count matches edges, 0 pending changes, 0 pending forwards, 0 dead letter.

Run with: `bash bench-hub.sh`
