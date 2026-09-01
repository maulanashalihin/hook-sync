---
title: Dedicated Hub
description: Star topology with a Go-only relay hub for 8+ nodes. Pebble KV backup, durable forwarding queue, crash recovery, pre-built binary + Docker.
---

import { Aside, Steps, Tabs, TabItem } from '@astrojs/starlight/components';

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
- Need a backup copy of all data without running a full edge node

## Why Dedicated Hub (Not Dual-Purpose)

A dual-purpose hub (serves `/api/items` AND relays) has a problem: the `syncing` flag suppresses triggers during apply, which also blocks forwarding. A dedicated hub has **no triggers at all** — all data enters via `/sync`, not local writes.

| | Dual-purpose hub | Dedicated hub |
|---|---|---|
| Serves `/api/items`? | Yes | No |
| Has triggers? | Yes | No |
| `syncing` flag problem? | Yes — blocks forward | No — no triggers |
| Client traffic? | Yes — competes with relay | No — pure relay |
| Backup? | SQLite file | Pebble KV (write-optimized) |
| Runtime | Go, Bun, Node | Go only (Pebble) |

## Install

Three ways to get the hub running. Pick one.

### Option 1: Pre-built binary (no Go toolchain needed)

Download from [GitHub Releases](https://github.com/maulanashalihin/hook-sync/releases):

```bash
# Linux amd64
curl -L https://github.com/maulanashalihin/hook-sync/releases/download/v0.1.0/hook-sync-hub-linux-amd64.tar.gz | tar xz
chmod +x hook-sync-hub-linux-amd64

# macOS Apple Silicon
curl -L https://github.com/maulanashalihin/hook-sync/releases/download/v0.1.0/hook-sync-hub-darwin-arm64.tar.gz | tar xz
chmod +x hook-sync-hub-darwin-arm64
```

Available platforms: `linux-amd64`, `linux-arm64`, `darwin-amd64` (Intel), `darwin-arm64` (Apple Silicon).

### Option 2: Docker

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

Pebble data persists in the `hub1-data` volume. Image is ~11MB (pure Go + Pebble, no CGO).

### Option 3: Build from source

```bash
git clone https://github.com/maulanashalihin/hook-sync.git
cd hook-sync/go
go build -o ../hook-sync-hub ./cmd/hub
```

Pure Go, no build tags, no CGO. Pebble is the only external dependency.

## Setup

### 1. Run the hub

```bash
./hook-sync-hub -id hub1 -listen :9010 -db hub1.pebble \
  -edge http://localhost:9001 \
  -edge http://localhost:9002 \
  -edge http://localhost:9003
```

The hub starts listening on `:9010`. It has no SQLite, no triggers, no `/api/items` — it's a pure relay + backup.

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

Edges don't know it's a hub — it's transparent. No edge code changes needed.

## CLI Flags

| Flag | Required | Default | Description |
|------|:--------:|:-------:|-------------|
| `-id` | ✅ | — | Hub identifier (e.g. `hub1`), sent as `X-Node-Id` header |
| `-listen` | ✅ | — | HTTP listen address (e.g. `:9010`) |
| `-db` | ✅ | — | Pebble DB path (e.g. `hub1.pebble`) |
| `-edge` | ✅ | — | Edge node URL (repeatable, e.g. `-edge http://localhost:9001 -edge http://localhost:9002`) |
| `-url` | ❌ | empty | This hub's full URL for multi-region loop prevention (e.g. `http://localhost:9010`). Only needed for hub-to-hub topology. |
| `-batch-ms` | ❌ | 50 | Forward sweep interval in milliseconds (background retry loop) |

## API

The hub exposes only two endpoints — no CRUD, no `/api/items`.

### POST /sync

Receive change batch from an edge or peer hub. Same wire protocol as edge nodes (see [Wire Protocol](/reference/protocol)).

**Request:**

```json
{
  "batch_id": 42,
  "changes": [
    { "op": "INSERT", "table": "items", "row": {"id": "abc", "name": "foo", "value": 42, "updated_at": 1700000000000}, "old_id": null }
  ]
}
```

**Response:**

```json
{ "applied": 1, "ack": 42 }
```

**Headers:**

| Header | From edge | From peer hub |
|--------|-----------|---------------|
| `X-Node-Id` | Edge's node ID | Hub's node ID |
| `X-Node-Url` | Empty (not sent) | Hub's URL (for loop prevention) |

**Pipeline (per request):**

1. `applyBackup` — persist row data to Pebble `data:{id}`
2. `enqueueForward` — persist `fwd:{n}` for each edge (except sender) **before ACK**
3. ACK sender immediately — edge deletes from its `_changes`
4. `tryForwardAll` — async, best-effort immediate forward

### GET /health

Hub status for monitoring and benchmarks.

**Response:**

```json
{
  "ok": true,
  "node_id": "hub1",
  "hub": true,
  "backup_items": 1500,
  "pending_forwards": 0,
  "edges": ["http://localhost:9001", "http://localhost:9002", "http://localhost:9003"]
}
```

| Field | Description |
|-------|-------------|
| `backup_items` | Count of `data:{id}` keys in Pebble (total rows in backup) |
| `pending_forwards` | Count of `fwd:{n}` keys (forwards waiting to be delivered) |
| `edges` | List of configured edge URLs |

## What the Hub Does

The hub does exactly two things: **backup** and **forward**.

### 1. Backup

Every received change is stored in Pebble KV under `data:{id}`:

- **INSERT / UPDATE**: `Set("data:{id}", rowJSON)` — overwrites previous value
- **DELETE**: `Delete("data:{id}")` — removes from backup

This is the hub's full backup copy of all data. Not queryable via SQL, but scannable by key prefix (`data:` prefix → all rows). Uses Pebble batch for atomicity — all changes in one commit.

### 2. Forward

Every received change is relayed to all other edges (except the sender):

- Hub ACKs the sender **immediately** — edge deletes from its `_changes`
- Forwarding is **asynchronous** — hub tries immediate forward, background sweep retries with backoff
- If hub crashes after ACK but before forward, the forwarding entry survives in Pebble → replay on restart

## Pebble Data Structure

The hub uses Pebble KV (LSM tree, write-optimized) with two key namespaces:

| Key pattern | Value | Purpose |
|-------------|-------|---------|
| `data:{id}` | Row JSON | Backup copy of all data. INSERT/UPDATE sets, DELETE removes. |
| `fwd:{n}` | `fwdEntry` JSON | Pending forward entry. One per edge per batch. Deleted on successful ACK. |

`fwdEntry` format:

```json
{
  "batch_id": 42,
  "changes": [...],
  "edge_url": "http://localhost:9002"
}
```

<Aside type="note" title="Why Pebble?">
Pebble (cockroachdb/pebble) is an LSM tree = write-optimized. Hub workload is ~100% write ingest (no client reads, no `/api/items`). Pebble is Go-native, RocksDB-compatible, CockroachDB's production engine.
</Aside>

## Durable Forwarding Queue

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

### Forward retry logic

- **Immediate**: `tryForwardAll()` runs async after each `/sync` receive — best-effort low latency
- **Background sweep**: `forwardSweep()` runs every `batchMs` (default 50ms) — retries all pending `fwd:` entries with exponential backoff (50/100/200/400/800ms, 5 attempts per tick)
- **Never dropped**: entries that exhaust all 5 backoff attempts stay in Pebble for the next tick — retried indefinitely until the edge ACKs

## Crash Recovery

Verified: kill hub mid-traffic → write 5 items while hub down → restart hub → all edges converge to equal count, 0 pending, 0 dead letter.

**On restart:**

1. `replayPending()` — counts pending `fwd:` entries, logs count
2. `forwardSweep()` — picks up all pending entries on next tick, retries
3. No special replay logic needed — entries are already in Pebble, forwardSweep already iterates them

## Hub Failure

Hub is a single point of failure. Mitigations:

| Strategy | How | Status |
|----------|-----|--------|
| Hub-backup | Run hub ←→ hub-backup (point-to-point). If hub dies, hub-backup takes over. Edges reconnect to hub-backup. | ✅ Works (same binary, different `-id`) |
| Multi-region | Each region has own hub. Hubs peer directly. If one region's hub dies, other regions still sync. | ✅ Works (see [Multi-Region](/topologies/multi-region)) |
| Automatic failover | Hub-backup promotion without manual switch | ❌ Not built yet (Phase 7) |

## Benchmark

1 Go hub + 3 edge nodes. 5 runs × 50 req per edge (150 total per run):

| Runtime (edges) | QPS median | QPS min | QPS max | Integrity |
|---------|--------:|--------:|--------:|:---------:|
| Go | 16,268 | 5,706 | 17,516 | 5/5 PASS |
| Bun | 19,750 | 12,931 | 27,683 | 5/5 PASS |
| Node | 13,193 | 148 | 21,660 | 5/5 PASS |

Integrity: all 3 edges have equal item count, hub backup count matches edges, 0 pending changes, 0 pending forwards, 0 dead letter.

Run with: `bash bench-hub.sh`
