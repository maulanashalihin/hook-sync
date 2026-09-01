---
title: Multi-Region
description: Hub-to-hub topology for cross-region sync. Loop prevention via X-Node-Url header.
---

# Multi-Region (Hub-to-Hub)

Each region has its own hub + edges. Hubs peer directly via `-edge` flag. Loop prevention via `X-Node-Url` header — hub skips forwarding back to the sender's URL.

```
  Region 1                         Region 2
  edge1 ──→ hub A ←── edge2        edge3 ──→ hub B ←── edge4
              ←────────────────→
                 hub-to-hub
```

## When to Use

- Cross-region sync (e.g., Singapore + US-East)
- Each region has multiple edge nodes
- Low-latency local writes, eventual consistency cross-region

## Setup

```bash
# Region 1 hub
./hook-sync-hub -id hubA -listen :9100 -url http://localhost:9100 -db hubA.pebble \
  -edge http://localhost:9101 \
  -edge http://localhost:9102 \
  -edge http://localhost:9200   # hub B (peer hub)

./hook-sync-mesh-go -id edge1 -db e1.db -listen :9101 -peer http://localhost:9100
./hook-sync-mesh-go -id edge2 -db e2.db -listen :9102 -peer http://localhost:9100

# Region 2 hub
./hook-sync-hub -id hubB -listen :9200 -url http://localhost:9200 -db hubB.pebble \
  -edge http://localhost:9201 \
  -edge http://localhost:9202 \
  -edge http://localhost:9100   # hub A (peer hub)

./hook-sync-mesh-go -id edge3 -db e3.db -listen :9201 -peer http://localhost:9200
./hook-sync-mesh-go -id edge4 -db e4.db -listen :9202 -peer http://localhost:9200
```

Edges in each region point to their local hub only. Hubs relay cross-region.

## Loop Prevention

Flow: `edge1` writes → `hub A` receives → forwards to `edge2` + `hub B` (with `X-Node-Url: hubA`) → `hub B` sees `X-Node-Url: hubA`, skips forwarding back to hub A → forwards to `edge3`, `edge4`. No loop.

**How it works:**

- Hub sends `X-Node-Url` header (its own URL, set via `--url` flag) with every forward
- Receiving hub reads `X-Node-Url` and skips the edge that matches — no forward back to sender
- Edge nodes don't send `X-Node-Url` (empty header), so hubs forward to all edges normally

No bridge nodes needed. Hubs peer directly. The `X-Node-Url` header is the only loop prevention mechanism — simple, no protocol changes, no origin tracking.

## Benchmark

Tested with `bash bench-multi-region.sh` — 5/5 PASS:

1. Cross-region convergence (edge1 write → edge3 receives)
2. Bidirectional (edge3 write → edge1 receives)
3. Persistence (kill all, restart, verify data)
4. Hub down + reconnect (hub A killed, restarted, convergence resumes)
5. Loop check (no infinite forwarding between hubs)

## Characteristics

| Property | Value |
|----------|-------|
| Connections | N-1 per region + hub-to-hub |
| Hops | 3-4 max (edge → hub → hub → edge) |
| Redundancy | Hub (mitigate with hub-backup) |
| SPOF | Hub |
| Cross-region latency | Async, batch-interval (50ms) + network RTT |
