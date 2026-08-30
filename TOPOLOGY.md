# Topology Recommendations

hook-sync currently supports **point-to-point only** (2 nodes, one `--peer` per node). This document covers what works today and what would need to change for larger fleets.

## Current State

Each node has one `--peer` URL. Both nodes ship and receive from each other.

```
Node A ←→ Node B
```

Tested and verified across all runtime pairs (Go, Bun, Node).

## The Simplest Multi-Node: Full Mesh

**Every node knows every other node. Each node ships changes to all peers directly.**

```
    A ←→ B
    ↕     ↕
    C ←→ D
```

No multi-hop. No relay. No hub. Each change goes directly from writer to all other nodes in 1 hop.

**Why this works with current protocol:** INSERT OR REPLACE with UUID PK is idempotent. If A ships to B and C, and B also ships to C, C receives the same change twice — safe, no duplicates. The `syncing` flag is not a problem because each node receives changes directly from the writer, not through intermediaries.

**What to build:** change `--peer` from single string to repeated flag:

```bash
./hook-sync-go -id nodeA -db a.db -listen :9001 \
  -peer http://localhost:9002 \
  -peer http://localhost:9003 \
  -peer http://localhost:9004
```

Ship to all peers. Each peer deduplicates via INSERT OR REPLACE. That's it.

**Bandwidth scaling:** each change sent to N-1 peers. Connections = N*(N-1)/2.

| Nodes | Connections | Each change sent | Bandwidth |
|-------|------------|-----------------|-----------|
| 2 | 1 | 1x | Fine |
| 3 | 3 | 2x | Fine |
| 5 | 10 | 4x | Fine |
| 10 | 45 | 9x | Getting heavy |
| 20 | 190 | 19x | Too much |

**Full mesh is the right choice up to ~5-7 nodes.** Beyond that, bandwidth and connection count make it impractical.

## When Full Mesh Doesn't Scale: Star + Relay

For 8+ nodes, use a hub. Edge nodes only connect to hub. Hub forwards changes between edges.

```
  edge1 ──→ hub ←── edge2
            ↑
         edge3, edge4, ...
```

**Why this needs new code:** edge1 writes, ships to hub. Hub applies. But hub's `syncing` flag suppresses triggers — hub does NOT re-capture and re-ship to edge2. Edge2 never sees edge1's changes.

**Fix: relay mode.** Hub receives changes from edge1, applies locally, AND forwards the raw changes to edge2, edge3, etc. The `syncing` flag still prevents trigger re-capture, but hub explicitly forwards received changes to other peers.

```
edge1 → hub (apply + forward) → edge2, edge3, edge4
```

**Trade-off:** hub is single point of failure. Mitigate: hub ←→ hub-backup (point-to-point, already works).

## Multi-Region: Hierarchical

Regional hubs in full mesh. Edge nodes star to regional hub.

```
  edge1 ──→ US hub ←── edge2
              ↕
  edge3 ──→ EU hub ←── edge4
```

US hub and EU hub in full mesh (2 peers each). Edge nodes peer to regional hub only.

## Topology Comparison

| Topology | Connections | Hops | Redundancy | SPOF | Best for |
|----------|------------|------|------------|------|----------|
| Point-to-point | 1 | 1 | None | No | 2 nodes (current) |
| Full mesh | N*(N-1)/2 | 1 | N-1 paths | No | 3-7 nodes |
| Star + relay | N-1 | 2 max | None | Hub | 8+ nodes |
| Ring | N | N/2 avg | 1 path | No | Not recommended |
| Chain | N-1 | N max | None | No | Not recommended |

## Implementation Priority

1. **Multi-peer support** (`--peer` repeated flag) — enables full mesh for 3-7 nodes. Smallest change, highest value. Just loop over peers in ship function.
2. **Relay mode** — enables star for 8+ nodes. Hub forwards received changes to other peers. Medium complexity.
3. **Watermark-based pull** — nodes ask "give me changes after X" instead of push. For unreliable networks / eventual consistency at scale. Highest complexity, defer until needed.

## What NOT to Do

- **Don't build topology management into the protocol.** Keep protocol simple (ship changes, ACK). Topology is a node-config concern.
- **Don't add a coordinator.** UUID PKs eliminate the need for central ID assignment. Keep it decentralized.
- **Don't add conflict resolution.** INSERT OR REPLACE with UUID PK = no conflicts. No CRDT, no vector clocks.
- **Don't build ring or chain topologies.** They add latency and fragility with no benefit over full mesh (small N) or star (large N).
