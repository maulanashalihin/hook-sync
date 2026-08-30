# Topology Recommendations

hook-sync currently supports **point-to-point only** (2 nodes). This document covers what works today, what doesn't, and what would need to change for each topology.

## Current State

Each node has one `--peer` URL. The connection is bidirectional — both nodes ship and receive from each other.

```
Node A ←→ Node B
```

This works. Tested and verified across all runtime pairs (Go, Bun, Node).

## Why Multi-Hop Doesn't Work Today

The `syncing` flag prevents infinite loops — when a node applies received changes, triggers are suppressed so changes aren't re-captured and re-shipped.

**Side effect:** changes don't propagate beyond 1 hop.

```
A writes → A ships to B → B applies (syncing=1, triggers suppressed)
                              ↓
                    B does NOT re-ship to C
                              ↓
                    C never sees A's changes
```

Example: 3-node ring A→B→C→A. A writes, ships to B. B applies, but B's triggers don't fire (syncing=1). B ships its own local changes to C, but NOT A's changes. C never gets A's data. **Ring, chain, and star topologies are broken with current code.**

## What Would Need to Change

To support multi-hop, a node must **forward received changes** without re-applying them. Two approaches:

### Option 1: Relay mode

Node receives changes from peer A, applies them locally, AND forwards them to peer B. The `syncing` flag still prevents local trigger re-capture, but the node explicitly forwards the raw received changes to its other peer.

```
A → B (apply + forward) → C (apply)
```

**Cost:** each node needs to track multiple peers and forward logic. Changes travel N hops with N× latency.

**Risk:** duplicate delivery. If A→B→C and A→C both exist (mesh), C receives the same change twice. INSERT OR REPLACE makes this safe (idempotent), but wastes bandwidth.

### Option 2: Change log with watermark

Each node tracks the highest `change_id` it has received from each peer. Instead of forwarding, nodes exchange their `_changes` table directly. Peer asks "give me everything after change_id X".

```
A: _changes has [1..100]
B: asks A "give me after 50" → A sends [51..100]
C: asks B "give me after 30" → B sends [31..100] (including A's changes that B applied)
```

**Cost:** requires protocol change from push to pull, or hybrid. More complex but handles mesh topology cleanly.

**Risk:** `_changes` table grows unbounded if peers are offline. Need compaction strategy.

## Topology Comparison

Assuming multi-hop support is added:

| Topology | Connections | Latency | Redundancy | Node failure | Best for |
|----------|------------|---------|------------|-------------|----------|
| Point-to-point | 1 | 1 hop | None | Other node loses sync | 2-node backup |
| Star | N-1 | 2 hops max | None (hub = SPOF) | Hub down = all disconnected | Edge → cloud |
| Ring | N | N/2 hops avg | 1 path | Breaks ring, needs reconnect | Small fleet, low overhead |
| Full mesh | N*(N-1)/2 | 1 hop | N-1 paths | Others still connected | Small fleet (≤5), max redundancy |
| Line/chain | N-1 | N hops max | None | Splits chain | Not recommended |

## Recommendations

### 2 nodes (current): point-to-point

Already works. No changes needed.

```
Mac/edge server ←→ VPS backup
```

Use case: live replica for failover. Write to primary, sync to backup. If primary dies, backup has full data.

### 3-5 nodes: full mesh (needs multi-peer support)

Every node peers with every other. 1-hop latency, max redundancy. INSERT OR REPLACE handles duplicate delivery safely.

```
    A ←→ B
    ↕  ×  ↕
    C ←→ D
```

**What to build:** change `--peer` from single string to repeated flag (`--peer URL1 --peer URL2`). Ship to all peers. Each peer deduplicates via INSERT OR REPLACE.

**Connection count:** 3 nodes = 3, 4 nodes = 6, 5 nodes = 10. Manageable.

### 6+ nodes: star with relay (needs relay mode)

One hub node, all edges connect to hub. Hub forwards changes between edges.

```
  edge1 ──→ hub ←── edge2
            ↑
         edge3
```

**What to build:** relay mode (Option 1). Hub receives from edge1, applies locally, forwards to edge2 + edge3.

**Trade-off:** hub is single point of failure. Mitigate with hub backup (hub ←→ hub-backup, point-to-point).

### Multi-region: hierarchical star

Regional hubs in full mesh, edge nodes star to regional hub.

```
  edge1 ──→ US hub ←── edge2
              ↕
  edge3 ──→ EU hub ←── edge4
```

**What to build:** multi-peer + relay mode. US hub and EU hub in full mesh (2 peers each). Edge nodes peer to regional hub only (1 peer).

## Implementation Priority

1. **Multi-peer support** (`--peer` repeated flag) — enables full mesh for 3-5 nodes. Smallest change, highest value.
2. **Relay mode** — enables star topology for 6+ nodes. Medium complexity.
3. **Watermark-based pull** — enables eventual consistency at scale. Highest complexity, defer until needed.

## What NOT to Do

- **Don't build topology management into the protocol.** Keep protocol simple (ship changes, ACK). Topology is a node-config concern.
- **Don't add a coordinator.** UUID PKs eliminate the need for central ID assignment. Keep it decentralized.
- **Don't add conflict resolution.** INSERT OR REPLACE with UUID PK = no conflicts. Last-write-wins via `updated_at` if needed, but don't build CRDT/vector clocks.
