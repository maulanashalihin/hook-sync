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
    ↕ ╳ ↕
    C ←→ D

A connects to B, C, D. B connects to A, C, D. Etc.
6 connections for 4 nodes. Every node talks to every other directly.
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


## When Full Mesh Doesn't Scale: Dedicated Hub

For 8+ nodes, use a **dedicated hub** — a node that only relays changes, does not serve client requests.

```
  edge1 ──POST /sync──→ hub ──POST /sync──→ edge2
         (changes)      │
                        ├──→ edge3
                        ├──→ edge4
                        └ apply to local SQLite (full backup)
```

### Why dedicated hub is simpler than dual-purpose hub

A dual-purpose hub (serves `/api/items` AND relays) has a problem: the `syncing` flag suppresses triggers during apply, which also blocks forwarding. Working around this is complex.

A dedicated hub has **no triggers at all**. All data enters via `/sync`, not local writes. No triggers = no `syncing` flag problem = forwarding just works.

| | Dual-purpose hub | Dedicated hub |
|---|---|---|
| Serves `/api/items`? | Yes | No |
| Has triggers? | Yes | No |
| `syncing` flag problem? | Yes — blocks forward | No — no triggers |
| Client traffic? | Yes — competes with relay | No — pure relay |
| What to build | Relay + work around syncing flag | Relay only |

### What the hub does

1. **Receive `/sync`** — accept changes from any edge
2. **Apply locally** — INSERT OR REPLACE (hub has full data copy, acts as backup)
3. **Forward** — send raw received changes to all other edges

Step 3 is the only new code. Steps 1 and 2 already work.

### Hub failure

Hub is single point of failure. Mitigate: run hub ←→ hub-backup (point-to-point, already works). If hub dies, hub-backup takes over. Edges reconnect to hub-backup.

### Edge reconnection

If an edge goes down and comes back, it ships all pending `_changes` to hub. Hub forwards to other edges. No data loss — same ACK + retry mechanism as point-to-point.

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

## Open Problems (Not Yet Solved)

### _changes table management with multiple peers

Current ACK protocol assumes 1 peer: ship → ACK → delete. With N peers in full mesh, when do we delete from `_changes`?

- **Delete after all ACK:** 1 peer down → `_changes` piles up for everyone
- **Delete after first ACK:** other peers miss the change → data loss for them
- **Watermark per peer (proposed):** track `last_acked` per peer in `_peer_state` table. Delete only changes that ALL peers have ACKed. Changes for offline peers stay until they reconnect.

```sql
CREATE TABLE _peer_state (
    peer_url TEXT PRIMARY KEY,
    last_acked INTEGER DEFAULT 0
);
```

Not yet implemented. Needs to be built together with multi-peer support.

### Relay mode: duplicate forwarding

In star topology, hub forwards received changes to other edges. If edge1 and edge2 both write, hub receives both, forwards both. But if hub also has full mesh with another hub, same change can loop. Need a "seen" set or origin tracking to prevent forwarding changes back to the node they came from.

Not yet designed.

## What NOT to Do

- **Don't build topology management into the protocol.** Keep protocol simple (ship changes, ACK). Topology is a node-config concern.
- **Don't add a coordinator.** UUID PKs eliminate the need for central ID assignment. Keep it decentralized.
- **Don't add conflict resolution.** INSERT OR REPLACE with UUID PK = no conflicts. No CRDT, no vector clocks.
- **Don't build ring or chain topologies.** They add latency and fragility with no benefit over full mesh (small N) or star (large N).
