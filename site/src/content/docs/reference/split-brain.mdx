---
title: Split-Brain Safety
description: Last-write-wins by timestamp conflict resolution. Both nodes always converge to the same state.
---

import { Aside } from '@astrojs/starlight/components';

# Split-Brain Safety

When the network splits and both nodes accept writes independently, hook-sync uses **last-write-wins by timestamp** to resolve conflicts on reconnect. This guarantees both nodes converge to the same state — no divergence, no silent data corruption.

## Conflict Scenarios

| Scenario | During partition | On reconnect | Data loss? |
|----------|-----------------|-------------|-----------|
| INSERT (new rows) | Both create different UUIDs | Merge — both rows appear | ❌ None |
| UPDATE same row | Node A: value=100, Node B: value=200 | Converge to higher `updated_at` | Older update dropped |
| DELETE vs UPDATE | Node A deletes, Node B updates | UPDATE wins if newer than delete | Delete intent dropped |

## UPDATE vs UPDATE

Two nodes update the same row during partition. On reconnect, the receiver checks `updated_at` before applying:

```
Incoming change:  { id: "X", value: 200, updated_at: T2 }
Existing row:     { id: "X", value: 100, updated_at: T1 }

if existing.updated_at > incoming.updated_at:
    skip (keep existing — it's newer)
else:
    INSERT OR REPLACE (apply incoming — it's newer)
```

Both nodes converge to the row with the higher `updated_at`. The older update is silently dropped — no divergence.

## DELETE vs UPDATE

Node A deletes a row, Node B updates the same row during partition. On reconnect:

```
Incoming DELETE:  { op: "DELETE", old_id: "X", row: { updated_at: T_delete } }
Existing row:     { id: "X", updated_at: T_update }

if existing.updated_at > delete.updated_at:
    skip delete (keep the update — it's newer)
else:
    DELETE FROM items WHERE id = X
```

If the row was updated after it was deleted (on the other node), the delete is skipped and the update wins. Both nodes converge.

<Aside type="note" title="DELETE carries the full OLD row">
DELETE changes include the full OLD row in `row` (not null), including `updated_at`. This is required for timestamp-based conflict resolution. The trigger captures `OLD.updated_at` at deletion time.
</Aside>

## INSERT during Partition

INSERTs are always safe — UUID primary keys guarantee no collision. Two nodes creating rows during partition produce different UUIDs. On reconnect, both rows merge. No conflict resolution needed.

## What This Does NOT Solve

Last-write-wins means the older update is silently dropped. If two users edit the same field on different nodes during partition, one edit is lost. This is acceptable for:

- **Append-heavy workloads** (event logs, telemetry)
- **Multi-tenant where tenants don't share rows**
- **Geographic distribution** (each region owns its data)

For **collaborative editing of shared rows**, use CRDT-based sync (e.g., cr-sqlite).

## Test Results

36 checks across all 3 runtimes — **36/36 PASS**:

| Runtime | Checks | Passed | Result |
|---------|------:|------:|:---------:|
| Go | 12 | 12 | ✅ PASS |
| Bun | 12 | 12 | ✅ PASS |
| Node | 12 | 12 | ✅ PASS |
| **Total** | **36** | **36** | **✅ ALL PASS** |

### Test Phases (per runtime)

1. Start both nodes, create shared item, verify sync
2. Network partition (kill both nodes)
3. Start nodes independently (no peer), update same item + create new items
4. Reconnect (restart with peer)
5. Verify convergence: same value, all items merged, 0 dead letter
6. DELETE vs UPDATE conflict test

Run with: `bash bench-splitbrain.sh` (all runtimes) or `bash bench-splitbrain.sh go` (single runtime)

## Why This Works Without Consensus

Traditional distributed systems use consensus (Raft, Paxos) to prevent split-brain: only one leader accepts writes. hook-sync takes a different approach:

1. **All nodes accept writes** — no leader, no quorum
2. **UUID PKs** — no ID collision across nodes
3. **Timestamp LWW** — conflicts resolve deterministically on reconnect
4. **Idempotent apply** — `INSERT OR REPLACE` makes re-sends safe

The tradeoff: last-write-wins drops the older update. The benefit: zero downtime, zero coordinator dependency, zero consensus overhead. Both nodes always converge.
