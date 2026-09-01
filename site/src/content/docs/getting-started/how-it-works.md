---
title: How It Works
description: The trigger + ACK + UUID replication model behind hook-sync.
---

import { Aside } from '@astrojs/starlight/components';

# How It Works

hook-sync adds replication to SQLite without consensus algorithms, Raft, or coordinators. Just three primitives: **triggers**, **ACK**, and **UUID**.

## The Flow

```
App write (native SQLite speed)
  → Trigger captures change to _changes table (same transaction)
  → Background timer: batch every 50ms (default)
  → Drain mode: ships until _changes is empty within each tick
  → HTTP POST {batch_id, changes} to peer
  → Peer: INSERT OR REPLACE (UUID PK = zero conflict)
         + timestamp check (last-write-wins for split-brain safety)
  → Peer returns {applied, ack: batch_id}
  → Sender deletes from _changes only after ACK confirms
```

**Write speed is identical with or without peers.** Sync runs in the background — it never blocks the write path. The client gets its response as soon as SQLite write + capture completes.

## Three Primitives

### 1. Triggers (Capture)

SQLite triggers capture every INSERT/UPDATE/DELETE into a `_changes` table, in the same transaction as the original write. This means:

- **Durable**: if the process crashes, un-shipped changes survive in the SQLite file
- **Transactional**: the change record and the data write succeed or fail together
- **Zero overhead to the client**: the trigger runs inside SQLite, not in application code

```sql
CREATE TRIGGER items_ai AFTER INSERT ON items
WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
BEGIN
  INSERT INTO _changes(op, row_id, row_data)
  VALUES('INSERT', NEW.id, json_object('id', NEW.id, ...));
END;
```

The `WHEN` clause checks a `syncing` flag — when applying received changes, the flag is set to 1, preventing triggers from re-capturing synced data (infinite loop prevention).

### 2. ACK (Reliability)

Changes are not deleted from `_changes` until the peer confirms receipt with a matching ACK. This handles:

- **Network failures**: ship fails → retry with backoff → changes stay in `_changes`
- **Peer down**: changes accumulate → retry every tick → ship on reconnect
- **ACK lost**: re-send is safe (idempotent `INSERT OR REPLACE` with UUID PK)

```
Sender                                Receiver
  │  POST /sync {batch_id, changes}    │
  │ ────────────────────────────────► │
  │                                   │ Apply (INSERT OR REPLACE)
  │                                   │ + timestamp conflict check
  │ ◄──────────────────────────────── │
  │  {applied, ack: batch_id}         │
  │                                   │
  │  If ack == batch_id:              │
  │    DELETE FROM _changes           │
  │    WHERE change_id <= ack         │
```

### 3. UUID (Conflict-free Multi-Writer)

Every table uses `id TEXT PRIMARY KEY` with UUID. This eliminates the need for:

- **Coordinator**: no central ID assignment
- **CRDTs**: no merge logic
- **Vector clocks**: no version tracking

Two nodes creating rows simultaneously produce different UUIDs. `INSERT OR REPLACE` never collides. On reconnect, both rows merge.

<Aside type="note" title="UUID version — pick what's fastest">
Any UUID version works (v4 or v7). Go prefers v7 (time-ordered → sequential B-tree insert). Bun: v4 has fastest generation (`crypto.randomUUID()` native), but an optimized v7 hex-table impl can win on sequential insert via B-tree locality. Node 26+ will have `crypto.randomUUIDv7()` native ([PR #62553](https://github.com/nodejs/node/pull/62553)). Benchmarks: `go/bench/bench_uuid.go`, `bun/bench-uuid.ts`.
</Aside>

## Hook Capture Mode (Go-only, opt-in)

Go also supports **hook capture mode** via the `hook/` library. Instead of SQL triggers, a `preupdate_hook` captures changes in-memory during the transaction, then a `commit_hook` flushes them to Pebble as a single batch.

| | Trigger mode | Hook mode |
|---|---|---|
| Capture | SQL triggers → `_changes` table | preupdate_hook → Pebble KV |
| Speed | Baseline | 35% faster (no `_changes` write) |
| Cross-runtime | Yes (Go, Bun, Node) | Go-only |
| Build tag | none | `sqlite_preupdate_hook` |

Both modes share the same wire protocol — trigger nodes sync to hook nodes transparently.

## What This Does NOT Do

- **No consensus**: no Raft, no leader election, no quorum
- **No coordinator**: no central node assigns IDs or orders writes
- **No CRDTs**: last-write-wins by timestamp, not merge-based
- **No real-time sync**: async, batch-interval latency (50ms default)

This is **eventual consistency with multi-writer active-active**. All nodes accept writes independently. Conflicts resolve on reconnect via timestamp. Zero data loss for INSERTs; last-write-wins for UPDATEs.
