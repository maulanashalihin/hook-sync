# hook-sync Protocol

Shared wire protocol for all hook-sync implementations (Go, Bun, Node).
Copy this protocol into any language — if your implementation speaks this format, it syncs with all other nodes regardless of runtime.

## Change Format

Each change is a JSON object:

```json
{
  "op": "INSERT",
  "table": "items",
  "row": { "id": "0191a2b3-...", "name": "foo", "value": 42, "created_at": 1700000000000, "updated_at": 1700000000000 },
  "old_id": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `op` | string | `"INSERT"`, `"UPDATE"`, or `"DELETE"` |
| `table` | string | Table name |
| `row` | object\|null | Full row values (column → value). Required for INSERT/UPDATE. For DELETE, contains the OLD row data (including `updated_at`) for timestamp-based conflict resolution. |
| `old_id` | string\|null | Row ID for DELETE. Null for INSERT/UPDATE. |

### DELETE changes

DELETE changes carry the full OLD row in `row` (not null). This includes `updated_at` — the timestamp of the row at deletion time. The receiver uses this to resolve DELETE vs UPDATE conflicts (see [Split-Brain Safety](#split-brain-safety)).

```json
{
  "op": "DELETE",
  "table": "items",
  "row": { "id": "abc-123", "name": "foo", "value": 42, "created_at": 1700000000000, "updated_at": 1700000005000 },
  "old_id": "abc-123"
}
```

## ACK-Based Sync

Changes are batched and sent via HTTP POST with a batch ID. The sender does NOT delete changes from its local `_changes` table until the peer confirms receipt with a matching ACK.

### Request

```
POST /sync
Content-Type: application/json
X-Node-Id: node1

{
  "batch_id": 42,
  "changes": [
    { "op": "INSERT", "table": "items", "row": {...}, "old_id": null },
    { "op": "UPDATE", "table": "items", "row": {...}, "old_id": null },
    { "op": "DELETE", "table": "items", "row": {...}, "old_id": "abc-123" }
  ]
}
```

### Response

```json
{ "applied": 3, "ack": 42 }
```

| Field | Type | Description |
|-------|------|-------------|
| `applied` | int | Number of changes successfully applied |
| `ack` | int64 | Echo of `batch_id` from request. Sender deletes changes where `change_id <= ack` only when `ack` matches the sent `batch_id`. |

### Flow

```
Sender                                Receiver
  │                                      │
  │ 1. Read _changes (LIMIT 10000)       │
  │ 2. batch_id = max(change_id)         │
  │ 3. POST /sync {batch_id, changes}    │
  │ ───────────────────────────────────► │
  │                                      │ 4. Apply (INSERT OR REPLACE)
  │                                      │    with timestamp conflict check
  │                                      │ 5. Return {applied, ack: batch_id}
  │ ◄─────────────────────────────────── │
  │ 6. If ack == batch_id:               │
  │    DELETE FROM _changes              │
  │    WHERE change_id <= ack            │
  │                                      │
```

### Retry & Dead Letter

Ship failures are classified into two types:

| Failure type | Cause | Behavior |
|-------------|-------|----------|
| **Connection error** | Peer unreachable, network down, connection refused | Retry with backoff (50/100/200/400/800ms, 5 attempts). If still unreachable, **keep changes in `_changes`** and try again next tick. No data loss. |
| **ACK mismatch** | Peer received but rejected data (protocol error) | Retry with backoff. After 5 failures, move to `_dead_letter` table for manual review. |

Connection errors never dead-letter — changes accumulate until the peer reconnects. This handles startup races (node A starts before node B is ready) and transient network partitions without data loss.

### Idempotency

`INSERT OR REPLACE` with UUID primary key makes re-sends safe. If the same batch is shipped 10 times, the result is identical — no duplicates. This handles the case where the ship succeeds but the ACK response is lost.

## Split-Brain Safety

When two nodes are partitioned (network split) and both accept writes independently, hook-sync uses **last-write-wins by timestamp** to resolve conflicts on reconnect. This guarantees both nodes converge to the same state — no divergence, no silent data corruption.

### UPDATE vs UPDATE

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

### DELETE vs UPDATE

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

### INSERT during partition

INSERTs are always safe — UUID primary keys guarantee no collision. Two nodes creating rows during partition produce different UUIDs. On reconnect, both rows merge. No conflict resolution needed.

### What this does NOT solve

Last-write-wins means the older update is silently dropped. If two users edit the same field on different nodes during partition, one edit is lost. This is acceptable for append-heavy workloads (event logs, telemetry, multi-tenant where tenants don't share rows). For collaborative editing of shared rows, use CRDT-based sync (e.g., cr-sqlite).

## Batch Interval & Drain Mode

Default: 50ms. Changes accumulate in `_changes` table between ship cycles.

**Drain mode**: within each tick, the sender ships batches repeatedly until `_changes` is empty. With batch-size 10000, 100K items converge in ~2s (was 60s with fixed LIMIT 100 and single-batch-per-tick).

## Durability

Changes are persisted in the `_changes` SQLite table at write time (via triggers in the same transaction). If the process crashes, un-shipped changes survive in the database and resume on restart.

## Dead Letter Queue

Changes that fail to ship after 5 retries due to **ACK mismatch** (protocol error, not connection error) are moved to `_dead_letter` table:

```sql
CREATE TABLE _dead_letter (
    dead_id INTEGER PRIMARY KEY AUTOINCREMENT,
    op TEXT,
    row_id TEXT,
    row_data TEXT,
    failed_at INTEGER,
    retry_count INTEGER DEFAULT 0
);
```

Connection errors (peer unreachable) do NOT dead-letter — changes stay in `_changes` and retry on every tick until the peer reconnects.

## Primary Keys

UUIDv7 (Go, time-ordered) or UUIDv4 (Bun/Node, `crypto.randomUUID()`). Eliminates conflicts in multi-writer setups — no coordinator, no CRDT, no collision.

Every table that participates in sync MUST have:

- `id TEXT PRIMARY KEY` (UUID)
- `updated_at INTEGER` (millisecond timestamp, used for last-write-wins conflict resolution)

## Capture Mechanism

All implementations use SQLite triggers + `_changes` table for durable capture:

```sql
CREATE TRIGGER items_ai AFTER INSERT ON items
WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
BEGIN
  INSERT INTO _changes(op, row_id, row_data)
  VALUES('INSERT', NEW.id, json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
    'created_at', NEW.created_at, 'updated_at', NEW.updated_at));
END;

CREATE TRIGGER items_au AFTER UPDATE ON items
WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
BEGIN
  INSERT INTO _changes(op, row_id, row_data)
  VALUES('UPDATE', NEW.id, json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
    'created_at', NEW.created_at, 'updated_at', NEW.updated_at));
END;

CREATE TRIGGER items_ad AFTER DELETE ON items
WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
BEGIN
  INSERT INTO _changes(op, row_id, row_data)
  VALUES('DELETE', OLD.id, json_object('id', OLD.id, 'name', OLD.name, 'value', OLD.value,
    'created_at', OLD.created_at, 'updated_at', OLD.updated_at));
END;
```

DELETE triggers capture the full OLD row (including `updated_at`) — not NULL. This is required for timestamp-based conflict resolution on DELETE vs UPDATE.

Changes are written in the same transaction as the original write, ensuring durability.

## Infinite Loop Prevention

Synced changes (received via `/sync`) must not be re-captured. All implementations use a `syncing` flag in `_meta` table, checked by trigger `WHEN` clause:

```sql
UPDATE _meta SET value = 1 WHERE key = 'syncing';
-- apply changes
UPDATE _meta SET value = 0 WHERE key = 'syncing';
-- commit transaction
```

The syncing flag is set and cleared within the same transaction that applies changes.

## REST API

All implementations expose the same REST API:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/items` | Create item |
| `POST` | `/api/items/batch` | Create multiple items in one transaction |
| `GET` | `/api/items` | List items (latest 100) |
| `GET` | `/api/items/:id` | Get single item |
| `PUT` | `/api/items/:id` | Update item |
| `DELETE` | `/api/items/:id` | Delete item |
| `POST` | `/sync` | Receive change batch with ACK (internal) |
| `GET` | `/health` | Health check + item count + pending changes + dead letter count |

## Implementing in a New Language

The protocol is language-agnostic. To add a new runtime:

1. **SQLite database** with `_changes`, `_meta`, `_dead_letter` tables
2. **Triggers** on your data tables (INSERT/UPDATE/DELETE) that capture changes to `_changes`
3. **Background ship loop** that reads `_changes`, POSTs to peer, deletes on ACK
4. **`/sync` endpoint** that receives changes, applies with timestamp conflict check, returns ACK
5. **`syncing` flag** to prevent infinite loop

That's it. No consensus algorithm, no Raft, no coordinator. Just triggers + HTTP + ACK.

Reference implementations: `go/cmd/server/main.go` (Go), `bun/server.ts` (Bun), `node/server.js` (Node). All three are ~300 lines and sync to each other.
