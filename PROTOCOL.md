# hook-sync Protocol

Shared wire protocol for all hook-sync implementations (Go, Bun, Node).

## Change Format

Each change is a JSON object:

```json
{
  "op": "INSERT",
  "table": "items",
  "row": { "id": "0191a2b3-...", "name": "foo", "value": 42 },
  "old_id": null
}
```

| Field | Type | Description |
|-------|------|-------------|
| `op` | string | `"INSERT"`, `"UPDATE"`, or `"DELETE"` |
| `table` | string | Table name |
| `row` | object\|null | Full row values (column → value). Required for INSERT/UPDATE. Null for DELETE. |
| `old_id` | string\|null | Row ID for DELETE. Null for INSERT/UPDATE. |

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
    { "op": "DELETE", "table": "items", "row": null, "old_id": "abc-123" }
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
  │                                      │ 5. Return {applied, ack: batch_id}
  │ ◄─────────────────────────────────── │
  │ 6. If ack == batch_id:               │
  │    DELETE FROM _changes              │
  │    WHERE change_id <= ack            │
  │                                      │
```

If ship fails (network error, non-200, ACK mismatch), sender retries with exponential backoff: 50ms, 100ms, 200ms, 400ms, 800ms. After 5 failures, changes are moved to `_dead_letter` table for manual review.

### Idempotency

`INSERT OR REPLACE` with UUID primary key makes re-sends safe. If the same batch is shipped 10 times, the result is identical — no duplicates. This handles the case where the ship succeeds but the ACK response is lost.

## Batch Interval

Default: 50ms. Changes accumulate in `_changes` table between ship cycles.

## Durability

Changes are persisted in the `_changes` SQLite table at write time (via triggers in the same transaction). If the process crashes, un-shipped changes survive in the database and resume on restart.

## Dead Letter Queue

Changes that fail to ship after 5 retries are moved to `_dead_letter` table:

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

## Primary Keys

UUIDv7 (Go, time-ordered) or UUIDv4 (Bun/Node, `crypto.randomUUID()`). Eliminates conflicts in multi-writer setups — no CRDT, no last-write-wins.

## Capture Mechanism

All implementations use SQLite triggers + `_changes` table for durable capture:

```sql
CREATE TRIGGER items_ai AFTER INSERT ON items
WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
BEGIN
  INSERT INTO _changes(op, row_id, row_data)
  VALUES('INSERT', NEW.id, json_object(...));
END;
```

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
| `GET` | `/api/items` | List items (latest 100) |
| `GET` | `/api/items/:id` | Get single item |
| `PUT` | `/api/items/:id` | Update item |
| `DELETE` | `/api/items/:id` | Delete item |
| `POST` | `/sync` | Receive change batch with ACK (internal) |
| `GET` | `/health` | Health check + item count + pending changes + dead letter count |
