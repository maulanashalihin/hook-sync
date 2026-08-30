# hook-sync Protocol

Shared wire protocol for all hook-sync implementations (Go, Bun, future languages).

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

## Batch Ship

Changes are batched and sent via HTTP POST:

```
POST /sync
Content-Type: application/json
X-Node-Id: node1

[
  { "op": "INSERT", "table": "items", "row": {...}, "old_id": null },
  { "op": "UPDATE", "table": "items", "row": {...}, "old_id": null },
  { "op": "DELETE", "table": "items", "row": null, "old_id": "abc-123" }
]
```

Response:

```json
{ "applied": 3 }
```

## Batch Interval

Default: 50ms. Configurable per implementation. Batch threshold (100 changes) triggers immediate ship regardless of interval.

## Primary Keys

UUIDv7 (time-ordered, RFC 9562). Eliminates conflicts in multi-writer setups — no CRDT, no last-write-wins.

## Capture Mechanisms

Each language implementation uses the most efficient available capture mechanism:

| Language | Capture mechanism | Overhead |
|----------|------------------|----------|
| Go | `sqlite3_preupdate_hook` | Zero (in-memory callback) |
| Bun | SQLite triggers + `_changes` table | 1 extra INSERT per change |

Both produce the same Change JSON format, so nodes in different languages can sync to each other.

## Infinite Loop Prevention

Synced changes (received via `/sync`) must not be re-captured and re-shipped. Each implementation handles this differently:

- **Go**: `syncing` flag checked in hook callback
- **Bun**: trigger `WHEN (SELECT syncing FROM _meta) = 0` clause

## REST API

All implementations expose the same REST API:

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/api/items` | Create item |
| `GET` | `/api/items` | List items (latest 100) |
| `GET` | `/api/items/:id` | Get single item |
| `PUT` | `/api/items/:id` | Update item |
| `DELETE` | `/api/items/:id` | Delete item |
| `POST` | `/sync` | Receive change batch (internal) |
| `GET` | `/health` | Health check + item count |
