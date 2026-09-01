---
title: Go Libraries
description: Importable Go packages for hook-sync — trigger-based and hook-based capture modes.
---

import { Tabs, TabItem, Aside } from '@astrojs/starlight/components';

# Go Libraries

The Go replication core is extracted into three importable packages. Pick capture mode at import time.

| Package | API | Capture | Build tag | Use when |
|---------|-----|--------|-----------|----------|
| `hooksync/` | `Change`, `Config`, `ShipWithAck()`, `ApplyChange()` | — (shared core) | none | Always (both modes depend on it) |
| `trigger/` | `Attach(db, config, tables)` | SQL triggers + `_changes` | none | Default — cross-runtime, database-level |
| `hook/` | `Open(path, config, tables)`, `OpenInMemory()` | preupdate_hook + Pebble | `sqlite_preupdate_hook` | Opt-in — 35% faster, Go-only, connection-level |

## Installation

```bash
go get hook-sync/go/hooksync
go get hook-sync/go/trigger
# or for hook mode:
go get hook-sync/go/hook
```

## Trigger Mode (Default)

Cross-runtime, database-level capture. Uses SQL triggers + `_changes` table. Syncs with Bun and Node nodes transparently.

```go
import (
    "hook-sync/hooksync"
    "hook-sync/trigger"
)

db, _ := sql.Open("sqlite3", "app.db")
mgr, _ := trigger.Attach(db, hooksync.Config{
    ID: "node1",
    Peers: []string{"http://peer:9002"},
    BatchMs: 50,
    BatchSize: 10000,
}, []string{"items"})
defer mgr.Stop()
```

## Hook Mode (35% Faster, Go-only)

Connection-level capture via `preupdate_hook` + Pebble KV. Eliminates the `_changes` table write — changes captured in-memory during transaction, flushed to Pebble on commit.

```go
import (
    "hook-sync/hooksync"
    "hook-sync/hook"
)

mgr, _ := hook.Open("app.db", hooksync.Config{
    ID: "node1",
    Peers: []string{"http://peer:9002"},
    BatchMs: 50,
    BatchSize: 10000,
}, []string{"items"})
defer mgr.Stop()
```

Build with the `sqlite_preupdate_hook` tag:

```bash
go build -tags sqlite_preupdate_hook -o myapp ./cmd/myapp
```

<Aside type="warning" title="Hook mode is Go-only">
`preupdate_hook` is a C-level SQLite API not exposed by `bun:sqlite` or `better-sqlite3`. Hook mode nodes still sync to trigger mode nodes — the wire protocol is identical.
</Aside>

## HTTP Server Wiring

Go's `trigger.Manager` implements `http.Handler`, so `/sync` is one line:

```go
http.Handle("/sync", mgr)  // mgr.ServeHTTP handles parse + apply + ACK
http.ListenAndServe(":9001", nil)
```

The `ServeHTTP` method:

1. Parses the JSON body (`batch_id` + `changes`)
2. Applies each change via `ApplyChange()` with LWW conflict check
3. Returns `{applied, ack: batch_id}`

## API Reference

### `hooksync.Config`

```go
type Config struct {
    ID        string   // node identifier
    Peers     []string // peer URLs (e.g., "http://localhost:9002")
    BatchMs   int      // ship interval in ms (default: 50)
    BatchSize int      // max changes per batch (default: 10000)
}
```

### `hooksync.Change`

```go
type Change struct {
    Op    string            // "INSERT", "UPDATE", "DELETE"
    Table string            // table name
    Row   map[string]any    // full row values
    OldID *string           // row ID for DELETE, nil for INSERT/UPDATE
}
```

### `hooksync.ApplyChange()`

Table-agnostic — column names come from `Change.Row` map, no hardcoded columns. `validTable()` validates table names (alphanumeric + underscore only) as defense-in-depth against SQL injection via table names (SQLite doesn't support parameterized table names).

### `hooksync.ShipWithAck()`

Reads pending changes, POSTs to peer, deletes on ACK match. Handles retry with exponential backoff (50/100/200/400/800ms, 5 attempts). Connection errors never dead-letter — changes stay in `_changes` and retry next tick.

## Binary Entry Points

The repo includes thin wrapper binaries in `go/cmd/`:

| Binary | Package | Description |
|--------|---------|-------------|
| `hook-sync-go` | `trigger/` | Single-peer, point-to-point |
| `hook-sync-mesh-go` | `trigger/` | Multi-peer, full mesh |
| `hook-sync-hookserver` | `hook/` | Hook capture mode (requires build tag) |
| `hook-sync-hub` | — (standalone) | Dedicated hub relay (Pebble KV, no SQLite) |
| `hook-sync-multitable` | `trigger/` | Multi-table (items + categories) |

Build all:

```bash
cd go
go build -o ../hook-sync-go ./cmd/server
go build -o ../hook-sync-mesh-go ./cmd/mesh
go build -tags sqlite_preupdate_hook -o ../hook-sync-hookserver ./cmd/hookserver
go build -o ../hook-sync-hub ./cmd/hub
```
