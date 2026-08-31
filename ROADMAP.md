# hook-sync Roadmap

> Product vision, gap analysis, and direction. Last updated: 2026-09-01.

## Current State

hook-sync is a working SQLite replication engine: trigger capture → `_changes` table → HTTP batch sync → ACK → delete. Multi-writer active-active, cross-runtime (Go/Bun/Node), split-brain safety via LWW timestamp, crash recovery via `_changes` persistence.

**Proven:**

- 36/36 split-brain tests PASS across 3 runtimes
- 500K item stress test, 9/9 PASS, zero data loss
- Cross-server vs Postgres: tied at fair durability, 3.8x faster at batch 10K
- 4 topologies: point-to-point, full mesh, hub/star, multi-region
- 3 capture modes: triggers (cross-runtime), hook+Pebble (Go, 89% faster), hook+memory (Go, fastest, no crash recovery)

**What's built:**

| Component | Status |
|-----------|--------|
| Wire protocol (PROTOCOL.md) | ✅ Done |
| Go implementation (single, mesh, hub, multitable) | ✅ Done |
| Bun implementation (single, mesh, multitable) | ✅ Done |
| Node implementation (single, mesh, multitable) | ✅ Done |
| hook+Pebble (preupdate_hook + commit_hook) | ✅ Done |
| hook+Memory (preupdate_hook, in-memory) | ✅ Done |
| Benchmark suite (9 scripts) | ✅ Done |
| Split-brain safety | ✅ Done |
| Crash recovery | ✅ Done |
| Multi-region (hub-to-hub) | ✅ Done |

## Gap Analysis

| Gap | Impact | Who cares |
|-----|--------|-----------|
| Standalone server only — not a library | Every app must talk HTTP to hook-sync, can't `import { replicate }` | All developers |
| LWW only — older update silently dropped | Not suitable for collaborative editing of shared rows | Collaborative apps |
| No partial replication — all nodes get all data | Can't shard, can't filter by tenant/region | Multi-tenant, large-scale |
| No auth/TLS — sync protocol is plain HTTP | Not production-safe on public network | Ops/infra teams |
| No observability — only `/health` JSON | No dashboard, no metrics, no alerting | Ops teams |
| No schema evolution — triggers hardcoded per table | Add column = manual trigger update | All developers |
| No real-time subscriptions — client must poll | No WebSocket/SSE for live updates | Real-time apps |
| No mobile/browser SDK — protocol exists, no client library | Can't use in local-first apps | Mobile/web developers |
| Hub = SPOF — manual hub-backup only | No automatic failover | Ops teams |
| No CDC routing — changes only sync to peers | Can't route to Kafka/S3/webhook | Data engineering |

## Product Direction

### Vision

**"Add one line of code, your SQLite app gets multi-writer replication across browser, mobile, and server — open-source, no consensus, no fork, no vendor lock-in."**

hook-sync evolves from a standalone replication server into an **open-source local-first sync engine** built on stock SQLite.

```
                    hook-sync Ecosystem
                           │
          ┌────────────────┼────────────────┐
          │                │                │
    hook-sync Core    hook-sync Client   hook-sync Server
    (embedded lib)    (browser/mobile)   (deployable binary)
          │                │                │
    Go module          TS/JS SDK         Go/Bun/Node
    npm package        WASM SQLite       standalone HTTP
    Python binding     native SQLite     + /sync endpoint
          │                │                │
          └────────────────┼────────────────┘
                           │
                    HTTP sync protocol
                    (PROTOCOL.md — already built)
                           │
                    ┌──────┴──────┐
                    │             │
              hook-sync Studio  hook-sync CDC
              (dashboard)      (change streams)
```

### Why this direction

1. **Solves real problem.** Sync engine is #1 pain point for local-first apps. Linear, Notion, Figma, Excalidraw — all build sync from scratch. hook-sync = open-source sync engine for everyone.
2. **Technically novel.** Stock SQLite + trigger capture + HTTP sync. No CRDT, no fork, no consensus algorithm.
3. **Simple to explain.** "SQLite replication that just works."
4. **Hard to replicate.** Protocol, trigger mechanism, split-brain safety, cross-runtime interop — all built and tested. Competitors rebuild from zero.
5. **Built on existing codebase.** 80% of core already exists. Needs extraction + client SDK + WASM.

### Competitive landscape

| Project | SQLite? | Multi-writer? | Local-first? | Self-host? | Open-source? |
|---------|:-------:|:-------------:|:------------:|:----------:|:------------:|
| **hook-sync** | ✅ stock | ✅ | ✅ (planned) | ✅ | ✅ |
| Litestream | ✅ | ❌ read-only | ❌ | ✅ | ✅ |
| rqlite | ✅ fork | ❌ leader-only | ❌ | ✅ | ✅ |
| libSQL/Turso | ✅ fork | ✅ | partial | hosted | ✅ |
| ElectricSQL | ❌ Postgres | ✅ CRDT | ✅ | ✅ | ✅ |
| PowerSync | ❌ Postgres | ✅ | ✅ | hosted | partial |
| Replicache | ❌ custom | ✅ | ✅ | ✅ | ✅ |
| Yjs/Automerge | ❌ no SQL | ✅ CRDT | ✅ | ✅ | ✅ |

hook-sync occupies a gap no one fills: **stock SQLite + multi-writer + local-first + self-hostable + open-source**.

## Roadmap

### Phase 1: Embedded Library (core extraction)

Extract standalone server into importable library. One line of code = replication.

#### Monorepo structure

Single repo, all runtimes + packages + docs + benchmarks together. Protocol changes = 1 commit across everything. No cross-repo drift, no coordinated releases, no split issue trackers.

```
hook-sync/
├── go/
│   ├── go.mod                    # module github.com/<user>/hook-sync/go
│   ├── hooksync/                 # shared core (importable)
│   │   ├── protocol.go           # Change, SyncRequest, SyncResponse
│   │   ├── ship.go               # batchShip, shipWithAck, retry/backoff
│   │   ├── apply.go              # applyChanges, LWW timestamp conflict check
│   │   └── config.go             # Config struct, peers, batch settings
│   ├── trigger/                  # default capture (importable, no build tag)
│   │   ├── attach.go             # Attach(db, config) — schema introspection + auto-trigger
│   │   └── server.go             # standalone server mode (thin wrapper)
│   ├── hook/                     # high-perf capture (importable, build tag: sqlite_preupdate_hook)
│   │   ├── open.go               # Open(path, config) — custom driver + preupdate/commit/rollback hook
│   │   └── server.go             # standalone server mode
│   ├── mesh/                     # mesh topology (refactor to use shared core)
│   ├── hub/                      # hub relay
│   ├── hookmem/                  # in-memory capture
│   ├── hookpebble/               # pebble capture
│   ├── bench/                    # benchmarks
│   └── cmd/                      # CLI entrypoints (standalone server binaries)
│
├── js/                           # npm package (Bun + Node + browser, unified)
│   ├── package.json              # name: "hook-sync"
│   ├── src/
│   │   ├── protocol.ts           # shared core
│   │   ├── ship.ts
│   │   ├── apply.ts
│   │   ├── trigger.ts            # trigger capture
│   │   └── server.ts             # standalone server
│   └── browser/                  # WASM client SDK (Phase 2)
│
├── PROTOCOL.md                   # wire protocol spec
├── TOPOLOGY.md                   # topology recommendations
├── ROADMAP.md                    # this file
├── README.md
├── bench-*.sh                    # benchmark scripts
└── BENCHMARK-REPORT.md           # benchmark results
```

**Go import:** `go get github.com/<user>/hook-sync/go/trigger`
**npm install:** `npm install hook-sync` (published from `js/`)

**Why monorepo (not split):** protocol changes touch library + server + bench + docs in 1 commit. Shared `hooksync/` core used by trigger + hook + server. Atomic refactors, single release tag, single issue tracker. Split only when release cadence, contributor teams, or licensing diverge — none of which apply now.

#### Two capture modes

| | `trigger` (default) | `hook` (opt-in) |
|---|---|---|
| API | `trigger.Attach(db, config)` | `hook.Open(path, config)` |
| Speed | 81K QPS (-67%) | 152K QPS (-36%) |
| Build tag | none | `-tags sqlite_preupdate_hook` |
| CGO | standard SQLite | mattn/go-sqlite3 only |
| Cross-runtime | Go, Bun, Node | Go only |
| Dependencies | none extra | Pebble |
| Schema introspection | required (auto-generate triggers) | not needed (hook is table-agnostic) |
| Connection pool | works (database-level triggers) | custom driver (connection-level hooks) |
| Use case | most apps (HTTP is bottleneck) | write-heavy direct-SQLite (batch, analytics, local-first) |

Both share the same wire protocol — trigger nodes sync to hook nodes, no interop issues.

#### Usage

**Go — trigger (default, universal):**

```go
import "hook-sync/go/trigger"

db, _ := sql.Open("sqlite3", "app.db")
trigger.Attach(db, hooksync.Config{
    Peers:   []string{"http://peer:9002"},
    BatchMs: 50,
})
// Existing INSERT/UPDATE/DELETE now replicates automatically.
```

**Go — hook (high-performance, Go-only):**

```go
import "hook-sync/go/hook"

// Build: go build -tags sqlite_preupdate_hook
db, _ := hook.Open("app.db", hooksync.Config{
    Peers:   []string{"http://peer:9002"},
    BatchMs: 50,
})
// preupdate_hook captures changes, Pebble stores pending, commit_hook flushes.
```

**Bun/Node — trigger only:**

```typescript
import { attachSync } from 'hook-sync';
const db = new Database('app.db');
attachSync(db, { peers: ['http://peer:9002'], batchMs: 50 });
// Same — writes now replicate.
```

#### Deliverables

- [ ] Extract shared core: protocol types, ship loop, ACK logic, retry, LWW conflict resolution, peer watermarks → `hooksync/`
- [ ] Extract trigger capture: schema introspection (`PRAGMA table_info`), auto-trigger SQL generation → `trigger/`
- [ ] Extract hook capture: custom driver registration, preupdate/commit/rollback hooks, Pebble batch → `hook/`
- [ ] `trigger.Attach(db, config)` — drop-in, works with existing `*sql.DB`
- [ ] `hook.Open(path, config)` — custom driver, connection-level hooks
- [ ] Auto-trigger generation from schema introspection (not hardcoded per table)
- [ ] Ship as: Go module, npm package (Bun/Node)
- [ ] Backward compat: standalone server mode still works (thin wrapper around library)

#### What's reusable from current codebase

- Wire protocol (PROTOCOL.md) → unchanged, shared by both packages
- Trigger capture mechanism → extract from `main.go:setupSchema()` → `trigger/`
- preupdate_hook + commit_hook + Pebble → extract from `hookpebble/main.go` → `hook/`
- ACK-based sync with retry → extract from `main.go:batchShip()` → `hooksync/`
- Split-brain safety (LWW) → extract from `main.go:applyChanges()` → `hooksync/`
- Crash recovery → `_changes` (trigger) / Pebble (hook) persistence, unchanged

### Phase 2: Browser Client SDK (local-first)

WASM SQLite in browser + hook-sync client = offline-first apps with zero backend sync code.

**Architecture:**

```
Browser (WASM SQLite)          Server (hook-sync)
     │                              │
     ├── hook-sync Client           ├── hook-sync Server
     │   (TS/JS SDK)                │   (current codebase)
     │                              │
     └── HTTP sync protocol ────────┘
```

**Deliverables:**

- [ ] `hook-sync-client` (TS/JS) — WASM SQLite (wa-sqlite/sql.js) + trigger setup + ship loop + /sync handler, runs in browser
- [ ] Offline queue: changes survive in IndexedDB/localStorage
- [ ] Online → sync to server + other devices
- [ ] Demo app: todo/notes app that works offline, syncs across devices

**Killer demo:**

1. Open web app → SQLite loads in browser via WASM
2. Work offline → changes queued in `_changes` table (browser storage)
3. Online → changes sync to server + other devices
4. Multiple devices → all converge, LWW conflict resolution
5. Zero backend code — deploy hook-sync Server, build app, sync works

### Phase 3: Real-time Transport

Add WebSocket/SSE alongside HTTP polling for real-time sync.

**Deliverables:**

- [ ] WebSocket transport option (in addition to HTTP batch)
- [ ] Server-push: peer gets notified instantly on change, not waiting for next batch tick
- [ ] Fallback to HTTP polling for environments without WebSocket
- [ ] Subscription API: client subscribes to table/row changes

### Phase 4: hook-sync Studio (Dashboard)

Web UI for managing hook-sync clusters.

**Deliverables:**

- [ ] Topology map (visual graph of nodes + connections)
- [ ] Sync lag, pending queue depth, dead letter count per node
- [ ] Throughput graphs (writes/sec, sync/sec)
- [ ] Conflict history (LWW resolutions)
- [ ] Node add/remove from UI
- [ ] Alerting (sync lag > threshold, dead letter > 0, node down)

### Phase 5: hook-sync CDC (Change Data Capture)

Route changes to multiple sinks beyond peer sync.

```
SQLite write
  → Trigger captures to _changes
  → Router dispatches to:
      ├── Peer sync (current — HTTP to other hook-sync nodes)
      ├── Webhook (notify external systems)
      ├── Kafka/NATS/SQS (event streaming)
      ├── S3/GCS (backup/archive)
      └── Elasticsearch/Meilisearch (search index)
```

**Deliverables:**

- [ ] Sink interface (pluggable, same change format)
- [ ] Webhook sink (simplest, first)
- [ ] Kafka/NATS sink
- [ ] S3 batch sink
- [ ] At-least-once delivery guarantee (ACK-based, same as peer sync)

### Phase 6: Mobile SDK

Native SQLite on mobile + hook-sync client = offline-first mobile apps.

**Deliverables:**

- [ ] Swift SDK (iOS — GRDB or raw SQLite)
- [ ] Kotlin SDK (Android — Room or raw SQLite)
- [ ] Offline queue, sync on reconnect
- [ ] Demo: mobile app that works offline, syncs to server

### Phase 7: Production Hardening

**Deliverables:**

- [ ] TLS + auth (mTLS, API key, or JWT for sync protocol)
- [ ] Hub automatic failover (hub-backup promotion, no manual switch)
- [ ] Partial replication (filter by table/tenant/region — not all nodes get all data)
- [ ] Schema evolution (auto-migrate triggers when schema changes)
- [ ] CRDT layer for collaborative fields (opt-in, `@crdt` annotation on specific columns)

## Ideas Not Pursued (Yet)

| Idea | Why not now |
|------|------------|
| Auto peer discovery (mDNS/gossip) | Adds complexity, mesh config is fine for now |
| Ring/chain topology | No benefit over mesh (small N) or star (large N) |
| Full CRDT sync | Complex, schema changes required — LWW covers most use cases |
| Compression | Benchmarked: not worth it on fast links (CPU > bandwidth save) |
| Watermark-based pull | Highest complexity, defer until needed (TOPOLOGY.md notes) |

## Decision Log

- **2026-09-01:** Roadmap created. Direction: embedded library + local-first sync engine. Chosen over standalone-server-only because the gap (stock SQLite + multi-writer + local-first + self-hostable) is unfilled by any existing project. 80% of core already built.
- **2026-09-01:** Dual-package capture strategy for Go library. `trigger` package (default, `Attach(db, config)`, no build tag, cross-runtime) + `hook` package (opt-in, `Open(path, config)`, `-tags sqlite_preupdate_hook`, Go-only, 89% faster). Shared `hooksync/` core (protocol, ship, ACK, LWW). User picks capture mode at import time. Both share wire protocol — trigger nodes sync to hook nodes. Bun/Node use trigger only (no CGO preupdate_hook).
- **2026-09-01:** Monorepo. Go (`go/` with `hooksync/`, `trigger/`, `hook/`, `mesh/`, `hub/`, `cmd/`) + JS (`js/` unified Bun/Node/browser) + docs + benchmarks in 1 repo. Protocol changes = 1 commit. Shared core atomic refactors. Single release tag, single issue tracker. Split only when release cadence, contributor teams, or licensing diverge — none apply now.
