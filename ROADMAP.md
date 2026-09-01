# hook-sync Roadmap

> Product vision, gap analysis, and direction. Last updated: 2026-09-01 (Phase 1 complete, Phase 3 + Phase 5 closed — research/analysis concluded neither WS transport nor CDC platform is worth pursuing for node-to-node).

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
| Go libraries (`hooksync/`, `trigger/`, `hook/`) | ✅ Done — Phase 1 |
| JS library (`hooksync.js` on npm, v0.1.3) | ✅ Done — Phase 1 |
| JS wrappers refactored to thin wrappers over `js/` library | ✅ Done |
| Website ([hook-sync.pages.dev](https://hook-sync.pages.dev)) | ✅ Done |

## Gap Analysis

| Gap | Impact | Who cares |
|-----|--------|-----------|
| ~~Standalone server only — not a library~~ ✅ Resolved | ~~Every app must talk HTTP to hook-sync~~ Now: `import { attach } from 'hooksync.js'` (npm) or `trigger.Attach()` (Go) | All developers |
| LWW only — older update silently dropped | Not suitable for collaborative editing of shared rows | Collaborative apps |
| No partial replication — all nodes get all data | Can't shard, can't filter by tenant/region | Multi-tenant, large-scale |
| No auth/TLS — sync protocol is plain HTTP | Not production-safe on public network | Ops/infra teams |
| No observability — only `/health` JSON | No dashboard, no metrics, no alerting | Ops teams |
| No schema evolution — triggers hardcoded per table | Add column = manual trigger update | All developers |
| ~~No real-time subscriptions — client must poll~~ ✅ Closed (Phase 3) | ~~No WebSocket/SSE for live updates~~ Research: HTTP = WS at same interval for node-to-node. Sync delay is batch-interval-bound, not transport-bound. `batchMs: 1` gives 1ms delay with HTTP. WS/SSE deferred to client SDK (Phase 2/6) where server-push is genuinely needed. See `RESEARCH-PHASE3-HTTP-VS-WS.md` | Real-time apps |
| No mobile/browser SDK — protocol exists, no client library | Can't use in local-first apps | Mobile/web developers |
| Hub = SPOF — manual hub-backup only | No automatic failover | Ops teams |
| ~~No CDC routing — changes only sync to peers~~ ✅ Closed (Phase 5) | ~~Can't route to Kafka/S3/webhook~~ Analysis: full CDC = different product category (Debezium, Kafka Connect). SQLite users don't build Kafka pipelines. Most use cases better served by peer sync (consumer needs data → make it a peer). Webhook = no proven demand. Moved to Ideas Not Pursued. | Data engineering |

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

### Phase 1: Embedded Library (core extraction) ✅ Done

Extract standalone server into importable library. One line of code = replication.

#### Monorepo structure

Single repo, all runtimes + packages + docs + benchmarks together. Protocol changes = 1 commit across everything. No cross-repo drift, no coordinated releases, no split issue trackers.

```
hook-sync/
├── go/
│   ├── go.mod                    # module hook-sync/go
│   ├── cmd/                      # binary entrypoints (deployable, not importable)
│   │   ├── server/main.go        #   standalone server (single-table, point-to-point)
│   │   ├── mesh/main.go          #   full mesh (multi-peer, per-peer watermark)
│   │   ├── hub/main.go           #   dedicated hub (pure relay, Pebble KV, no client requests)
│   │   └── multitable/main.go    #   multi-table (items + categories)
│   ├── hookmem/                  # experimental: preupdate_hook + in-memory (benchmark baseline, no persistence)
│   ├── hookpebble/               # experimental: preupdate_hook + Pebble (prototype for hook/ library — Phase 1)
│   ├── bench/                    # direct SQLite benchmarks (trigger overhead, hook vs trigger)
│   ├── hooksync/                 # shared core (importable) — ✅ Phase 1 done
│   ├── trigger/                  # trigger capture (importable) — ✅ Phase 1 done
│   └── hook/                     # hook capture (importable) — ✅ Phase 1 done
│
├── bun/                          # Bun implementation (Bun.serve + bun:sqlite)
│   ├── server.ts                 #   single-table, point-to-point
│   ├── server-mesh.ts            #   full mesh
│   └── server-multitable.ts      #   multi-table
│
├── node/                         # Node.js implementation (hyper-express + better-sqlite3)
│   ├── server.js                 #   single-table, point-to-point
│   ├── server-mesh.js            #   full mesh
│   └── server-multitable.js      #   multi-table
│
├── js/                           # unified npm package: hooksync.js (Bun + Node) — ✅ Phase 1 done
│   ├── package.json              #   name: "hooksync.js"
│   ├── src/
│   │   ├── index.ts              #   re-exports attach(), types
│   │   ├── types.ts              #   Change, Config, SyncRequest/Response, SqliteDatabase
│   │   ├── apply.ts              #   table-agnostic LWW apply
│   │   ├── ship.ts               #   shipWithAck() via fetch()
│   │   └── trigger.ts            #   attach() → Manager (triggers, ship loop, watermarks)
│   └── browser/                  #   WASM client SDK (Phase 2 — not started)
│
├── PROTOCOL.md                   # wire protocol spec
├── TOPOLOGY.md                   # topology recommendations
├── ROADMAP.md                    # this file
├── README.md
├── site/                        # Astro Starlight website (hook-sync.pages.dev)
├── bench-*.sh                    # benchmark scripts
└── BENCHMARK-REPORT.md           # benchmark results
```
**Go import:** `go get github.com/maulanashalihin/hook-sync/go/trigger`
**npm install:** `npm install hooksync.js` (published from `js/`)

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

- [x] Extract shared core: protocol types, ship loop, ACK logic, retry, LWW conflict resolution, peer watermarks → `hooksync/`
- [x] Extract trigger capture: schema introspection (`PRAGMA table_info`), auto-trigger SQL generation → `trigger/`
- [x] Extract hook capture: custom driver registration, preupdate/commit/rollback hooks, Pebble batch → `hook/`
- [x] `trigger.Attach(db, config)` — drop-in, works with existing `*sql.DB`
- [x] `hook.Open(path, config)` — custom driver, connection-level hooks
- [x] Auto-trigger generation from schema introspection (not hardcoded per table)
- [x] Ship as: Go module, npm package (`hooksync.js` v0.1.3 on npm)
- [x] Backward compat: standalone server mode still works (thin wrapper around library)

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

### Phase 3: Real-time Transport ✅ Closed (researched, not implemented)

**Decision:** HTTP remains the sole transport for node-to-node sync. WebSocket adds complexity for zero benefit in the core engine.

**Research:** `RESEARCH-PHASE3-HTTP-VS-WS.md` — 14-variant benchmark (HTTP vs WS at 1/5/10/50ms intervals + event-driven flush every 1/10/100 writes).

**Findings:**

- HTTP and WS are statistically identical at the same batch interval (9,819 vs 9,747 QPS at 1ms, 6,296 vs 5,815 at 50ms). Sync delay = batch interval, not transport.
- Event-driven (ship every N writes): HTTP wins 2.1x at flush=1, 1.1x at flush=10, ties at flush=100. WS ack-resolver pattern has higher per-batch overhead than HTTP fetch.
- HTTP wins 10/12 operational categories: stateless, proxy-friendly, restart-resilient, universal `fetch()`, no connection management, mesh = 0 persistent connections.
- WS only wins for client-facing (browser/mobile server-push) — that belongs in client SDK (Phase 2/6), not core engine.

**What replaces WS for real-time:**

- `batchMs: 1` — already supported, gives 1ms sync delay with HTTP.
- Event-driven ship trigger — optional post-commit callback that ships immediately. `hook/` library already does this via `commit_hook`.

**Deliverables (closed — not pursued):**

- [x] ~~WebSocket transport option~~ — researched, not implemented. HTTP sufficient.
- [x] ~~Server-push~~ — solved by lowering `batchMs` or event-driven ship trigger.
- [x] ~~Fallback to HTTP polling~~ — HTTP is the only transport, no fallback needed.
- [ ] Subscription API (table/row changes) — deferred to Phase 2 client SDK (SSE/WS in client layer, not core engine).

### Phase 4: hook-sync Studio (Dashboard)

Web UI for managing hook-sync clusters.

**Deliverables:**

- [ ] Topology map (visual graph of nodes + connections)
- [ ] Sync lag, pending queue depth, dead letter count per node
- [ ] Throughput graphs (writes/sec, sync/sec)
- [ ] Conflict history (LWW resolutions)
- [ ] Node add/remove from UI
- [ ] Alerting (sync lag > threshold, dead letter > 0, node down)

### Phase 5: hook-sync CDC (Change Data Capture) ✅ Closed (analyzed, not implemented)

**Decision:** Full CDC platform moved to Ideas Not Pursued. No proven demand. Different product category.

**Analysis:**

- `_changes` table is already a CDC log — data is there. The question is whether routing to external sinks is hook-sync's job.
- Full CDC (Kafka/S3/Elasticsearch) = different product category. Debezium, Kafka Connect, Airbyte do this better. SQLite users don't build Kafka pipelines — they chose SQLite for simplicity.
- Each sink has different delivery semantics (ordering, retention, backpressure, retry). Not "one interface, many sinks" — 4 separate products.
- `_changes` is deleted after peer ACK — no replay capability. CDC needs durable event log with multi-consumer retention = different data model.
- Webhook sink: no proven demand. Consumer needs data? Make it a peer (already built). Consumer needs event only? Niche (Slack/Zapier integration plumbing, not core sync).
- Distracts from differentiating features (Phase 2 browser SDK, Phase 6 mobile SDK) that make hook-sync unique.

**Deliverables (closed — not pursued):**

- [x] ~~Sink interface (pluggable)~~ — overengineering for zero proven sinks
- [x] ~~Webhook sink~~ — no proven demand. Revisit if users request.
- [x] ~~Kafka/NATS sink~~ — scope creep. Use Debezium/Kafka Connect.
- [x] ~~S3 batch sink~~ — backup = Litestream's job.
- [x] ~~At-least-once delivery~~ — already built in `hooksync/` core for peer sync.

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
| CDC routing (Kafka/S3/Elasticsearch sinks) | Different product category (Debezium, Kafka Connect). SQLite users don't build Kafka pipelines. Each sink = different delivery semantics. No proven demand. Consumer needs data → make it a peer. |
| Webhook sink | No proven demand. Consumer needs data → peer sync (already built). Consumer needs event only → niche integration plumbing. Revisit if users request. |

## Decision Log

- **2026-09-01:** Roadmap created. Direction: embedded library + local-first sync engine. Chosen over standalone-server-only because the gap (stock SQLite + multi-writer + local-first + self-hostable) is unfilled by any existing project. 80% of core already built.
- **2026-09-01:** Dual-package capture strategy for Go library. `trigger` package (default, `Attach(db, config)`, no build tag, cross-runtime) + `hook` package (opt-in, `Open(path, config)`, `-tags sqlite_preupdate_hook`, Go-only, 89% faster). Shared `hooksync/` core (protocol, ship, ACK, LWW). User picks capture mode at import time. Both share wire protocol — trigger nodes sync to hook nodes. Bun/Node use trigger only (no CGO preupdate_hook).
- **2026-09-01:** Monorepo. Go (`go/` with `hooksync/`, `trigger/`, `hook/`, `mesh/`, `hub/`, `cmd/`) + JS (`js/` unified Bun/Node/browser) + docs + benchmarks in 1 repo. Protocol changes = 1 commit. Shared core atomic refactors. Single release tag, single issue tracker. Split only when release cadence, contributor teams, or licensing diverge — none apply now.
- **2026-09-01:** Phase 1 complete. Go libraries (`hooksync/`, `trigger/`, `hook/`) extracted and importable. JS library published as `hooksync.js` v0.1.3 on npm. All 4 JS wrappers (Bun single/mesh/multitable, Node single/mesh/multitable) refactored to thin wrappers over `js/` library. 36/36 split-brain tests pass. Website built with Astro Starlight, deployed to hook-sync.pages.dev. Gap "Standalone server only — not a library" resolved.
- **2026-09-01:** Phase 3 closed. Research (`RESEARCH-PHASE3-HTTP-VS-WS.md`, 14-variant benchmark) concluded HTTP wins for node-to-node sync. HTTP and WS statistically identical at same batch interval — sync delay is interval-bound, not transport-bound. Event-driven: HTTP wins 2.1x at flush=1. HTTP wins 10/12 operational categories (stateless, proxy-friendly, restart-resilient, universal fetch). WS/SSE deferred to client SDK (Phase 2/6) where server-push is genuinely needed. Real-time latency achievable via `batchMs: 1` or event-driven ship trigger (commit_hook callback) — no WS required. Gap "No real-time subscriptions" closed for node-to-node; subscription API deferred to client SDK layer.
- **2026-09-01:** Phase 5 closed. Full CDC (Kafka/S3/Elasticsearch/webhook sinks) moved to Ideas Not Pursued. Analysis: full CDC = different product category (Debezium, Kafka Connect). SQLite users chose SQLite for simplicity, not for building Kafka pipelines. Each sink has different delivery semantics — not "one interface, many sinks" but 4 separate products. `_changes` deleted after peer ACK = no replay capability (CDC needs durable event log). Webhook: no proven demand — consumer needs data → make it a peer (already built); consumer needs event only → niche integration plumbing. Distracts from differentiating features (browser/mobile SDK). Gap "No CDC routing" closed.
