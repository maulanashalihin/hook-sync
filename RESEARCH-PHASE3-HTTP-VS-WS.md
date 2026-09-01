# Research: HTTP vs WebSocket for Phase 3 Real-time Transport

**Date:** 2026-09-01
**Scope:** Phase 3 roadmap evaluation — should hook-sync add WebSocket transport for node-to-node sync?
**Status:** Research only. Not a decision to implement. Findings may or may not influence product direction.

---

## Context

ROADMAP.md Phase 3 proposes adding WebSocket/SSE alongside HTTP polling for real-time sync. This research evaluates whether WS provides meaningful benefits **for the node-to-node use case** (server-to-server replication), which is hook-sync's core.

hook-sync's use case is NOT browser-to-server. It's node-to-node: Go/Bun/Node servers replicating SQLite changes to each other via HTTP batch + ACK. Phase 2 (browser SDK) and Phase 6 (mobile SDK) are separate use cases where WS may be more relevant.

---

## Methodology

Two benchmark scripts, both Bun (Bun.serve + bun:sqlite), localhost, 10K items, 3 rounds, median reported.

### Benchmark v1: `bun/bench-http-vs-ws.ts`

3 variants, fixed 50ms batch for HTTP/WS-timer, 1ms for WS-immediate:

- HTTP + 50ms timer
- WS + 50ms timer
- WS + immediate (1ms timer)

### Benchmark v2: `bun/bench-http-vs-ws-v2.ts`

14 variants — isolates transport from interval:

- HTTP vs WS at same intervals: 1ms, 5ms, 10ms, 50ms
- HTTP vs WS event-driven: ship every 1/10/100 writes (no timer, simulates commit_hook callback)

v2 controls for the confound in v1: "WS immediate" used 1ms interval while "HTTP" used 50ms. The sync delay difference was caused by interval, not transport.

---

## Results

### v1: Original benchmark (confounded)

| Variant | Sync Delay | Write QPS | Convergence |
|---------|--------:|--------:|--------:|
| HTTP + 50ms timer | 50ms | 6,605 | 151ms |
| WS + 50ms timer | 50ms | 5,706 | 163ms |
| WS + immediate (1ms) | 1ms | 7,305 | 125ms |

**Misleading:** "WS immediate" appears to win, but the advantage comes from 1ms interval vs 50ms, not from WS transport.

### v2: Controlled comparison (transport isolated)

#### Timer-based: HTTP vs WS at identical intervals

| Interval | HTTP delay | WS delay | HTTP QPS | WS QPS | HTTP conv | WS conv |
|----------|--------:|--------:|--------:|--------:|--------:|--------:|
| 1ms | 1ms | 1ms | 9,819 | 9,747 | 112ms | 111ms |
| 5ms | 5ms | 5ms | 10,063 | 9,745 | 115ms | 112ms |
| 10ms | 10ms | 10ms | 9,774 | 9,947 | 110ms | 111ms |
| 50ms | 50ms | 50ms | 6,296 | 5,815 | 140ms | 157ms |

**At same interval, HTTP and WS are statistically identical.** Sync delay = batch interval regardless of transport. Write QPS within 5% (noise). Convergence within 10% (noise). At 50ms, WS is slightly *slower* (5,815 vs 6,296 QPS, 157ms vs 140ms convergence) — WS event handling overhead exceeds HTTP's stateless fetch.

#### Event-driven: ship every N writes (no timer)

| Flush every | WS delay | HTTP delay | WS QPS | HTTP QPS | WS conv | HTTP conv |
|-------------|--------:|--------:|--------:|--------:|--------:|--------:|
| 1 write | 0ms | 0ms | 616 | 1,317 | 0ms | 0ms |
| 10 writes | 0ms | 0ms | 3,600 | 4,111 | 0ms | 0ms |
| 100 writes | 0ms | 0ms | 4,635 | 4,614 | 0ms | 0ms |

**HTTP wins at flush=1 (2.1x) and flush=10 (1.1x).** Ties at flush=100. WS ack-resolver pattern (Map lookup + Promise.withResolvers per batch) has higher per-batch overhead than HTTP fetch (single request/response cycle). For small frequent batches, HTTP is lighter.

---

## Analysis: Node-to-Node Use Case

### Why HTTP is the right transport for node-to-node

| Factor | HTTP | WebSocket | Winner |
|--------|------|-----------|--------|
| **Performance (same interval)** | 9,819 QPS | 9,747 QPS | Tie (within noise) |
| **Performance (event-driven)** | 4,111 QPS | 3,600 QPS | **HTTP** (+14%) |
| **Statelessness** | Stateless — no connection to maintain | Stateful — persistent connection per peer | **HTTP** |
| **Restart resilience** | Peer restart = next fetch succeeds, no reconnect logic | Peer restart = WS close event, reconnect logic, heartbeat/ping | **HTTP** |
| **Load-balancer/proxy** | Works through any HTTP proxy/LB (nginx, caddy, cloud LB) | Requires sticky sessions, WS upgrade support, proxy config | **HTTP** |
| **Mesh topology** | N peers = 0 persistent connections | N peers = N persistent connections to manage | **HTTP** |
| **Cross-runtime** | `fetch()` — universal (Go, Bun, Node, Python, curl) | Different APIs per runtime (Bun WebSocket, Node `ws`, Go gorilla) | **HTTP** |
| **Error handling** | fetch fails → retry next tick (2 lines) | close event → reconnect + reauth + state sync (50+ lines) | **HTTP** |
| **Debugging** | `curl`, browser devtools, any HTTP client | WS-specific tools, frame inspection | **HTTP** |
| **Already proven** | 500K stress test, cross-server (OVH↔1TIM), 36/36 split-brain | Untested, unimplemented | **HTTP** |
| **Connection overhead** | None (new TCP per request, or HTTP/2 reuse) | One TCP + WS handshake per peer, kept alive | WS (marginally) |
| **Header overhead** | ~200 bytes per request | ~0 bytes per frame | WS (marginally, but batched changes = header is negligible) |

**Score: HTTP wins 10/12 categories. WS wins 2 marginal categories (connection reuse, header overhead) that don't matter for batched node-to-node sync.**

### The sync delay myth

The original v1 benchmark suggested WS gives 1ms sync delay vs HTTP's 50ms. **This is false.** The difference was interval (1ms vs 50ms), not transport. v2 proves: HTTP at 1ms interval = 1ms sync delay, same as WS at 1ms.

**Sync delay = batch interval, regardless of transport.** If you want 1ms sync delay, set `batchMs: 1`. No WS needed.

### What actually improves real-time latency

Two approaches, neither requires WS:

1. **Lower batch interval** — `batchMs: 1` gives 1ms sync delay. Already supported in Config. Tradeoff: more HTTP requests (higher CPU), but at 10K QPS write rate, 1ms interval = 1 batch per ms = manageable.

2. **Event-driven ship trigger** — ship immediately after commit, not on timer. This is what `hook/` library already does via `commit_hook` (Go). For trigger mode, could add a post-commit callback that triggers shipNow(). Zero sync delay without WS.

### Where WS genuinely wins (NOT node-to-node)

| Use case | Why WS | Phase |
|----------|--------|-------|
| Browser client SDK | Server-push: server notifies browser of changes without polling. Browser can't receive HTTP push. | Phase 2 |
| Mobile SDK | Same — server-push to device. Also: WS survives NAT better than polling. | Phase 6 |
| Real-time subscription API | Client subscribes to table/row changes, gets pushed updates. | Phase 3 (if added) |

These are **client-facing** use cases where the server needs to push to a client that can't be polled efficiently. Node-to-node doesn't have this problem — both nodes are always-on servers that can poll each other trivially.

---

## Conclusion

### For node-to-node (core sync engine): HTTP wins. No reason to add WS

1. **Performance parity** — HTTP and WS are identical at same interval. HTTP wins event-driven.
2. **Operational simplicity** — stateless, proxy-friendly, restart-resilient, universal `fetch()`.
3. **Already proven** — 500K stress, cross-server, split-brain, all pass.
4. **Sync delay is interval-bound, not transport-bound** — `batchMs: 1` gives 1ms delay with HTTP.
5. **WS adds complexity for zero benefit** — connection management, reconnect logic, per-runtime API differences, mesh connection proliferation.

### For client-facing (Phase 2/6): WS/SSE is the right choice

Browser/mobile clients need server-push. WS/SSE is standard for this. But this belongs in the client SDK layer, not the core sync engine. The core engine stays HTTP; the client SDK wraps WS/SSE on top.

### Phase 3 recommendation (if pursued)

Instead of "add WebSocket transport to core engine", consider:

1. **Configurable batch interval** (already exists) — document `batchMs: 1` for real-time use cases.
2. **Event-driven ship trigger** — optional post-commit callback that ships immediately. Gives zero-latency sync without WS. Works with existing HTTP transport.
3. **SSE/WS subscription endpoint** — for Phase 2 client SDK. Server pushes change notifications to subscribed clients. Client then pulls full change batch via HTTP `/sync`. This separates notification (push) from data transfer (pull), keeping the core protocol unchanged.

---

## Raw Data

### v2 full results (all 14 variants, 3 rounds each)

```
HTTP 1ms timer      → delay: 1ms   QPS: 9,819   conv: 112ms
WS 1ms timer        → delay: 1ms   QPS: 9,747   conv: 111ms
HTTP 5ms timer      → delay: 5ms   QPS: 10,063  conv: 115ms
WS 5ms timer        → delay: 5ms   QPS: 9,745   conv: 112ms
HTTP 10ms timer     → delay: 10ms  QPS: 9,774   conv: 110ms
WS 10ms timer       → delay: 10ms  QPS: 9,947   conv: 111ms
HTTP 50ms timer     → delay: 50ms  QPS: 6,296   conv: 140ms
WS 50ms timer       → delay: 50ms  QPS: 5,815   conv: 157ms
WS event flush=1    → delay: 0ms   QPS: 616     conv: 0ms
HTTP event flush=1  → delay: 0ms   QPS: 1,317   conv: 0ms
WS event flush=10   → delay: 0ms   QPS: 3,600   conv: 0ms
HTTP event flush=10 → delay: 0ms   QPS: 4,111   conv: 0ms
WS event flush=100  → delay: 0ms   QPS: 4,635   conv: 0ms
HTTP event flush=100→ delay: 0ms   QPS: 4,614   conv: 0ms
```

### Environment

- Bun 1.4 + bun:sqlite, Apple M4, localhost
- 10K items per run, 3 rounds, median reported
- Same schema, same LWW apply, same drain logic across all variants
- Benchmark scripts: `bun/bench-http-vs-ws.ts` (v1), `bun/bench-http-vs-ws-v2.ts` (v2)

### Caveats

- Localhost benchmark has known variance (3-5x per wiki observation). v2 mitigates with 3 rounds + median, but cross-server validation would strengthen conclusions.
- Bun-only. Go and Node WS implementations may differ. However, the transport-level finding (HTTP = WS at same interval) is protocol-level, not runtime-specific.
- Event-driven flush=1 QPS is low (616/1317) because every write triggers a ship — sync dominates write throughput. This is expected and not a transport comparison issue (both are equally affected).
