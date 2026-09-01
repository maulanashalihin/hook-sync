# Publication Kit — hook-sync

Copy-paste ready content untuk launch. Posting butuh akun Anda sendiri.

---

## 1. Show HN (Hacker News)

**Title (max 80 char):**

```
Show HN: hook-sync – SQLite replication with multi-writer, no Raft, 3.8x faster than Postgres
```

**Body:**

```
Hi HN, I built hook-sync — SQLite replication that gives you multi-writer active-active sync across Go, Bun, and Node.js, without any consensus algorithm.

The problem: SQLite is the fastest SQL database in the world, but it can't replicate. Existing solutions all have tradeoffs:

- Litestream: read-only replication (backup, not multi-writer)
- rqlite: Raft consensus (leader-only writes, quorum overhead)
- libSQL/Turso: SQLite fork (not stock SQLite, hosted)
- ElectricSQL/PowerSync: Postgres + CRDT, hosted/paid

hook-sync takes a different approach: SQL triggers capture changes → HTTP batch sync → ACK-based retry → last-write-wins by timestamp. No Raft, no coordinator, no fork. Just triggers + HTTP + UUID.

How it works:
1. Attach to your existing SQLite db (one function call)
2. Library auto-creates triggers + _changes table
3. Background loop ships changes to peers every 50ms
4. Peers ACK; changes deleted only after ACK
5. Conflict resolution: last-write-wins by updated_at timestamp

Benchmark (batch 10K, localhost):
- hook-sync: 31,558 QPS
- Postgres: 8,278 QPS
- 0% write penalty — sync runs in background, write speed identical with or without peers

4 topologies built and tested:
- Point-to-point (2 nodes)
- Full mesh (3-7 nodes)
- Dedicated hub (8+ nodes, Go relay with Pebble KV)
- Multi-region (hub-to-hub)

Split-brain safe: 36/36 convergence tests pass. Both nodes always converge to the same state.

What's intentionally missing (and why):
- No consensus — UUID PKs mean zero conflict, no coordinator needed
- No CRDT — timestamp LWW is simpler and sufficient for most apps
- No fork — works with stock SQLite (mattn/go-sqlite3, bun:sqlite, better-sqlite3)

GitHub: https://github.com/maulanashalihin/hook-sync
Docs: https://hook-sync.pages.dev
npm: https://www.npmjs.com/package/hooksync.js

I'd love feedback on the protocol design, especially the trigger-based capture approach vs WAL shipping or CDC.
```

---

## 2. Twitter/X Thread

```
1/ SQLite is the fastest SQL database in the world.

But it can't replicate.

Until now.

I built hook-sync — multi-writer SQLite replication for Go, Bun, and Node.js.

No Raft. No coordinator. No fork.

🧵👇

2/ The approach is dead simple:

• SQL triggers capture changes
• HTTP batch sync to peers (50ms)
• ACK-based retry
• Last-write-wins by timestamp
• UUID PKs = zero conflict

No consensus algorithm. No leader election. Just triggers + HTTP + UUID.

3/ Why not existing solutions?

Litestream → read-only (backup, not sync)
rqlite → Raft quorum (leader-only writes)
libSQL/Turso → SQLite fork (not stock)
ElectricSQL → Postgres + CRDT (hosted)

hook-sync = stock SQLite + multi-writer + self-hostable + open-source

4/ Benchmark (batch 10K, localhost):

hook-sync: 31,558 QPS
Postgres:   8,278 QPS

3.8x faster. 0% write penalty — sync runs in background.

Write speed identical with or without peers.

5/ Split-brain safe.

Both nodes always converge to the same state. 36/36 convergence tests pass.

Crash recovery: changes survive in SQLite _changes table. Resume on restart. Zero data loss.

6/ 4 topologies, all built and tested:

↔ Point-to-point (2 nodes)
🕸 Full mesh (3-7 nodes)
⭐ Dedicated hub (8+ nodes, Pebble KV)
🌍 Multi-region (hub-to-hub)

7/ GitHub: https://github.com/maulanashalihin/hook-sync
Docs: https://hook-sync.pages.dev
npm: npm install hooksync.js

Open-source. Self-hostable. No vendor lock-in.

If you build with SQLite, give it a try. Feedback welcome 🙏
```

---

## 3. Reddit — r/SQLite

**Title:**

```
hook-sync: multi-writer SQLite replication without Raft or fork (open-source)
```

**Body:**

```
I built a SQLite replication library that does multi-writer active-active sync — no consensus algorithm, no SQLite fork, works with stock SQLite.

How: SQL triggers → _changes table → HTTP batch sync → ACK retry → last-write-wins by timestamp. UUID PKs mean zero conflict.

Works across Go (mattn/go-sqlite3), Bun (bun:sqlite), and Node.js (better-sqlite3). All three sync to each other transparently.

Benchmark: 31,558 QPS at batch 10K (3.8x Postgres), 0% write penalty.

GitHub: https://github.com/maulanashalihin/hook-sync
Docs: https://hook-sync.pages.dev

Compared to:
- Litestream: read-only, not multi-writer
- rqlite: Raft, leader-only writes
- libSQL: fork of SQLite, not stock

Would love feedback from SQLite users — does trigger-based capture work for your use case?
```

---

## 4. Reddit — r/golang

**Title:**

```
hook-sync: SQLite replication library for Go — trigger capture + HTTP sync, no Raft
```

**Body:**

```
I built a Go library for SQLite replication that does multi-writer active-active sync without Raft or any consensus algorithm.

Two capture modes:
- trigger/ — SQL triggers + _changes table (cross-runtime, syncs with Bun/Node)
- hook/ — preupdate_hook + Pebble KV (35% faster, Go-only)

Both share the same wire protocol. Manager implements http.Handler, so /sync endpoint is one line: `http.Handle("/sync", mgr)`.

Benchmark: 31,558 QPS at batch 10K, 0% write penalty (sync runs in background).

go get hook-sync/go/hooksync
go get hook-sync/go/trigger

GitHub: https://github.com/maulanashalihin/hook-sync
Docs: https://hook-sync.pages.dev/runtimes/go/

Pre-built binaries included: single-peer, mesh, hookserver, hub (Pebble KV relay for 8+ nodes).
```

---

## 5. Reddit — r/node

**Title:**

```
hook-sync: multi-writer SQLite replication for Node.js (better-sqlite3) — no Raft, 0% write penalty
```

**Body:**

```
npm install hooksync.js

I built a SQLite replication library for Node.js that does multi-writer active-active sync. Works with better-sqlite3. No consensus algorithm, no leader election.

How: SQL triggers capture changes → HTTP batch sync to peers → ACK retry → last-write-wins by timestamp.

You wire your own HTTP server (http.createServer, Express, Hono — anything). The library handles triggers, ship loop, conflict resolution, and crash recovery.

Syncs cross-runtime: Node.js nodes sync with Go and Bun nodes transparently. Same wire protocol.

Benchmark: 31,558 QPS at batch 10K, 0% write penalty.

GitHub: https://github.com/maulanashalihin/hook-sync
Docs: https://hook-sync.pages.dev/runtimes/node/
npm: https://www.npmjs.com/package/hooksync.js
```

---

## Posting Checklist

- [ ] Show HN — post ke <https://news.ycombinator.com/submit> (pilih "Show HN")
- [ ] Twitter/X — thread, schedule peak time (9-11am EST weekday)
- [ ] r/SQLite — <https://www.reddit.com/r/sqlite/submit>
- [ ] r/golang — <https://www.reddit.com/r/golang/submit>
- [ ] r/node — <https://www.reddit.com/r/node/submit>
- [ ] awesome-go PR — <https://github.com/avelino/awesome-go>
- [ ] awesome-sqlite PR — <https://github.com/pseudoxor/awesome-sqlite>
- [ ] Dev.to — cross-post tutorial version
- [ ] GitHub README — tambah demo GIF + badge

## Timing

- Post Show HN Selasa-Rabu 8-10am EST (traffic tertinggi)
- Twitter thread 1 jam setelah HN
- Reddit cross-post 2-3 jam setelah HN (kalau HN dapat traction)
- Jangan post semua sekaligus — stagger supaya tidak spam
