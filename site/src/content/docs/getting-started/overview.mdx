---
title: Overview
description: SQLite replication that just works. Multi-server, multi-writer, multi-runtime. Zero data loss.
---

import { Card, CardGrid } from '@astrojs/starlight/components';

# hook-sync

> SQLite replication that just works. Multi-server, multi-writer, multi-runtime. Zero data loss.

SQLite is the fastest SQL database in the world — zero config, single file, serverless. But it can't replicate. Until now.

hook-sync adds replication to SQLite via triggers + HTTP sync. No consensus algorithm. No Raft. No coordinator. Just triggers, ACK, and UUID. The result: **3.8x faster than Postgres at batch 10K**, with multi-writer active-active, crash recovery, and split-brain safety.

## Why hook-sync?

| Problem | Postgres | Litestream | rqlite | hook-sync |
|---------|----------|------------|--------|-----------|
| Replication | ✅ WAL streaming | ✅ WAL to S3 | ✅ Raft consensus | ✅ Trigger + ACK |
| Multi-writer | ❌ Primary-only | ❌ Read-only replica | ❌ Leader-only | ✅ All nodes write |
| Zero write penalty | ❌ WAL sender overhead | ✅ Async | ❌ Raft quorum per write | ✅ Async, 0% overhead |
| Cross-runtime | ❌ C server only | ❌ Go only | ❌ Go only | ✅ Go, Bun, Node interop |
| Split-brain safety | ✅ Sync replication | ❌ | ✅ Raft | ✅ Timestamp LWW |
| Crash recovery | ✅ WAL replay | ✅ S3 restore | ✅ Raft log | ✅ `_changes` replay |
| Speed (batch 10K) | 8,278 QPS | N/A | N/A (Raft quorum) | **31,558 QPS** |

hook-sync occupies a gap no other project fills: **SQLite simplicity + multi-writer replication + cross-runtime interop**, without consensus overhead.

<CardGrid stagger>
 <Card title="Quick Start" icon="rocket">
  Get up and running in 5 minutes with Go, Bun, or Node.
 </Card>
 <Card title="Libraries" icon="package">
  Importable packages for Go and JS — trigger-based or hook-based capture.
 </Card>
 <Card title="Topologies" icon="diagram">
  Point-to-point, full mesh, dedicated hub, multi-region.
 </Card>
 <Card title="Wire Protocol" icon="document">
  Language-agnostic protocol spec — implement in any language.
 </Card>
</CardGrid>
