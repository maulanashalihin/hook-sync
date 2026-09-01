#!/usr/bin/env bash
# bench-trigger.sh — Measure trigger overhead through HTTP
#
# Compares HTTP write throughput:
#   1. Baseline: Go server without triggers (no _changes, no sync)
#   2. With triggers: Go server with production hook-sync schema (triggers + _changes)
#
# Both serve same HTTP API (POST /api/items), same Fiber + SQLite + UUID.
# If trigger overhead is measurable through HTTP, baseline should be faster.
#
# Usage: bash bench-trigger.sh

set -uo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"

PORT=19010
RUNS=10
REQ=200 # concurrent writes per run

cleanup() {
	pkill -9 -f "hook-sync-go.*19010" 2>/dev/null || true
	sleep 1
	rm -f "$ROOT"/trigger-bench-*.db "$ROOT"/trigger-bench-*.db-wal "$ROOT"/trigger-bench-*.db-shm 2>/dev/null || true
}

run_bench() {
	local label="$1"
	local extra_flags="$2"

	cleanup
	echo "=== $label ==="

	nohup "$ROOT/hook-sync-go" -id trigger-bench -db "$ROOT/trigger-bench.db" -listen ":$PORT" $extra_flags >/tmp/trigger-bench.log 2>&1 &
	sleep 2

	local h
	h=$(curl -sf "http://localhost:$PORT/health" 2>/dev/null || echo "FAIL")
	if [[ "$h" == "FAIL" ]]; then
		echo "  ERROR: server not ready"
		echo "  log: $(tail -3 /tmp/trigger-bench.log 2>/dev/null | tr '\n' '|')"
		cleanup
		return
	fi
	echo "  server ready"

	bun -e "
const URL='http://localhost:$PORT';
const RUNS=$RUNS, REQ=$REQ;

async function writeBatch(n) {
  const t0 = performance.now();
  const p = [];
  for (let i = 0; i < n; i++) p.push(fetch(URL+'/api/items',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:'item-'+i,value:i})}).then(r=>r.json()));
  await Promise.all(p);
  return Math.round(n / ((performance.now()-t0)/1000));
}

// Warmup
await writeBatch(20);
await new Promise(r => setTimeout(r, 500));

const qps = [];
for (let i = 0; i < RUNS; i++) {
  const q = await writeBatch(REQ);
  qps.push(q);
}
qps.sort((a,b) => a - b);
console.log('  QPS: min='+qps[0]+' med='+qps[5]+' max='+qps[9]);
console.log('  All QPS: '+qps.join(', '));
" 2>&1

	cleanup
	echo
}

# Build Go binary
echo "Building Go..."
cd "$ROOT/go" && go build -o "$ROOT/hook-sync-go" ./cmd/server 2>&1
cd "$ROOT"
echo

echo "############################################"
echo "# Trigger Overhead Benchmark (HTTP)"
echo "# ${RUNS} runs × ${REQ} concurrent writes"
echo "############################################"
echo

# 1. Baseline: no triggers
run_bench "BASELINE (no triggers, no sync)" "-no-trigger"

# 2. With triggers (production schema, no peer)
run_bench "WITH TRIGGERS (production schema, no peer)" ""

echo "############################################"
echo "# Done — compare QPS medians"
echo "############################################"
