#!/usr/bin/env bash
# bench-dual-ack.sh — Benchmark ACK-based dual-writer sync, 1 runtime at a time.
# Usage: bash bench-dual-ack.sh
set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(pwd)"

RUNS=10
REQ=100  # per node per run (200 total dual-writer)

cleanup() {
	pkill -9 -f "hook-sync-go|bun/server|node server" 2>/dev/null || true
	sleep 2
	rm -f "$ROOT"/*.db "$ROOT"/*.db-wal "$ROOT"/*.db-shm 2>/dev/null || true
}

benchmark_runtime() {
	local label="$1"
	local start_cmd_a="$2"
	local start_cmd_b="$3"
	local port_a="$4"
	local port_b="$5"

	cleanup
	echo "=== $label ==="

	# Start both nodes
	eval "$start_cmd_a" &>/tmp/bench_a.log &
	eval "$start_cmd_b" &>/tmp/bench_b.log &
	sleep 2

	# Verify both up
	local ha hb
	ha=$(curl -sf "http://localhost:$port_a/health" 2>/dev/null || echo "FAIL")
	hb=$(curl -sf "http://localhost:$port_b/health" 2>/dev/null || echo "FAIL")
	if [[ "$ha" == "FAIL" || "$hb" == "FAIL" ]]; then
		echo "  ERROR: nodes not ready"
		echo "  A log: $(tail -3 /tmp/bench_a.log 2>/dev/null | tr '\n' '|')"
		echo "  B log: $(tail -3 /tmp/bench_b.log 2>/dev/null | tr '\n' '|')"
		cleanup
		return
	fi
	echo "  nodes ready"

	# Run benchmark via bun (fast HTTP client)
	bun -e "
const A='http://localhost:$port_a', B='http://localhost:$port_b';
const RUNS=$RUNS, REQ=$REQ;

async function dualWrite(n) {
  const t0 = performance.now();
  const p = [];
  for (let i = 0; i < n; i++) p.push(fetch(A+'/api/items',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:'a'+i,value:i})}).then(r=>r.json()));
  for (let i = 0; i < n; i++) p.push(fetch(B+'/api/items',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({name:'b'+i,value:i})}).then(r=>r.json()));
  await Promise.all(p);
  return Math.round((n*2) / ((performance.now()-t0)/1000));
}

async function checkIntegrity() {
  await new Promise(r => setTimeout(r, 1500));
  const ha = await fetch(A+'/health').then(r=>r.json());
  const hb = await fetch(B+'/health').then(r=>r.json());
  const aTotal = ha.item_count;
  const bTotal = hb.item_count;
  const pass = aTotal === bTotal && ha.pending_changes === 0 && hb.pending_changes === 0 && ha.dead_letter === 0 && hb.dead_letter === 0;
  return { aTotal, bTotal, aPending: ha.pending_changes, bPending: hb.pending_changes, aDead: ha.dead_letter, bDead: hb.dead_letter, pass };
}

// Warmup
await dualWrite(20);
await new Promise(r => setTimeout(r, 1000));

const qps = [];
const integ = [];
let expectedTotal = 0;

for (let i = 0; i < RUNS; i++) {
  const q = await dualWrite(REQ);
  qps.push(q);
  expectedTotal += REQ * 2;
  const c = await checkIntegrity();
  integ.push(c.pass);
  if (!c.pass) {
    console.error('  run '+(i+1)+' FAIL: A='+c.aTotal+' B='+c.bTotal+' pendingA='+c.aPending+' pendingB='+c.bPending+' deadA='+c.aDead+' deadB='+c.bDead);
  }
}

qps.sort((a,b) => a - b);
const passCount = integ.filter(x=>x).length;
console.log('  QPS: min='+qps[0]+' med='+qps[5]+' max='+qps[9]);
console.log('  Integrity: '+passCount+'/'+RUNS+' PASS (expected '+expectedTotal+' items per node)');
console.log('  All QPS: '+qps.join(', '));
" 2>&1

	cleanup
	echo
}

# Build Go binary first
echo "Building Go..."
cd "$ROOT/go" && go build -o "$ROOT/hook-sync-go" . 2>&1
cd "$ROOT"

echo
echo "############################################"
echo "# ACK-based dual-writer benchmark"
echo "# ${RUNS} runs × ${REQ} req per node (200 total per run)"
echo "############################################"
echo

# --- Go ---
benchmark_runtime "GO" \
	"$ROOT/hook-sync-go -id goA -db $ROOT/goA.db -listen :9001 -peer http://localhost:9002 -batch-ms 50" \
	"$ROOT/hook-sync-go -id goB -db $ROOT/goB.db -listen :9002 -peer http://localhost:9001 -batch-ms 50" \
	9001 9002

# --- Bun ---
benchmark_runtime "BUN" \
	"bun run $ROOT/bun/server.ts --id bunA --db $ROOT/bunA.db --listen :9001 --peer http://localhost:9002 --batch-ms 50" \
	"bun run $ROOT/bun/server.ts --id bunB --db $ROOT/bunB.db --listen :9002 --peer http://localhost:9001 --batch-ms 50" \
	9001 9002

# --- Node ---
benchmark_runtime "NODE" \
	"cd $ROOT/node && node server.js --id nodeA --db $ROOT/nodeA.db --listen :9001 --peer http://localhost:9002 --batch-ms 50" \
	"cd $ROOT/node && node server.js --id nodeB --db $ROOT/nodeB.db --listen :9002 --peer http://localhost:9001 --batch-ms 50" \
	9001 9002

echo "############################################"
echo "# Done"
echo "############################################"
