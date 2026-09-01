#!/usr/bin/env bash
# bench-fullmesh.sh — Benchmark full mesh topology (4 nodes, all-to-all sync)
# Usage: bash bench-fullmesh.sh
set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(pwd)"

RUNS=5
REQ=50 # per node per run (200 total across 4 nodes)
NODES=4

# Ports: 9001, 9002, 9003, 9004
PORTS=(9001 9002 9003 9004)

cleanup() {
	pkill -9 -f "hook-sync-mesh-go|server-mesh" 2>/dev/null || true
	sleep 1
	rm -f "$ROOT"/mesh-*.db "$ROOT"/mesh-*.db-wal "$ROOT"/mesh-*.db-shm 2>/dev/null || true
}

# Build peer args for a node, excluding its own port
peer_args() {
	local self_port="$1"
	local args=""
	for p in "${PORTS[@]}"; do
		if [[ "$p" != "$self_port" ]]; then
			args+=" --peer http://localhost:$p"
		fi
	done
	echo "$args"
}

benchmark_runtime() {
	local label="$1"
	shift
	local start_cmds=("$@")

	cleanup
	echo "=== $label — full mesh ($NODES nodes) ==="

	# Start all nodes
	local logs=()
	for i in "${!start_cmds[@]}"; do
		local port="${PORTS[$i]}"
		local log="/tmp/mesh_${label}_${port}.log"
		logs+=("$log")
		eval "${start_cmds[$i]}" &>"$log" &
	done
	sleep 3

	# Verify all nodes up
	local all_ok=true
	for port in "${PORTS[@]}"; do
		local h
		h=$(curl -sf "http://localhost:$port/health" 2>/dev/null || echo "FAIL")
		if [[ "$h" == "FAIL" ]]; then
			echo "  ERROR: node :$port not ready"
			all_ok=false
		fi
	done
	if ! $all_ok; then
		for log in "${logs[@]}"; do
			echo "  log $log: $(tail -3 "$log" 2>/dev/null | tr '\n' '|')"
		done
		cleanup
		return
	fi
	echo "  all $NODES nodes ready"

	# Run benchmark via bun (fast HTTP client)
	bun -e "
const PORTS = [$(
		IFS=,
		echo "${PORTS[*]}"
	)];
const RUNS = $RUNS, REQ = $REQ, NODES = $NODES;
const URLS = PORTS.map(p => 'http://localhost:' + p);

async function meshWrite(n) {
  const t0 = performance.now();
  const p = [];
  for (let i = 0; i < n; i++) {
    for (const url of URLS) {
      p.push(fetch(url + '/api/items', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name: 'x' + i, value: i})
      }).then(r => r.json()));
    }
  }
  await Promise.all(p);
  return Math.round((n * NODES) / ((performance.now() - t0) / 1000));
}

async function checkIntegrity() {
  await new Promise(r => setTimeout(r, 2000));
  const healths = await Promise.all(URLS.map(u => fetch(u + '/health').then(r => r.json())));
  const counts = healths.map(h => h.item_count);
  const pending = healths.map(h => h.pending_changes);
  const dead = healths.map(h => h.dead_letter);
  const allEqual = counts.every(c => c === counts[0]);
  const noPending = pending.every(p => p === 0);
  const noDead = dead.every(d => d === 0);
  const pass = allEqual && noPending && noDead;
  return { counts, pending, dead, pass };
}

// Warmup
await meshWrite(10);
await new Promise(r => setTimeout(r, 1000));

const qps = [];
const integ = [];
let expectedTotal = 0;

for (let i = 0; i < RUNS; i++) {
  const q = await meshWrite(REQ);
  qps.push(q);
  expectedTotal += REQ * NODES;
  const c = await checkIntegrity();
  integ.push(c.pass);
  if (!c.pass) {
    console.error('  run ' + (i+1) + ' FAIL: counts=[' + c.counts.join(',') + '] pending=[' + c.pending.join(',') + '] dead=[' + c.dead.join(',') + ']');
  }
}

qps.sort((a, b) => a - b);
const passCount = integ.filter(x => x).length;
const med = qps[Math.floor(qps.length / 2)];
console.log('  QPS: min=' + qps[0] + ' med=' + med + ' max=' + qps[qps.length - 1]);
console.log('  Integrity: ' + passCount + '/' + RUNS + ' PASS (expected ' + expectedTotal + ' items per node)');
console.log('  All QPS: ' + qps.join(', '));
" 2>&1

	cleanup
	echo
}

# Build Go mesh binary first
echo "Building Go mesh..."
cd "$ROOT/go" && go build -o "$ROOT/hook-sync-mesh-go" ./cmd/mesh 2>&1
cd "$ROOT"
echo

echo "############################################"
echo "# Full mesh benchmark — $NODES nodes, all-to-all"
echo "# ${RUNS} runs × ${REQ} req per node ($((REQ * NODES)) total per run)"
echo "############################################"
echo

# --- Go ---
benchmark_runtime "GO" \
	"$ROOT/hook-sync-mesh-go -id mesh-go1 -db $ROOT/mesh-go1.db -listen :9001$(peer_args 9001) -batch-ms 50" \
	"$ROOT/hook-sync-mesh-go -id mesh-go2 -db $ROOT/mesh-go2.db -listen :9002$(peer_args 9002) -batch-ms 50" \
	"$ROOT/hook-sync-mesh-go -id mesh-go3 -db $ROOT/mesh-go3.db -listen :9003$(peer_args 9003) -batch-ms 50" \
	"$ROOT/hook-sync-mesh-go -id mesh-go4 -db $ROOT/mesh-go4.db -listen :9004$(peer_args 9004) -batch-ms 50"

# --- Bun ---
benchmark_runtime "BUN" \
	"bun run $ROOT/bun/server-mesh.ts --id mesh-bun1 --db $ROOT/mesh-bun1.db --listen :9001$(peer_args 9001) --batch-ms 50" \
	"bun run $ROOT/bun/server-mesh.ts --id mesh-bun2 --db $ROOT/mesh-bun2.db --listen :9002$(peer_args 9002) --batch-ms 50" \
	"bun run $ROOT/bun/server-mesh.ts --id mesh-bun3 --db $ROOT/mesh-bun3.db --listen :9003$(peer_args 9003) --batch-ms 50" \
	"bun run $ROOT/bun/server-mesh.ts --id mesh-bun4 --db $ROOT/mesh-bun4.db --listen :9004$(peer_args 9004) --batch-ms 50"

# --- Node ---
benchmark_runtime "NODE" \
	"cd $ROOT/node && node server-mesh.js --id mesh-node1 --db $ROOT/mesh-node1.db --listen :9001$(peer_args 9001) --batch-ms 50" \
	"cd $ROOT/node && node server-mesh.js --id mesh-node2 --db $ROOT/mesh-node2.db --listen :9002$(peer_args 9002) --batch-ms 50" \
	"cd $ROOT/node && node server-mesh.js --id mesh-node3 --db $ROOT/mesh-node3.db --listen :9003$(peer_args 9003) --batch-ms 50" \
	"cd $ROOT/node && node server-mesh.js --id mesh-node4 --db $ROOT/mesh-node4.db --listen :9004$(peer_args 9004) --batch-ms 50"

echo "############################################"
echo "# Done"
echo "############################################"
