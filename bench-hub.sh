#!/usr/bin/env bash
# bench-hub.sh — Benchmark dedicated hub topology (1 Go hub + 3 edges, star)
# Usage: bash bench-hub.sh
set -euo pipefail

cd "$(dirname "$0")"
ROOT="$(pwd)"

RUNS=5
REQ=50 # per edge per run (150 total across 3 edges)
EDGES=3

# Hub port
HUB_PORT=9010
# Edge ports
PORTS=(9001 9002 9003)

cleanup() {
	pkill -9 -f "hook-sync-hub|hook-sync-mesh-go|server-mesh" 2>/dev/null || true
	sleep 1
	rm -rf "$ROOT"/hub-bench-*.pebble "$ROOT"/hub-bench-*.db "$ROOT"/hub-bench-*.db-wal "$ROOT"/hub-bench-*.db-shm 2>/dev/null || true
}

# Build edge args: each edge peers only to hub
edge_peer_args() {
	echo " --peer http://localhost:$HUB_PORT"
}

# Build hub edge args: hub forwards to all edges
hub_edge_args() {
	local args=""
	for p in "${PORTS[@]}"; do
		args+=" -edge http://localhost:$p"
	done
	echo "$args"
}

benchmark_runtime() {
	local label="$1"
	shift
	local start_cmds=("$@")

	cleanup
	echo "=== $label — dedicated hub ($EDGES edges + 1 Go hub) ==="

	# Start hub first (Go-only, always)
	local hub_log="/tmp/hub-bench_${label}_hub.log"
	"$ROOT/hook-sync-hub" -id "hub-${label}" -listen ":$HUB_PORT" -db "$ROOT/hub-bench-${label}.pebble"$(hub_edge_args) -batch-ms 50 &>"$hub_log" &
	sleep 2

	# Start edges
	local logs=("$hub_log")
	for i in "${!start_cmds[@]}"; do
		local port="${PORTS[$i]}"
		local log="/tmp/hub-bench_${label}_${port}.log"
		logs+=("$log")
		eval "${start_cmds[$i]}" &>"$log" &
		sleep 1
	done
	sleep 2

	# Verify all nodes up (hub + edges)
	local all_ok=true

	# Check hub
	local hh
	hh=$(curl -sf "http://localhost:$HUB_PORT/health" 2>/dev/null || echo "FAIL")
	if [[ "$hh" == "FAIL" ]]; then
		echo "  ERROR: hub :$HUB_PORT not ready"
		all_ok=false
	fi

	# Check edges
	for port in "${PORTS[@]}"; do
		local h
		h=$(curl -sf "http://localhost:$port/health" 2>/dev/null || echo "FAIL")
		if [[ "$h" == "FAIL" ]]; then
			echo "  ERROR: edge :$port not ready"
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
	echo "  hub + $EDGES edges ready"

	# Run benchmark via bun (fast HTTP client)
	bun -e "
const HUB_PORT = $HUB_PORT;
const PORTS = [$(
		IFS=,
		echo "${PORTS[*]}"
	)];
const RUNS = $RUNS, REQ = $REQ, EDGES = $EDGES;
const EDGE_URLS = PORTS.map(p => 'http://localhost:' + p);
const HUB_URL = 'http://localhost:' + HUB_PORT;

async function starWrite(n) {
  const t0 = performance.now();
  const p = [];
  for (let i = 0; i < n; i++) {
    for (const url of EDGE_URLS) {
      p.push(fetch(url + '/api/items', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({name: 'x' + i, value: i})
      }).then(r => r.json()));
    }
  }
  await Promise.all(p);
  return Math.round((n * EDGES) / ((performance.now() - t0) / 1000));
}

async function checkIntegrity() {
  await new Promise(r => setTimeout(r, 2000));
  // Check edges
  const edgeHealths = await Promise.all(EDGE_URLS.map(u => fetch(u + '/health').then(r => r.json())));
  const counts = edgeHealths.map(h => h.item_count);
  const pending = edgeHealths.map(h => h.pending_changes);
  const dead = edgeHealths.map(h => h.dead_letter);
  const allEqual = counts.every(c => c === counts[0]);
  const noPending = pending.every(p => p === 0);
  const noDead = dead.every(d => d === 0);
  // Check hub
  const hubHealth = await fetch(HUB_URL + '/health').then(r => r.json());
  const hubBackup = hubHealth.backup_items;
  const hubPendingFwd = hubHealth.pending_forwards;
  const hubBackupMatch = hubBackup === counts[0];
  const noPendingFwd = hubPendingFwd === 0;
  const pass = allEqual && noPending && noDead && hubBackupMatch && noPendingFwd;
  return { counts, pending, dead, hubBackup, hubPendingFwd, pass };
}

// Warmup
await starWrite(10);
await new Promise(r => setTimeout(r, 1000));

const qps = [];
const integ = [];
let expectedTotal = 0;

for (let i = 0; i < RUNS; i++) {
  const q = await starWrite(REQ);
  qps.push(q);
  expectedTotal += REQ * EDGES;
  const c = await checkIntegrity();
  integ.push(c.pass);
  if (!c.pass) {
    console.error('  run ' + (i+1) + ' FAIL: edgeCounts=[' + c.counts.join(',') + '] pending=[' + c.pending.join(',') + '] dead=[' + c.dead.join(',') + '] hubBackup=' + c.hubBackup + ' hubPendingFwd=' + c.hubPendingFwd);
  }
}

qps.sort((a, b) => a - b);
const passCount = integ.filter(x => x).length;
const med = qps[Math.floor(qps.length / 2)];
console.log('  QPS: min=' + qps[0] + ' med=' + med + ' max=' + qps[qps.length - 1]);
console.log('  Integrity: ' + passCount + '/' + RUNS + ' PASS (expected ' + expectedTotal + ' items per edge + hub backup)');
console.log('  All QPS: ' + qps.join(', '));
" 2>&1

	cleanup
	echo
}

# Build Go binaries first
echo "Building Go mesh + hub..."
cd "$ROOT/go" && go build -o "$ROOT/hook-sync-mesh-go" ./cmd/mesh 2>&1 && go build -o "$ROOT/hook-sync-hub" ./cmd/hub 2>&1
cd "$ROOT"
echo

echo "############################################"
echo "# Dedicated hub benchmark — 1 Go hub + $EDGES edges (star)"
echo "# ${RUNS} runs × ${REQ} req per edge ($((REQ * EDGES)) total per run)"
echo "############################################"
echo

# --- Go edges + Go hub ---
benchmark_runtime "GO" \
	"$ROOT/hook-sync-mesh-go -id hub-bench-go1 -db $ROOT/hub-bench-go1.db -listen :9001$(edge_peer_args) -batch-ms 50" \
	"$ROOT/hook-sync-mesh-go -id hub-bench-go2 -db $ROOT/hub-bench-go2.db -listen :9002$(edge_peer_args) -batch-ms 50" \
	"$ROOT/hook-sync-mesh-go -id hub-bench-go3 -db $ROOT/hub-bench-go3.db -listen :9003$(edge_peer_args) -batch-ms 50"

# --- Bun edges + Go hub ---
benchmark_runtime "BUN" \
	"bun run $ROOT/bun/server-mesh.ts --id hub-bench-bun1 --db $ROOT/hub-bench-bun1.db --listen :9001$(edge_peer_args) --batch-ms 50" \
	"bun run $ROOT/bun/server-mesh.ts --id hub-bench-bun2 --db $ROOT/hub-bench-bun2.db --listen :9002$(edge_peer_args) --batch-ms 50" \
	"bun run $ROOT/bun/server-mesh.ts --id hub-bench-bun3 --db $ROOT/hub-bench-bun3.db --listen :9003$(edge_peer_args) --batch-ms 50"

# --- Node edges + Go hub ---
benchmark_runtime "NODE" \
	"cd $ROOT/node && node server-mesh.js --id hub-bench-node1 --db $ROOT/hub-bench-node1.db --listen :9001$(edge_peer_args) --batch-ms 50" \
	"cd $ROOT/node && node server-mesh.js --id hub-bench-node2 --db $ROOT/hub-bench-node2.db --listen :9002$(edge_peer_args) --batch-ms 50" \
	"cd $ROOT/node && node server-mesh.js --id hub-bench-node3 --db $ROOT/hub-bench-node3.db --listen :9003$(edge_peer_args) --batch-ms 50"

echo "############################################"
echo "# Done"
echo "############################################"
