#!/usr/bin/env bash
# bench-hookpebble-vs-trigger.sh
#
# Compares replication throughput: trigger-based (go/cmd/server) vs hook+Pebble (go/hookpebble)
# 2 nodes each, 100K writes via batch endpoint, measure write QPS + convergence time + integrity
#
# Usage: bash bench-hookpebble-vs-trigger.sh

set -uo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"

PORT_A=19101
PORT_B=19102
RUNS=5
ITEMS=100000

cleanup() {
	pkill -9 -f "hook-sync-go.*1910" 2>/dev/null || true
	pkill -9 -f "hook-sync-hookpebble.*1910" 2>/dev/null || true
	sleep 1
	rm -f "$ROOT"/hp-bench-*.db "$ROOT"/hp-bench-*.db-wal "$ROOT"/hp-bench-*.db-shm 2>/dev/null || true
	rm -rf "$ROOT"/hp-bench-*.pebble 2>/dev/null || true
}

run_bench() {
	local label="$1"
	local binary="$2"
	local extra_flags="$3"

	cleanup
	echo "=== $label ==="

	# Start node A (writer)
	nohup "$binary" -id hp-a -db "$ROOT/hp-bench-a.db" -listen ":$PORT_A" -peer "http://localhost:$PORT_B" $extra_flags >/tmp/hp-a.log 2>&1 &
	sleep 2

	# Start node B (replica)
	nohup "$binary" -id hp-b -db "$ROOT/hp-bench-b.db" -listen ":$PORT_B" -peer "http://localhost:$PORT_A" $extra_flags >/tmp/hp-b.log 2>&1 &
	sleep 2

	# Health check
	local ha hb
	ha=$(curl -sf "http://localhost:$PORT_A/health" 2>/dev/null || echo "FAIL")
	hb=$(curl -sf "http://localhost:$PORT_B/health" 2>/dev/null || echo "FAIL")
	if [[ "$ha" == "FAIL" || "$hb" == "FAIL" ]]; then
		echo "  ERROR: server not ready"
		echo "  A log: $(tail -3 /tmp/hp-a.log 2>/dev/null | tr '\n' '|')"
		echo "  B log: $(tail -3 /tmp/hp-b.log 2>/dev/null | tr '\n' '|')"
		cleanup
		return
	fi
	echo "  both servers ready"

	# Run benchmark: batch write ITEMS to node A, wait for convergence
	bun -e "
const URL='http://localhost:$PORT_A';
const URL_B='http://localhost:$PORT_B';
const RUNS=$RUNS, ITEMS=$ITEMS;

async function writeBatch(n) {
	const items = [];
	for (let i = 0; i < n; i++) items.push({name:'item-'+i, value:i});
	const t0 = performance.now();
	const r = await fetch(URL+'/api/items/batch', {
		method:'POST', headers:{'Content-Type':'application/json'},
		body: JSON.stringify(items)
	});
	await r.json();
	return Math.round(n / ((performance.now()-t0)/1000));
}

async function getCount(url) {
	const r = await fetch(url+'/health');
	const j = await r.json();
	return j.item_count;
}

async function waitForConverge(target, timeoutMs) {
	const t0 = performance.now();
	while (performance.now() - t0 < timeoutMs) {
		const ha = await (await fetch(URL+'/health')).json();
		const hb = await (await fetch(URL_B+'/health')).json();
		if (ha.item_count === target && hb.item_count === target && ha.pending_changes === 0 && hb.pending_changes === 0) {
			return Math.round((performance.now()-t0)/1000);
		}
		await new Promise(r => setTimeout(r, 100));
	}
	return -1; // timeout
}

const qps = [];
const convergeTimes = [];

for (let run = 0; run < RUNS; run++) {
	// Write
	const q = await writeBatch(ITEMS);
	qps.push(q);

	// Wait for convergence
	const ct = await waitForConverge((run+1)*ITEMS, 30000);
	convergeTimes.push(ct);

	// Verify integrity
	const ha = await (await fetch(URL+'/health')).json();
	const hb = await (await fetch(URL_B+'/health')).json();
	const ok = ha.item_count === hb.item_count && ha.pending_changes === 0 && hb.pending_changes === 0;
	console.log('  run '+(run+1)+': write='+q+' QPS, converge='+ct+'s, A='+ha.item_count+' B='+hb.item_count+' pendingA='+ha.pending_changes+' pendingB='+hb.pending_changes+' '+(ok?'PASS':'FAIL'));
}

qps.sort((a,b)=>a-b);
convergeTimes.sort((a,b)=>a-b);
console.log('  QPS: min='+qps[0]+' med='+qps[Math.floor(qps.length/2)]+' max='+qps[qps.length-1]);
console.log('  Converge: min='+convergeTimes[0]+'s med='+convergeTimes[Math.floor(convergeTimes.length/2)]+'s max='+convergeTimes[convergeTimes.length-1]+'s');
" 2>&1

	cleanup
	echo
}

# Build both binaries
echo "Building trigger server (go/cmd/server)..."
cd "$ROOT/go" && go build -o "$ROOT/hook-sync-go" ./cmd/server 2>&1
echo "Building hookmem server (go/hookmem, in-memory)..."
cd "$ROOT/go" && go build -tags sqlite_preupdate_hook -o "$ROOT/hook-sync-hookmem" ./hookmem/ 2>&1
echo "Building hookpebble server (go/hookpebble, preupdate_hook + commit_hook + Pebble)..."
cd "$ROOT/go" && go build -tags sqlite_preupdate_hook -o "$ROOT/hook-sync-hookpebble" ./hookpebble/ 2>&1
cd "$ROOT"
echo

echo "############################################"
echo "# hookpebble vs trigger replication benchmark"
echo "# ${RUNS} runs × ${ITEMS} batch writes, 2-node replication"
echo "############################################"
echo

# 1. Trigger-based (production)
run_bench "TRIGGER (go/cmd/server, _changes + SQL triggers)" "$ROOT/hook-sync-go" ""

# 2. Hook+Pebble (new protocol)
run_bench "HOOK+PEBBLE (go/cmd/hookpebble, preupdate_hook + commit_hook + Pebble)" "$ROOT/hook-sync-hookpebble" ""

# 3. Hook+Memory (no Pebble, no persistence)
run_bench "HOOK+MEMORY (go/hookmem, preupdate_hook + in-memory slice)" "$ROOT/hook-sync-hookmem" ""

echo "############################################"
echo "# Done — compare QPS + convergence"
echo "############################################"
