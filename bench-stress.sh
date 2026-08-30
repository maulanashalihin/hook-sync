#!/usr/bin/env bash
# bench-volume.sh — Massive volume test: convergence time, persistence, consistency
#
# Writes large volumes (10K, 100K, 500K items) via batch endpoint, then verifies:
#   1. Convergence time — how long until replica has all items
#   2. Persistence — kill node, restart, verify data survives in SQLite file
#   3. Consistency — exact item count match, 0 pending, 0 dead letter
#
# Tests all 3 runtimes (Go, Bun, Node). Each runtime tested independently.
#
# Usage: bash bench-volume.sh [runtime]
#   runtime: go | bun | node | all (default: all)

set -uo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"

PORT_A=19020
PORT_B=19021
DB_A=/tmp/volume_a.db
DB_B=/tmp/volume_b.db
LOG_A=/tmp/volume_a.log
LOG_B=/tmp/volume_b.log

RUNTIME="${1:-all}"

# Volume levels: items to write per test
VOLUMES=(10000 100000 500000)
BATCH_SIZE=10000 # items per HTTP batch request

# Per-runtime results
GO_RESULTS=""
BUN_RESULTS=""
NODE_RESULTS=""
GO_RAN=0
BUN_RAN=0
NODE_RAN=0

cleanup() {
	pkill -9 -f "hook-sync-go|bun/server|node server" 2>/dev/null || true
	sleep 1
	rm -f $DB_A $DB_B $DB_A-wal $DB_A-shm $DB_B-wal $DB_B-shm $LOG_A $LOG_B 2>/dev/null || true
}

start_cmd() {
	local runtime=$1 id=$2 db=$3 port=$4 peer=$5
	case $runtime in
	go)
		if [ -z "$peer" ]; then
			echo "$ROOT/hook-sync-go -id $id -db $db -listen :$port -batch-ms 50 -batch-size 10000"
		else
			echo "$ROOT/hook-sync-go -id $id -db $db -listen :$port -peer $peer -batch-ms 50 -batch-size 10000"
		fi
		;;
	bun)
		if [ -z "$peer" ]; then
			echo "bun run $ROOT/bun/server.ts --id $id --db $db --listen :$port --batch-ms 50"
		else
			echo "bun run $ROOT/bun/server.ts --id $id --db $db --listen :$port --peer $peer --batch-ms 50"
		fi
		;;
	node)
		if [ -z "$peer" ]; then
			echo "cd $ROOT/node && node server.js --id $id --db $db --listen :$port --batch-ms 50"
		else
			echo "cd $ROOT/node && node server.js --id $id --db $db --listen :$port --peer $peer --batch-ms 50"
		fi
		;;
	esac
}

start_node() {
	local runtime=$1 id=$2 db=$3 port=$4 peer=$5 log=$6
	local cmd
	cmd=$(start_cmd "$runtime" "$id" "$db" "$port" "$peer")
	nohup bash -c "$cmd" >$log 2>&1 &
	sleep 2
}

kill_nodes() {
	pkill -9 -f "hook-sync-go|bun/server|node server" 2>/dev/null || true
	sleep 1
}

get_field() {
	local url=$1 field=$2
	curl -s "$url" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$field','MISSING'))" 2>/dev/null || echo "MISSING"
}

# Wait until both nodes have expected count and 0 pending
# Returns converge time in seconds (echo)
wait_converge() {
	local expected=$1 max_wait=$2
	local start_ts
	start_ts=$(date +%s)
	for _ in $(seq 1 "$max_wait"); do
		sleep 1
		local a_count b_count a_pending b_pending
		a_count=$(get_field "http://localhost:$PORT_A/health" "item_count")
		b_count=$(get_field "http://localhost:$PORT_B/health" "item_count")
		a_pending=$(get_field "http://localhost:$PORT_A/health" "pending_changes")
		b_pending=$(get_field "http://localhost:$PORT_B/health" "pending_changes")
		if [ "$a_count" = "$expected" ] && [ "$b_count" = "$expected" ] && [ "$a_pending" = "0" ] && [ "$b_pending" = "0" ]; then
			local now_ts elapsed
			now_ts=$(date +%s)
			elapsed=$((now_ts - start_ts))
			echo "$elapsed"
			return 0
		fi
	done
	echo "TIMEOUT"
	return 1
}

run_volume_test() {
	local runtime=$1
	local upper
	upper=$(echo "$runtime" | tr '[:lower:]' '[:upper:]')
	local results=""

	echo
	echo "============================================"
	echo "  Volume Test: $upper"
	echo "============================================"

	for vol in "${VOLUMES[@]}"; do
		echo
		echo "--- Volume: $vol items ---"
		cleanup

		# Start both nodes
		start_node "$runtime" volA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
		start_node "$runtime" volB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

		# Verify both up
		local ha hb
		ha=$(curl -sf "http://localhost:$PORT_A/health" 2>/dev/null || echo "FAIL")
		hb=$(curl -sf "http://localhost:$PORT_B/health" 2>/dev/null || echo "FAIL")
		if [[ "$ha" == "FAIL" || "$hb" == "FAIL" ]]; then
			echo "  ERROR: nodes not ready"
			echo "  A log: $(tail -3 $LOG_A 2>/dev/null | tr '\n' '|')"
			echo "  B log: $(tail -3 $LOG_B 2>/dev/null | tr '\n' '|')"
			results="$results  ❌ $vol: nodes failed to start"
			cleanup
			continue
		fi

		# Write volume via batch endpoint to node A
		echo "  Writing $vol items to node A (batch=$BATCH_SIZE)..."
		local write_start write_end write_time
		write_start=$(date +%s%N)
		bun -e "
const URL='http://localhost:$PORT_A';
const TOTAL=$vol, BATCH=$BATCH_SIZE;

async function writeBatch(items) {
  const resp = await fetch(URL+'/api/items/batch', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(items),
  });
  return await resp.json();
}

let written = 0;
while (written < TOTAL) {
  const n = Math.min(BATCH, TOTAL - written);
  const items = [];
  for (let i = 0; i < n; i++) {
    items.push({name: 'item-' + (written + i), value: written + i});
  }
  await writeBatch(items);
  written += n;
}
console.log('  Written: ' + written + ' items');
" 2>&1
		write_end=$(date +%s%N)
		write_time=$(((write_end - write_start) / 1000000))
		echo "  Write time: ${write_time}ms"

		# Wait for convergence
		echo "  Waiting for convergence..."
		local converge_time
		converge_time=$(wait_converge "$vol" 120)
		if [ "$converge_time" = "TIMEOUT" ]; then
			local a_count b_count
			a_count=$(get_field "http://localhost:$PORT_A/health" "item_count")
			b_count=$(get_field "http://localhost:$PORT_B/health" "item_count")
			echo "  ❌ TIMEOUT: A=$a_count B=$b_count (expected=$vol)"
			results="$results  ❌ $vol: convergence timeout (A=$a_count B=$b_count)"
			cleanup
			continue
		fi
		echo "  Converge time: ${converge_time}s"

		# Verify consistency
		local a_count b_count a_pending b_pending a_dead b_dead
		a_count=$(get_field "http://localhost:$PORT_A/health" "item_count")
		b_count=$(get_field "http://localhost:$PORT_B/health" "item_count")
		a_pending=$(get_field "http://localhost:$PORT_A/health" "pending_changes")
		b_pending=$(get_field "http://localhost:$PORT_B/health" "pending_changes")
		a_dead=$(get_field "http://localhost:$PORT_A/health" "dead_letter")
		b_dead=$(get_field "http://localhost:$PORT_B/health" "dead_letter")

		local consistency_pass=true
		if [ "$a_count" != "$vol" ]; then consistency_pass=false; fi
		if [ "$b_count" != "$vol" ]; then consistency_pass=false; fi
		if [ "$a_pending" != "0" ]; then consistency_pass=false; fi
		if [ "$b_pending" != "0" ]; then consistency_pass=false; fi
		if [ "$a_dead" != "0" ]; then consistency_pass=false; fi
		if [ "$b_dead" != "0" ]; then consistency_pass=false; fi

		if [ "$consistency_pass" = "true" ]; then
			echo "  ✅ Consistency: A=$a_count B=$b_count, pending=0, dead=0"
		else
			echo "  ❌ Consistency: A=$a_count B=$b_count, pendingA=$a_pending pendingB=$b_pending, deadA=$a_dead deadB=$b_dead"
			results="$results  ❌ $vol: consistency fail"
			cleanup
			continue
		fi

		# Persistence test: kill both, restart, verify data survives
		echo "  Persistence test: kill both nodes..."
		kill_nodes

		start_node "$runtime" volA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
		start_node "$runtime" volB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

		sleep 3

		local p_count_a p_count_b
		p_count_a=$(get_field "http://localhost:$PORT_A/health" "item_count")
		p_count_b=$(get_field "http://localhost:$PORT_B/health" "item_count")

		if [ "$p_count_a" = "$vol" ] && [ "$p_count_b" = "$vol" ]; then
			echo "  ✅ Persistence: data survives restart (A=$p_count_a B=$p_count_b)"
			results="$results  ✅ $vol: write=${write_time}ms converge=${converge_time}s consistency=PASS persistence=PASS"
		else
			echo "  ❌ Persistence: data lost on restart (A=$p_count_a B=$p_count_b, expected=$vol)"
			results="$results  ❌ $vol: persistence fail (A=$p_count_a B=$p_count_b)"
		fi

		cleanup
	done

	echo
	echo "  $upper Results:"
	for r in $results; do
		echo "  $r"
	done

	case "$runtime" in
	go)
		GO_RESULTS="$results"
		GO_RAN=1
		;;
	bun)
		BUN_RESULTS="$results"
		BUN_RAN=1
		;;
	node)
		NODE_RESULTS="$results"
		NODE_RAN=1
		;;
	esac
}

# Build Go binary
echo "Building Go..."
cd "$ROOT/go" && go build -o "$ROOT/hook-sync-go" . 2>&1
cd "$ROOT"

echo ""
echo "############################################"
echo "# hook-sync Volume Test"
echo "# Volumes: ${VOLUMES[*]} items"
echo "# Batch size: $BATCH_SIZE per HTTP request"
echo "# Tests: convergence time, persistence, consistency"
echo "# Runtimes: ${RUNTIME}"
echo "############################################"

case "$RUNTIME" in
go) run_volume_test "go" ;;
bun) run_volume_test "bun" ;;
node) run_volume_test "node" ;;
all)
	run_volume_test "go"
	run_volume_test "bun"
	run_volume_test "node"
	;;
*)
	echo "Unknown runtime: $RUNTIME (use: go, bun, node, all)"
	exit 1
	;;
esac

# Summary
echo ""
echo "############################################"
echo "# Volume Test Summary"
echo "############################################"
echo ""
[ "$GO_RAN" -eq 1 ] && echo "GO:" && echo "$GO_RESULTS" | tr ' ' '\n' | grep -E '✅|❌' | sed 's/^/  /'
[ "$BUN_RAN" -eq 1 ] && echo "BUN:" && echo "$BUN_RESULTS" | tr ' ' '\n' | grep -E '✅|❌' | sed 's/^/  /'
[ "$NODE_RAN" -eq 1 ] && echo "NODE:" && echo "$NODE_RESULTS" | tr ' ' '\n' | grep -E '✅|❌' | sed 's/^/  /'
echo ""

cleanup
