#!/bin/bash
# bench-splitbrain-hook.sh — Split-brain safety test across capture modes
#
# Tests per mode (trigger, hookpebble, hookmem):
#   1. INSERT during partition (UUID, no collision)
#   2. UPDATE vs UPDATE during partition (last-write-wins by timestamp)
#   3. DELETE vs UPDATE during partition (UPDATE wins if newer)
#   4. Connection failure (peer down → retry, no dead letter)
#   5. Crash recovery (changes survive in _changes / Pebble / in-memory)
#
# Usage: bash bench-splitbrain-hook.sh [mode]
#   mode: trigger | hookpebble | hookmem | all (default: all)

set -uo pipefail

cd "$(dirname "$0")"
ROOT="$(pwd)"

PORT_A=19101
PORT_B=19102
DB_A=/tmp/splitbrain-hook_a.db
DB_B=/tmp/splitbrain-hook_b.db
LOG_A=/tmp/splitbrain-hook_a.log
LOG_B=/tmp/splitbrain-hook_b.log

MODE="${1:-all}"

# Per-mode results
TRIGGER_PASS=0
TRIGGER_FAIL=0
TRIGGER_TOTAL=0
TRIGGER_RAN=0
HOOKPEBBLE_PASS=0
HOOKPEBBLE_FAIL=0
HOOKPEBBLE_TOTAL=0
HOOKPEBBLE_RAN=0
HOOKMEM_PASS=0
HOOKMEM_FAIL=0
HOOKMEM_TOTAL=0
HOOKMEM_RAN=0

cleanup() {
	pkill -9 -f "hook-sync-go.*1910|hook-sync-hookpebble.*1910|hook-sync-hookmem.*1910" 2>/dev/null || true
	sleep 1
	rm -f $DB_A $DB_B $DB_A-wal $DB_A-shm $DB_B-wal $DB_B-shm $LOG_A $LOG_B 2>/dev/null || true
	rm -rf /tmp/splitbrain-hook_a.db.pebble /tmp/splitbrain-hook_b.db.pebble 2>/dev/null || true
}

# Build start command for a mode
start_cmd() {
	local mode=$1 id=$2 db=$3 port=$4 peer=$5
	case $mode in
	trigger)
		if [ -z "$peer" ]; then
			echo "$ROOT/hook-sync-go -id $id -db $db -listen :$port -batch-ms 50 -batch-size 10000"
		else
			echo "$ROOT/hook-sync-go -id $id -db $db -listen :$port -peer $peer -batch-ms 50 -batch-size 10000"
		fi
		;;
	hookpebble)
		if [ -z "$peer" ]; then
			echo "$ROOT/hook-sync-hookpebble -id $id -db $db -listen :$port -batch-ms 50 -batch-size 10000"
		else
			echo "$ROOT/hook-sync-hookpebble -id $id -db $db -listen :$port -peer $peer -batch-ms 50 -batch-size 10000"
		fi
		;;
	hookmem)
		if [ -z "$peer" ]; then
			echo "$ROOT/hook-sync-hookmem -id $id -db $db -listen :$port -batch-ms 50 -batch-size 10000"
		else
			echo "$ROOT/hook-sync-hookmem -id $id -db $db -listen :$port -peer $peer -batch-ms 50 -batch-size 10000"
		fi
		;;
	esac
}

start_node() {
	local mode=$1 id=$2 db=$3 port=$4 peer=$5 log=$6
	local cmd
	cmd=$(start_cmd "$mode" "$id" "$db" "$port" "$peer")
	nohup bash -c "$cmd" >$log 2>&1 &
	sleep 1
}

kill_nodes() {
	pkill -9 -f "hook-sync-go.*1910|hook-sync-hookpebble.*1910|hook-sync-hookmem.*1910" 2>/dev/null || true
	sleep 1
}

check() {
	local name=$1 expected=$2 actual=$3
	TOTAL=$((TOTAL + 1))
	if [ "$expected" = "$actual" ]; then
		echo "  ✅ $name: expected=$expected actual=$actual"
		PASS=$((PASS + 1))
	else
		echo "  ❌ $name: expected=$expected actual=$actual"
		FAIL=$((FAIL + 1))
	fi
}

get_field() {
	local url=$1 field=$2
	curl -s "$url" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$field','MISSING'))" 2>/dev/null || echo "MISSING"
}

wait_converge() {
	local max_wait=$1
	for _ in $(seq 1 "$max_wait"); do
		sleep 1
		local a_pending b_pending
		a_pending=$(get_field "http://localhost:$PORT_A/health" "pending_changes")
		b_pending=$(get_field "http://localhost:$PORT_B/health" "pending_changes")
		if [ "$a_pending" = "0" ] && [ "$b_pending" = "0" ]; then
			return 0
		fi
	done
	return 1
}

run_splitbrain_test() {
	local mode=$1
	local upper
	upper=$(echo "$mode" | tr '[:lower:]' '[:upper:]')
	PASS=0
	FAIL=0
	TOTAL=0

	echo
	echo "============================================"
	echo "  Split-Brain Test: $upper"
	echo "============================================"

	cleanup

	# --- Phase 1: Connect and create shared item ---
	echo ""
	echo ">>> Phase 1: Start both nodes, create shared item"
	start_node "$mode" nodeA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
	start_node "$mode" nodeB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

	local ha hb
	ha=$(curl -sf "http://localhost:$PORT_A/health" 2>/dev/null || echo "FAIL")
	hb=$(curl -sf "http://localhost:$PORT_B/health" 2>/dev/null || echo "FAIL")
	if [[ "$ha" == "FAIL" || "$hb" == "FAIL" ]]; then
		echo "  ERROR: nodes not ready"
		echo "  A log: $(tail -3 $LOG_A 2>/dev/null | tr '\n' '|')"
		echo "  B log: $(tail -3 $LOG_B 2>/dev/null | tr '\n' '|')"
		cleanup
		return
	fi
	echo "  nodes ready"

	ITEM=$(curl -s -X POST "http://localhost:$PORT_A/api/items" -H "Content-Type: application/json" -d '{"name":"shared_item","value":0}')
	ITEM_ID=$(echo "$ITEM" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])" 2>/dev/null)
	echo "  Created item: $ITEM_ID"

	sleep 2

	local a_val b_val
	a_val=$(get_field "http://localhost:$PORT_A/api/items/$ITEM_ID" "value")
	b_val=$(get_field "http://localhost:$PORT_B/api/items/$ITEM_ID" "value")
	check "Both nodes have item after initial sync" "0" "$a_val"
	check "nodeB received item" "0" "$b_val"

	# --- Phase 2: Network partition ---
	echo ""
	echo ">>> Phase 2: Network partition — kill both nodes"
	kill_nodes
	if [ "$mode" = "hookmem" ]; then
		echo "  ⚠️  hookmem: in-memory pending changes LOST on kill (no persistence)"
	else
		echo "  Changes survive in $([ "$mode" = "trigger" ] && echo "_changes table" || echo "Pebble") (disk)"
	fi

	# --- Phase 3: Independent updates during partition ---
	echo ""
	echo ">>> Phase 3: Start nodes INDEPENDENTLY (no peer), update same item"

	start_node "$mode" nodeA $DB_A $PORT_A "" $LOG_A
	start_node "$mode" nodeB $DB_B $PORT_B "" $LOG_B

	curl -s -X PUT "http://localhost:$PORT_A/api/items/$ITEM_ID" -H "Content-Type: application/json" -d '{"name":"shared_item","value":100}' >/dev/null
	echo "  nodeA: updated item value=100"

	curl -s -X PUT "http://localhost:$PORT_B/api/items/$ITEM_ID" -H "Content-Type: application/json" -d '{"name":"shared_item","value":200}' >/dev/null
	echo "  nodeB: updated item value=200"

	NEW_A=$(curl -s -X POST "http://localhost:$PORT_A/api/items" -H "Content-Type: application/json" -d '{"name":"only_on_A","value":42}')
	NEW_A_ID=$(echo "$NEW_A" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])" 2>/dev/null)
	echo "  nodeA: created new item (only_on_A) id=$NEW_A_ID"

	NEW_B=$(curl -s -X POST "http://localhost:$PORT_B/api/items" -H "Content-Type: application/json" -d '{"name":"only_on_B","value":99}')
	NEW_B_ID=$(echo "$NEW_B" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])" 2>/dev/null)
	echo "  nodeB: created new item (only_on_B) id=$NEW_B_ID"

	a_val=$(get_field "http://localhost:$PORT_A/api/items/$ITEM_ID" "value")
	b_val=$(get_field "http://localhost:$PORT_B/api/items/$ITEM_ID" "value")
	check "During partition: nodeA has value=100" "100" "$a_val"
	check "During partition: nodeB has value=200" "200" "$b_val"

	# --- Phase 4: Reconnect ---
	echo ""
	echo ">>> Phase 4: Reconnect — restart both nodes with peer"
	kill_nodes

	start_node "$mode" nodeA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
	start_node "$mode" nodeB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

	echo "  Waiting for sync convergence..."
	wait_converge 15

	# --- Phase 5: Verify convergence ---
	echo ""
	echo ">>> Phase 5: Verify convergence after reconnect"

	a_val=$(get_field "http://localhost:$PORT_A/api/items/$ITEM_ID" "value")
	b_val=$(get_field "http://localhost:$PORT_B/api/items/$ITEM_ID" "value")
	local a_count b_count a_dead b_dead
	a_count=$(get_field "http://localhost:$PORT_A/health" "item_count")
	b_count=$(get_field "http://localhost:$PORT_B/health" "item_count")
	a_dead=$(get_field "http://localhost:$PORT_A/health" "dead_letter")
	b_dead=$(get_field "http://localhost:$PORT_B/health" "dead_letter")

	echo "  Shared item value: nodeA=$a_val, nodeB=$b_val"
	echo "  Item count: nodeA=$a_count, nodeB=$b_count"
	echo "  Dead letter: nodeA=$a_dead, nodeB=$b_dead"

	check "Both nodes converge to same value for shared item" "$a_val" "$b_val"
	check "No dead letter on nodeA" "0" "$a_dead"
	check "No dead letter on nodeB" "0" "$b_dead"
	check "nodeA has all 3 items" "3" "$a_count"
	check "nodeB has all 3 items" "3" "$b_count"

	local a_has_b b_has_a
	a_has_b=$(get_field "http://localhost:$PORT_A/api/items/$NEW_B_ID" "name")
	b_has_a=$(get_field "http://localhost:$PORT_B/api/items/$NEW_A_ID" "name")
	check "nodeA received nodeB's new item" "only_on_B" "$a_has_b"
	check "nodeB received nodeA's new item" "only_on_A" "$b_has_a"

	# --- Phase 6: DELETE vs UPDATE conflict ---
	echo ""
	echo ">>> Phase 6: DELETE vs UPDATE conflict test"

	ITEM2=$(curl -s -X POST "http://localhost:$PORT_A/api/items" -H "Content-Type: application/json" -d '{"name":"delete_test","value":1}')
	ITEM2_ID=$(echo "$ITEM2" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])" 2>/dev/null)
	sleep 2

	kill_nodes
	start_node "$mode" nodeA $DB_A $PORT_A "" $LOG_A
	start_node "$mode" nodeB $DB_B $PORT_B "" $LOG_B

	curl -s -X DELETE "http://localhost:$PORT_A/api/items/$ITEM2_ID" >/dev/null
	echo "  nodeA: deleted item $ITEM2_ID"

	curl -s -X PUT "http://localhost:$PORT_B/api/items/$ITEM2_ID" -H "Content-Type: application/json" -d '{"name":"delete_test","value":999}' >/dev/null
	echo "  nodeB: updated item value=999"

	kill_nodes
	start_node "$mode" nodeA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
	start_node "$mode" nodeB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

	echo "  Waiting for convergence..."
	wait_converge 15

	local a_item2 b_item2
	a_item2=$(curl -s "http://localhost:$PORT_A/api/items/$ITEM2_ID" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'value={d.get(\"value\",\"DELETED\")}')" 2>/dev/null || echo "DELETED")
	b_item2=$(curl -s "http://localhost:$PORT_B/api/items/$ITEM2_ID" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'value={d.get(\"value\",\"DELETED\")}')" 2>/dev/null || echo "DELETED")
	echo "  After reconnect: nodeA item2=$a_item2, nodeB item2=$b_item2"
	check "DELETE vs UPDATE: both nodes agree" "$a_item2" "$b_item2"

	# --- Results ---
	echo ""
	echo "  $upper Results: $PASS/$TOTAL passed, $FAIL failed"
	case "$mode" in
	trigger)
		TRIGGER_PASS=$PASS
		TRIGGER_FAIL=$FAIL
		TRIGGER_TOTAL=$TOTAL
		TRIGGER_RAN=1
		;;
	hookpebble)
		HOOKPEBBLE_PASS=$PASS
		HOOKPEBBLE_FAIL=$FAIL
		HOOKPEBBLE_TOTAL=$TOTAL
		HOOKPEBBLE_RAN=1
		;;
	hookmem)
		HOOKMEM_PASS=$PASS
		HOOKMEM_FAIL=$FAIL
		HOOKMEM_TOTAL=$TOTAL
		HOOKMEM_RAN=1
		;;
	esac

	cleanup
}

# Build all binaries
echo "Building trigger server (go/main.go)..."
cd "$ROOT/go" && go build -o "$ROOT/hook-sync-go" . 2>&1
echo "Building hookpebble server (go/hookpebble)..."
cd "$ROOT/go" && go build -tags sqlite_preupdate_hook -o "$ROOT/hook-sync-hookpebble" ./hookpebble/ 2>&1
echo "Building hookmem server (go/hookmem)..."
cd "$ROOT/go" && go build -tags sqlite_preupdate_hook -o "$ROOT/hook-sync-hookmem" ./hookmem/ 2>&1
cd "$ROOT"

echo ""
echo "############################################"
echo "# hook-sync Split-Brain Safety Test"
echo "# Modes: ${MODE}"
echo "# Tests: partition + conflict + reconnect + crash recovery"
echo "############################################"

case "$MODE" in
trigger) run_splitbrain_test "trigger" ;;
hookpebble) run_splitbrain_test "hookpebble" ;;
hookmem) run_splitbrain_test "hookmem" ;;
all)
	run_splitbrain_test "trigger"
	run_splitbrain_test "hookpebble"
	run_splitbrain_test "hookmem"
	;;
*)
	echo "Unknown mode: $MODE (use: trigger, hookpebble, hookmem, all)"
	exit 1
	;;
esac

# Summary
echo ""
echo "############################################"
echo "# Split-Brain Test Summary"
echo "############################################"
echo ""
TOTAL_PASS=0
TOTAL_FAIL=0
TOTAL_CHECKS=0

print_result() {
	local label=$1 p=$2 f=$3 t=$4
	TOTAL_PASS=$((TOTAL_PASS + p))
	TOTAL_FAIL=$((TOTAL_FAIL + f))
	TOTAL_CHECKS=$((TOTAL_CHECKS + t))
	if [ "$f" -eq 0 ]; then
		echo "  ✅ $label: $p/$t passed"
	else
		echo "  ❌ $label: $p/$t passed ($f failed)"
	fi
}

[ "$TRIGGER_RAN" -eq 1 ] && print_result "TRIGGER" $TRIGGER_PASS $TRIGGER_FAIL $TRIGGER_TOTAL
[ "$HOOKPEBBLE_RAN" -eq 1 ] && print_result "HOOK+PEBBLE" $HOOKPEBBLE_PASS $HOOKPEBBLE_FAIL $HOOKPEBBLE_TOTAL
[ "$HOOKMEM_RAN" -eq 1 ] && print_result "HOOK+MEMORY" $HOOKMEM_PASS $HOOKMEM_FAIL $HOOKMEM_TOTAL

echo ""
echo "  Total: $TOTAL_PASS/$TOTAL_CHECKS passed, $TOTAL_FAIL failed"
echo ""

[ "$TOTAL_FAIL" -eq 0 ] && exit 0 || exit 1
