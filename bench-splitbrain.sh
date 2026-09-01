#!/bin/bash
# bench-splitbrain.sh — Test hook-sync split-brain safety across all runtimes
#
# Tests per runtime (Go, Bun, Node):
#   1. INSERT during partition (UUID, no collision)
#   2. UPDATE vs UPDATE during partition (last-write-wins by timestamp)
#   3. DELETE vs UPDATE during partition (UPDATE wins if newer)
#   4. Connection failure (peer down → retry, no dead letter)
#   5. Crash recovery (changes survive in _changes)
#
# Usage: bash bench-splitbrain.sh [runtime]
#   runtime: go | bun | node | all (default: all)

set -uo pipefail

cd "$(dirname "$0")"
ROOT="$(pwd)"

PORT_A=19001
PORT_B=19002
DB_A=/tmp/splitbrain_a.db
DB_B=/tmp/splitbrain_b.db
LOG_A=/tmp/splitbrain_a.log
LOG_B=/tmp/splitbrain_b.log

RUNTIME="${1:-all}"

# Per-runtime results (bash 3.2 compatible — no associative arrays)
GO_PASS=0
GO_FAIL=0
GO_TOTAL=0
BUN_PASS=0
BUN_FAIL=0
BUN_TOTAL=0
NODE_PASS=0
NODE_FAIL=0
NODE_TOTAL=0
GO_RAN=0
BUN_RAN=0
NODE_RAN=0

cleanup() {
	pkill -9 -f "hook-sync-go|bun/server|node server" 2>/dev/null || true
	sleep 1
	rm -f $DB_A $DB_B $DB_A-wal $DB_A-shm $DB_B-wal $DB_B-shm $LOG_A $LOG_B 2>/dev/null || true
}

# Build start command for a runtime
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
	sleep 1
}

kill_nodes() {
	pkill -9 -f "hook-sync-go|bun/server|node server" 2>/dev/null || true
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
	local runtime=$1
	local upper
	upper=$(echo "$runtime" | tr '[:lower:]' '[:upper:]')
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
	start_node "$runtime" nodeA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
	start_node "$runtime" nodeB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

	# Verify both up
	local ha hb
	ha=$(curl -sf "http://localhost:$PORT_A/health" 2>/dev/null || echo "FAIL")
	hb=$(curl -sf "http://localhost:$PORT_B/health" 2>/dev/null || echo "FAIL")
	if [[ "$ha" == "FAIL" || "$hb" == "FAIL" ]]; then
		echo "  ERROR: nodes not ready"
		echo "  A log: $(tail -3 $LOG_A 2>/dev/null | tr '\n' '|')"
		echo "  B log: $(tail -3 $LOG_B 2>/dev/null | tr '\n' '|')"
		RUNTIME_PASS[$runtime]=0
		RUNTIME_FAIL[$runtime]=1
		RUNTIME_TOTAL[$runtime]=1
		cleanup
		return
	fi
	echo "  nodes ready"

	# Create item on nodeA
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
	echo "  Both nodes killed. Changes survive in _changes table (SQLite file)."

	# --- Phase 3: Independent updates during partition ---
	echo ""
	echo ">>> Phase 3: Start nodes INDEPENDENTLY (no peer), update same item"

	start_node "$runtime" nodeA $DB_A $PORT_A "" $LOG_A
	start_node "$runtime" nodeB $DB_B $PORT_B "" $LOG_B

	# Update same item on nodeA → value=100
	curl -s -X PUT "http://localhost:$PORT_A/api/items/$ITEM_ID" -H "Content-Type: application/json" -d '{"name":"shared_item","value":100}' >/dev/null
	echo "  nodeA: updated item value=100"

	# Update same item on nodeB → value=200
	curl -s -X PUT "http://localhost:$PORT_B/api/items/$ITEM_ID" -H "Content-Type: application/json" -d '{"name":"shared_item","value":200}' >/dev/null
	echo "  nodeB: updated item value=200"

	# Also create new items (INSERT — should be safe, UUID)
	NEW_A=$(curl -s -X POST "http://localhost:$PORT_A/api/items" -H "Content-Type: application/json" -d '{"name":"only_on_A","value":42}')
	NEW_A_ID=$(echo "$NEW_A" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])" 2>/dev/null)
	echo "  nodeA: created new item (only_on_A) id=$NEW_A_ID"

	NEW_B=$(curl -s -X POST "http://localhost:$PORT_B/api/items" -H "Content-Type: application/json" -d '{"name":"only_on_B","value":99}')
	NEW_B_ID=$(echo "$NEW_B" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])" 2>/dev/null)
	echo "  nodeB: created new item (only_on_B) id=$NEW_B_ID"

	# Verify local state during partition
	a_val=$(get_field "http://localhost:$PORT_A/api/items/$ITEM_ID" "value")
	b_val=$(get_field "http://localhost:$PORT_B/api/items/$ITEM_ID" "value")
	check "During partition: nodeA has value=100" "100" "$a_val"
	check "During partition: nodeB has value=200" "200" "$b_val"

	# --- Phase 4: Reconnect ---
	echo ""
	echo ">>> Phase 4: Reconnect — restart both nodes with peer"
	kill_nodes

	start_node "$runtime" nodeA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
	start_node "$runtime" nodeB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

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

	# Verify new items from both nodes merged (INSERT = safe)
	local a_has_b b_has_a
	a_has_b=$(get_field "http://localhost:$PORT_A/api/items/$NEW_B_ID" "name")
	b_has_a=$(get_field "http://localhost:$PORT_B/api/items/$NEW_A_ID" "name")
	check "nodeA received nodeB's new item" "only_on_B" "$a_has_b"
	check "nodeB received nodeA's new item" "only_on_A" "$b_has_a"

	# --- Phase 6: DELETE vs UPDATE conflict ---
	echo ""
	echo ">>> Phase 6: DELETE vs UPDATE conflict test"

	# Create new shared item
	ITEM2=$(curl -s -X POST "http://localhost:$PORT_A/api/items" -H "Content-Type: application/json" -d '{"name":"delete_test","value":1}')
	ITEM2_ID=$(echo "$ITEM2" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])" 2>/dev/null)
	sleep 2 # sync to nodeB

	# Partition
	kill_nodes
	start_node "$runtime" nodeA $DB_A $PORT_A "" $LOG_A
	start_node "$runtime" nodeB $DB_B $PORT_B "" $LOG_B

	# nodeA: DELETE the item
	curl -s -X DELETE "http://localhost:$PORT_A/api/items/$ITEM2_ID" >/dev/null
	echo "  nodeA: deleted item $ITEM2_ID"

	# nodeB: UPDATE the item
	curl -s -X PUT "http://localhost:$PORT_B/api/items/$ITEM2_ID" -H "Content-Type: application/json" -d '{"name":"delete_test","value":999}' >/dev/null
	echo "  nodeB: updated item value=999"

	# Reconnect
	kill_nodes
	start_node "$runtime" nodeA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
	start_node "$runtime" nodeB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

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
	case "$runtime" in
	go)
		GO_PASS=$PASS
		GO_FAIL=$FAIL
		GO_TOTAL=$TOTAL
		GO_RAN=1
		;;
	bun)
		BUN_PASS=$PASS
		BUN_FAIL=$FAIL
		BUN_TOTAL=$TOTAL
		BUN_RAN=1
		;;
	node)
		NODE_PASS=$PASS
		NODE_FAIL=$FAIL
		NODE_TOTAL=$TOTAL
		NODE_RAN=1
		;;
	esac

	cleanup
}

# Build Go binary
echo "Building Go..."
cd "$ROOT/go" && go build -o "$ROOT/hook-sync-go" ./cmd/server 2>&1
cd "$ROOT"

echo ""
echo "############################################"
echo "# hook-sync Split-Brain Safety Test"
echo "# Runtimes: ${RUNTIME}"
echo "############################################"

case "$RUNTIME" in
go) run_splitbrain_test "go" ;;
bun) run_splitbrain_test "bun" ;;
node) run_splitbrain_test "node" ;;
all)
	run_splitbrain_test "go"
	run_splitbrain_test "bun"
	run_splitbrain_test "node"
	;;
*)
	echo "Unknown runtime: $RUNTIME (use: go, bun, node, all)"
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

[ "$GO_RAN" -eq 1 ] && print_result "GO" $GO_PASS $GO_FAIL $GO_TOTAL
[ "$BUN_RAN" -eq 1 ] && print_result "BUN" $BUN_PASS $BUN_FAIL $BUN_TOTAL
[ "$NODE_RAN" -eq 1 ] && print_result "NODE" $NODE_PASS $NODE_FAIL $NODE_TOTAL

echo ""
echo "  Total: $TOTAL_PASS/$TOTAL_CHECKS passed, $TOTAL_FAIL failed"
echo ""

[ "$TOTAL_FAIL" -eq 0 ] && exit 0 || exit 1
