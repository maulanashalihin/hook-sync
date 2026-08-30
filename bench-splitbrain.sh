#!/bin/bash
# bench-splitbrain.sh — Test hook-sync split-brain safety
#
# Scenario:
#   1. Two nodes connected, shared item synced
#   2. Network partition (kill both)
#   3. Node A updates item (value=100)
#   4. Node B updates same item (value=200)
#   5. Reconnect — let sync converge
#   6. Check: what value wins? Data loss?
#
# Also tests:
#   - INSERT during partition (new UUID, no collision)
#   - DELETE during partition (delete vs update conflict)
#   - Crash recovery (changes survive in _changes)

set -e

BINARY="${BINARY:-./hook-sync-go}"
PORT_A=19001
PORT_B=19002
DB_A=/tmp/splitbrain_a.db
DB_B=/tmp/splitbrain_b.db
LOG_A=/tmp/splitbrain_a.log
LOG_B=/tmp/splitbrain_b.log

PASS=0
FAIL=0
TOTAL=0

cleanup() {
	pkill -9 hook-sync-go 2>/dev/null || true
	rm -f $DB_A $DB_B $LOG_A $LOG_B
}

start_node() {
	local id=$1 db=$2 port=$3 peer=$4 log=$5
	if [ -z "$peer" ]; then
		nohup $BINARY -id $id -db $db -listen ":$port" -batch-ms 50 -batch-size 10000 >$log 2>&1 &
	else
		nohup $BINARY -id $id -db $db -listen ":$port" -peer "$peer" -batch-ms 50 -batch-size 10000 >$log 2>&1 &
	fi
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
	curl -s "$url" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$field','MISSING'))"
}

echo "============================================"
echo "  hook-sync Split-Brain Safety Test"
echo "============================================"
echo ""

# --- Setup ---
echo ">>> Cleanup"
cleanup

# --- Phase 1: Connect and create shared item ---
echo ""
echo ">>> Phase 1: Start both nodes, create shared item"
start_node nodeA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
start_node nodeB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

# Create item on nodeA
ITEM=$(curl -s -X POST "http://localhost:$PORT_A/api/items" -H "Content-Type: application/json" -d '{"name":"shared_item","value":0}')
ITEM_ID=$(echo "$ITEM" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
echo "  Created item: $ITEM_ID"

# Wait for sync
sleep 2

A_VAL=$(get_field "http://localhost:$PORT_A/api/items/$ITEM_ID" "value")
B_VAL=$(get_field "http://localhost:$PORT_B/api/items/$ITEM_ID" "value")
check "Both nodes have item after initial sync" "0" "$A_VAL"
check "nodeB received item" "0" "$B_VAL"

# --- Phase 2: Network partition ---
echo ""
echo ">>> Phase 2: Network partition — kill both nodes"
pkill -9 hook-sync-go 2>/dev/null
sleep 1
echo "  Both nodes killed. Changes survive in _changes table (SQLite file)."

# --- Phase 3: Independent updates during partition ---
echo ""
echo ">>> Phase 3: Start nodes INDEPENDENTLY (no peer), update same item"

# Start nodeA alone (no peer)
start_node nodeA $DB_A $PORT_A "" $LOG_A
# Start nodeB alone (no peer)
start_node nodeB $DB_B $PORT_B "" $LOG_B

# Update same item on nodeA → value=100
curl -s -X PUT "http://localhost:$PORT_A/api/items/$ITEM_ID" -H "Content-Type: application/json" -d '{"name":"shared_item","value":100}' >/dev/null
echo "  nodeA: updated item value=100"

# Update same item on nodeB → value=200
curl -s -X PUT "http://localhost:$PORT_B/api/items/$ITEM_ID" -H "Content-Type: application/json" -d '{"name":"shared_item","value":200}' >/dev/null
echo "  nodeB: updated item value=200"

# Also create new items (INSERT — should be safe, UUID)
NEW_A=$(curl -s -X POST "http://localhost:$PORT_A/api/items" -H "Content-Type: application/json" -d '{"name":"only_on_A","value":42}')
NEW_A_ID=$(echo "$NEW_A" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
echo "  nodeA: created new item (only_on_A) id=$NEW_A_ID"

NEW_B=$(curl -s -X POST "http://localhost:$PORT_B/api/items" -H "Content-Type: application/json" -d '{"name":"only_on_B","value":99}')
NEW_B_ID=$(echo "$NEW_B" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
echo "  nodeB: created new item (only_on_B) id=$NEW_B_ID"

# Verify local state during partition
A_VAL=$(get_field "http://localhost:$PORT_A/api/items/$ITEM_ID" "value")
B_VAL=$(get_field "http://localhost:$PORT_B/api/items/$ITEM_ID" "value")
check "During partition: nodeA has value=100" "100" "$A_VAL"
check "During partition: nodeB has value=200" "200" "$B_VAL"

# --- Phase 4: Reconnect ---
echo ""
echo ">>> Phase 4: Reconnect — restart both nodes with peer"
pkill -9 hook-sync-go 2>/dev/null
sleep 1

start_node nodeA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
start_node nodeB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

# Wait for sync convergence
echo "  Waiting for sync convergence..."
for i in $(seq 1 10); do
	sleep 1
	A_PENDING=$(get_field "http://localhost:$PORT_A/health" "pending_changes")
	B_PENDING=$(get_field "http://localhost:$PORT_B/health" "pending_changes")
	echo "  t=${i}s: nodeA pending=$A_PENDING, nodeB pending=$B_PENDING"
	if [ "$A_PENDING" = "0" ] && [ "$B_PENDING" = "0" ]; then
		break
	fi
done

# --- Phase 5: Verify convergence ---
echo ""
echo ">>> Phase 5: Verify convergence after reconnect"

A_VAL=$(get_field "http://localhost:$PORT_A/api/items/$ITEM_ID" "value")
B_VAL=$(get_field "http://localhost:$PORT_B/api/items/$ITEM_ID" "value")
A_COUNT=$(get_field "http://localhost:$PORT_A/health" "item_count")
B_COUNT=$(get_field "http://localhost:$PORT_B/health" "item_count")
A_DEAD=$(get_field "http://localhost:$PORT_A/health" "dead_letter")
B_DEAD=$(get_field "http://localhost:$PORT_B/health" "dead_letter")

echo "  Shared item value: nodeA=$A_VAL, nodeB=$B_VAL"
echo "  Item count: nodeA=$A_COUNT, nodeB=$B_COUNT"
echo "  Dead letter: nodeA=$A_DEAD, nodeB=$B_DEAD"

# Both nodes should have same value (last-write-wins, but both converge to SAME value)
check "Both nodes converge to same value for shared item" "$A_VAL" "$B_VAL"
check "No dead letter on nodeA" "0" "$A_DEAD"
check "No dead letter on nodeB" "0" "$B_DEAD"

# Both should have 3 items: original + only_on_A + only_on_B
check "nodeA has all 3 items" "3" "$A_COUNT"
check "nodeB has all 3 items" "3" "$B_COUNT"

# Verify new items from both nodes merged (INSERT = safe)
A_HAS_B=$(get_field "http://localhost:$PORT_A/api/items/$NEW_B_ID" "name")
B_HAS_A=$(get_field "http://localhost:$PORT_B/api/items/$NEW_A_ID" "name")
check "nodeA received nodeB's new item" "only_on_B" "$A_HAS_B"
check "nodeB received nodeA's new item" "only_on_A" "$B_HAS_A"

# --- Phase 6: DELETE vs UPDATE conflict ---
echo ""
echo ">>> Phase 6: DELETE vs UPDATE conflict test"

# Create new shared item
ITEM2=$(curl -s -X POST "http://localhost:$PORT_A/api/items" -H "Content-Type: application/json" -d '{"name":"delete_test","value":1}')
ITEM2_ID=$(echo "$ITEM2" | python3 -c "import sys,json;print(json.load(sys.stdin)['id'])")
sleep 2 # sync to nodeB

# Partition
pkill -9 hook-sync-go 2>/dev/null
sleep 1
start_node nodeA $DB_A $PORT_A "" $LOG_A
start_node nodeB $DB_B $PORT_B "" $LOG_B

# nodeA: DELETE the item
curl -s -X DELETE "http://localhost:$PORT_A/api/items/$ITEM2_ID" >/dev/null
echo "  nodeA: deleted item $ITEM2_ID"

# nodeB: UPDATE the item
curl -s -X PUT "http://localhost:$PORT_B/api/items/$ITEM2_ID" -H "Content-Type: application/json" -d '{"name":"delete_test","value":999}' >/dev/null
echo "  nodeB: updated item value=999"

# Reconnect
pkill -9 hook-sync-go 2>/dev/null
sleep 1
start_node nodeA $DB_A $PORT_A "http://localhost:$PORT_B" $LOG_A
start_node nodeB $DB_B $PORT_B "http://localhost:$PORT_A" $LOG_B

echo "  Waiting for convergence..."
for i in $(seq 1 10); do
	sleep 1
	A_PENDING=$(get_field "http://localhost:$PORT_A/health" "pending_changes")
	B_PENDING=$(get_field "http://localhost:$PORT_B/health" "pending_changes")
	if [ "$A_PENDING" = "0" ] && [ "$B_PENDING" = "0" ]; then
		break
	fi
done

A_ITEM2=$(curl -s "http://localhost:$PORT_A/api/items/$ITEM2_ID" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'value={d.get(\"value\",\"DELETED\")}')" 2>/dev/null || echo "DELETED")
B_ITEM2=$(curl -s "http://localhost:$PORT_B/api/items/$ITEM2_ID" | python3 -c "import sys,json; d=json.load(sys.stdin); print(f'value={d.get(\"value\",\"DELETED\")}')" 2>/dev/null || echo "DELETED")
echo "  After reconnect: nodeA item2=$A_ITEM2, nodeB item2=$B_ITEM2"
check "DELETE vs UPDATE: both nodes agree" "$A_ITEM2" "$B_ITEM2"

# --- Results ---
echo ""
echo "============================================"
echo "  Results: $PASS/$TOTAL passed, $FAIL failed"
echo "============================================"

if [ $FAIL -gt 0 ]; then
	echo ""
	echo "⚠️  Split-brain conflicts detected:"
	echo "  - UPDATE vs UPDATE: last-write-wins (one update silently lost)"
	echo "  - DELETE vs UPDATE: one operation wins (depends on sync order)"
	echo "  - INSERT: safe (UUID, no collision)"
	echo ""
	echo "  This is expected for async replication without consensus."
	echo "  Use case matters: append-heavy = safe, update-same-row = conflict."
else
	echo ""
	echo "All checks passed — both nodes converge to consistent state."
fi

# Cleanup
cleanup
