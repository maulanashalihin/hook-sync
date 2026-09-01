#!/usr/bin/env bash
# bench-multi-region.sh — Multi-region topology test via hub-to-hub
#
# Region 1: edge1, edge2 → hub A
# Region 2: edge3, edge4 → hub B
# Hub-to-hub: hub A ←→ hub B (X-Node-Url header prevents loop)
#
# Tests:
#   1. Cross-region convergence — write to edge1, verify edge3+edge4 get data
#   2. Bidirectional — write to edge3, verify edge1+edge2 get data
#   3. Persistence — kill all, restart, verify data survives
#   4. Hub down + reconnect — kill hub B, write, restart, verify converge
#   5. Consistency — all nodes equal count, 0 pending, 0 dead letter
#
# All nodes are Go (hubs = Pebble, edges = mesh).

set -uo pipefail
cd "$(dirname "$0")"
ROOT="$(pwd)"

# Ports
HUB_A_PORT=9100
HUB_B_PORT=9200
E1_PORT=9101
E2_PORT=9102
E3_PORT=9201
E4_PORT=9202

DB_DIR=/tmp/multi-region
mkdir -p "$DB_DIR"

cleanup() {
	pkill -9 -f "hook-sync-hub|hook-sync-mesh-go" 2>/dev/null || true
	sleep 1
	rm -rf "$DB_DIR"/*.db "$DB_DIR"/*.db-wal "$DB_DIR"/*.db-shm "$DB_DIR"/*.pebble 2>/dev/null || true
}
trap cleanup EXIT

get_field() {
	local url=$1 field=$2
	curl -s "$url" 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('$field','MISSING'))" 2>/dev/null || echo "MISSING"
}

wait_all_converge() {
	local expected=$1 max_wait=$2
	local start_ts
	start_ts=$(date +%s)
	for _ in $(seq 1 "$max_wait"); do
		sleep 1
		local all_ok=true
		for port in $E1_PORT $E2_PORT $E3_PORT $E4_PORT; do
			local count pending
			count=$(get_field "http://localhost:$port/health" "item_count")
			pending=$(get_field "http://localhost:$port/health" "pending_changes")
			if [ "$count" != "$expected" ] || [ "$pending" != "0" ]; then
				all_ok=false
			fi
		done
		if [ "$all_ok" = "true" ]; then
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

check_all() {
	local label=$1 expected=$2
	local all_pass=true
	echo "  $label:"
	for port in $E1_PORT $E2_PORT $E3_PORT $E4_PORT; do
		local count pending dead id
		count=$(get_field "http://localhost:$port/health" "item_count")
		pending=$(get_field "http://localhost:$port/health" "pending_changes")
		dead=$(get_field "http://localhost:$port/health" "dead_letter")
		id=$(get_field "http://localhost:$port/health" "node_id")
		if [ "$count" = "$expected" ] && [ "$pending" = "0" ] && [ "$dead" = "0" ]; then
			echo "    ✅ $id: count=$count pending=$pending dead=$dead"
		else
			echo "    ❌ $id: count=$count pending=$pending dead=$dead (expected=$expected)"
			all_pass=false
		fi
	done
	if [ "$all_pass" = "true" ]; then
		return 0
	else
		return 1
	fi
}

write_items() {
	local port=$1 count=$2
	bun -e "
const URL='http://localhost:$port';
const TOTAL=$count, BATCH=1000;
async function writeBatch(items) {
  const resp = await fetch(URL+'/api/items/batch', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(items),
  });
  if (!resp.ok) { console.log('ERROR: ' + resp.status); return; }
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
console.log('  Written: ' + written + ' items to port $port');
" 2>&1
}

start_all() {
	local keep_db="${1:-no}"
	pkill -9 -f "hook-sync-hub|hook-sync-mesh-go" 2>/dev/null || true
	sleep 1
	if [ "$keep_db" != "keep" ]; then
		rm -rf "$DB_DIR"/*.db "$DB_DIR"/*.db-wal "$DB_DIR"/*.db-shm "$DB_DIR"/*.pebble 2>/dev/null || true
	fi

	# Hub A — edges: edge1, edge2, hub B
	nohup "$ROOT/hook-sync-hub" -id hubA -listen ":$HUB_A_PORT" \
		-url "http://localhost:$HUB_A_PORT" -db "$DB_DIR/hubA.pebble" \
		-edge "http://localhost:$E1_PORT" \
		-edge "http://localhost:$E2_PORT" \
		-edge "http://localhost:$HUB_B_PORT" \
		>"$DB_DIR/hubA.log" 2>&1 &

	# Hub B — edges: edge3, edge4, hub A
	nohup "$ROOT/hook-sync-hub" -id hubB -listen ":$HUB_B_PORT" \
		-url "http://localhost:$HUB_B_PORT" -db "$DB_DIR/hubB.pebble" \
		-edge "http://localhost:$E3_PORT" \
		-edge "http://localhost:$E4_PORT" \
		-edge "http://localhost:$HUB_A_PORT" \
		>"$DB_DIR/hubB.log" 2>&1 &

	# Edges
	nohup "$ROOT/hook-sync-mesh-go" -id edge1 -db "$DB_DIR/e1.db" -listen ":$E1_PORT" \
		-peer "http://localhost:$HUB_A_PORT" -batch-ms 50 -batch-size 10000 >"$DB_DIR/edge1.log" 2>&1 &
	nohup "$ROOT/hook-sync-mesh-go" -id edge2 -db "$DB_DIR/e2.db" -listen ":$E2_PORT" \
		-peer "http://localhost:$HUB_A_PORT" -batch-ms 50 -batch-size 10000 >"$DB_DIR/edge2.log" 2>&1 &
	nohup "$ROOT/hook-sync-mesh-go" -id edge3 -db "$DB_DIR/e3.db" -listen ":$E3_PORT" \
		-peer "http://localhost:$HUB_B_PORT" -batch-ms 50 -batch-size 10000 >"$DB_DIR/edge3.log" 2>&1 &
	nohup "$ROOT/hook-sync-mesh-go" -id edge4 -db "$DB_DIR/e4.db" -listen ":$E4_PORT" \
		-peer "http://localhost:$HUB_B_PORT" -batch-ms 50 -batch-size 10000 >"$DB_DIR/edge4.log" 2>&1 &

	sleep 3
}

echo "############################################"
echo "# Multi-Region Topology Test (hub-to-hub)"
echo "# Region 1: edge1,edge2 → hubA"
echo "# Region 2: edge3,edge4 → hubB"
echo "# Hub-to-hub: hubA ←→ hubB"
echo "############################################"
echo ""

# Build
echo "Building..."
cd "$ROOT/go" && go build -o "$ROOT/hook-sync-mesh-go" ./cmd/mesh && go build -o "$ROOT/hook-sync-hub" ./cmd/hub 2>&1
cd "$ROOT"
echo ""

PASS=0
FAIL=0

# --- Setup ---
echo "=== Setup: start 2 hubs, 4 edges ==="
start_all

echo "  Health check:"
ALL_UP=true
for port in $HUB_A_PORT $HUB_B_PORT $E1_PORT $E2_PORT $E3_PORT $E4_PORT; do
	id=""
	id=$(get_field "http://localhost:$port/health" "node_id")
	if [ "$id" = "MISSING" ]; then
		echo "    ❌ port $port not responding"
		ALL_UP=false
	else
		echo "    ✅ $id (port $port)"
	fi
done

if [ "$ALL_UP" != "true" ]; then
	echo "  ERROR: not all nodes up"
	FAIL=$((FAIL + 1))
	exit 1
fi
echo ""

# --- Phase 1: Cross-region (R1 → R2) ---
echo "=== Phase 1: Write 1000 to edge1 (Region 1), verify Region 2 converges ==="
write_items $E1_PORT 1000
echo "  Waiting for cross-region convergence..."
CONV_TIME=$(wait_all_converge 1000 30)
if [ "$CONV_TIME" = "TIMEOUT" ]; then
	echo "  ❌ TIMEOUT"
	FAIL=$((FAIL + 1))
else
	echo "  Converge time: ${CONV_TIME}s"
	if check_all "All nodes" 1000; then
		echo "  ✅ PASS: cross-region R1→R2"
		PASS=$((PASS + 1))
	else
		echo "  ❌ FAIL: consistency"
		FAIL=$((FAIL + 1))
	fi
fi
echo ""

# --- Phase 2: Bidirectional (R2 → R1) ---
echo "=== Phase 2: Write 1000 to edge3 (Region 2), verify Region 1 converges ==="
write_items $E3_PORT 1000
echo "  Waiting for cross-region convergence..."
CONV_TIME=$(wait_all_converge 2000 30)
if [ "$CONV_TIME" = "TIMEOUT" ]; then
	echo "  ❌ TIMEOUT"
	FAIL=$((FAIL + 1))
else
	echo "  Converge time: ${CONV_TIME}s"
	if check_all "All nodes" 2000; then
		echo "  ✅ PASS: cross-region R2→R1"
		PASS=$((PASS + 1))
	else
		echo "  ❌ FAIL: consistency"
		FAIL=$((FAIL + 1))
	fi
fi
echo ""

# --- Phase 3: Persistence ---
echo "=== Phase 3: Persistence — kill all, restart, verify data survives ==="
start_all keep
sleep 3
if check_all "After restart" 2000; then
	echo "  ✅ PASS: persistence"
	PASS=$((PASS + 1))
else
	echo "  ❌ FAIL: persistence"
	FAIL=$((FAIL + 1))
fi
echo ""

# --- Phase 4: Hub B down + reconnect ---
echo "=== Phase 4: Kill hub B, write to edge1, restart hub B, verify converge ==="
pkill -9 -f "hubB" 2>/dev/null || true
sleep 2

write_items $E1_PORT 500
sleep 3

# Region 2 edges should still have 2000 (hub B down, no forward)
R2_COUNT=$(get_field "http://localhost:$E3_PORT/health" "item_count")
echo "  Region 2 edge3 count while hub B down: $R2_COUNT (expected 2000)"

# Restart hub B
echo "  Restarting hub B..."
nohup "$ROOT/hook-sync-hub" -id hubB -listen ":$HUB_B_PORT" \
	-url "http://localhost:$HUB_B_PORT" -db "$DB_DIR/hubB.pebble" \
	-edge "http://localhost:$E3_PORT" \
	-edge "http://localhost:$E4_PORT" \
	-edge "http://localhost:$HUB_A_PORT" \
	>"$DB_DIR/hubB.log" 2>&1 &
sleep 3

echo "  Waiting for convergence after hub B reconnect..."
CONV_TIME=$(wait_all_converge 2500 30)
if [ "$CONV_TIME" = "TIMEOUT" ]; then
	echo "  ❌ TIMEOUT"
	FAIL=$((FAIL + 1))
else
	echo "  Converge time: ${CONV_TIME}s"
	if check_all "All nodes after hub B reconnect" 2500; then
		echo "  ✅ PASS: hub down + reconnect — no data loss"
		PASS=$((PASS + 1))
	else
		echo "  ❌ FAIL: consistency after hub reconnect"
		FAIL=$((FAIL + 1))
	fi
fi
echo ""

# --- Phase 5: Loop check — verify pending_forwards = 0 on both hubs ---
echo "=== Phase 5: Loop check — verify no infinite forwards ==="
HUB_A_FWD=$(get_field "http://localhost:$HUB_A_PORT/health" "pending_forwards")
HUB_B_FWD=$(get_field "http://localhost:$HUB_B_PORT/health" "pending_forwards")
echo "  hubA pending_forwards: $HUB_A_FWD"
echo "  hubB pending_forwards: $HUB_B_FWD"
if [ "$HUB_A_FWD" = "0" ] && [ "$HUB_B_FWD" = "0" ]; then
	echo "  ✅ PASS: no loop — pending_forwards = 0 on both hubs"
	PASS=$((PASS + 1))
else
	echo "  ❌ FAIL: pending forwards stuck (loop?)"
	FAIL=$((FAIL + 1))
fi
echo ""

# --- Summary ---
echo "############################################"
echo "# Multi-Region Test Summary"
echo "############################################"
echo ""
echo "  PASS: $PASS"
echo "  FAIL: $FAIL"
echo ""
if [ "$FAIL" -eq 0 ]; then
	echo "  ✅ ALL PASS — multi-region hub-to-hub works"
else
	echo "  ❌ SOME FAILED"
fi
echo ""
