#!/bin/bash
# Benchmark: write throughput + sync delay
# Writes N items to node1, measures:
# 1. Write QPS (local write speed, no sync overhead in write path)
# 2. Sync delay (time from last write to all items visible on node2)

set -e

NODE1="http://localhost:9001"
NODE2="http://localhost:9002"
N=${1:-1000}

echo "=== hook-sync benchmark: $N writes ==="
echo ""

# 1. Write throughput
echo "--- Write throughput ---"
START=$(python3 -c "import time; print(time.time())")

for i in $(seq 1 $N); do
  curl -s -X POST "$NODE1/api/items" \
    -H 'Content-Type: application/json' \
    -d "{\"name\":\"item-$i\",\"value\":$i}" > /dev/null
done

END=$(python3 -c "import time; print(time.time())")
ELAPSED=$(python3 -c "print(f'{$END - $START:.3f}')")
QPS=$(python3 -c "print(f'{$N / ($END - $START):.0f}')")

echo "Writes: $N"
echo "Time: ${ELAPSED}s"
echo "Throughput: $QPS writes/sec"
echo ""

# 2. Sync delay — wait until node2 has all N items
echo "--- Sync delay ---"
SYNC_START=$(python3 -c "import time; print(time.time())")

while true; do
  COUNT=$(curl -s "$NODE2/health" | python3 -c "import json,sys; print(json.load(sys.stdin)['item_count'])")
  if [ "$COUNT" -ge "$N" ]; then
    break
  fi
  sleep 0.01
done

SYNC_END=$(python3 -c "import time; print(time.time())")
SYNC_DELAY=$(python3 -c "print(f'{($SYNC_END - $SYNC_START) * 1000:.0f}')")

echo "Synced $COUNT items to node2"
echo "Sync delay (last write → all visible): ${SYNC_DELAY}ms"
echo ""

# 3. Verify data integrity
echo "--- Data integrity ---"
NODE1_COUNT=$(curl -s "$NODE1/health" | python3 -c "import json,sys; print(json.load(sys.stdin)['item_count'])")
NODE2_COUNT=$(curl -s "$NODE2/health" | python3 -c "import json,sys; print(json.load(sys.stdin)['item_count'])")
echo "node1 items: $NODE1_COUNT"
echo "node2 items: $NODE2_COUNT"

if [ "$NODE1_COUNT" -eq "$NODE2_COUNT" ]; then
  echo "✅ Count match — sync complete"
else
  echo "❌ Count mismatch — node1=$NODE1_COUNT node2=$NODE2_COUNT"
fi

# 4. Spot check first and last item
echo ""
echo "--- Spot check ---"
FIRST=$(curl -s "$NODE1/api/items" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[-1]['id'])")
LAST=$(curl -s "$NODE1/api/items" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d[0]['id'])")

echo "First item on node1:"
curl -s "$NODE1/api/items/$FIRST" | python3 -m json.tool
echo "First item on node2:"
curl -s "$NODE2/api/items/$FIRST" | python3 -m json.tool

echo ""
echo "=== Summary ==="
echo "Writes: $N | Throughput: $QPS QPS | Sync delay: ${SYNC_DELAY}ms | Integrity: $NODE1_COUNT=$NODE2_COUNT"
