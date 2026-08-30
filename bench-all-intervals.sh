#!/bin/bash
# bench-all-intervals.sh — Run bench-interval.js for each batch interval
# Restarts nodes with different -batch-ms, runs benchmark, collects results

set -e

HSYNC="/Volumes/data/Project/hook-sync/hook-sync"
DBDIR="/Volumes/data/Project/hook-sync"
RESULTS="/tmp/interval-results.txt"

> "$RESULTS"

for MS in 10 25 50 100 200 500; do
  echo "=== Testing batch-ms=$MS ==="
  
  # Stop existing nodes
  pkill -f "hook-sync -id" 2>/dev/null || true
  sleep 1
  
  # Clean DBs
  rm -f "$DBDIR"/node1.db* "$DBDIR"/node2.db*
  
  # Start nodes with this interval
  nohup "$HSYNC" -id node1 -db "$DBDIR/node1.db" -listen :9001 -peer http://localhost:9002 -batch-ms $MS > /tmp/node1.log 2>&1 &
  sleep 0.5
  nohup "$HSYNC" -id node2 -db "$DBDIR/node2.db" -listen :9002 -peer http://localhost:9001 -batch-ms $MS > /tmp/node2.log 2>&1 &
  sleep 2
  
  # Verify health
  N1=$(curl -s http://localhost:9001/health 2>/dev/null)
  N2=$(curl -s http://localhost:9002/health 2>/dev/null)
  if [ -z "$N1" ] || [ -z "$N2" ]; then
    echo "FAILED to start nodes with batch-ms=$MS"
    cat /tmp/node1.log
    cat /tmp/node2.log
    continue
  fi
  echo "Nodes healthy: $N1 / $N2"
  
  # Run benchmark for this interval
  echo ""
  echo "--- Benchmark: batch-ms=$MS ---"
  cd "$DBDIR"
  bun -e "
const NODE1 = 'http://localhost:9001';
const NODE2 = 'http://localhost:9002';

function stats(arr) {
  const sorted = [...arr].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    min: sorted[0]?.toFixed(2) ?? 0,
    max: sorted[sorted.length - 1]?.toFixed(2) ?? 0,
    mean: (sum / sorted.length).toFixed(2),
    p50: sorted[Math.floor(sorted.length * 0.5)]?.toFixed(2) ?? 0,
    p95: sorted[Math.floor(sorted.length * 0.95)]?.toFixed(2) ?? 0,
  };
}

async function syncDelay(n) {
  const delays = [];
  for (let i = 0; i < n; i++) {
    const t0 = performance.now();
    const resp = await fetch(NODE1 + '/api/items', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name: 'test-' + i + '-' + Date.now(), value: i}),
    });
    const data = await resp.json();
    const id = data.id;
    let found = false;
    let attempts = 0;
    while (!found && attempts < 100) {
      attempts++;
      try {
        const r = await fetch(NODE2 + '/api/items/' + id);
        if (r.ok) {
          const item = await r.json();
          if (item.id === id) found = true;
        }
      } catch {}
      if (!found) await Bun.sleep(5);
    }
    delays.push(performance.now() - t0);
  }
  return delays;
}

async function writeThroughput(n) {
  const t0 = performance.now();
  const promises = [];
  for (let i = 0; i < n; i++) {
    promises.push(fetch(NODE1 + '/api/items', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name: 'burst-' + i, value: i}),
    }).then(r => r.json()));
  }
  await Promise.all(promises);
  const elapsed = performance.now() - t0;
  return {qps: Math.round((n / elapsed) * 1000), elapsed: elapsed.toFixed(2)};
}

async function burstSync(n) {
  const t0 = performance.now();
  const promises = [];
  const ids = [];
  for (let i = 0; i < n; i++) {
    promises.push(fetch(NODE1 + '/api/items', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({name: 'burst-sync-' + i + '-' + Date.now(), value: i}),
    }).then(r => r.json()).then(d => ids.push(d.id)));
  }
  await Promise.all(promises);
  const writeDone = performance.now();
  
  // Poll until all found
  let foundCount = 0;
  let pollRounds = 0;
  while (foundCount < ids.length && pollRounds < 200) {
    pollRounds++;
    foundCount = 0;
    for (const id of ids) {
      try {
        const r = await fetch(NODE2 + '/api/items/' + id);
        if (r.ok) foundCount++;
      } catch {}
    }
    if (foundCount < ids.length) await Bun.sleep(5);
  }
  const allVisible = performance.now();
  return {
    writeMs: (writeDone - t0).toFixed(2),
    syncMs: (allVisible - writeDone).toFixed(2),
    found: foundCount,
    total: ids.length,
  };
}

async function run() {
  const MS = $MS;
  
  // Sync delay
  const delays = await syncDelay(20);
  const s = stats(delays);
  
  // Write throughput
  const tp = await writeThroughput(100);
  
  // Burst sync
  const burst = await burstSync(100);
  
  const line = '| ' + MS + 'ms     | ' + s.p50 + 'ms   | ' + s.p95 + 'ms   | ' + tp.qps + '       | ' + burst.syncMs + 'ms      | ' + burst.found + '/' + burst.total + '       |';
  console.log(line);
  return line;
}

run().then(line => {
  const fs = require('fs');
  fs.appendFileSync('$RESULTS', line + '\n');
});
" 2>&1
done

pkill -f "hook-sync -id" 2>/dev/null || true

echo ""
echo "=== RESULTS ==="
echo "| Interval | Sync p50 | Sync p95 | Write QPS | Burst sync | Burst found |"
echo "|----------|---------:|---------:|----------:|-----------:|-------------|"
cat "$RESULTS"
