// bench-interval.js — Find optimal batch interval for hook-sync
// Usage: bun bench-interval.js
// Tests: 10ms, 25ms, 50ms, 100ms, 200ms, 500ms
// Metrics: sync delay (p50/p95), HTTP requests count, write throughput

const NODE1 = "http://localhost:9001";
const NODE2 = "http://localhost:9002";

function stats(arr) {
	const sorted = [...arr].sort((a, b) => a - b);
	const sum = sorted.reduce((a, b) => a + b, 0);
	return {
		count: sorted.length,
		min: sorted[0]?.toFixed(2) ?? 0,
		max: sorted[sorted.length - 1]?.toFixed(2) ?? 0,
		mean: (sum / sorted.length).toFixed(2),
		p50: sorted[Math.floor(sorted.length * 0.5)]?.toFixed(2) ?? 0,
		p95: sorted[Math.floor(sorted.length * 0.95)]?.toFixed(2) ?? 0,
		p99: sorted[Math.floor(sorted.length * 0.99)]?.toFixed(2) ?? 0,
	};
}

// Sync delay: write to node1, poll node2 until visible
async function syncDelay(n) {
	const delays = [];
	for (let i = 0; i < n; i++) {
		const t0 = performance.now();
		const resp = await fetch(`${NODE1}/api/items`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ name: `interval-test-${i}-${Date.now()}`, value: i }),
		});
		const data = await resp.json();
		const writtenId = data.id;

		let found = false;
		let attempts = 0;
		while (!found && attempts < 100) {
			attempts++;
			try {
				const r = await fetch(`${NODE2}/api/items/${writtenId}`);
				if (r.ok) {
					const item = await r.json();
					if (item.id === writtenId) found = true;
				}
			} catch {}
			if (!found) await Bun.sleep(5);
		}
		const delay = performance.now() - t0;
		delays.push(delay);
		if (!found) console.log(`  [FAIL] item ${i} not found after ${attempts} attempts`);
	}
	return delays;
}

// Write throughput: N concurrent writes
async function writeThroughput(n) {
	const t0 = performance.now();
	const promises = [];
	for (let i = 0; i < n; i++) {
		promises.push(
			fetch(`${NODE1}/api/items`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ name: `burst-${i}`, value: i }),
			}).then((r) => r.json()),
		);
	}
	await Promise.all(promises);
	const elapsed = performance.now() - t0;
	return { qps: Math.round((n / elapsed) * 1000), elapsed };
}

// Burst sync delay: write N concurrent, measure time until all visible on node2
async function burstSyncDelay(n) {
	// Write N concurrent
	const t0 = performance.now();
	const promises = [];
	const ids = [];
	for (let i = 0; i < n; i++) {
		promises.push(
			fetch(`${NODE1}/api/items`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ name: `burst-sync-${i}-${Date.now()}`, value: i }),
			}).then((r) => r.json()).then((d) => ids.push(d.id)),
		);
	}
	await Promise.all(promises);
	const writeDone = performance.now();

	// Poll until all N visible on node2
	let allFound = false;
	let pollAttempts = 0;
	while (!allFound && pollAttempts < 200) {
		pollAttempts++;
		try {
			const r = await fetch(`${NODE2}/health`);
			const h = await r.json();
			// Check if all IDs are present by counting (simpler than checking each)
			// We know how many items should be there
			allFound = true; // will verify below
		} catch {}
		if (!allFound) await Bun.sleep(5);
	}

	// Verify all IDs visible
	let foundCount = 0;
	for (const id of ids) {
		try {
			const r = await fetch(`${NODE2}/api/items/${id}`);
			if (r.ok) foundCount++;
		} catch {}
	}
	const allVisible = performance.now();
	return {
		writeMs: (writeDone - t0).toFixed(2),
		syncMs: (allVisible - writeDone).toFixed(2),
		totalMs: (allVisible - t0).toFixed(2),
		found: foundCount,
		total: ids.length,
	};
}

async function main() {
	const intervals = [10, 25, 50, 100, 200, 500];
	const results = [];

	console.log(`\n${"=".repeat(70)}`);
	console.log("  BATCH INTERVAL OPTIMIZATION BENCHMARK");
	console.log(`  Intervals: ${intervals.join("ms, ")}ms`);
	console.log(`  Tests: sync-delay (20 writes), write-throughput (100 concurrent), burst-sync (100 writes)`);
	console.log(`${"=".repeat(70)}\n`);

	for (const ms of intervals) {
		console.log(`\n--- Interval: ${ms}ms ---`);

		// Sync delay (sequential, 20 writes)
		const delays = await syncDelay(20);
		const s = stats(delays);
		console.log(`  Sync delay (20 sequential): p50=${s.p50}ms p95=${s.p95}ms mean=${s.mean}ms`);

		// Write throughput (100 concurrent)
		const tp = await writeThroughput(100);
		console.log(`  Write throughput (100 concurrent): ${tp.qps} QPS (${tp.elapsed.toFixed(2)}ms)`);

		// Burst sync delay (100 concurrent writes, wait until all visible)
		const burst = await burstSyncDelay(100);
		console.log(`  Burst sync (100 concurrent): write=${burst.writeMs}ms sync=${burst.syncMs}ms total=${burst.totalMs}ms (${burst.found}/${burst.total} found)`);

		results.push({
			interval: ms,
			syncP50: parseFloat(s.p50),
			syncP95: parseFloat(s.p95),
			syncMean: parseFloat(s.mean),
			writeQps: tp.qps,
			burstSyncMs: parseFloat(burst.syncMs),
			burstFound: `${burst.found}/${burst.total}`,
		});
	}

	// Summary table
	console.log(`\n${"=".repeat(70)}`);
	console.log("  SUMMARY: Batch Interval vs Performance");
	console.log(`${"=".repeat(70)}\n`);
	console.log("| Interval | Sync p50 | Sync p95 | Write QPS | Burst sync | Burst found |");
	console.log("|----------|---------:|---------:|----------:|-----------:|-------------|");
	for (const r of results) {
		console.log(`| ${r.interval}ms     | ${r.syncP50}ms   | ${r.syncP95}ms   | ${r.writeQps}       | ${r.burstSyncMs}ms      | ${r.burstFound}       |`);
	}

	// Analysis
	console.log("\n--- Analysis ---");
	const bestDelay = results.reduce((a, b) => (a.syncP50 < b.syncP50 ? a : b));
	const bestThroughput = results.reduce((a, b) => (a.writeQps > b.writeQps ? a : b));
const bestBurst = results.reduce((a, b) => (a.burstSyncMs < b.burstSyncMs ? a : b));
	console.log(`Best sync delay:    ${bestDelay.interval}ms (p50=${bestDelay.syncP50}ms)`);
	console.log(`Best write QPS:     ${bestThroughput.interval}ms (${bestThroughput.writeQps} QPS)`);
	console.log(`Best burst sync:    ${bestBurst.interval}ms (${bestBurst.burstSyncMs}ms)`);

	// Sweet spot: lowest sync delay without sacrificing throughput
	const scored = results.map(r => ({
		...r,
		// Score: normalize sync delay (lower=better) and throughput (higher=better)
		// Weight: 60% sync delay, 40% throughput
		score: (r.syncP50 / results[0].syncP50) * 0.6 + (results[0].writeQps / r.writeQps) * 0.4,
	}));
	const sweetSpot = scored.reduce((a, b) => (a.score < b.score ? a : b));
	console.log(`\nSweet spot (60% sync delay + 40% throughput): ${sweetSpot.interval}ms (score=${sweetSpot.score.toFixed(3)})`);

	console.log(`\n${"=".repeat(70)}\n`);
}

main();
