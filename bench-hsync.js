// bench-hsync.js — Benchmark hook-sync using same methodology as bench.js
// Usage: bun bench-hsync.js <test> [count]
// test: write-latency | read-latency | sync-delay | write-throughput | read-throughput | all

const NODE1 = "http://localhost:9001";
const NODE2 = "http://localhost:9002";

const test = process.argv[2] || "all";
const count = parseInt(process.argv[3] || "100");

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

async function writeLatency(url, label, n) {
	const times = [];
	for (let i = 0; i < n; i++) {
		const t0 = performance.now();
		const resp = await fetch(`${url}/api/items`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ name: `item-${i}`, value: i }),
		});
		await resp.json();
		times.push(performance.now() - t0);
	}
	console.log(`\n=== WRITE LATENCY: ${label} (${n} requests) ===`);
	console.log(stats(times));
	return times;
}

async function readLatency(url, label, n) {
	const times = [];
	for (let i = 0; i < n; i++) {
		const t0 = performance.now();
		const resp = await fetch(`${url}/api/items`);
		await resp.json();
		times.push(performance.now() - t0);
	}
	console.log(`\n=== READ LATENCY: ${label} (${n} requests) ===`);
	console.log(stats(times));
	return times;
}

async function syncDelay(writeUrl, readUrl, label, n) {
	const delays = [];
	for (let i = 0; i < n; i++) {
		// Write
		const t0 = performance.now();
		const resp = await fetch(`${writeUrl}/api/items`, {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ name: `sync-test-${i}-${Date.now()}`, value: i }),
		});
		const data = await resp.json();
		const writtenId = data.id;

		// Poll read until we see the new item
		let found = false;
		let attempts = 0;
		while (!found && attempts < 50) {
			attempts++;
			try {
				const r = await fetch(`${readUrl}/api/items/${writtenId}`);
				if (r.ok) {
					const item = await r.json();
					if (item.id === writtenId) {
						found = true;
					}
				}
			} catch {}
			if (!found) await Bun.sleep(10);
		}
		const delay = performance.now() - t0;
		delays.push(delay);
		if (!found)
			console.log(`  [FAIL] item ${i} not found after ${attempts} attempts`);
	}
	console.log(
		`\n=== SYNC DELAY: ${label} (${n} writes, poll until visible) ===`,
	);
	console.log(stats(delays));
	return delays;
}

async function writeThroughput(url, label, n) {
	const t0 = performance.now();
	const promises = [];
	for (let i = 0; i < n; i++) {
		promises.push(
			fetch(`${url}/api/items`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ name: `burst-${i}`, value: i }),
			}).then((r) => r.json()),
		);
	}
	await Promise.all(promises);
	const elapsed = performance.now() - t0;
	const qps = ((n / elapsed) * 1000).toFixed(0);
	console.log(`\n=== WRITE THROUGHPUT: ${label} (${n} concurrent) ===`);
	console.log({ total_ms: elapsed.toFixed(2), qps, count: n });
	return { qps: parseFloat(qps), elapsed };
}

async function readThroughput(url, label, n) {
	const t0 = performance.now();
	const promises = [];
	for (let i = 0; i < n; i++) {
		promises.push(fetch(`${url}/api/items`).then((r) => r.json()));
	}
	await Promise.all(promises);
	const elapsed = performance.now() - t0;
	const qps = ((n / elapsed) * 1000).toFixed(0);
	console.log(`\n=== READ THROUGHPUT: ${label} (${n} concurrent) ===`);
	console.log({ total_ms: elapsed.toFixed(2), qps, count: n });
	return { qps: parseFloat(qps), elapsed };
}

async function writeThroughputDual(url1, url2, label, n) {
	const t0 = performance.now();
	const promises = [];
	for (let i = 0; i < n; i++) {
		const url = i % 2 === 0 ? url1 : url2;
		promises.push(
			fetch(`${url}/api/items`, {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ name: `dual-${i}`, value: i }),
			}).then((r) => r.json()),
		);
	}
	await Promise.all(promises);
	const elapsed = performance.now() - t0;
	const qps = ((n / elapsed) * 1000).toFixed(0);
	console.log(
		`\n=== WRITE THROUGHPUT (dual-node): ${label} (${n} concurrent, round-robin) ===`,
	);
	console.log({ total_ms: elapsed.toFixed(2), qps, count: n });
	return { qps: parseFloat(qps), elapsed };
}

async function readThroughputDual(url1, url2, label, n) {
	const t0 = performance.now();
	const promises = [];
	for (let i = 0; i < n; i++) {
		const url = i % 2 === 0 ? url1 : url2;
		promises.push(fetch(`${url}/api/items`).then((r) => r.json()));
	}
	await Promise.all(promises);
	const elapsed = performance.now() - t0;
	const qps = ((n / elapsed) * 1000).toFixed(0);
	console.log(
		`\n=== READ THROUGHPUT (dual-node): ${label} (${n} concurrent, round-robin) ===`,
	);
	console.log({ total_ms: elapsed.toFixed(2), qps, count: n });
	return { qps: parseFloat(qps), elapsed };
}

async function main() {
	console.log(`\n${"=".repeat(60)}`);
	console.log(`  BENCHMARK: HOOK-SYNC — ${test}`);
	console.log(`  NODE1: ${NODE1}`);
	console.log(`  NODE2: ${NODE2}`);
	console.log(`  Count: ${count}`);
	console.log(`${"=".repeat(60)}\n`);

	// hook-sync: both nodes can write, bidirectional sync (like cr-sqlite)
	if (test === "write-latency" || test === "all") {
		await writeLatency(NODE1, "node1 (multi-writer)", count);
		await writeLatency(NODE2, "node2 (multi-writer)", count);
	}
	if (test === "read-latency" || test === "all") {
		await readLatency(NODE1, "node1", count);
		await readLatency(NODE2, "node2", count);
	}
	if (test === "sync-delay" || test === "all") {
		await syncDelay(NODE1, NODE2, "node1→node2 (preupdate hook sync)", Math.min(count, 20));
		await syncDelay(NODE2, NODE1, "node2→node1 (preupdate hook sync)", Math.min(count, 20));
	}
	if (test === "write-throughput" || test === "all") {
		await writeThroughput(NODE1, "node1 only", count);
		await writeThroughputDual(NODE1, NODE2, "node1+node2 round-robin", count);
	}
	if (test === "read-throughput" || test === "all") {
		await readThroughputDual(NODE1, NODE2, "node1+node2 round-robin", count);
	}

	console.log(`\n${"=".repeat(60)}`);
	console.log("  BENCHMARK COMPLETE");
	console.log(`${"=".repeat(60)}\n`);
}

main();
