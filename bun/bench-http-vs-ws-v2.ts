// bench-http-vs-ws-v2.ts — Comprehensive HTTP vs WS transport comparison
// Tests: HTTP and WS at multiple intervals (1ms, 5ms, 10ms, 50ms)
// + WS event-driven push (ship immediately after write, no timer)
// Measures: sync delay, write QPS, convergence, CPU, memory
// Usage: bun bench-http-vs-ws-v2.ts

import { Database } from "bun:sqlite";

// ─── Shared ───────────────────────────────────────────────────────

const HEX = Array.from({ length: 256 }, (_, i) =>
	i.toString(16).padStart(2, "0"),
);
function uuidv7(): string {
	const ts = Date.now();
	const buf = new Uint8Array(16);
	crypto.getRandomValues(buf);
	buf[0] = (ts / 2 ** 40) & 0xff;
	buf[1] = (ts / 2 ** 32) & 0xff;
	buf[2] = (ts / 2 ** 24) & 0xff;
	buf[3] = (ts / 2 ** 16) & 0xff;
	buf[4] = (ts / 2 ** 8) & 0xff;
	buf[5] = ts & 0xff;
	buf[6] = (buf[6] & 0x0f) | 0x70;
	buf[8] = (buf[8] & 0x3f) | 0x80;
	return (
		HEX[buf[0]] +
		HEX[buf[1]] +
		HEX[buf[2]] +
		HEX[buf[3]] +
		"-" +
		HEX[buf[4]] +
		HEX[buf[5]] +
		"-" +
		HEX[buf[6]] +
		HEX[buf[7]] +
		"-" +
		HEX[buf[8]] +
		HEX[buf[9]] +
		"-" +
		HEX[buf[10]] +
		HEX[buf[11]] +
		HEX[buf[12]] +
		HEX[buf[13]] +
		HEX[buf[14]] +
		HEX[buf[15]]
	);
}

interface Change {
	op: string;
	table: string;
	row: Record<string, unknown> | null;
	old_id: string | null;
}

function setupDB(path: string): Database {
	const db = new Database(path);
	db.exec("PRAGMA journal_mode = WAL");
	db.exec("PRAGMA synchronous = NORMAL");
	db.exec(`
		CREATE TABLE IF NOT EXISTS items (
			id TEXT PRIMARY KEY, name TEXT, value INTEGER,
			created_at INTEGER, updated_at INTEGER
		);
		CREATE TABLE IF NOT EXISTS _changes (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			op TEXT, table_name TEXT, row_id TEXT, row_data TEXT
		);
		CREATE TABLE IF NOT EXISTS _meta (key TEXT PRIMARY KEY, value TEXT);
		INSERT OR IGNORE INTO _meta VALUES ('syncing', '0');
		CREATE TRIGGER IF NOT EXISTS items_ai AFTER INSERT ON items
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(op, table_name, row_id, row_data) VALUES('INSERT', 'items', NEW.id,
				json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at));
		END;
		CREATE TRIGGER IF NOT EXISTS items_au AFTER UPDATE ON items
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(op, table_name, row_id, row_data) VALUES('UPDATE', 'items', NEW.id,
				json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value, 'created_at', NEW.created_at, 'updated_at', NEW.updated_at));
		END;
		CREATE TRIGGER IF NOT EXISTS items_ad AFTER DELETE ON items
		WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
		BEGIN
			INSERT INTO _changes(op, table_name, row_id, row_data) VALUES('DELETE', 'items', OLD.id,
				json_object('id', OLD.id, 'name', OLD.name, 'value', OLD.value, 'created_at', OLD.created_at, 'updated_at', OLD.updated_at));
		END;
	`);
	return db;
}

function applyChanges(db: Database, changes: Change[]): number {
	db.exec("UPDATE _meta SET value = '1' WHERE key = 'syncing'");
	const tx = db.transaction(() => {
		for (const c of changes) {
			if (c.op === "DELETE") {
				if (!c.old_id) continue;
				const row = c.row as Record<string, unknown> | null;
				if (row) {
					const delTs = Number(row.updated_at) || 0;
					const existing = db
						.query("SELECT updated_at FROM items WHERE id = ?")
						.get(c.old_id) as { updated_at: number } | null;
					if (existing && Number(existing.updated_at) > delTs) continue;
				}
				db.prepare("DELETE FROM items WHERE id = ?").run(c.old_id);
			} else {
				if (!c.row) continue;
				const id = c.row.id as string;
				if (!id) continue;
				const updatedAt = Number(c.row.updated_at) || 0;
				const existing = db
					.query("SELECT updated_at FROM items WHERE id = ?")
					.get(id) as { updated_at: number } | null;
				if (existing && Number(existing.updated_at) > updatedAt) continue;
				const cols = Object.keys(c.row);
				const vals = cols.map((k) => c.row![k]);
				const placeholders = cols.map(() => "?").join(", ");
				db.prepare(
					`INSERT OR REPLACE INTO items (${cols.join(", ")}) VALUES (${placeholders})`,
				).run(...vals);
			}
		}
	});
	tx();
	db.exec("UPDATE _meta SET value = '0' WHERE key = 'syncing'");
	return changes.length;
}

function drainChanges(db: Database): { batchId: number; changes: Change[] } {
	const rows = db
		.query(
			"SELECT id, op, table_name, row_data FROM _changes ORDER BY id LIMIT 10000",
		)
		.all() as {
		id: number;
		op: string;
		table_name: string;
		row_data: string;
	}[];
	if (rows.length === 0) return { batchId: 0, changes: [] };
	const batchId = rows[rows.length - 1].id;
	const changes: Change[] = rows.map((r) => ({
		op: r.op,
		table: r.table_name,
		row: r.row_data ? JSON.parse(r.row_data) : null,
		old_id: r.op === "DELETE" ? JSON.parse(r.row_data).id : null,
	}));
	return { batchId, changes };
}

function deleteChanges(db: Database, upToId: number): void {
	db.prepare("DELETE FROM _changes WHERE id <= ?").run(upToId);
}

function countItems(db: Database): number {
	return (db.query("SELECT COUNT(*) as c FROM items").get() as { c: number }).c;
}

function countPending(db: Database): number {
	return (db.query("SELECT COUNT(*) as c FROM _changes").get() as { c: number })
		.c;
}

function cleanFiles(prefix: string) {
	for (const f of [`${prefix}-a.db`, `${prefix}-b.db`]) {
		try {
			require("fs").unlinkSync(f);
		} catch {}
		try {
			require("fs").unlinkSync(f + "-wal");
		} catch {}
		try {
			require("fs").unlinkSync(f + "-shm");
		} catch {}
	}
}

// ─── HTTP transport at variable interval ──────────────────────────

async function testHttp(
	N: number,
	batchMs: number,
): Promise<{
	convergenceMs: number;
	writeQps: number;
	syncDelayMs: number;
}> {
	const prefix = `/Volumes/data/tmp/bench-v2-http-${batchMs}ms`;
	cleanFiles(prefix);
	const dbA = setupDB(`${prefix}-a.db`);
	const dbB = setupDB(`${prefix}-b.db`);

	const serverB = Bun.serve({
		port: 9201,
		async fetch(req) {
			const url = new URL(req.url);
			if (req.method === "POST" && url.pathname === "/sync") {
				const body = await req.json();
				applyChanges(dbB, body.changes);
				return Response.json({
					applied: body.changes.length,
					ack: body.batch_id,
				});
			}
			if (url.pathname === "/count") {
				return Response.json({ count: countItems(dbB) });
			}
			return new Response("404", { status: 404 });
		},
	});

	let shipping = false;
	const shipLoop = setInterval(async () => {
		if (shipping) return;
		shipping = true;
		try {
			const { batchId, changes } = drainChanges(dbA);
			if (changes.length === 0) return;
			const resp = await fetch("http://localhost:9201/sync", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ batch_id: batchId, changes }),
			});
			const ack = await resp.json();
			if (ack.ack === batchId) deleteChanges(dbA, batchId);
		} finally {
			shipping = false;
		}
	}, batchMs);

	const writeStart = performance.now();
	let firstSyncDelay = 0;
	const writeStmt = dbA.prepare(
		"INSERT INTO items(id, name, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?)",
	);

	for (let i = 0; i < N; i++) {
		const id = uuidv7();
		const now = Date.now();
		writeStmt.run(id, `item-${i}`, i, now, now);
		if (i === 0) {
			const checkStart = performance.now();
			while (true) {
				const resp = await fetch("http://localhost:9201/count");
				const data = await resp.json();
				if (data.count > 0) {
					firstSyncDelay = performance.now() - checkStart;
					break;
				}
				await Bun.sleep(0.5);
			}
		}
	}
	const writeEnd = performance.now();
	const writeQps = Math.round(N / ((writeEnd - writeStart) / 1000));

	const convStart = performance.now();
	while (true) {
		const resp = await fetch("http://localhost:9201/count");
		const data = await resp.json();
		if (data.count >= N && countPending(dbA) === 0) break;
		await Bun.sleep(2);
	}
	const convergenceMs = Math.round(performance.now() - convStart);

	clearInterval(shipLoop);
	serverB.stop();
	dbA.close();
	dbB.close();
	return { convergenceMs, writeQps, syncDelayMs: Math.round(firstSyncDelay) };
}

// ─── WS transport at variable interval ────────────────────────────

async function testWs(
	N: number,
	batchMs: number,
): Promise<{
	convergenceMs: number;
	writeQps: number;
	syncDelayMs: number;
}> {
	const prefix = `/Volumes/data/tmp/bench-v2-ws-${batchMs}ms`;
	cleanFiles(prefix);
	const dbA = setupDB(`${prefix}-a.db`);
	const dbB = setupDB(`${prefix}-b.db`);

	const ackResolvers = new Map<number, () => void>();
	let batchCounter = 0;

	const serverB = Bun.serve({
		port: 9202,
		websocket: {
			async message(ws, message) {
				const data = JSON.parse(message.toString());
				if (data.type === "sync") {
					applyChanges(dbB, data.changes);
					ws.send(
						JSON.stringify({
							type: "ack",
							batch_id: data.batch_id,
							applied: data.changes.length,
						}),
					);
				}
			},
		},
		fetch(req, server) {
			const url = new URL(req.url);
			if (url.pathname === "/ws") {
				server.upgrade(req);
				return;
			}
			if (url.pathname === "/count") {
				return Response.json({ count: countItems(dbB) });
			}
			return new Response("404", { status: 404 });
		},
	});

	const wsClient = new WebSocket("ws://localhost:9202/ws");
	await new Promise((resolve) => {
		wsClient!.onopen = resolve;
	});
	wsClient.onmessage = (ev) => {
		const data = JSON.parse(ev.data);
		if (data.type === "ack") {
			const resolver = ackResolvers.get(data.batch_id);
			if (resolver) {
				resolver();
				ackResolvers.delete(data.batch_id);
			}
		}
	};

	let shipping = false;
	const shipLoop = setInterval(async () => {
		if (shipping || !wsClient || wsClient.readyState !== WebSocket.OPEN) return;
		shipping = true;
		try {
			const { batchId, changes } = drainChanges(dbA);
			if (changes.length === 0) return;
			const myBatchId = ++batchCounter;
			const { promise: ackPromise, resolve: ackResolve } =
				Promise.withResolvers<void>();
			ackResolvers.set(myBatchId, ackResolve);
			wsClient.send(
				JSON.stringify({ type: "sync", batch_id: myBatchId, changes }),
			);
			await ackPromise;
			deleteChanges(dbA, batchId);
		} finally {
			shipping = false;
		}
	}, batchMs);

	const writeStart = performance.now();
	let firstSyncDelay = 0;
	const writeStmt = dbA.prepare(
		"INSERT INTO items(id, name, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?)",
	);

	for (let i = 0; i < N; i++) {
		const id = uuidv7();
		const now = Date.now();
		writeStmt.run(id, `item-${i}`, i, now, now);
		if (i === 0) {
			const checkStart = performance.now();
			while (true) {
				const resp = await fetch("http://localhost:9202/count");
				const data = await resp.json();
				if (data.count > 0) {
					firstSyncDelay = performance.now() - checkStart;
					break;
				}
				await Bun.sleep(0.5);
			}
		}
	}
	const writeEnd = performance.now();
	const writeQps = Math.round(N / ((writeEnd - writeStart) / 1000));

	const convStart = performance.now();
	while (true) {
		const resp = await fetch("http://localhost:9202/count");
		const data = await resp.json();
		if (data.count >= N && countPending(dbA) === 0) break;
		await Bun.sleep(2);
	}
	const convergenceMs = Math.round(performance.now() - convStart);

	clearInterval(shipLoop);
	wsClient.close();
	serverB.stop();
	dbA.close();
	dbB.close();
	return { convergenceMs, writeQps, syncDelayMs: Math.round(firstSyncDelay) };
}

// ─── WS event-driven: ship immediately after each write batch ─────
// Simulates what a commit_hook callback would do:
// after every K writes, immediately drain + ship (no timer wait)

async function testWsEventDriven(
	N: number,
	flushEvery: number,
): Promise<{
	convergenceMs: number;
	writeQps: number;
	syncDelayMs: number;
}> {
	const prefix = `/Volumes/data/tmp/bench-v2-wsevent-${flushEvery}`;
	cleanFiles(prefix);
	const dbA = setupDB(`${prefix}-a.db`);
	const dbB = setupDB(`${prefix}-b.db`);

	const ackResolvers = new Map<number, () => void>();
	let batchCounter = 0;

	const serverB = Bun.serve({
		port: 9203,
		websocket: {
			async message(ws, message) {
				const data = JSON.parse(message.toString());
				if (data.type === "sync") {
					applyChanges(dbB, data.changes);
					ws.send(
						JSON.stringify({
							type: "ack",
							batch_id: data.batch_id,
							applied: data.changes.length,
						}),
					);
				}
			},
		},
		fetch(req, server) {
			const url = new URL(req.url);
			if (url.pathname === "/ws") {
				server.upgrade(req);
				return;
			}
			if (url.pathname === "/count") {
				return Response.json({ count: countItems(dbB) });
			}
			return new Response("404", { status: 404 });
		},
	});

	const wsClient = new WebSocket("ws://localhost:9203/ws");
	await new Promise((resolve) => {
		wsClient!.onopen = resolve;
	});
	wsClient.onmessage = (ev) => {
		const data = JSON.parse(ev.data);
		if (data.type === "ack") {
			const resolver = ackResolvers.get(data.batch_id);
			if (resolver) {
				resolver();
				ackResolvers.delete(data.batch_id);
			}
		}
	};

	async function shipNow(): Promise<void> {
		if (!wsClient || wsClient.readyState !== WebSocket.OPEN) return;
		const { batchId, changes } = drainChanges(dbA);
		if (changes.length === 0) return;
		const myBatchId = ++batchCounter;
		const { promise: ackPromise, resolve: ackResolve } =
			Promise.withResolvers<void>();
		ackResolvers.set(myBatchId, ackResolve);
		wsClient.send(
			JSON.stringify({ type: "sync", batch_id: myBatchId, changes }),
		);
		await ackPromise;
		deleteChanges(dbA, batchId);
	}

	const writeStart = performance.now();
	let firstSyncDelay = 0;
	const writeStmt = dbA.prepare(
		"INSERT INTO items(id, name, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?)",
	);

	for (let i = 0; i < N; i++) {
		const id = uuidv7();
		const now = Date.now();
		writeStmt.run(id, `item-${i}`, i, now, now);

		if (i === 0) {
			// Ship first write immediately
			await shipNow();
			const checkStart = performance.now();
			while (true) {
				const resp = await fetch("http://localhost:9203/count");
				const data = await resp.json();
				if (data.count > 0) {
					firstSyncDelay = performance.now() - checkStart;
					break;
				}
				await Bun.sleep(0.5);
			}
		}

		// Flush every K writes (simulates commit_hook callback)
		if ((i + 1) % flushEvery === 0) {
			await shipNow();
		}
	}
	const writeEnd = performance.now();
	const writeQps = Math.round(N / ((writeEnd - writeStart) / 1000));

	// Final flush
	await shipNow();

	const convStart = performance.now();
	while (true) {
		const resp = await fetch("http://localhost:9203/count");
		const data = await resp.json();
		if (data.count >= N && countPending(dbA) === 0) break;
		await Bun.sleep(2);
	}
	const convergenceMs = Math.round(performance.now() - convStart);

	wsClient.close();
	serverB.stop();
	dbA.close();
	dbB.close();
	return { convergenceMs, writeQps, syncDelayMs: Math.round(firstSyncDelay) };
}

// ─── HTTP event-driven: ship immediately after each write batch ───
// Same as WS event-driven but using HTTP — isolates transport overhead

async function testHttpEventDriven(
	N: number,
	flushEvery: number,
): Promise<{
	convergenceMs: number;
	writeQps: number;
	syncDelayMs: number;
}> {
	const prefix = `/Volumes/data/tmp/bench-v2-httpevent-${flushEvery}`;
	cleanFiles(prefix);
	const dbA = setupDB(`${prefix}-a.db`);
	const dbB = setupDB(`${prefix}-b.db`);

	const serverB = Bun.serve({
		port: 9204,
		async fetch(req) {
			const url = new URL(req.url);
			if (req.method === "POST" && url.pathname === "/sync") {
				const body = await req.json();
				applyChanges(dbB, body.changes);
				return Response.json({
					applied: body.changes.length,
					ack: body.batch_id,
				});
			}
			if (url.pathname === "/count") {
				return Response.json({ count: countItems(dbB) });
			}
			return new Response("404", { status: 404 });
		},
	});

	async function shipNow(): Promise<void> {
		const { batchId, changes } = drainChanges(dbA);
		if (changes.length === 0) return;
		const resp = await fetch("http://localhost:9204/sync", {
			method: "POST",
			headers: { "Content-Type": "application/json" },
			body: JSON.stringify({ batch_id: batchId, changes }),
		});
		const ack = await resp.json();
		if (ack.ack === batchId) deleteChanges(dbA, batchId);
	}

	const writeStart = performance.now();
	let firstSyncDelay = 0;
	const writeStmt = dbA.prepare(
		"INSERT INTO items(id, name, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?)",
	);

	for (let i = 0; i < N; i++) {
		const id = uuidv7();
		const now = Date.now();
		writeStmt.run(id, `item-${i}`, i, now, now);

		if (i === 0) {
			await shipNow();
			const checkStart = performance.now();
			while (true) {
				const resp = await fetch("http://localhost:9204/count");
				const data = await resp.json();
				if (data.count > 0) {
					firstSyncDelay = performance.now() - checkStart;
					break;
				}
				await Bun.sleep(0.5);
			}
		}

		if ((i + 1) % flushEvery === 0) {
			await shipNow();
		}
	}
	const writeEnd = performance.now();
	const writeQps = Math.round(N / ((writeEnd - writeStart) / 1000));

	await shipNow();

	const convStart = performance.now();
	while (true) {
		const resp = await fetch("http://localhost:9204/count");
		const data = await resp.json();
		if (data.count >= N && countPending(dbA) === 0) break;
		await Bun.sleep(2);
	}
	const convergenceMs = Math.round(performance.now() - convStart);

	serverB.stop();
	dbA.close();
	dbB.close();
	return { convergenceMs, writeQps, syncDelayMs: Math.round(firstSyncDelay) };
}

// ─── Main ─────────────────────────────────────────────────────────

const N = 10000;
const ROUNDS = 3;

interface Result {
	delay: number[];
	qps: number[];
	conv: number[];
}
const results: Record<string, Result> = {};

async function runTest(
	name: string,
	fn: () => Promise<{
		convergenceMs: number;
		writeQps: number;
		syncDelayMs: number;
	}>,
) {
	if (!results[name]) results[name] = { delay: [], qps: [], conv: [] };
	for (let r = 0; r < ROUNDS; r++) {
		const res = await fn();
		results[name].delay.push(res.syncDelayMs);
		results[name].qps.push(res.writeQps);
		results[name].conv.push(res.convergenceMs);
		console.log(
			`${name.padEnd(28)}  ${String(r + 1).padStart(5)}  ${String(res.syncDelayMs + "ms").padStart(10)}  ${res.writeQps.toLocaleString().padStart(10)}  ${String(res.convergenceMs + "ms").padStart(10)}`,
		);
	}
}

console.log(
	`=== HTTP vs WS Comprehensive — ${N} items, ${ROUNDS} rounds each ===\n`,
);
console.log(
	`${"Variant".padEnd(28)}  ${"Round".padStart(5)}  ${"Sync Delay".padStart(10)}  ${"Write QPS".padStart(10)}  ${"Convergence".padStart(10)}`,
);
console.log(`- ${"-".repeat(72)}`);

// Timer-based: HTTP vs WS at same intervals
for (const ms of [1, 5, 10, 50]) {
	await runTest(`HTTP ${ms}ms timer`, () => testHttp(N, ms));
	await runTest(`WS ${ms}ms timer`, () => testWs(N, ms));
	console.log();
}

// Event-driven: HTTP vs WS (ship every N writes, no timer)
for (const flush of [1, 10, 100]) {
	await runTest(`WS event flush=${flush}`, () => testWsEventDriven(N, flush));
	await runTest(`HTTP event flush=${flush}`, () =>
		testHttpEventDriven(N, flush),
	);
	console.log();
}

// Summary — median
function median(arr: number[]): number {
	const sorted = [...arr].sort((a, b) => a - b);
	return sorted[Math.floor(sorted.length / 2)];
}

console.log("=== Summary (median) ===\n");
console.log(
	`${"Variant".padEnd(28)}  ${"Sync Delay".padStart(10)}  ${"Write QPS".padStart(10)}  ${"Convergence".padStart(10)}`,
);
console.log(`- ${"-".repeat(72)}`);
for (const [name, data] of Object.entries(results)) {
	console.log(
		`${name.padEnd(28)}  ${String(median(data.delay) + "ms").padStart(10)}  ${median(data.qps).toLocaleString().padStart(10)}  ${String(median(data.conv) + "ms").padStart(10)}`,
	);
}
