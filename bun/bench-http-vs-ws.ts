// bench-http-vs-ws.ts — HTTP vs WebSocket transport for hook-sync
// Tests 3 variants: HTTP+timer, WS+timer, WS+immediate
// Measures: sync delay, throughput, convergence time
// Usage: bun bench-http-vs-ws.ts

import { Database } from "bun:sqlite";

// ─── Shared: UUID, schema, triggers, LWW apply ───────────────────

const HEX = Array.from({ length: 256 }, (_, i) => i.toString(16).padStart(2, "0"));
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
	return HEX[buf[0]]+HEX[buf[1]]+HEX[buf[2]]+HEX[buf[3]]+"-"+HEX[buf[4]]+HEX[buf[5]]+"-"+HEX[buf[6]]+HEX[buf[7]]+"-"+HEX[buf[8]]+HEX[buf[9]]+"-"+HEX[buf[10]]+HEX[buf[11]]+HEX[buf[12]]+HEX[buf[13]]+HEX[buf[14]]+HEX[buf[15]];
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
					const existing = db.query("SELECT updated_at FROM items WHERE id = ?").get(c.old_id) as { updated_at: number } | null;
					if (existing && Number(existing.updated_at) > delTs) continue;
				}
				db.prepare("DELETE FROM items WHERE id = ?").run(c.old_id);
			} else {
				if (!c.row) continue;
				const id = c.row.id as string;
				if (!id) continue;
				const updatedAt = Number(c.row.updated_at) || 0;
				const existing = db.query("SELECT updated_at FROM items WHERE id = ?").get(id) as { updated_at: number } | null;
				if (existing && Number(existing.updated_at) > updatedAt) continue;
				const cols = Object.keys(c.row);
				const vals = cols.map((k) => c.row![k]);
				const placeholders = cols.map(() => "?").join(", ");
				db.prepare(`INSERT OR REPLACE INTO items (${cols.join(", ")}) VALUES (${placeholders})`).run(...vals);
			}
		}
	});
	tx();
	db.exec("UPDATE _meta SET value = '0' WHERE key = 'syncing'");
	return changes.length;
}

function drainChanges(db: Database): { batchId: number; changes: Change[] } {
	const rows = db.query("SELECT id, op, table_name, row_data FROM _changes ORDER BY id LIMIT 10000").all() as {
		id: number; op: string; table_name: string; row_data: string;
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
	return (db.query("SELECT COUNT(*) as c FROM _changes").get() as { c: number }).c;
}

// ─── Variant 1: HTTP + timer ──────────────────────────────────────

async function testHttpTimer(N: number, batchMs: number): Promise<{
	convergenceMs: number;
	writeQps: number;
	syncDelayMs: number;
}> {
	// Clean DBs
	for (const f of ["/Volumes/data/tmp/bench-http-a.db", "/Volumes/data/tmp/bench-http-b.db"]) {
		try { require("fs").unlinkSync(f); } catch {}
		try { require("fs").unlinkSync(f + "-wal"); } catch {}
		try { require("fs").unlinkSync(f + "-shm"); } catch {}
	}

	const dbA = setupDB("/Volumes/data/tmp/bench-http-a.db");
	const dbB = setupDB("/Volumes/data/tmp/bench-http-b.db");

	// Node B: HTTP server receiving sync
	const serverB = Bun.serve({
		port: 9101,
		async fetch(req) {
			if (req.method === "POST" && new URL(req.url).pathname === "/sync") {
				const body = await req.json();
				applyChanges(dbB, body.changes);
				return Response.json({ applied: body.changes.length, ack: body.batch_id });
			}
			if (req.method === "GET" && new URL(req.url).pathname === "/count") {
				return Response.json({ count: countItems(dbB) });
			}
			return new Response("404", { status: 404 });
		},
	});

	// Node A: ship loop on timer
	let shipping = false;
	const shipLoop = setInterval(async () => {
		if (shipping) return;
		shipping = true;
		try {
			const { batchId, changes } = drainChanges(dbA);
			if (changes.length === 0) return;
			const resp = await fetch("http://localhost:9101/sync", {
				method: "POST",
				headers: { "Content-Type": "application/json" },
				body: JSON.stringify({ batch_id: batchId, changes }),
			});
			const ack = await resp.json();
			if (ack.ack === batchId) {
				deleteChanges(dbA, batchId);
			}
		} finally {
			shipping = false;
		}
	}, batchMs);

	// Write N items to A, measure sync delay for first item
	const writeStart = performance.now();
	let firstSyncDelay = 0;
	const writeStmt = dbA.prepare("INSERT INTO items(id, name, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?)");

	for (let i = 0; i < N; i++) {
		const id = uuidv7();
		const now = Date.now();
		writeStmt.run(id, `item-${i}`, i, now, now);
		// Check sync delay for first item
		if (i === 0) {
			const checkStart = performance.now();
			while (true) {
				const resp = await fetch("http://localhost:9101/count");
				const data = await resp.json();
				if (data.count > 0) {
					firstSyncDelay = performance.now() - checkStart;
					break;
				}
				await Bun.sleep(1);
			}
		}
	}
	const writeEnd = performance.now();
	const writeQps = Math.round(N / ((writeEnd - writeStart) / 1000));

	// Wait for convergence
	const convStart = performance.now();
	while (true) {
		const resp = await fetch("http://localhost:9101/count");
		const data = await resp.json();
		if (data.count >= N && countPending(dbA) === 0) break;
		await Bun.sleep(5);
	}
	const convergenceMs = Math.round(performance.now() - convStart);

	clearInterval(shipLoop);
	serverB.stop();
	dbA.close();
	dbB.close();

	return { convergenceMs, writeQps, syncDelayMs: Math.round(firstSyncDelay) };
}

// ─── Variant 2: WS + timer ────────────────────────────────────────

async function testWsTimer(N: number, batchMs: number): Promise<{
	convergenceMs: number;
	writeQps: number;
	syncDelayMs: number;
}> {
	for (const f of ["/Volumes/data/tmp/bench-wst-a.db", "/Volumes/data/tmp/bench-wst-b.db"]) {
		try { require("fs").unlinkSync(f); } catch {}
		try { require("fs").unlinkSync(f + "-wal"); } catch {}
		try { require("fs").unlinkSync(f + "-shm"); } catch {}
	}

	const dbA = setupDB("/Volumes/data/tmp/bench-wst-a.db");
	const dbB = setupDB("/Volumes/data/tmp/bench-wst-b.db");

	let wsClient: WebSocket | null = null;
	const ackResolvers = new Map<number, () => void>();

	// Node B: WS server
	const serverB = Bun.serve({
		port: 9102,
		websocket: {
			async message(ws, message) {
				const data = JSON.parse(message.toString());
				if (data.type === "sync") {
					applyChanges(dbB, data.changes);
					ws.send(JSON.stringify({ type: "ack", batch_id: data.batch_id, applied: data.changes.length }));
				}
			},
		},
		fetch(req, server) {
			if (new URL(req.url).pathname === "/ws") {
				server.upgrade(req);
				return;
			}
			if (new URL(req.url).pathname === "/count") {
				return Response.json({ count: countItems(dbB) });
			}
			return new Response("404", { status: 404 });
		},
	});

	// Node A: connect WS
	wsClient = new WebSocket("ws://localhost:9102/ws");
	await new Promise((resolve) => { wsClient!.onopen = resolve; });
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

	// Ship loop on timer
	let shipping = false;
	const shipLoop = setInterval(async () => {
		if (shipping || !wsClient || wsClient.readyState !== WebSocket.OPEN) return;
		shipping = true;
		try {
			const { batchId, changes } = drainChanges(dbA);
			if (changes.length === 0) return;
			const ackPromise = new Promise<void>((resolve) => {
				ackResolvers.set(batchId, resolve);
			});
			wsClient.send(JSON.stringify({ type: "sync", batch_id: batchId, changes }));
			await ackPromise;
			deleteChanges(dbA, batchId);
		} finally {
			shipping = false;
		}
	}, batchMs);

	// Write N items
	const writeStart = performance.now();
	let firstSyncDelay = 0;
	const writeStmt = dbA.prepare("INSERT INTO items(id, name, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?)");

	for (let i = 0; i < N; i++) {
		const id = uuidv7();
		const now = Date.now();
		writeStmt.run(id, `item-${i}`, i, now, now);
		if (i === 0) {
			const checkStart = performance.now();
			while (true) {
				const resp = await fetch("http://localhost:9102/count");
				const data = await resp.json();
				if (data.count > 0) {
					firstSyncDelay = performance.now() - checkStart;
					break;
				}
				await Bun.sleep(1);
			}
		}
	}
	const writeEnd = performance.now();
	const writeQps = Math.round(N / ((writeEnd - writeStart) / 1000));

	// Wait for convergence
	const convStart = performance.now();
	while (true) {
		const resp = await fetch("http://localhost:9102/count");
		const data = await resp.json();
		if (data.count >= N && countPending(dbA) === 0) break;
		await Bun.sleep(5);
	}
	const convergenceMs = Math.round(performance.now() - convStart);

	clearInterval(shipLoop);
	wsClient.close();
	serverB.stop();
	dbA.close();
	dbB.close();

	return { convergenceMs, writeQps, syncDelayMs: Math.round(firstSyncDelay) };
}

// ─── Variant 3: WS + immediate push ───────────────────────────────

async function testWsImmediate(N: number): Promise<{
	convergenceMs: number;
	writeQps: number;
	syncDelayMs: number;
}> {
	for (const f of ["/Volumes/data/tmp/bench-wsi-a.db", "/Volumes/data/tmp/bench-wsi-b.db"]) {
		try { require("fs").unlinkSync(f); } catch {}
		try { require("fs").unlinkSync(f + "-wal"); } catch {}
		try { require("fs").unlinkSync(f + "-shm"); } catch {}
	}

	const dbA = setupDB("/Volumes/data/tmp/bench-wsi-a.db");
	const dbB = setupDB("/Volumes/data/tmp/bench-wsi-b.db");

	const ackResolvers = new Map<number, () => void>();
	let batchCounter = 0;

	// Node B: WS server
	const serverB = Bun.serve({
		port: 9103,
		websocket: {
			async message(ws, message) {
				const data = JSON.parse(message.toString());
				if (data.type === "sync") {
					applyChanges(dbB, data.changes);
					ws.send(JSON.stringify({ type: "ack", batch_id: data.batch_id, applied: data.changes.length }));
				}
			},
		},
		fetch(req, server) {
			if (new URL(req.url).pathname === "/ws") {
				server.upgrade(req);
				return;
			}
			if (new URL(req.url).pathname === "/count") {
				return Response.json({ count: countItems(dbB) });
			}
			return new Response("404", { status: 404 });
		},
	});

	// Node A: connect WS
	const wsClient = new WebSocket("ws://localhost:9103/ws");
	await new Promise((resolve) => { wsClient!.onopen = resolve; });
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

	// Immediate push: micro-batch timer (1ms — effectively immediate, but with backpressure)
	let shipping = false;
	const shipLoop = setInterval(async () => {
		if (shipping || !wsClient || wsClient.readyState !== WebSocket.OPEN) return;
		shipping = true;
		try {
			const { batchId, changes } = drainChanges(dbA);
			if (changes.length === 0) return;
			const myBatchId = ++batchCounter;
			const { promise: ackPromise, resolve: ackResolve } = Promise.withResolvers<void>();
			ackResolvers.set(myBatchId, ackResolve);
			wsClient.send(JSON.stringify({ type: "sync", batch_id: myBatchId, changes }));
			await ackPromise;
			deleteChanges(dbA, batchId);
		} finally {
			shipping = false;
		}
	}, 1); // 1ms = effectively immediate

	// Write N items
	const writeStart = performance.now();
	let firstSyncDelay = 0;
	const writeStmt = dbA.prepare("INSERT INTO items(id, name, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?)");

	for (let i = 0; i < N; i++) {
		const id = uuidv7();
		const now = Date.now();
		writeStmt.run(id, `item-${i}`, i, now, now);

		if (i === 0) {
			const checkStart = performance.now();
			while (true) {
				const resp = await fetch("http://localhost:9103/count");
				const data = await resp.json();
				if (data.count > 0) {
					firstSyncDelay = performance.now() - checkStart;
					break;
				}
				await Bun.sleep(1);
			}
		}
	}
	const writeEnd = performance.now();
	const writeQps = Math.round(N / ((writeEnd - writeStart) / 1000));

	// Wait for convergence
	const convStart = performance.now();
	while (true) {
		const resp = await fetch("http://localhost:9103/count");
		const data = await resp.json();
		if (data.count >= N && countPending(dbA) === 0) break;
		await Bun.sleep(5);
	}
	const convergenceMs = Math.round(performance.now() - convStart);
	clearInterval(shipLoop);

	wsClient.close();
	serverB.stop();
	dbA.close();
	dbB.close();

	return { convergenceMs, writeQps, syncDelayMs: Math.round(firstSyncDelay) };
}

// ─── Main ─────────────────────────────────────────────────────────

const N = 10000;
const BATCH_MS = 50;
const ROUNDS = 3;

console.log(`=== HTTP vs WebSocket — ${N} items, ${BATCH_MS}ms batch, ${ROUNDS} rounds ===\n`);
console.log(`${"Variant".padEnd(20)}  ${"Round".padStart(5)}  ${"Sync Delay".padStart(12)}  ${"Write QPS".padStart(12)}  ${"Convergence".padStart(12)}`);
console.log(`- ${"-".repeat(70)}`);

const results: Record<string, { delay: number[]; qps: number[]; conv: number[] }> = {
	"HTTP + 50ms timer": { delay: [], qps: [], conv: [] },
	"WS + 50ms timer": { delay: [], qps: [], conv: [] },
	"WS + immediate": { delay: [], qps: [], conv: [] },
};

for (let r = 0; r < ROUNDS; r++) {
	// HTTP
	const http = await testHttpTimer(N, BATCH_MS);
	results["HTTP + 50ms timer"].delay.push(http.syncDelayMs);
	results["HTTP + 50ms timer"].qps.push(http.writeQps);
	results["HTTP + 50ms timer"].conv.push(http.convergenceMs);
	console.log(`${"HTTP + 50ms timer".padEnd(20)}  ${String(r+1).padStart(5)}  ${String(http.syncDelayMs + "ms").padStart(12)}  ${http.writeQps.toLocaleString().padStart(12)}  ${String(http.convergenceMs + "ms").padStart(12)}`);

	// WS timer
	const wst = await testWsTimer(N, BATCH_MS);
	results["WS + 50ms timer"].delay.push(wst.syncDelayMs);
	results["WS + 50ms timer"].qps.push(wst.writeQps);
	results["WS + 50ms timer"].conv.push(wst.convergenceMs);
	console.log(`${"WS + 50ms timer".padEnd(20)}  ${String(r+1).padStart(5)}  ${String(wst.syncDelayMs + "ms").padStart(12)}  ${wst.writeQps.toLocaleString().padStart(12)}  ${String(wst.convergenceMs + "ms").padStart(12)}`);

	// WS immediate
	const wsi = await testWsImmediate(N);
	results["WS + immediate"].delay.push(wsi.syncDelayMs);
	results["WS + immediate"].qps.push(wsi.writeQps);
	results["WS + immediate"].conv.push(wsi.convergenceMs);
	console.log(`${"WS + immediate".padEnd(20)}  ${String(r+1).padStart(5)}  ${String(wsi.syncDelayMs + "ms").padStart(12)}  ${wsi.writeQps.toLocaleString().padStart(12)}  ${String(wsi.convergenceMs + "ms").padStart(12)}`);

	console.log();
}

// Summary — median
function median(arr: number[]): number {
	const sorted = [...arr].sort((a, b) => a - b);
	return sorted[Math.floor(sorted.length / 2)];
}

console.log("=== Summary (median) ===\n");
console.log(`${"Variant".padEnd(20)}  ${"Sync Delay".padStart(12)}  ${"Write QPS".padStart(12)}  ${"Convergence".padStart(12)}`);
console.log(`- ${"-".repeat(70)}`);
for (const [name, data] of Object.entries(results)) {
	console.log(`${name.padEnd(20)}  ${String(median(data.delay) + "ms").padStart(12)}  ${median(data.qps).toLocaleString().padStart(12)}  ${String(median(data.conv) + "ms").padStart(12)}`);
}
