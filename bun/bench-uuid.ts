// bench-uuid.ts — UUIDv4 vs UUIDv7 as SQLite TEXT PRIMARY KEY in bun:sqlite
// Usage: bun bench-uuid.ts

import { Database } from "bun:sqlite";

// --- UUIDv7 (time-ordered, RFC 9562) — optimized with hex lookup table ---
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
	return (
		HEX[buf[0]] + HEX[buf[1]] + HEX[buf[2]] + HEX[buf[3]] + "-" +
		HEX[buf[4]] + HEX[buf[5]] + "-" +
		HEX[buf[6]] + HEX[buf[7]] + "-" +
		HEX[buf[8]] + HEX[buf[9]] + "-" +
		HEX[buf[10]] + HEX[buf[11]] + HEX[buf[12]] + HEX[buf[13]] + HEX[buf[14]] + HEX[buf[15]]
	);
}

// --- UUIDv4 ---
function uuidv4(): string {
	return crypto.randomUUID();
}

function benchSequential(db: Database, n: number, useV7: boolean): number {
	db.exec("DELETE FROM items");
	const gen = useV7 ? uuidv7 : uuidv4;
	const stmt = db.prepare(
		"INSERT INTO items(id, name, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?)"
	);
	const start = performance.now();
	for (let i = 0; i < n; i++) {
		const id = gen();
		const now = Date.now();
		stmt.run(id, `item-${i}`, i, now, now);
	}
	const elapsed = performance.now() - start;
	return Math.round((n / elapsed) * 1000);
}

function benchTransaction(db: Database, n: number, useV7: boolean): number {
	db.exec("DELETE FROM items");
	const gen = useV7 ? uuidv7 : uuidv4;
	const stmt = db.prepare(
		"INSERT INTO items(id, name, value, created_at, updated_at) VALUES(?, ?, ?, ?, ?)"
	);
	const start = performance.now();
	db.exec("BEGIN");
	for (let i = 0; i < n; i++) {
		const id = gen();
		const now = Date.now();
		stmt.run(id, `item-${i}`, i, now, now);
	}
	db.exec("COMMIT");
	const elapsed = performance.now() - start;
	return Math.round((n / elapsed) * 1000);
}

function benchGenOnly(n: number, useV7: boolean): number {
	const gen = useV7 ? uuidv7 : uuidv4;
	const start = performance.now();
	for (let i = 0; i < n; i++) {
		gen();
	}
	const elapsed = performance.now() - start;
	return Math.round((n / elapsed) * 1000);
}

// --- Main ---
const db = new Database(":memory:");
db.exec("PRAGMA journal_mode = WAL");
db.exec("PRAGMA synchronous = NORMAL");
db.exec(`
	CREATE TABLE items (
		id TEXT PRIMARY KEY, name TEXT, value INTEGER,
		created_at INTEGER, updated_at INTEGER
	)
`);

const sizes = [100, 1000, 10000, 100000];

console.log("=== UUIDv4 vs UUIDv7 — bun:sqlite TEXT PRIMARY KEY ===\n");

// Sequential
console.log("--- Sequential (per-write commit) ---");
console.log(`${"N".padEnd(8)}  ${"v4 QPS".padStart(12)}  ${"v7 QPS".padStart(12)}  ${"v4/v7".padStart(8)}  winner`);
console.log(`- ${"-".repeat(58)}`);
for (const n of sizes) {
	const v4 = benchSequential(db, n, false);
	const v7 = benchSequential(db, n, true);
	const ratio = (v4 / v7).toFixed(2);
	const winner = v4 > v7 ? "v4" : "v7";
	console.log(`${String(n).padEnd(8)}  ${String(v4).padStart(12)}  ${String(v7).padStart(12)}  ${ratio.padStart(7)}x  ${winner}`);
}

console.log();

// Transaction
console.log("--- Transaction (single commit) ---");
console.log(`${"N".padEnd(8)}  ${"v4 QPS".padStart(12)}  ${"v7 QPS".padStart(12)}  ${"v4/v7".padStart(8)}  winner`);
console.log(`- ${"-".repeat(58)}`);
for (const n of sizes) {
	const v4 = benchTransaction(db, n, false);
	const v7 = benchTransaction(db, n, true);
	const ratio = (v4 / v7).toFixed(2);
	const winner = v4 > v7 ? "v4" : "v7";
	console.log(`${String(n).padEnd(8)}  ${String(v4).padStart(12)}  ${String(v7).padStart(12)}  ${ratio.padStart(7)}x  ${winner}`);
}

console.log();

// Generation only
console.log("--- UUID generation only (no DB) ---");
console.log(`${"N".padEnd(8)}  ${"v4 QPS".padStart(12)}  ${"v7 QPS".padStart(12)}  ${"v4/v7".padStart(8)}  winner`);
console.log(`- ${"-".repeat(58)}`);
for (const n of sizes) {
	const v4 = benchGenOnly(n, false);
	const v7 = benchGenOnly(n, true);
	const ratio = (v4 / v7).toFixed(2);
	const winner = v4 > v7 ? "v4" : "v7";
	console.log(`${String(n).padEnd(8)}  ${String(v4).padStart(12)}  ${String(v7).padStart(12)}  ${ratio.padStart(7)}x  ${winner}`);
}

db.close();
