// node/bench-baseline-vs-trigger.js
//
// Direct SQLite benchmark (no HTTP): baseline vs triggers
// 100K INSERTs per run, 10 runs, report median QPS
//
// Run: node node/bench-baseline-vs-trigger.js

const Database = require("better-sqlite3");
const crypto = require("crypto");
const fs = require("fs");

const TOTAL = 100_000;
const RUNS = 10;
const DB_PATH = "/tmp/bench-node-trigger.db";

function bench(mode) {
	const qpsList = [];

	for (let run = 0; run < RUNS; run++) {
		for (const ext of ["", "-wal", "-shm"]) {
			try {
				fs.unlinkSync(DB_PATH + ext);
			} catch {}
		}

		const db = new Database(DB_PATH);
		db.pragma("journal_mode = WAL");
		db.pragma("synchronous = NORMAL");
		db.pragma("busy_timeout = 5000");

		db.exec(`CREATE TABLE items (
      id TEXT PRIMARY KEY, name TEXT, value INTEGER,
      created_at INTEGER, updated_at INTEGER, node_id TEXT
    )`);

		if (mode === "triggers") {
			db.exec(`CREATE TABLE _changes (
        change_id INTEGER PRIMARY KEY AUTOINCREMENT,
        op TEXT, row_id TEXT, row_data TEXT
      )`);

			db.exec(`CREATE TRIGGER items_ai AFTER INSERT ON items
      BEGIN
        INSERT INTO _changes(op, row_id, row_data) VALUES('INSERT', NEW.id,
          json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
            'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
      END`);
		}

		const stmt = db.prepare(
			"INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)",
		);
		const stmtCount = db.prepare("SELECT COUNT(*) as c FROM items");
		const stmtChanges =
			mode === "triggers"
				? db.prepare("SELECT COUNT(*) as c FROM _changes")
				: null;

		const t0 = performance.now();

		const insertMany = db.transaction(() => {
			const now = Date.now();
			for (let i = 0; i < TOTAL; i++) {
				stmt.run(crypto.randomUUID(), `item-${i}`, i, now, now, "bench");
			}
		});
		insertMany();

		const elapsed = (performance.now() - t0) / 1000;
		const qps = Math.round(TOTAL / elapsed);
		qpsList.push(qps);

		const count = stmtCount.get();
		if (count.c !== TOTAL)
			throw new Error(`VERIFY FAIL: items=${count.c} expected=${TOTAL}`);

		if (mode === "triggers") {
			const cc = stmtChanges.get();
			if (cc.c !== TOTAL)
				throw new Error(`VERIFY FAIL: _changes=${cc.c} expected=${TOTAL}`);
		}

		db.close();
	}

	return qpsList;
}

function median(arr) {
	const s = [...arr].sort((a, b) => a - b);
	return s[Math.floor(s.length / 2)];
}
function min(arr) {
	return Math.min(...arr);
}
function max(arr) {
	return Math.max(...arr);
}

console.log("============================================");
console.log("Benchmark: baseline vs triggers (Node + better-sqlite3)");
console.log(
	`  ${TOTAL} INSERTs per run, ${RUNS} runs, direct SQLite (no HTTP)`,
);
console.log("  WAL mode, synchronous=NORMAL, single transaction");
console.log("============================================\n");

const results = {};

for (const mode of ["baseline", "triggers"]) {
	console.log(`>>> ${mode}`);
	const qps = bench(mode);
	results[mode] = qps;
	console.log(`  QPS: min=${min(qps)} med=${median(qps)} max=${max(qps)}`);
	console.log(`  All: ${qps.join(", ")}\n`);
}

console.log("============================================");
console.log("Summary");
console.log("============================================\n");

const baseMed = median(results["baseline"]);
const trigMed = median(results["triggers"]);
const overhead = (((baseMed - trigMed) / baseMed) * 100).toFixed(1);

console.log("| Mode     | QPS median | vs baseline |");
console.log("|----------|------------|-------------|");
console.log(`| baseline | ${String(baseMed).padStart(10)} |       —     |`);
console.log(
	`| triggers | ${String(trigMed).padStart(10)} | -${overhead}%      |`,
);
console.log();
console.log(`Trigger overhead: -${overhead}% (${baseMed} → ${trigMed} QPS)`);
