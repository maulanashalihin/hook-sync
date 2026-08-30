// bench-trigger-overhead.ts
// Measure trigger overhead: with vs without triggers, both HTTP and direct SQLite
// Usage: bun run bench-trigger-overhead.ts

import { Database } from "bun:sqlite";
import { createServer } from "http";

const crypto = globalThis.crypto;

function stats(arr: number[]) {
  const sorted = [...arr].sort((a, b) => a - b);
  const sum = sorted.reduce((a, b) => a + b, 0);
  return {
    min: +sorted[0].toFixed(2),
    mean: +(sum / sorted.length).toFixed(2),
    p50: +sorted[Math.floor(sorted.length * 0.5)].toFixed(2),
    p95: +sorted[Math.floor(sorted.length * 0.95)].toFixed(2),
    max: +sorted[sorted.length - 1].toFixed(2),
  };
}

// --- Direct SQLite benchmark (no HTTP) ---
function directBench(label: string, withTriggers: boolean, writes: number) {
  const db = new Database(":memory:");
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = NORMAL");
  db.exec(`CREATE TABLE items(
    id TEXT PRIMARY KEY, name TEXT, value INTEGER,
    created_at INTEGER, updated_at INTEGER, node_id TEXT
  )`);

  if (withTriggers) {
    db.exec(`CREATE TABLE _meta(key TEXT PRIMARY KEY, value INTEGER); INSERT INTO _meta(key, value) VALUES('syncing', 0)`);
    db.exec(`CREATE TABLE _changes(change_id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, row_id TEXT, row_data TEXT)`);
    db.exec(`CREATE TRIGGER items_ai AFTER INSERT ON items
      WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
      BEGIN
        INSERT INTO _changes(op, row_id, row_data) VALUES('INSERT', NEW.id,
          json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
            'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
      END`);
    db.exec(`CREATE TRIGGER items_au AFTER UPDATE ON items
      WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
      BEGIN
        INSERT INTO _changes(op, row_id, row_data) VALUES('UPDATE', NEW.id,
          json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
            'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
      END`);
    db.exec(`CREATE TRIGGER items_ad AFTER DELETE ON items
      WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
      BEGIN
        INSERT INTO _changes(op, row_id, row_data) VALUES('DELETE', OLD.id, NULL);
      END`);
  }

  const stmt = db.prepare("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)");

  // Sequential
  const t0 = performance.now();
  for (let i = 0; i < writes; i++) {
    stmt.run(crypto.randomUUID(), `item-${i}`, i, Date.now(), Date.now(), "bench");
  }
  const seqElapsed = performance.now() - t0;
  const seqQps = Math.round((writes / seqElapsed) * 1000);

  // Transaction
  db.exec("DELETE FROM items");
  const t1 = performance.now();
  db.transaction(() => {
    for (let i = 0; i < writes; i++) {
      stmt.run(crypto.randomUUID(), `item-${i}`, i, Date.now(), Date.now(), "bench");
    }
  })();
  const txElapsed = performance.now() - t1;
  const txQps = Math.round((writes / txElapsed) * 1000);

  const changesCount = withTriggers ? db.query("SELECT COUNT(*) as c FROM _changes").get().c : 0;
  const rowsCount = db.query("SELECT COUNT(*) as c FROM items").get().c;
  db.close();

  return { label, withTriggers, writes, seqQps, seqElapsed: +seqElapsed.toFixed(2), txQps, txElapsed: +txElapsed.toFixed(2), rows: rowsCount, changes: changesCount };
}

// --- HTTP benchmark ---
async function httpBench(label: string, withTriggers: boolean, concurrent: number) {
  const dbPath = `/tmp/bench-trigger-${Date.now()}.db`;
  const db = new Database(dbPath);
  db.exec("PRAGMA journal_mode = WAL");
  db.exec("PRAGMA synchronous = NORMAL");
  db.exec(`CREATE TABLE items(
    id TEXT PRIMARY KEY, name TEXT, value INTEGER,
    created_at INTEGER, updated_at INTEGER, node_id TEXT
  )`);

  if (withTriggers) {
    db.exec(`CREATE TABLE _meta(key TEXT PRIMARY KEY, value INTEGER); INSERT INTO _meta(key, value) VALUES('syncing', 0)`);
    db.exec(`CREATE TABLE _changes(change_id INTEGER PRIMARY KEY AUTOINCREMENT, op TEXT, row_id TEXT, row_data TEXT)`);
    db.exec(`CREATE TRIGGER items_ai AFTER INSERT ON items
      WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
      BEGIN
        INSERT INTO _changes(op, row_id, row_data) VALUES('INSERT', NEW.id,
          json_object('id', NEW.id, 'name', NEW.name, 'value', NEW.value,
            'created_at', NEW.created_at, 'updated_at', NEW.updated_at, 'node_id', NEW.node_id));
      END`);
  }

  const insertStmt = db.prepare("INSERT INTO items(id, name, value, created_at, updated_at, node_id) VALUES(?, ?, ?, ?, ?, ?)");

  const server = createServer((req, res) => {
    if (req.method === "POST" && req.url === "/api/items") {
      let body = "";
      req.on("data", (chunk) => (body += chunk));
      req.on("end", () => {
        const data = JSON.parse(body);
        const id = crypto.randomUUID();
        const now = Date.now();
        insertStmt.run(id, data.name, data.value, now, now, "bench");
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ id }));
      });
    } else if (req.method === "GET" && req.url === "/health") {
      const { count } = db.query("SELECT COUNT(*) as count FROM items").get();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: true, item_count: count }));
    } else {
      res.writeHead(404);
      res.end();
    }
  });

  return new Promise((resolve) => {
    server.listen(0, async () => {
      const port = (server.address() as any).port;
      const url = `http://localhost:${port}`;

      // Write throughput
      const t0 = performance.now();
      const promises = [];
      for (let i = 0; i < concurrent; i++) {
        promises.push(
          fetch(`${url}/api/items`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ name: `burst-${i}`, value: i }),
          }).then((r) => r.json())
        );
      }
      await Promise.all(promises);
      const elapsed = performance.now() - t0;
      const qps = Math.round((concurrent / elapsed) * 1000);

      // Write latency
      const latencies: number[] = [];
      for (let i = 0; i < 100; i++) {
        const lt0 = performance.now();
        await fetch(`${url}/api/items`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ name: `lat-${i}`, value: i }),
        }).then((r) => r.json());
        latencies.push(performance.now() - lt0);
      }

      const changesCount = withTriggers ? db.query("SELECT COUNT(*) as c FROM _changes").get().c : 0;

      server.close();
      db.close();
      try { Bun.file(dbPath).unlink?.(); } catch {}

      resolve({
        label,
        withTriggers,
        concurrent,
        httpQps: qps,
        httpElapsed: +elapsed.toFixed(2),
        httpWriteLatency: stats(latencies),
        changesFired: changesCount,
      });
    });
  });
}

// --- Main ---
async function main() {
  console.log("=".repeat(70));
  console.log("  TRIGGER OVERHEAD BENCHMARK — Bun (bun:sqlite + http.createServer)");
  console.log("=".repeat(70));

  // Direct SQLite — no triggers
  console.log("\n--- Direct SQLite (no HTTP) ---");
  const directNoTrig = directBench("No triggers", false, 10000);
  const directWithTrig = directBench("With triggers", true, 10000);

  console.log("\n| Mode | Triggers | Sequential QPS | Transaction QPS | Changes |");
  console.log("|------|----------|---------------:|----------------:|--------:|");
  console.log(`| ${directNoTrig.label} | ❌ | ${directNoTrig.seqQps.toLocaleString()} | ${directNoTrig.txQps.toLocaleString()} | ${directNoTrig.changes} |`);
  console.log(`| ${directWithTrig.label} | ✅ | ${directWithTrig.seqQps.toLocaleString()} | ${directWithTrig.txQps.toLocaleString()} | ${directWithTrig.changes} |`);

  const seqDrop = ((1 - directWithTrig.seqQps / directNoTrig.seqQps) * 100).toFixed(1);
  const txDrop = ((1 - directWithTrig.txQps / directNoTrig.txQps) * 100).toFixed(1);
  console.log(`\nTrigger overhead: Sequential -${seqDrop}%, Transaction -${txDrop}%`);

  // HTTP — no triggers vs with triggers
  console.log("\n--- HTTP (100 concurrent) ---");
  const httpNoTrig: any = await httpBench("No triggers", false, 100);
  const httpWithTrig: any = await httpBench("With triggers", true, 100);

  console.log("\n| Mode | Triggers | HTTP QPS | Write latency p50 | Changes |");
  console.log("|------|----------|---------:|------------------:|--------:|");
  console.log(`| ${httpNoTrig.label} | ❌ | ${httpNoTrig.httpQps.toLocaleString()} | ${httpNoTrig.httpWriteLatency.p50}ms | ${httpNoTrig.changesFired} |`);
  console.log(`| ${httpWithTrig.label} | ✅ | ${httpWithTrig.httpQps.toLocaleString()} | ${httpWithTrig.httpWriteLatency.p50}ms | ${httpWithTrig.changesFired} |`);

  const httpDrop = ((1 - httpWithTrig.httpQps / httpNoTrig.httpQps) * 100).toFixed(1);
  console.log(`\nTrigger overhead (HTTP): -${httpDrop}%`);

  console.log("\n" + "=".repeat(70));
  console.log("  SUMMARY");
  console.log("=".repeat(70));
  console.log(`Direct SQLite Sequential: ${directNoTrig.seqQps.toLocaleString()} → ${directWithTrig.seqQps.toLocaleString()} QPS (-${seqDrop}%)`);
  console.log(`Direct SQLite Transaction: ${directNoTrig.txQps.toLocaleString()} → ${directWithTrig.txQps.toLocaleString()} QPS (-${txDrop}%)`);
  console.log(`HTTP throughput: ${httpNoTrig.httpQps.toLocaleString()} → ${httpWithTrig.httpQps.toLocaleString()} QPS (-${httpDrop}%)`);
  console.log(`\nTrigger = 1 extra INSERT + json_object() per change.`);
  console.log(`Overhead is real but SQLite is so fast that HTTP server dominates.`);
}

main();
