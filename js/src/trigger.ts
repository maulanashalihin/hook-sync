// hook-sync — trigger-based change capture + sync manager

import { applyChange, validTable } from "./apply.ts";
import { shipWithAck } from "./ship.ts";
import type {
	Change,
	Config,
	HealthStatus,
	Manager,
	SqliteDatabase,
	SqliteStatement,
} from "./types.ts";

const BACKOFF_MS = [50, 100, 200, 400, 800];

class TriggerManager implements Manager {
	private db: SqliteDatabase;
	private id: string;
	private peers: string[];
	private batchMs: number;
	private batchSize: number;
	private tables: string[];
	private timer: ReturnType<typeof setInterval> | null = null;
	private shipping = false;

	// Precompiled statements
	private stmtSyncOn: SqliteStatement;
	private stmtSyncOff: SqliteStatement;
	private stmtPeerChanges: SqliteStatement;
	private stmtUpdatePeerAck: SqliteStatement;
	private stmtPeerStates: SqliteStatement;
	private stmtMinAck: SqliteStatement;
	private stmtDeleteChanges: SqliteStatement;
	private stmtDeadLetter: SqliteStatement;
	private stmtPendingChanges: SqliteStatement;
	private stmtDeadLetterCount: SqliteStatement;

	constructor(db: SqliteDatabase, config: Config, tables: string[]) {
		this.db = db;
		this.id = config.id;
		this.peers = config.peers;
		this.batchMs = config.batchMs ?? 50;
		this.batchSize = config.batchSize ?? 10000;
		this.tables = tables;

		this.setupSyncTables();
		for (const table of tables) {
			this.generateTriggers(table);
		}
		this.initPeerState();

		// Precompile
		this.stmtSyncOn = db.prepare("UPDATE _meta SET value = 1 WHERE key = 'syncing'");
		this.stmtSyncOff = db.prepare("UPDATE _meta SET value = 0 WHERE key = 'syncing'");
		this.stmtPeerChanges = db.prepare(
			"SELECT change_id, op, table_name, row_id, row_data FROM _changes WHERE change_id > ? ORDER BY change_id LIMIT ?",
		);
		this.stmtUpdatePeerAck = db.prepare(
			"UPDATE _peer_state SET last_acked = ? WHERE peer_url = ?",
		);
		this.stmtPeerStates = db.prepare("SELECT peer_url, last_acked FROM _peer_state");
		this.stmtMinAck = db.prepare("SELECT MIN(last_acked) as min_ack FROM _peer_state");
		this.stmtDeleteChanges = db.prepare("DELETE FROM _changes WHERE change_id <= ?");
		this.stmtDeadLetter = db.prepare(
			"INSERT INTO _dead_letter(op, row_id, row_data, failed_at, retry_count) VALUES(?, ?, ?, ?, ?)",
		);
		this.stmtPendingChanges = db.prepare("SELECT COUNT(*) as count FROM _changes");
		this.stmtDeadLetterCount = db.prepare("SELECT COUNT(*) as count FROM _dead_letter");

		// Start ship loop
		this.timer = setInterval(() => this.shipLoop(), this.batchMs);
	}

	private setupSyncTables(): void {
		this.db.exec(`
			CREATE TABLE IF NOT EXISTS _meta (
				key TEXT PRIMARY KEY,
				value INTEGER
			);
			INSERT OR IGNORE INTO _meta(key, value) VALUES('syncing', 0);

			CREATE TABLE IF NOT EXISTS _changes (
				change_id INTEGER PRIMARY KEY AUTOINCREMENT,
				op TEXT,
				table_name TEXT,
				row_id TEXT,
				row_data TEXT
			);

			CREATE TABLE IF NOT EXISTS _dead_letter (
				dead_id INTEGER PRIMARY KEY AUTOINCREMENT,
				op TEXT,
				row_id TEXT,
				row_data TEXT,
				failed_at INTEGER,
				retry_count INTEGER DEFAULT 0
			);

			CREATE TABLE IF NOT EXISTS _peer_state (
				peer_url TEXT PRIMARY KEY,
				last_acked INTEGER DEFAULT 0
			);
		`);
	}

	private generateTriggers(table: string): void {
		if (!validTable(table)) {
			throw new Error(`invalid table name: ${table}`);
		}

		const rows = this.db.prepare(`PRAGMA table_info(${table})`).all() as {
			name: string;
		}[];
		if (rows.length === 0) {
			throw new Error(`table ${table} not found or has no columns`);
		}

		const cols = rows.map((r) => r.name);
		const newArgs = cols.map((c) => `'${c}', NEW.${c}`).join(", ");
		const oldArgs = cols.map((c) => `'${c}', OLD.${c}`).join(", ");

		this.db.exec(`
			CREATE TRIGGER IF NOT EXISTS ${table}_ai AFTER INSERT ON ${table}
			WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
			BEGIN
				INSERT INTO _changes(op, table_name, row_id, row_data) VALUES('INSERT', '${table}', NEW.id,
					json_object(${newArgs}));
			END;

			CREATE TRIGGER IF NOT EXISTS ${table}_au AFTER UPDATE ON ${table}
			WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
			BEGIN
				INSERT INTO _changes(op, table_name, row_id, row_data) VALUES('UPDATE', '${table}', NEW.id,
					json_object(${newArgs}));
			END;

			CREATE TRIGGER IF NOT EXISTS ${table}_ad AFTER DELETE ON ${table}
			WHEN (SELECT value FROM _meta WHERE key = 'syncing') = 0
			BEGIN
				INSERT INTO _changes(op, table_name, row_id, row_data) VALUES('DELETE', '${table}', OLD.id,
					json_object(${oldArgs}));
			END;
		`);
	}

	private initPeerState(): void {
		const stmt = this.db.prepare(
			"INSERT OR IGNORE INTO _peer_state(peer_url, last_acked) VALUES(?, 0)",
		);
		for (const peer of this.peers) {
			stmt.run(peer);
		}
	}

	private shipLoop(): void {
		if (this.shipping) return;
		if (this.peers.length === 0) return;

		this.shipping = true;
		(async () => {
			try {
				const peers = this.stmtPeerStates.all() as {
					peer_url: string;
					last_acked: number;
				}[];
				await Promise.all(
					peers.map((p) => this.shipToPeer(p.peer_url, p.last_acked)),
				);
				this.cleanupChanges();
			} finally {
				this.shipping = false;
			}
		})();
	}

	private async shipToPeer(peerUrl: string, lastAcked: number): Promise<void> {
		const rows = this.stmtPeerChanges.all(lastAcked, this.batchSize) as {
			change_id: number;
			op: string;
			table_name: string;
			row_id: string;
			row_data: string | null;
		}[];
		if (rows.length === 0) return;

		const batchId = rows[rows.length - 1].change_id;
		const changes: Change[] = [];
		for (const r of rows) {
			let row: Record<string, unknown> | null = null;
			if (r.row_data) {
				try {
					row = JSON.parse(r.row_data) as Record<string, unknown>;
				} catch {
					row = null;
				}
			}
			changes.push({
				op: r.op as Change["op"],
				table: r.table_name,
				row,
				old_id: r.op === "DELETE" ? r.row_id : null,
			});
		}

		let connError = false;
		for (let attempt = 0; attempt < BACKOFF_MS.length; attempt++) {
			const resp = await shipWithAck(this.id, batchId, changes, peerUrl);
			if (resp === null) {
				connError = true;
			} else if (resp.ack === batchId) {
				this.stmtUpdatePeerAck.run(batchId, peerUrl);
				return;
			} else {
				// ACK mismatch — protocol error, dead-letter and advance watermark
				console.error(
					`[${this.id}] peer ${peerUrl} ACK mismatch: got ${resp.ack} want ${batchId}, dead-lettering`,
				);
				const now = Date.now();
				for (const r of rows) {
					this.stmtDeadLetter.run(r.op, r.row_id, r.row_data, now, BACKOFF_MS.length);
				}
				this.stmtUpdatePeerAck.run(batchId, peerUrl);
				return;
			}

			if (attempt < BACKOFF_MS.length - 1) {
				await new Promise((resolve) => setTimeout(resolve, BACKOFF_MS[attempt]));
			}
		}

		// Peer unreachable after all retries — keep changes for next cycle
		console.error(
			`[${this.id}] peer ${peerUrl} unreachable after ${BACKOFF_MS.length} retries, will retry next cycle`,
		);
	}

	private cleanupChanges(): void {
		const { min_ack } = this.stmtMinAck.get() as { min_ack: number | null };
		if (min_ack && min_ack > 0) {
			this.stmtDeleteChanges.run(min_ack);
		}
	}

	applyChanges(changes: Change[]): number {
		const tx = this.db.transaction((changes: Change[]): number => {
			this.stmtSyncOn.run();
			let applied = 0;
			try {
				for (const c of changes) {
					if (applyChange(this.db, c)) applied++;
				}
			} finally {
				this.stmtSyncOff.run();
			}
			return applied;
		});
		return tx(changes);
	}

	health(): HealthStatus {
		let totalItems = 0;
		for (const table of this.tables) {
			const { count } = this.db.prepare(`SELECT COUNT(*) as count FROM ${table}`).get() as {
				count: number;
			};
			totalItems += count;
		}
		const { count: pendingChanges } = this.stmtPendingChanges.get() as { count: number };
		const { count: deadLetter } = this.stmtDeadLetterCount.get() as { count: number };
		return {
			ok: true,
			node_id: this.id,
			item_count: totalItems,
			pending_changes: pendingChanges,
			dead_letter: deadLetter,
			peers: this.peers,
		};
	}

	stop(): void {
		if (this.timer) {
			clearInterval(this.timer);
			this.timer = null;
		}
	}
}

/**
 * Attach trigger-based change capture to an existing SQLite database.
 * Creates _meta, _changes, _dead_letter, _peer_state tables, generates
 * triggers for each table via schema introspection, and starts the
 * background ship loop. The db should be opened with WAL mode.
 */
export function attach(db: SqliteDatabase, config: Config, tables: string[]): Manager {
	return new TriggerManager(db, config, tables);
}
