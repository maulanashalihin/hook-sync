// hook-sync — SQLite replication library
// Trigger-based change capture, ACK-based sync, last-write-wins conflict resolution.
// Works with bun:sqlite and better-sqlite3 — caller injects the database instance.

export { attach } from "./trigger.ts";
export { applyChange, validTable } from "./apply.ts";
export { shipWithAck } from "./ship.ts";

export type {
	Change,
	Config,
	HealthStatus,
	Manager,
	SyncRequest,
	SyncResponse,
	SqliteDatabase,
	SqliteStatement,
} from "./types.ts";
