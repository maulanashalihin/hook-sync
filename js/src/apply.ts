// hook-sync — table-agnostic LWW apply logic

import type { Change, SqliteDatabase } from "./types.ts";

/**
 * validTable checks that a table name contains only alphanumeric characters
 * and underscores. Table names cannot be parameterized in SQLite, so this
 * is defense-in-depth against injection via the wire protocol.
 */
export function validTable(name: string): boolean {
	return /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(name);
}

/** Convert any numeric value from JSON parse to a safe integer timestamp. */
function toInt64(v: unknown): number {
	if (typeof v === "number") return Math.trunc(v);
	if (typeof v === "bigint") return Number(v);
	return 0;
}

/**
 * Apply a single change with last-write-wins conflict resolution.
 * Table-agnostic: column names and values come from c.row.
 * Every table MUST have `id` (TEXT PRIMARY KEY) and `updated_at`
 * (INTEGER millisecond timestamp) columns per the wire protocol.
 *
 * Returns true if applied, false if skipped (LWW) or invalid.
 * Caller manages transaction and syncing flag.
 */
export function applyChange(db: SqliteDatabase, c: Change): boolean {
	if (c.op === "INSERT" || c.op === "UPDATE") {
		return applyUpsert(db, c);
	}
	if (c.op === "DELETE") {
		return applyDelete(db, c);
	}
	return false;
}

function applyUpsert(db: SqliteDatabase, c: Change): boolean {
	if (!c.row || !validTable(c.table)) return false;

	const id = c.row["id"];
	if (typeof id !== "string" || id === "") return false;

	const updatedAt = toInt64(c.row["updated_at"]);

	// Last-write-wins: skip if existing row is newer than incoming
	const existing = db.prepare(`SELECT updated_at FROM ${c.table} WHERE id = ?`).get(id) as
		| { updated_at: number }
		| undefined;
	if (existing && toInt64(existing.updated_at) > updatedAt) {
		return false;
	}

	// Build dynamic INSERT OR REPLACE from row keys
	const cols = Object.keys(c.row);
	const vals = cols.map((col) => c.row![col]);
	const placeholders = cols.map(() => "?").join(", ");

	const sql = `INSERT OR REPLACE INTO ${c.table} (${cols.join(", ")}) VALUES (${placeholders})`;
	const result = db.prepare(sql).run(...vals);
	return result.changes > 0;
}

function applyDelete(db: SqliteDatabase, c: Change): boolean {
	if (!c.old_id || !validTable(c.table)) return false;

	// Last-write-wins: skip delete if row was updated after deletion
	if (c.row) {
		const deleteUpdatedAt = toInt64(c.row["updated_at"]);
		const existing = db.prepare(`SELECT updated_at FROM ${c.table} WHERE id = ?`).get(c.old_id) as
			| { updated_at: number }
			| undefined;
		if (existing && toInt64(existing.updated_at) > deleteUpdatedAt) {
			return false; // row was updated after delete, keep the update
		}
	}

	const result = db.prepare(`DELETE FROM ${c.table} WHERE id = ?`).run(c.old_id);
	return result.changes > 0;
}
