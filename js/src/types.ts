// hook-sync — shared types

/** A single change captured by a trigger or received via sync. */
export interface Change {
  op: "INSERT" | "UPDATE" | "DELETE";
  table: string;
  row: Record<string, unknown> | null;
  old_id: string | null;
}

/** Wire protocol: sender → receiver. */
export interface SyncRequest {
  batch_id: number;
  changes: Change[];
}

/** Wire protocol: receiver → sender. */
export interface SyncResponse {
  applied: number;
  ack: number;
}

/** Per-node configuration passed to attach(). */
export interface Config {
  id: string;
  peers: string[];
  batchMs?: number;
  batchSize?: number;
}

/** Health snapshot from Manager.health(). */
export interface HealthStatus {
  ok: boolean;
  node_id: string;
  item_count: number;
  pending_changes: number;
  dead_letter: number;
  peers: string[];
}

/**
 * Minimal SQLite interface — compatible with both bun:sqlite and better-sqlite3.
 * Callers inject their own database instance; the library never imports a binding.
 */
export interface SqliteStatement {
  run(...params: unknown[]): { changes: number; lastInsertRowid: number | bigint };
  get(...params: unknown[]): unknown;
  all(...params: unknown[]): unknown[];
}

export interface SqliteDatabase {
  exec(sql: string): void;
  prepare(sql: string): SqliteStatement;
  transaction<T extends (...args: never[]) => unknown>(fn: T): T;
}

/** Manager returned by attach(). */
export interface Manager {
  /** Apply a batch of received changes (LWW conflict resolution). Returns count applied. */
  applyChanges(changes: Change[]): number;
  /** Stop the ship loop. */
  stop(): void;
  /** Health snapshot. */
  health(): HealthStatus;
}
