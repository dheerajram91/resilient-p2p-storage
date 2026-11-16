/**
 * Incremental metadata log — authoritative ONLY on the Primary.
 *
 * Shares the MetadataStore's DatabaseSync handle so a namespace write and its log
 * append commit atomically in one transaction. Replicas do not append to this log;
 * they advance their lsn from push/pull batches instead.
 */

import type { DatabaseSync } from 'node:sqlite'
import type { Op } from './types.js'

export interface LogEntry {
  lsn: number
  op: Op
}

export class MetaLog {
  constructor(private db: DatabaseSync) {
    db.exec(
      'CREATE TABLE IF NOT EXISTS meta_log (lsn INTEGER PRIMARY KEY, op TEXT NOT NULL, payload TEXT NOT NULL, ts_ms INTEGER NOT NULL)'
    )
  }

  append(lsn: number, op: Op, tsMs: number): void {
    this.db
      .prepare('INSERT INTO meta_log (lsn, op, payload, ts_ms) VALUES (?, ?, ?, ?)')
      .run(lsn, op.op, JSON.stringify(op), tsMs)
  }

  /** All entries with lsn > fromLsn, ascending, up to `limit`. */
  read(fromLsn: number, limit: number): LogEntry[] {
    const rows = this.db
      .prepare('SELECT lsn, payload FROM meta_log WHERE lsn > ? ORDER BY lsn LIMIT ?')
      .all(fromLsn, limit) as Array<{ lsn: number; payload: string }>
    return rows.map((r) => ({ lsn: Number(r.lsn), op: JSON.parse(r.payload) as Op }))
  }

  latestLsn(): number {
    const row = this.db.prepare('SELECT COALESCE(MAX(lsn), 0) AS m FROM meta_log').get() as { m: number }
    return Number(row.m)
  }

  /** Drop entries older than beforeLsn (retention: keep >= 2 snapshot cycles). */
  purge(beforeLsn: number): void {
    this.db.prepare('DELETE FROM meta_log WHERE lsn < ?').run(beforeLsn)
  }
}
