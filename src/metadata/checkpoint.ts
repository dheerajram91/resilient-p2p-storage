/**
 * Periodic Content-Addressed Checkpoint Service (Phase 8).
 *
 * Produces periodic, immutable, content-addressed snapshots of the SQLite metadata
 * hierarchy and stores them directly in the distributed ContentStore.
 *
 * Benefits:
 *   1. Cold-node bootstrap: Joining or recovering nodes download checkpoint chunks
 *      in parallel from peer content holders rather than requesting a large live
 *      snapshot dump from the Primary.
 *   2. Bounded transaction log: Historical operations in meta_log prior to the
 *      active checkpoint are safely pruned, bounding SQLite file growth.
 */

import { createHash } from 'node:crypto'
import type { MetadataStore } from './store.js'
import type { MetaLog } from './log.js'
import type { ContentStore } from '../content/store.js'
import { log } from '../logger.js'

export interface CheckpointRecord {
  lsn: number
  chunkHash: string
  size: number
  sha256: string
  createdAt: number
}

export interface CheckpointOpts {
  intervalHours?: number
  retentionDays?: number
  deltaLogRetentionOps?: number
}

export class CheckpointService {
  private timer?: NodeJS.Timeout

  constructor(
    private store: MetadataStore,
    private contentStore: ContentStore,
    private metaLog: MetaLog | null,
    private opts: CheckpointOpts = {}
  ) {
    this.ensureSchema()
  }

  private ensureSchema(): void {
    this.store.db.exec(`
      CREATE TABLE IF NOT EXISTS checkpoints (
        lsn INTEGER PRIMARY KEY,
        chunk_hash TEXT NOT NULL,
        size INTEGER NOT NULL,
        sha256 TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
    `)
  }

  get retentionDays(): number {
    return this.opts.retentionDays ?? 7
  }

  get intervalHours(): number {
    return this.opts.intervalHours ?? 24
  }

  /**
   * Produce a content-addressed snapshot chunk, store it in ContentStore,
   * record the checkpoint in the database, and prune stale transaction log entries.
   */
  async createCheckpoint(): Promise<CheckpointRecord> {
    this.ensureSchema()
    const { lsn, blob } = this.store.snapshot()
    const hash = createHash('sha256').update(blob).digest('hex')
    const size = blob.length
    const now = Date.now()

    // Store in content store under content hash
    await this.contentStore.put(hash, blob)

    this.store.db
      .prepare(
        'INSERT OR REPLACE INTO checkpoints (lsn, chunk_hash, size, sha256, created_at) VALUES (?, ?, ?, ?, ?)'
      )
      .run(lsn, hash, size, hash, now)

    const record: CheckpointRecord = {
      lsn,
      chunkHash: hash,
      size,
      sha256: hash,
      createdAt: now
    }

    log.info('checkpoint created', {
      lsn,
      chunk: hash.slice(0, 12),
      sizeBytes: size
    })

    // Retention: prune historical checkpoints older than retentionDays (keeping at least the latest)
    this.pruneHistorical(now)

    return record
  }

  /** Fetch the latest checkpoint record, if any. */
  getLatestCheckpoint(): CheckpointRecord | null {
    this.ensureSchema()
    const row = this.store.db
      .prepare('SELECT lsn, chunk_hash, size, sha256, created_at FROM checkpoints ORDER BY lsn DESC LIMIT 1')
      .get() as
      | { lsn: number; chunk_hash: string; size: number; sha256: string; created_at: number }
      | undefined

    if (!row) return null
    return {
      lsn: Number(row.lsn),
      chunkHash: row.chunk_hash,
      size: Number(row.size),
      sha256: row.sha256,
      createdAt: Number(row.created_at)
    }
  }

  /** List all checkpoints, newest first. */
  listCheckpoints(): CheckpointRecord[] {
    this.ensureSchema()
    const rows = this.store.db
      .prepare('SELECT lsn, chunk_hash, size, sha256, created_at FROM checkpoints ORDER BY lsn DESC')
      .all() as Array<{ lsn: number; chunk_hash: string; size: number; sha256: string; created_at: number }>

    return rows.map((r) => ({
      lsn: Number(r.lsn),
      chunkHash: r.chunk_hash,
      size: Number(r.size),
      sha256: r.sha256,
      createdAt: Number(r.created_at)
    }))
  }

  /** Restore the local metadata hierarchy from a content-addressed checkpoint chunk. */
  async restoreFromCheckpoint(record: CheckpointRecord): Promise<void> {
    const blob = await this.contentStore.get(record.chunkHash)
    const verifiedHash = createHash('sha256').update(blob).digest('hex')
    if (verifiedHash !== record.chunkHash) {
      throw new Error(`Checkpoint chunk ${record.chunkHash.slice(0, 12)} integrity failed`)
    }
    this.store.load(blob)
    log.info('restored metadata from checkpoint', {
      lsn: record.lsn,
      chunk: record.chunkHash.slice(0, 12)
    })
  }

  /** Prune historical checkpoints and prune meta_log older than the oldest retained checkpoint. */
  private pruneHistorical(now: number): void {
    const cutoff = now - this.retentionDays * 86400000
    // Keep at least the latest checkpoint regardless of timestamp
    const latest = this.getLatestCheckpoint()
    if (!latest) return

    this.store.db
      .prepare('DELETE FROM checkpoints WHERE created_at < ? AND lsn < ?')
      .run(cutoff, latest.lsn)

    // Find the minimum retained checkpoint LSN
    const row = this.store.db.prepare('SELECT COALESCE(MIN(lsn), ?) AS min_lsn FROM checkpoints').get(latest.lsn) as {
      min_lsn: number
    }
    const minLsn = Number(row.min_lsn)

    if (this.metaLog && minLsn > 0) {
      this.metaLog.purge(minLsn)
      log.debug('meta_log pruned before checkpoint', { minLsn })
    }
  }

  /** Start periodic background checkpointing. */
  start(): void {
    if (this.intervalHours <= 0) return
    const intervalMs = this.intervalHours * 3600000
    this.timer = setInterval(() => {
      this.createCheckpoint().catch((err) => {
        log.warn('periodic checkpoint failed', { error: String(err) })
      })
    }, intervalMs)
  }

  stop(): void {
    if (this.timer) {
      clearInterval(this.timer)
      this.timer = undefined
    }
  }
}
