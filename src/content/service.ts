/**
 * Content service (DETAILED_DESIGN §4, §5, §11) — the RF=2 write/read paths.
 *
 * Write: verify/flush this node and a distinct healthy backup before submitting
 * metadata. Protect both copies from relocation deletion through metadata apply.
 *
 * Read (with read-repair):
 *   resolve file → manifest → chunks; serve each chunk from local disk (verifying
 *   integrity), else fetch from the active/backup holder. A locally-corrupt chunk
 *   is repaired from the good replica.
 */

import { extname } from 'node:path'
import { randomUUID } from 'node:crypto'
import type { PeerId } from '@libp2p/interface'
import type { NodeHost } from '../p2p/host.js'
import type { MetadataStore } from '../metadata/store.js'
import type { ReplicationService } from '../replication/service.js'
import type { ClusterView } from '../p2p/announce.js'
import type { LocationRec } from '../metadata/types.js'
import { ContentStore, CorruptChunkError } from './store.js'
import type { ContentTransfer } from './transfer.js'
import { chunkBuffer } from './chunker.js'
import type { Counters } from '../api/counters.js'
import { log } from '../logger.js'

export interface ContentServiceOpts {
  chunkSize: number
  quotaBytes: number
  /** Desired replication factor (used for admission control, §10). */
  replicationFactor: number
  /**
   * Ceiling (in used bytes) for opportunistic read-warm caching. A node only
   * caches chunks it doesn't own while at/below this mark, so warming never
   * fights the rebalancer (§10). Typically quota × rebalance low-water fraction.
   */
  warmCacheMaxBytes: number
  /** Reconciler sweep interval (ms); 0 disables the periodic reconciler. */
  reconcileIntervalMs: number
  /** Max chunks repaired per reconcile sweep (rate limit). */
  reconcileBatch: number
  /** Placement guard: skip peers the health detector believes are down. */
  isHealthy?: (peerId: string) => boolean
  /** Phase 7: optional monotonic counters surfaced by the /metrics endpoint. */
  counters?: Counters
}

export type WriteFileResult =
  | { ok: true; lsn: number; fileId?: number; chunks: number; replaced?: boolean }
  | { error: string; code?: 'ENOSPC' | 'EUNAVAILABLE' | 'EIO' }

export class ContentService {
  private reconcileTimer?: NodeJS.Timeout
  private reconciling = false

  constructor(
    private node: NodeHost,
    private content: ContentStore,
    private transfer: ContentTransfer,
    private meta: MetadataStore,
    private replication: ReplicationService,
    private cluster: ClusterView,
    private opts: ContentServiceOpts
  ) {}

  get selfId(): string {
    return this.node.peerId.toString()
  }

  start(): void {
    this.transfer.registerHandlers((lsn) => this.meta.lsn >= lsn)
    if (this.opts.reconcileIntervalMs > 0) {
      this.reconcileTimer = setInterval(() => void this.reconcileOnce(), this.opts.reconcileIntervalMs)
    }
  }

  stop(): void {
    if (this.reconcileTimer) clearInterval(this.reconcileTimer)
  }

  usedBytes(): number {
    return this.content.usedBytes()
  }

  localChunks(): Promise<string[]> {
    return this.content.list()
  }

  hasLocal(hash: string): Promise<boolean> {
    return this.content.has(hash)
  }

  corruptLocalForTest(hash: string): Promise<boolean> {
    return this.content.corruptForTest(hash)
  }

  // ---- write path -----------------------------------------------------------

  async writeFile(path: string, bytes: Buffer, contentType?: string): Promise<WriteFileResult> {
    try {
      await this.content.whenReady()
    } catch (err) {
      log.error('content store unavailable for write', { path, error: String(err) })
      return { error: String(err), code: (err as NodeJS.ErrnoException).code === 'ENOSPC' ? 'ENOSPC' : 'EIO' }
    }
    const admit = this.admitWrite(bytes.length)
    if ('error' in admit) return admit

    const chunks = bytes.length === 0 ? [] : chunkBuffer(bytes, this.opts.chunkSize)
    const token = randomUUID()
    const hashes = [...new Set(chunks.map((c) => c.hash))]
    const backups: { peer: PeerId; hash: string; token: string }[] = []
    let submitted = false
    let definitive = false
    let committedLsn: number | undefined
    try {
      for (const hash of hashes) await this.content.pin(hash, token)
      const confirmed = new Map<string, string>()
      const reserved = new Map<string, number>()
      for (const c of chunks) {
        if (confirmed.has(c.hash)) continue
        await this.content.put(c.hash, c.bytes)
        const exclude = [this.selfId]
        let backup: string | undefined
        while (true) {
          const dest = this.choosePlacement(c.size, exclude, reserved)
          if (!dest) break
          exclude.push(dest.id)
          // Record before dialing: a lost response may still have installed a pin.
          backups.push({ peer: dest.peer, hash: c.hash, token })
          try {
            if (await this.transfer.putChunk(dest.peer, c.hash, c.bytes, token)) {
              if (this.opts.isHealthy && !this.opts.isHealthy(dest.id)) continue
              backup = dest.id
              reserved.set(dest.id, (reserved.get(dest.id) ?? 0) + c.size)
              break
            }
          } catch (err) {
            log.warn('durable backup failed', { hash: c.hash.slice(0, 12), backup: dest.id, error: String(err) })
          }
        }
        if (!backup) {
          if (exclude.length === 1) return { error: 'ENOSPC: no healthy backup has room for the chunk', code: 'ENOSPC' }
          return { error: 'no verified healthy backup is available', code: 'EUNAVAILABLE' }
        }
        confirmed.set(c.hash, backup)
      }
      if ([...confirmed.values()].some((id) => this.opts.isHealthy && !this.opts.isHealthy(id))) {
        return { error: 'backup became unhealthy before metadata submission', code: 'EUNAVAILABLE' }
      }
      submitted = true
      const result = await this.replication.createFile({
        path,
        size: bytes.length,
        ext: extname(path).replace(/^\./, '') || null,
        contentType: contentType ?? null,
        chunks: chunks.map((c) => ({ hash: c.hash, size: c.size, backupPeer: confirmed.get(c.hash)! })),
        activePeer: this.selfId
      })
      definitive = !('error' in result && result.code === 'EUNAVAILABLE')
      if ('error' in result) return result
      committedLsn = result.lsn
      this.opts.counters?.inc('uploads_total')
      return { ok: true, lsn: result.lsn, fileId: result.fileId, chunks: chunks.length, replaced: result.replaced }
    } catch (err) {
      log.error('durable write failed', { path, error: String(err) })
      return {
        error: String(err),
        code: (err as NodeJS.ErrnoException).code === 'ENOSPC' ? 'ENOSPC' : 'EIO'
      }
    } finally {
      // Unavailable/timeout responses may still commit, so retain their pins.
      // Only a successful commit or definitive rejection makes release safe.
      if (!submitted || definitive) {
        for (const hash of hashes) {
          try {
            await this.content.unpin(hash, token)
          } catch (err) {
            log.warn('local upload protection retained after release failure', { hash: hash.slice(0, 12), error: String(err) })
          }
        }
        for (const b of backups) {
          try {
            await this.transfer.releaseChunk(b.peer, b.hash, b.token, committedLsn)
          } catch (err) {
            log.warn('backup protection retained after release failure', { hash: b.hash.slice(0, 12), error: String(err) })
          }
        }
      } else {
        log.warn('content protection retained for uncertain metadata commit', { path })
      }
    }
  }

  /** §5 placement: least-utilized healthy peer with room, excluding given ids. */
  private choosePlacement(size: number, exclude: string[], reserved = new Map<string, number>()): { id: string; peer: PeerId } | null {
    const healthy = this.opts.isHealthy
    const candidates = this.cluster
      .all()
      .filter((p) => p.peer && !exclude.includes(p.peerId))
      .filter((p) => !healthy || healthy(p.peerId)) // never place on a peer believed down
      .filter((p) => p.usedBytes + (reserved.get(p.peerId) ?? 0) + size <= (p.quotaBytes ?? this.opts.quotaBytes))
    if (candidates.length === 0) return null
    candidates.sort((a, b) => a.usedBytes / (a.quotaBytes || 1) - b.usedBytes / (b.quotaBytes || 1))
    const best = candidates[0]
    return { id: best.peerId, peer: best.peer as PeerId }
  }

  // ---- capacity: admission control + reporting (DETAILED_DESIGN §10) ---------

  /** Healthy nodes (self + peers believed up) with their used/quota/free bytes. */
  private capacityNodes(): { peerId: string; used: number; quota: number; free: number }[] {
    const healthy = this.opts.isHealthy
    const selfUsed = this.content.usedBytes()
    const nodes = [
      { peerId: this.selfId, used: selfUsed, quota: this.opts.quotaBytes, free: Math.max(0, this.opts.quotaBytes - selfUsed) }
    ]
    for (const p of this.cluster.all()) {
      if (p.peerId === this.selfId || !p.peer) continue
      if (healthy && !healthy(p.peerId)) continue
      const quota = p.quotaBytes || this.opts.quotaBytes
      nodes.push({ peerId: p.peerId, used: p.usedBytes, quota, free: Math.max(0, quota - p.usedBytes) })
    }
    return nodes
  }

  /**
   * Cluster-wide admission control (§10): accept a write only if the cluster can
   * hold RF copies of it. The active copy lands wholly on this node, so this node
   * must have room for the whole file; the cluster must have RF× headroom overall,
   * spread over at least RF distinct nodes. Otherwise reject with ENOSPC.
   */
  admitWrite(size: number): { ok: true } | { error: string; code: 'ENOSPC' } {
    const rf = this.opts.replicationFactor
    const nodes = this.capacityNodes()
    if (rf !== 2) return { error: 'only RF=2 durable writes are supported', code: 'ENOSPC' }
    if (this.opts.isHealthy && !this.opts.isHealthy(this.selfId)) {
      return { error: 'ENOSPC: local active holder is unhealthy', code: 'ENOSPC' }
    }
    const self = nodes.find((n) => n.peerId === this.selfId)
    const totalFree = nodes.reduce((s, n) => s + n.free, 0)
    const withRoom = nodes.filter((n) => n.free >= Math.min(size, this.opts.chunkSize)).length
    if (!self || self.free < size) {
      return { error: `ENOSPC: node has no room for the active copy (free ${self?.free ?? 0} < ${size})`, code: 'ENOSPC' }
    }
    if (totalFree < rf * size) {
      return { error: `ENOSPC: cluster cannot hold RF=${rf} copies (free ${totalFree} < ${rf * size})`, code: 'ENOSPC' }
    }
    if (withRoom < rf) {
      return { error: `ENOSPC: fewer than RF=${rf} nodes have room`, code: 'ENOSPC' }
    }
    return { ok: true }
  }

  /** Capacity snapshot for observability (GET /admin/capacity). */
  capacity(): object {
    const rf = this.opts.replicationFactor
    const nodes = this.capacityNodes()
    const totalUsed = nodes.reduce((s, n) => s + n.used, 0)
    const totalQuota = nodes.reduce((s, n) => s + n.quota, 0)
    const totalFree = nodes.reduce((s, n) => s + n.free, 0)
    return {
      replicationFactor: rf,
      admitHeadroomBytes: Math.floor(totalFree / rf),
      cluster: { nodes: nodes.length, totalUsed, totalQuota, totalFree },
      nodes: nodes.map((n) => ({
        peerId: n.peerId.slice(-8),
        used: n.used,
        quota: n.quota,
        free: n.free,
        pct: n.quota ? Math.round((n.used / n.quota) * 100) : 0
      }))
    }
  }

  // ---- rebalance hooks (used by RebalanceService, §10) ----------------------

  /** §5 placement helper, exposed for the rebalance service. */
  placement(size: number, exclude: string[]): { id: string; peer: PeerId } | null {
    return this.choosePlacement(size, exclude)
  }

  /** Read a verified local chunk (rebalance move source). */
  getLocalBytes(hash: string): Promise<Buffer> {
    return this.content.get(hash)
  }

  /** Delete a local chunk after its copy has been relocated elsewhere (rebalance). */
  async deleteLocalChunk(hash: string, expectedGeneration?: number): Promise<boolean> {
    // The committed relocation can precede local metadata apply. Pins and the
    // captured generation guard deletion without consulting that stale view.
    const deleted = await this.content.delete(hash, expectedGeneration)
    if (!deleted) log.debug('relocation deletion skipped for protected/newly confirmed chunk', { hash: hash.slice(0, 12) })
    return deleted
  }

  isPinned(hash: string): boolean {
    return this.content.isPinned(hash)
  }

  localGeneration(hash: string): number {
    return this.content.generation(hash)
  }

  // ---- reconciler (DETAILED_DESIGN §9) --------------------------------------

  /**
   * Restore RF=2 for every chunk this node is the active holder of that is still
   * `degraded` (fresh write not yet backed up, or backup lost to a failure).
   * Runs on EVERY node on a timer and on demand (wakeReconcile): after a
   * promotion the survivor is active+degraded and already holds the bytes, so it
   * simply copies to a fresh healthy peer and flips the location back to `ok`.
   */
  async reconcileOnce(): Promise<number> {
    if (this.reconciling) return 0
    // Don't reconcile from a stale view while still catching metadata up.
    if (this.meta.mode !== 'push') return 0
    this.reconciling = true
    let repaired = 0
    try {
      const rows = this.meta.degradedActiveLocations(this.selfId).slice(0, this.opts.reconcileBatch)
      for (const loc of rows) {
        try {
          if (await this.reconcileOne(loc)) repaired++
        } catch (err) {
          log.warn('reconcile chunk failed', { hash: loc.chunk_hash.slice(0, 12), error: String(err) })
        }
      }
    } finally {
      this.reconciling = false
    }
    if (repaired > 0) log.info('reconcile: RF=2 restored', { chunks: repaired })
    if (repaired > 0) this.opts.counters?.inc('reconciled_chunks_total', repaired)
    return repaired
  }

  /** Run a reconcile sweep promptly (e.g. right after a promotion) without waiting for the timer. */
  wakeReconcile(): void {
    setImmediate(() => void this.reconcileOnce())
  }

  private async reconcileOne(loc: LocationRec): Promise<boolean> {
    // Re-read the freshest state — another sweep may have handled it.
    const fresh = this.meta.getLocation(loc.chunk_hash)
    if (!fresh || fresh.state === 'ok' || fresh.active_peer !== this.selfId) return false
    const expected = { active: fresh.active_peer, backup: fresh.backup_peer }

    // Ensure we can supply verified bytes for the copy.
    let bytes: Buffer | null = null
    if (await this.content.has(fresh.chunk_hash)) {
      try {
        bytes = await this.content.get(fresh.chunk_hash)
      } catch {
        bytes = await this.fetchFromPeers(fresh.chunk_hash) // corrupt local → pull a good copy
      }
    } else {
      bytes = await this.fetchFromPeers(fresh.chunk_hash)
    }
    if (!bytes) {
      log.warn('reconcile: no source bytes, skipping', { hash: fresh.chunk_hash.slice(0, 12) })
      return false
    }
    await this.content.put(fresh.chunk_hash, bytes)

    const exclude = [this.selfId, fresh.backup_peer].filter((x): x is string => !!x)
    const dest = this.choosePlacement(fresh.size, exclude)
    if (!dest) {
      log.warn('reconcile: no healthy placement available', { hash: fresh.chunk_hash.slice(0, 12) })
      return false
    }
    const ok = await this.transfer.putChunk(dest.peer, fresh.chunk_hash, bytes)
    if (!ok) return false
    const res = await this.replication.setBackup(fresh.chunk_hash, dest.id, 'ok', expected)
    if ('error' in res) {
      log.warn('reconcile: setBackup failed', { hash: fresh.chunk_hash.slice(0, 12), error: res.error })
      return false
    }
    log.info('reconcile: new backup placed', {
      hash: fresh.chunk_hash.slice(0, 12),
      active: this.selfId.slice(-8),
      backup: dest.id.slice(-8)
    })
    return true
  }

  // ---- read path ------------------------------------------------------------

  async readFile(path: string): Promise<{ bytes: Buffer; contentType: string | null } | { error: string }> {
    const file = this.meta.resolveFile(path)
    if (!file) return { error: 'not found' }
    if (file.manifest_id == null) return { error: 'file has no content' }
    const manifest = this.meta.getManifest(Number(file.manifest_id))
    if (!manifest) return { error: 'manifest missing' }

    const parts: Buffer[] = []
    for (const hash of manifest.chunkHashes) {
      const bytes = await this.readChunk(hash)
      if (!bytes) return { error: `chunk ${hash.slice(0, 12)} unavailable` }
      parts.push(bytes)
    }
    this.opts.counters?.inc('reads_total')
    return { bytes: Buffer.concat(parts), contentType: file.content_type }
  }

  /** Read one chunk: local (verified, self-repairing) → peer holders. */
  private async readChunk(hash: string): Promise<Buffer | null> {
    if (await this.content.has(hash)) {
      try {
        return await this.content.get(hash)
      } catch (err) {
        if (err instanceof CorruptChunkError) {
          log.warn('local chunk corrupt → read-repair', { hash: hash.slice(0, 12) })
          const good = await this.fetchFromPeers(hash)
          if (good) {
            await this.content.put(hash, good)
            log.info('read-repair complete', { hash: hash.slice(0, 12) })
            this.opts.counters?.inc('read_repairs_total')
            return good
          }
          return null
        }
        throw err
      }
    }
    // Not held locally — fetch from a holder. Opportunistically warm this
    // replica's cache, but ONLY while we're below the rebalance low-water mark:
    // warming a near-full node would re-create the very capacity pressure the
    // rebalancer just relieved (read-warm vs offload oscillation, §10).
    const bytes = await this.fetchFromPeers(hash)
    if (bytes && this.content.usedBytes() + bytes.length <= this.opts.warmCacheMaxBytes) {
      try {
        await this.content.put(hash, bytes)
      } catch (err) {
        log.warn('read warm-cache write failed', { hash: hash.slice(0, 12), error: String(err) })
      }
    }
    return bytes
  }

  private async fetchFromPeers(hash: string): Promise<Buffer | null> {
    const loc = this.meta.getLocation(hash)
    if (!loc) return null
    const holders = [loc.active_peer, loc.backup_peer].filter((x): x is string => !!x)
    for (const id of holders) {
      if (id === this.selfId) continue
      const peer = this.cluster.get(id)?.peer
      if (!peer) continue
      try {
        const bytes = await this.transfer.getChunk(peer, hash)
        if (bytes) return bytes
      } catch (err) {
        log.debug('peer fetch failed', { holder: id.slice(-8), error: String(err) })
      }
    }
    return null
  }
}
