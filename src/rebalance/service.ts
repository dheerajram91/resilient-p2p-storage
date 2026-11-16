/**
 * Rebalance / capacity service (DETAILED_DESIGN §10).
 *
 * Keeps each node within its storage budget using two-threshold hysteresis:
 *
 *   T1 = high * quota   (start offloading)
 *   T2 = low  * quota   (stop offloading)   with low < high, so the gap damps oscillation
 *
 * Runs on EVERY caught-up node. When `used > T1`, it offloads chunks it holds —
 * largest first — to the least-utilized healthy peer with room, until `used < T2`.
 *
 * RF is preserved at all times: a chunk always has two holders, so we move only
 * ONE of the two copies and we delete our local copy *after* the location map has
 * been updated (via the Primary). If we hold the `active` copy we relocate active
 * (keeping the existing backup); if we hold the `backup` copy we move the backup.
 * We never touch a chunk that isn't `ok` (RF work already in flight) or whose
 * other copy is on an unhealthy node (moving ours could drop below RF).
 */

import type { NodeHost } from '../p2p/host.js'
import type { MetadataStore } from '../metadata/store.js'
import type { ReplicationService } from '../replication/service.js'
import type { ContentService } from '../content/service.js'
import type { ContentTransfer } from '../content/transfer.js'
import type { LocationRec } from '../metadata/types.js'
import type { Counters } from '../api/counters.js'
import { log } from '../logger.js'

export interface RebalanceOpts {
  quotaBytes: number
  /** Start offloading above this fraction of quota (T1). */
  high: number
  /** Stop offloading below this fraction of quota (T2). */
  low: number
  /** Sweep interval (ms); 0 disables the periodic sweep. */
  intervalMs: number
  /** Skip peers the health detector believes are down. */
  isHealthy?: (peerId: string) => boolean
  /** Phase 7: optional monotonic counters surfaced by the /metrics endpoint. */
  counters?: Counters
}

export class RebalanceService {
  private timer?: NodeJS.Timeout
  private running = false

  constructor(
    private node: NodeHost,
    private content: ContentService,
    private transfer: ContentTransfer,
    private meta: MetadataStore,
    private replication: ReplicationService,
    private opts: RebalanceOpts
  ) {}

  get selfId(): string {
    return this.node.peerId.toString()
  }

  start(): void {
    if (this.opts.intervalMs > 0) {
      this.timer = setInterval(() => void this.sweepOnce(), this.opts.intervalMs)
    }
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer)
  }

  /**
   * One offload sweep. Returns the number of chunks moved. No-op unless this node
   * is over T1 and its metadata view is current (mode='push').
   */
  async sweepOnce(): Promise<number> {
    if (this.running) return 0
    if (this.meta.mode !== 'push') return 0 // don't move from a stale view while catching up
    this.running = true
    let moved = 0
    try {
      const quota = this.opts.quotaBytes
      const highBytes = quota * this.opts.high
      const lowBytes = quota * this.opts.low
      let used = this.content.usedBytes()
      if (used <= highBytes) return 0

      log.info('rebalance: over high-water, offloading', {
        usedPct: Math.round((used / quota) * 100),
        highPct: Math.round(this.opts.high * 100),
        lowPct: Math.round(this.opts.low * 100)
      })

      // Largest chunks first: fewest moves to get back under T2.
      const held = this.meta.heldChunksBySize(this.selfId)
      for (const loc of held) {
        if (used <= lowBytes) break
        if (!(await this.content.hasLocal(loc.chunk_hash))) continue // only move what we physically hold
        if (await this.offloadOne(loc)) {
          used -= loc.size
          moved++
        }
      }
    } finally {
      this.running = false
    }
    if (moved > 0) {
      this.opts.counters?.inc('rebalance_moves_total', moved)
      log.info('rebalance: offloaded chunks', {
        moved,
        usedPct: Math.round((this.content.usedBytes() / this.opts.quotaBytes) * 100)
      })
    }
    return moved
  }

  /**
   * Move one of a chunk's two copies off this node while preserving RF.
   * Returns true only if the copy was placed elsewhere AND the map updated AND
   * the local copy removed.
   */
  private async offloadOne(loc: LocationRec): Promise<boolean> {
    // Re-read the freshest state — the map may have changed since the listing.
    const fresh = this.meta.getLocation(loc.chunk_hash)
    if (!fresh) return false
    if (fresh.state !== 'ok') return false // RF work in flight; leave it to the reconciler

    const amActive = fresh.active_peer === this.selfId
    const amBackup = fresh.backup_peer === this.selfId
    if (!amActive && !amBackup) return false
    if (this.content.isPinned(fresh.chunk_hash)) return false
    const generation = this.content.localGeneration(fresh.chunk_hash)

    // The copy we are NOT moving must be on a healthy node, or moving ours risks
    // dropping below RF.
    const otherHolder = amActive ? fresh.backup_peer : fresh.active_peer
    if (!otherHolder) return false
    if (this.opts.isHealthy && !this.opts.isHealthy(otherHolder)) return false

    // Pick a destination: healthy, has room, not us, not the copy we're keeping.
    const dest = this.content.placement(fresh.size, [this.selfId, otherHolder])
    if (!dest) return false

    // Supply verified bytes; a corrupt local copy is left for read-repair, not moved.
    let bytes: Buffer
    try {
      bytes = await this.content.getLocalBytes(fresh.chunk_hash)
    } catch (err) {
      log.warn('rebalance: local copy unreadable, skipping', { hash: fresh.chunk_hash.slice(0, 12), error: String(err) })
      return false
    }

    const sent = await this.transfer.putChunk(dest.peer, fresh.chunk_hash, bytes)
    if (!sent) return false

    // Update the location map via the Primary BEFORE deleting locally.
    const res = amActive
      ? await this.replication.relocateActive(fresh.chunk_hash, dest.id, fresh.backup_peer, 'ok',
          { active: fresh.active_peer, backup: fresh.backup_peer })
      : await this.replication.setBackup(fresh.chunk_hash, dest.id, 'ok',
          { active: fresh.active_peer, backup: fresh.backup_peer })
    if ('error' in res) {
      log.warn('rebalance: map update failed, keeping local copy', { hash: fresh.chunk_hash.slice(0, 12), error: res.error })
      return false
    }

    if (!(await this.content.deleteLocalChunk(fresh.chunk_hash, generation))) return false
    log.info('rebalance: moved chunk', {
      hash: fresh.chunk_hash.slice(0, 12),
      role: amActive ? 'active' : 'backup',
      to: dest.id.slice(-8)
    })
    return true
  }
}
