/**
 * Availability service (DETAILED_DESIGN §8) — reacts to health transitions.
 *
 * On a peer going DOWN:
 *   - if it was the Primary and we are a replica → hand off to the election
 *     coordinator (Phase 3 M2);
 *   - if we are the Primary → repair metadata so no chunk's only live copy points
 *     at the dead node (promotion, §8.2);
 *   - either way, wake the local reconciler so RF=2 is rebuilt promptly (§9).
 *
 * On a peer coming back UP: wake the reconciler — a fresh healthy placement
 * target is now available for any still-degraded chunk.
 *
 * Phase 7 (proposal §4.6): while ROTATION_INTERVAL_MS is configured, periodically
 * swap a chunk's active and backup copies (both healthy) so the passive copy
 * serves real traffic and both copies stay battle-tested.
 *
 * Owns the HealthDetector's lifecycle; all cross-cutting reactions live here so
 * index.ts wiring stays declarative.
 */

import type { HealthDetector } from '../p2p/health.js'
import type { ReplicationService } from '../replication/service.js'
import type { ContentService } from '../content/service.js'
import type { ClusterView } from '../p2p/announce.js'
import type { MetadataStore } from '../metadata/store.js'
import type { LocationRec } from '../metadata/types.js'
import type { Counters } from '../api/counters.js'
import { log } from '../logger.js'

/** Phase 7 (§8.4): proactive passive→active rotation options. */
export interface RotationOpts {
  /** Swap sweep interval (ms); 0 disables rotation (default). */
  intervalMs?: number
  /** Max chunks swapped per sweep. */
  batch?: number
  counters?: Counters
}

export class AvailabilityService {
  private promotionTimer?: NodeJS.Timeout
  private rotationTimer?: NodeJS.Timeout
  private promoting = false
  private rotating = false
  private rotationCursor = 0
  /** Election hook, wired in Phase 3 M2. Called (on replicas) when the Primary dies. */
  onPrimaryDown?: (downPeer: string) => void

  constructor(
    private detector: HealthDetector,
    private replication: ReplicationService,
    private content: ContentService,
    private cluster: ClusterView,
    private store: MetadataStore,
    private opts: RotationOpts = {}
  ) {}

  start(): void {
    this.detector.onDown((peer) => {
      void this.handleDown(peer).catch((err) => log.warn('availability reaction failed', { error: String(err) }))
    })
    this.detector.onUp((peer) => this.handleUp(peer))
    this.detector.start()
    // A quorum leader can be elected after the original down event was observed.
    this.promotionTimer = setInterval(() => void this.repairDownPeers(), 1000)
    // Phase 7 (§8.4): proactive passive→active rotation, when configured.
    if ((this.opts.intervalMs ?? 0) > 0) {
      this.rotationTimer = setInterval(() => void this.rotateOnce(), this.opts.intervalMs)
    }
  }

  stop(): void {
    this.detector.stop()
    if (this.promotionTimer) clearInterval(this.promotionTimer)
    if (this.rotationTimer) clearInterval(this.rotationTimer)
  }

  private async repairDownPeers(): Promise<void> {
    if (this.promoting || !this.replication.isPrimary) return
    this.promoting = true
    try {
      for (const peer of this.detector.downPeers()) {
        await this.replication.applyPromotion(peer, (id) => this.detector.isHealthy(id))
      }
    } catch (err) {
      log.warn('availability promotion deferred', { error: String(err) })
    } finally {
      this.promoting = false
    }
  }

  private async handleDown(peer: string): Promise<void> {
    const primaryId = this.cluster.primaryPeer()?.toString()

    if (peer === primaryId && !this.replication.isPrimary) {
      log.warn('availability: PRIMARY is down — initiating failover', { peer: peer.slice(-8) })
      this.onPrimaryDown?.(peer)
    }

    // The Primary is the single writer, so only it repairs location metadata.
    if (this.replication.isPrimary) {
      const summary = await this.replication.applyPromotion(peer, (id) => this.detector.isHealthy(id))
      if (summary.promoted > 0 || summary.backupsCleared > 0 || summary.atRisk > 0) {
        log.info('availability: promotion result', { ...summary })
      }
    }

    this.content.wakeReconcile()
  }

  private handleUp(peer: string): void {
    log.info('availability: peer recovered', { peer: peer.slice(-8) })
    this.content.wakeReconcile()
  }

  /**
   * Proactive rotation (proposal §4.6): swap active and backup for chunks whose
   * two holders are both healthy, so the passive copy serves real traffic and
   * both copies stay battle-tested. Primary-only, rate-limited per sweep, and
   * skipped while the metadata view is stale or the chunk is pinned for an
   * in-flight upload.
   */
  private async rotateOnce(): Promise<void> {
    if (this.rotating || this.promoting || !this.replication.isPrimary) return
    if (this.store.mode !== 'push') return // don't rotate from a stale view
    this.rotating = true
    let swapped = 0
    try {
      const batch = Math.max(1, this.opts.batch ?? 8)
      const candidates = this.store.allLocations()
        .filter((l): l is LocationRec & { backup_peer: string } =>
          l.state === 'ok' && l.backup_peer !== null && l.active_peer !== l.backup_peer
        )
        .sort((a, b) => a.chunk_hash.localeCompare(b.chunk_hash))
      if (candidates.length === 0) return
      for (let i = 0; i < candidates.length && swapped < batch; i++) {
        const loc = candidates[(this.rotationCursor + i) % candidates.length]
        if (!this.detector.isHealthy(loc.active_peer) || !this.detector.isHealthy(loc.backup_peer)) continue
        if (this.content.isPinned(loc.chunk_hash)) continue
        if (!(await this.swapOne(loc))) continue
        swapped++
      }
      this.rotationCursor = (this.rotationCursor + batch) % candidates.length
    } finally {
      this.rotating = false
    }
    if (swapped > 0) {
      this.opts.counters?.inc('rotation_swaps_total', swapped)
      log.info('rotation: passive→active swap complete', { swapped })
    }
  }

  /** Swap active and backup for one chunk; true if the metadata commit succeeded. */
  private async swapOne(loc: { chunk_hash: string; active_peer: string; backup_peer: string }): Promise<boolean> {
    const result = await this.replication.relocateActive(
      loc.chunk_hash, loc.backup_peer, loc.active_peer, 'ok',
      { active: loc.active_peer, backup: loc.backup_peer }
    )
    return !('error' in result)
  }
}
