/**
 * Election coordinator (DETAILED_DESIGN §8.3) — picks a new Primary when the
 * current one dies. Runs on every replica.
 *
 * Deterministic election over the gossip view (no voting rounds — safe under the
 * trusted-PC + single-failure assumption): the winner is the healthy candidate
 * with the highest committed lsn, breaking ties by anchor-role then lowest
 * peerId. Because every node computes the same function over the same gossip
 * data, they agree without a coordinator; the epoch guard in ReplicationService
 * deterministically resolves the rare split before it can diverge.
 *
 * The winner bumps the term to max(seen epoch)+1, assumes the Primary role,
 * repairs any locations that pointed at the dead Primary, and starts serving.
 * Losers keep replicating and simply start pulling from the new Primary.
 */

import type { NodeHost } from '../p2p/host.js'
import type { ClusterView } from '../p2p/announce.js'
import type { HealthDetector } from '../p2p/health.js'
import type { ReplicationService } from '../replication/service.js'
import type { ContentService } from '../content/service.js'
import type { NodeRole } from '../config.js'
import { log } from '../logger.js'

export interface ElectionOpts {
  role: NodeRole
  /** Let gossip lsn/health settle before computing the winner. */
  settleMs: number
  /** If no healthy Primary has emerged this long after an election, retry. */
  timeoutMs: number
}

interface Candidate {
  peerId: string
  lsn: number
  role: NodeRole
}

export class ElectionCoordinator {
  private electing = false

  constructor(
    private node: NodeHost,
    private cluster: ClusterView,
    private detector: HealthDetector,
    private replication: ReplicationService,
    private content: ContentService,
    private opts: ElectionOpts
  ) {}

  get selfId(): string {
    return this.node.peerId.toString()
  }

  /** Called when the Primary is detected DOWN. */
  onPrimaryDown(downPeer: string): void {
    void this.run(downPeer)
  }

  private async run(downPeer: string): Promise<void> {
    if (this.electing || this.replication.isPrimary) return
    this.electing = true
    try {
      // Let a couple of gossip cycles settle so lsn/health are current.
      await sleep(this.opts.settleMs)
      if (this.replication.isPrimary) return

      // Someone healthy may already have taken over during the settle window.
      const known = this.cluster.primaryPeer()?.toString()
      if (known && known !== downPeer && this.detector.isHealthy(known)) {
        log.info('election: a new Primary already exists', { primary: known.slice(-8) })
        return
      }

      const candidates = this.gatherCandidates(downPeer)
      const winner = this.pickWinner(candidates)
      const newEpoch = this.nextEpoch()
      log.info('election: computed winner', {
        winner: winner.peerId.slice(-8),
        self: this.selfId.slice(-8),
        epoch: newEpoch,
        candidates: candidates.length
      })

      if (winner.peerId === this.selfId) {
        this.replication.becomePrimary(newEpoch)
        // As the new Primary, repair locations that still point at the dead one.
        await this.replication.applyPromotion(downPeer, (id) => this.detector.isHealthy(id))
        this.content.wakeReconcile()
      }

      // Verify a healthy Primary emerges; otherwise (e.g. the winner also died) retry.
      setTimeout(() => this.verify(downPeer), this.opts.timeoutMs)
    } catch (err) {
      log.warn('election failed', { error: String(err) })
    } finally {
      this.electing = false
    }
  }

  private verify(downPeer: string): void {
    if (this.replication.isPrimary) return
    const known = this.cluster.primaryPeer()?.toString()
    if (!known || known === downPeer || !this.detector.isHealthy(known)) {
      log.warn('election: no healthy Primary yet — retrying')
      void this.run(downPeer)
    }
  }

  private gatherCandidates(downPeer: string): Candidate[] {
    const list: Candidate[] = [{ peerId: this.selfId, lsn: this.replication.currentLsn, role: this.opts.role }]
    for (const p of this.cluster.all()) {
      if (p.peerId === downPeer || p.peerId === this.selfId || !p.peer) continue
      if (!this.detector.isHealthy(p.peerId)) continue
      list.push({ peerId: p.peerId, lsn: p.lsn, role: p.role })
    }
    return list
  }

  /** Highest lsn, then anchor-preferred, then lowest peerId. */
  private pickWinner(cands: Candidate[]): Candidate {
    return [...cands].sort(
      (a, b) =>
        b.lsn - a.lsn ||
        rank(a.role) - rank(b.role) ||
        (a.peerId < b.peerId ? -1 : a.peerId > b.peerId ? 1 : 0)
    )[0]
  }

  private nextEpoch(): number {
    let max = this.replication.epoch
    for (const p of this.cluster.all()) max = Math.max(max, p.epoch ?? 0)
    return max + 1
  }
}

const rank = (role: NodeRole): number => (role === 'anchor' ? 0 : 1)
const sleep = (ms: number): Promise<void> => new Promise((r) => setTimeout(r, ms))
