/**
 * Health detector (DETAILED_DESIGN §8.1) — turns the soft gossip cluster-view
 * into explicit per-peer liveness so availability logic can react to failures.
 *
 * Signal: ClusterView records `lastSeen` for every peer on each gossip exchange.
 * A peer that stops gossiping goes stale. We classify staleness into:
 *   up      — seen within `suspectAfterMs`
 *   suspect — stale past `suspectAfterMs`; we actively ping to confirm liveness
 *   down    — stale past `downAfterMs` (and a confirming ping, if attempted, failed)
 * A peer that resumes gossiping (or answers a ping) transitions back to `up`.
 *
 * Emits onDown/onUp so the Primary can promote replicas off a dead node and
 * every node can wake its reconciler. Purely local observation — never part of
 * the metadata SHA-256.
 */

import type { ClusterView } from './announce.js'
import type { NodeHost } from './host.js'
import { log } from '../logger.js'

export type Health = 'up' | 'suspect' | 'down'

export interface HealthOpts {
  /** Detection sweep interval. */
  intervalMs: number
  /** Stale beyond this => suspect (start confirming with pings). */
  suspectAfterMs: number
  /** Stale beyond this (and ping failed) => down. */
  downAfterMs: number
  /** Cap each confirming ping so a dead peer can't stall the sweep (default 2s). */
  pingTimeoutMs?: number
}

type Listener = (peerId: string) => void

interface Entry {
  state: Health
  pingOkAt: number
}

export class HealthDetector {
  private entries = new Map<string, Entry>()
  private downCbs: Listener[] = []
  private upCbs: Listener[] = []
  private timer?: NodeJS.Timeout
  private sweeping = false

  constructor(
    private node: NodeHost,
    private cluster: ClusterView,
    private opts: HealthOpts
  ) {}

  onDown(cb: Listener): void {
    this.downCbs.push(cb)
  }

  onUp(cb: Listener): void {
    this.upCbs.push(cb)
  }

  start(): void {
    this.timer = setInterval(() => void this.sweep(), this.opts.intervalMs)
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer)
  }

  state(peerId: string): Health {
    return this.entries.get(peerId)?.state ?? 'up'
  }

  /** Placement/routing guard: avoid peers we believe are down (unknown = healthy). */
  isHealthy(peerId: string): boolean {
    return this.state(peerId) !== 'down'
  }

  /** Peers currently classified as down. */
  downPeers(): string[] {
    return [...this.entries.entries()].filter(([, e]) => e.state === 'down').map(([id]) => id)
  }

  /** Snapshot for observability (/admin/health). */
  snapshot(): Array<{ peerId: string; state: Health; ageMs: number }> {
    const now = Date.now()
    return this.cluster.all().map((p) => {
      const seen = Math.max(p.lastSeen, this.entries.get(p.peerId)?.pingOkAt ?? 0)
      return { peerId: p.peerId, state: this.state(p.peerId), ageMs: now - seen }
    })
  }

  private async sweep(): Promise<void> {
    if (this.sweeping) return
    this.sweeping = true
    try {
      const now = Date.now()
      for (const p of this.cluster.all()) {
        const entry = this.entries.get(p.peerId) ?? { state: 'up' as Health, pingOkAt: 0 }
        const seen = Math.max(p.lastSeen, entry.pingOkAt)
        const age = now - seen

        let next: Health
        if (age <= this.opts.suspectAfterMs) {
          next = 'up'
        } else if (age <= this.opts.downAfterMs) {
          next = 'suspect'
          // Actively confirm liveness — a successful ping refreshes the clock and
          // keeps a merely gossip-quiet-but-alive peer from being declared down.
          if (await this.ping(p.peerId, p.peer)) {
            entry.pingOkAt = Date.now()
            next = 'up'
          }
        } else {
          // Past the down threshold: one last ping so a transient gossip stall
          // doesn't trip a false positive.
          if (await this.ping(p.peerId, p.peer)) {
            entry.pingOkAt = Date.now()
            next = 'up'
          } else {
            next = 'down'
          }
        }

        this.transition(p.peerId, entry, next)
      }
    } finally {
      this.sweeping = false
    }
  }

  private transition(peerId: string, entry: Entry, next: Health): void {
    const prev = entry.state
    if (prev === next) {
      this.entries.set(peerId, entry)
      return
    }
    entry.state = next
    this.entries.set(peerId, entry)
    if (next === 'down') {
      log.warn('health: peer DOWN', { peer: peerId.slice(-8), was: prev })
      this.emit(this.downCbs, peerId)
    } else if (next === 'up' && prev === 'down') {
      log.info('health: peer UP', { peer: peerId.slice(-8) })
      this.emit(this.upCbs, peerId)
    } else if (next === 'suspect') {
      log.debug('health: peer suspect', { peer: peerId.slice(-8) })
    }
  }

  private emit(cbs: Listener[], peerId: string): void {
    for (const cb of cbs) {
      try {
        cb(peerId)
      } catch (err) {
        log.warn('health listener threw', { error: String(err) })
      }
    }
  }

  private async ping(peerId: string, peer?: import('@libp2p/interface').PeerId): Promise<boolean> {
    if (!peer) return false
    try {
      await this.node.services.ping.ping(peer, { signal: AbortSignal.timeout(this.opts.pingTimeoutMs ?? 2000) })
      return true
    } catch {
      log.debug('health ping failed', { peer: peerId.slice(-8) })
      return false
    }
  }
}
