/**
 * Announce / gossip layer — a minimal precursor to the Phase 3 health service.
 *
 * Each node periodically sends its own status to every connected peer and records
 * theirs. This gives every node a soft "cluster view": who the Primary is (so
 * writes/pulls can be routed to it) and each peer's latest lsn (freshness).
 *
 * Deliberately NOT part of the metadata SHA-256 — this is per-node observation.
 */

import type { NodeHost } from './host.js'
import type { PeerId } from '@libp2p/interface'
import { PROTOCOLS, handle, rpc } from './protocols.js'
import { log } from '../logger.js'

export interface AnnounceMsg {
  peerId: string
  name: string
  role: 'anchor' | 'ordinary'
  isPrimary: boolean
  /** Primary term: bumped on each election so a stale ex-Primary can be out-ranked. */
  epoch: number
  lsn: number
  usedBytes: number
  quotaBytes: number
  /** Phase 8: Tiered role ('relay' | 'leaf' | 'auto'). */
  tierRole?: 'relay' | 'leaf' | 'auto'
}

export interface PeerInfo extends AnnounceMsg {
  lastSeen: number
  peer?: PeerId
}

export class ClusterView {
  private peers = new Map<string, PeerInfo>()
  private timer?: NodeJS.Timeout

  constructor(
    private node: NodeHost,
    private selfInfo: () => AnnounceMsg,
    public readonly maxContainers: number = 1000
  ) {}

  start(intervalMs: number): void {
    handle<AnnounceMsg, AnnounceMsg>(this.node, PROTOCOLS.announce, async (req, ctx) => {
      this.update(req, ctx.peer)
      return this.selfInfo() // gossip is bidirectional: reply with our own status
    })
    this.timer = setInterval(() => void this.broadcast(), intervalMs)
    // Announce promptly when a new peer connects (don't wait for the interval).
    this.node.addEventListener('peer:connect', () => void this.broadcast())
  }

  stop(): void {
    if (this.timer) clearInterval(this.timer)
  }

  private update(msg: AnnounceMsg, peer?: PeerId): void {
    const prev = this.peers.get(msg.peerId)
    if (!prev && this.peers.size >= this.maxContainers) {
      log.warn('cluster capacity reached, ignoring peer announcement', {
        peer: msg.peerId.slice(-8),
        name: msg.name,
        currentSize: this.peers.size,
        maxContainers: this.maxContainers
      })
      return
    }
    this.peers.set(msg.peerId, { ...msg, lastSeen: Date.now(), peer: peer ?? prev?.peer })
  }

  private async broadcast(): Promise<void> {
    const me = this.selfInfo()
    const seen = new Set<string>()
    for (const conn of this.node.getConnections()) {
      const id = conn.remotePeer.toString()
      if (seen.has(id)) continue
      seen.add(id)
      try {
        const reply = await rpc<AnnounceMsg, AnnounceMsg>(this.node, conn.remotePeer, PROTOCOLS.announce, me)
        this.update(reply, conn.remotePeer)
      } catch (err) {
        log.debug('announce failed', { peer: id.slice(-8), error: String(err) })
      }
    }
  }

  /** The PeerId object of the known Primary, if any peer advertises isPrimary. */
  primaryPeer(): PeerId | undefined {
    return this.bestPrimary()?.peer
  }

  /** Best current Primary claim among peers: highest epoch, then lowest peerId. */
  private bestPrimary(): PeerInfo | undefined {
    let best: PeerInfo | undefined
    for (const info of this.peers.values()) {
      if (!info.isPrimary || !info.peer) continue
      if (!best || info.epoch > best.epoch || (info.epoch === best.epoch && info.peerId < best.peerId)) {
        best = info
      }
    }
    return best
  }

  /**
   * A peer whose Primary claim out-ranks (epoch, peerId) — i.e. a running
   * Primary should step down in its favour. Used to resolve a split-brain.
   */
  betterPrimaryThan(epoch: number, peerId: string): PeerInfo | undefined {
    const best = this.bestPrimary()
    if (!best) return undefined
    const outranks = best.epoch > epoch || (best.epoch === epoch && best.peerId < peerId)
    return outranks ? best : undefined
  }

  get(peerId: string): PeerInfo | undefined {
    return this.peers.get(peerId)
  }

  all(): PeerInfo[] {
    return [...this.peers.values()]
  }

  get size(): number {
    return this.peers.size
  }

  /** Phase 8: Filter peers that act as Tier-1 Relays. */
  relayPeers(relayRoster: string[] = []): PeerInfo[] {
    return this.all().filter((p) => {
      if (relayRoster.length > 0) return relayRoster.includes(p.name)
      return p.tierRole === 'relay' || p.role === 'anchor'
    })
  }

  /** Phase 8: Filter peers that act as Tier-2 Leaves. */
  leafPeers(relayRoster: string[] = []): PeerInfo[] {
    const relays = new Set(this.relayPeers(relayRoster).map((p) => p.peerId))
    return this.all().filter((p) => !relays.has(p.peerId) && !p.isPrimary)
  }
}
