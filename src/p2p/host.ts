/**
 * libp2p host construction for the node agent.
 *
 * Phase 0 transport/discovery stack:
 *  - TCP transport
 *  - Noise encryption (authenticated, encrypted channels — kept even for trusted PCs)
 *  - Yamux stream multiplexing
 *  - mDNS peer discovery (zero-config on a LAN)
 *  - @libp2p/bootstrap discovery (static peers for cross-network / mesh-VPN, where
 *    mDNS multicast does not reach)
 *  - identify + ping services (ping powers the heartbeat health check)
 *
 * Later phases add: kad-dht discovery, circuit-relay (or rely on a mesh VPN),
 * and application protocols (metadata sync, content transfer).
 */

import { createLibp2p, type Libp2p } from 'libp2p'
import { tcp } from '@libp2p/tcp'
import { noise } from '@chainsafe/libp2p-noise'
import { yamux } from '@chainsafe/libp2p-yamux'
import { mdns } from '@libp2p/mdns'
import { bootstrap } from '@libp2p/bootstrap'
import { identify, type Identify } from '@libp2p/identify'
import { ping, type PingService } from '@libp2p/ping'
import type { PrivateKey } from '@libp2p/interface'
import type { NodeConfig } from '../config.js'

export interface NodeServices {
  identify: Identify
  ping: PingService
  [key: string]: unknown
}

export type NodeHost = Libp2p<NodeServices>

export async function createHost(config: NodeConfig, privateKey: PrivateKey): Promise<NodeHost> {
  // Discovery: mDNS for zero-config LAN, @libp2p/bootstrap for cross-network /
  // mesh-VPN peers (mDNS multicast does not cross a VPN). Either or both may be
  // active; both feed the same `peer:discovery` -> dial handler in index.ts.
  const peerDiscovery: Array<ReturnType<typeof mdns> | ReturnType<typeof bootstrap>> = []
  if (config.mdnsEnabled) {
    // Advertise/scan frequently so tests form a cluster quickly.
    peerDiscovery.push(mdns({ interval: 2000 }))
  }
  if (config.bootstrapPeers.length > 0) {
    peerDiscovery.push(bootstrap({ list: config.bootstrapPeers }))
  }

  // Announce override: on a mesh VPN we advertise only the overlay multiaddr so
  // peers dial the reachable 100.x address instead of an unrelated local NIC.
  const addresses: { listen: string[]; announce?: string[] } = {
    listen: [`/ip4/0.0.0.0/tcp/${config.listenPort}`]
  }
  if (config.announceMultiaddrs.length > 0) {
    addresses.announce = config.announceMultiaddrs
  }

  const node = await createLibp2p({
    privateKey,
    addresses,
    transports: [tcp()],
    connectionEncrypters: [noise()],
    streamMuxers: [yamux()],
    peerDiscovery,
    services: {
      identify: identify(),
      ping: ping()
    }
  })

  return node as NodeHost
}
