/**
 * Node agent entrypoint.
 *
 * Phase 0: libp2p host + mDNS discovery + heartbeat.
 * Phase 1: metadata store + replication (primary push / replica pull) + admin API.
 *
 * Every PC runs this same process; behaviour is controlled purely by env vars
 * (see deploy/docker-compose.yml). Content, availability, rebalance, WebDAV,
 * MCP, and optional fixed-voter metadata consensus share this host.
 */

import { mkdirSync } from 'node:fs'
import { dirname } from 'node:path'
import { loadConfig } from './config.js'
import { log, setScope } from './logger.js'
import { createHost, type NodeHost } from './p2p/host.js'
import { loadOrCreateIdentity } from './p2p/identity.js'
import { MetadataStore } from './metadata/store.js'
import { MetaLog } from './metadata/log.js'
import { CheckpointService } from './metadata/checkpoint.js'
import { ClusterView, type AnnounceMsg } from './p2p/announce.js'
import { HealthDetector } from './p2p/health.js'
import { ReplicationService } from './replication/service.js'
import { ContentStore } from './content/store.js'
import { ContentTransfer } from './content/transfer.js'
import { ContentService } from './content/service.js'
import { AvailabilityService } from './availability/service.js'
import { ElectionCoordinator } from './availability/election.js'
import { RebalanceService } from './rebalance/service.js'
import { AdminApi } from './api/admin.js'
import { WebdavApi } from './api/webdav.js'
import { McpApi } from './api/mcp.js'
import { MetricsApi } from './api/metrics.js'
import { Counters } from './api/counters.js'
import { BasicAuth } from './auth/basic.js'

async function heartbeatOnce(node: NodeHost): Promise<void> {
  const conns = node.getConnections()
  if (conns.length === 0) {
    log.debug('heartbeat: no peers connected')
    return
  }
  const seen = new Set<string>()
  for (const conn of conns) {
    const peerId = conn.remotePeer
    const key = peerId.toString()
    if (seen.has(key)) continue
    seen.add(key)
    try {
      const rttMs = await node.services.ping.ping(peerId)
      log.debug('heartbeat ok', { peer: key.slice(-8), rttMs })
    } catch (err) {
      log.warn('heartbeat failed', { peer: key.slice(-8), error: String(err) })
    }
  }
}

async function main(): Promise<void> {
  const config = loadConfig()
  setScope(config.name)

  log.info('starting node agent', {
    role: config.role,
    isPrimary: config.isPrimary,
    replicationFactor: config.replicationFactor,
    storageQuotaGB: Math.round(config.storageQuotaBytes / 1024 / 1024 / 1024),
    bootstrapPeers: config.bootstrapPeers.length,
    mdns: config.mdnsEnabled,
    announce: config.announceMultiaddrs.length,
    dbPath: config.dbPath
  })

  // node:sqlite does not create parent directories — ensure they exist.
  mkdirSync(dirname(config.dbPath), { recursive: true })
  const store = new MetadataStore(config.dbPath)
  const metaLog = config.isPrimary || config.metadataVoters.length > 0 ? new MetaLog(store.db) : null
  log.info('metadata store ready', { lsn: store.lsn, sha: store.sha256().slice(0, 8) })

  const contentStore = new ContentStore(config.contentDir, config.storageKey)
  await contentStore.whenReady()

  const identity = await loadOrCreateIdentity(config.dataDir)
  const node = await createHost(config, identity)
  await node.start()

  log.info('libp2p host started', { peerId: node.peerId.toString() })
  for (const addr of node.getMultiaddrs()) {
    log.info('listening', { multiaddr: addr.toString() })
  }

  node.addEventListener('peer:discovery', (evt) => {
    const peer = evt.detail.id
    log.info('peer discovered', { peer: peer.toString().slice(-8) })
    // Discovered peers are not auto-dialed in libp2p 2.x — dial to form the mesh.
    if (node.getConnections(peer).length === 0) {
      node.dial(peer).catch((err) => {
        log.debug('dial failed', { peer: peer.toString().slice(-8), error: String(err) })
      })
    }
  })
  node.addEventListener('peer:connect', (evt) => {
    log.info('peer connected', { peer: evt.detail.toString().slice(-8) })
  })
  node.addEventListener('peer:disconnect', (evt) => {
    log.warn('peer disconnected', { peer: evt.detail.toString().slice(-8) })
  })

  // Soft cluster view (gossip): who is Primary, each peer's lsn + capacity.
  // isPrimary/epoch are read live from replication so an elected Primary is
  // advertised the moment it takes over (late-bound to break the ctor cycle).
  let replicationRef: ReplicationService | undefined
  const selfInfo = (): AnnounceMsg => ({
    peerId: node.peerId.toString(),
    name: config.name,
    role: config.role,
    isPrimary: replicationRef?.isPrimary ?? false,
    epoch: replicationRef?.epoch ?? 0,
    lsn: store.lsn,
    usedBytes: contentStore.usedBytes(),
    quotaBytes: config.storageQuotaBytes,
    tierRole: config.tierRole
  })
  const cluster = new ClusterView(node, selfInfo, config.maxContainers)

  // Phase 3: turn the gossip view into explicit up/suspect/down liveness.
  const detector = new HealthDetector(node, cluster, {
    intervalMs: config.healthDetectIntervalMs,
    suspectAfterMs: config.healthSuspectMs,
    downAfterMs: config.healthDownMs
  })

  const counters = new Counters()
  const replication = new ReplicationService(node, store, cluster, metaLog, {
    isPrimary: config.isPrimary,
    pushIntervalMs: config.pushIntervalMs,
    batchSize: config.batchSize,
    name: config.name,
    voters: config.metadataVoters,
    counters,
    tierEnabled: config.tierEnabled,
    tierRole: config.tierRole,
    tierScaleThreshold: config.tierScaleThreshold,
    relayRoster: config.relayRoster
  })
  replicationRef = replication

  const checkpointService = new CheckpointService(store, contentStore, metaLog, {
    intervalHours: config.checkpointIntervalHours,
    retentionDays: config.checkpointRetentionDays,
    deltaLogRetentionOps: config.deltaLogRetentionOps
  })
  if (config.isPrimary || config.role === 'anchor') {
    checkpointService.start()
  }

  const contentTransfer = new ContentTransfer(node, contentStore)
  const content = new ContentService(node, contentStore, contentTransfer, store, replication, cluster, {
    chunkSize: config.chunkSizeBytes,
    quotaBytes: config.storageQuotaBytes,
    replicationFactor: config.replicationFactor,
    warmCacheMaxBytes: Math.floor(config.storageQuotaBytes * config.rebalanceLow),
    reconcileIntervalMs: config.reconcileIntervalMs,
    reconcileBatch: config.reconcileBatch,
    isHealthy: (id) => detector.isHealthy(id),
    counters
  })

  // Phase 3: promotion (Primary) + reconciler wake, driven by health transitions.
  // Phase 7 (§8.4): + proactive passive→active rotation (ROTATION_INTERVAL_MS).
  const availability = new AvailabilityService(detector, replication, content, cluster, store, {
    intervalMs: config.rotationIntervalMs,
    batch: config.rotationBatch,
    counters
  })
  // Phase 3 M2: on Primary loss, run a deterministic election.
  if (!replication.quorumEnabled) {
    const election = new ElectionCoordinator(node, cluster, detector, replication, content, {
      role: config.role,
      settleMs: config.electionSettleMs,
      timeoutMs: config.electionTimeoutMs
    })
    availability.onPrimaryDown = (downPeer) => election.onPrimaryDown(downPeer)
  }

  // Phase 4: capacity offload with two-threshold hysteresis (runs on every node).
  const rebalance = new RebalanceService(node, content, contentTransfer, store, replication, {
    quotaBytes: config.storageQuotaBytes,
    high: config.rebalanceHigh,
    low: config.rebalanceLow,
    intervalMs: config.rebalanceEnabled ? config.rebalanceIntervalMs : 0,
    isHealthy: (id) => detector.isHealthy(id),
    counters
  })

  const admin = new AdminApi({
    port: config.adminPort,
    node,
    store,
    replication,
    cluster,
    content,
    detector,
    rebalance,
    name: config.name,
    role: config.role,
    isPrimary: config.isPrimary,
    auth: config.adminUsers ? new BasicAuth(config.adminUsers, config.adminRealm) : undefined
  })

  // Phase 7: Prometheus scrape endpoint (opt-in via METRICS_ENABLED).
  const metrics = config.metricsEnabled
    ? new MetricsApi({
        port: config.metricsPort,
        content,
        store,
        replication,
        detector,
        cluster,
        quotaBytes: config.storageQuotaBytes,
        counters
      })
    : null

  // Phase 5: mountable WebDAV surface + per-folder ACL enforcement (own port).
  const webdav = config.webdavEnabled
    ? new WebdavApi({
        port: config.webdavPort,
        realm: config.webdavRealm,
        allowAnon: config.webdavAllowAnon,
        store,
        content,
        replication,
        auth: new BasicAuth(config.webdavUsers, config.webdavRealm)
      })
    : null

  // Phase 6: MCP endpoint for AI agents (own port; same routing/ACL path).
  const mcp = config.mcpEnabled
    ? new McpApi({
        port: config.mcpPort,
        realm: config.mcpRealm,
        allowAnon: config.mcpAllowAnon,
        serverName: 'p2p-storage',
        serverVersion: '0.1.0',
        store,
        content,
        replication,
        auth: new BasicAuth(config.mcpUsers, config.mcpRealm)
      })
    : null

  cluster.start(config.heartbeatIntervalMs)
  replication.start()
  content.start()
  availability.start()
  rebalance.start()
  admin.start()
  webdav?.start()
  mcp?.start()
  metrics?.start()

  const timer = setInterval(() => {
    void heartbeatOnce(node)
  }, config.heartbeatIntervalMs)

  let shuttingDown = false
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) return
    shuttingDown = true
    log.info('shutting down', { signal })
    clearInterval(timer)
    admin.stop()
    checkpointService.stop()
    webdav?.stop()
    mcp?.stop()
    metrics?.stop()
    rebalance.stop()
    availability.stop()
    content.stop()
    cluster.stop()
    replication.stop()
    try {
      await node.stop()
    } catch (err) {
      log.error('error during shutdown', { error: String(err) })
    }
    try {
      store.close()
    } catch {
      /* ignore */
    }
    process.exit(0)
  }

  process.on('SIGINT', () => void shutdown('SIGINT'))
  process.on('SIGTERM', () => void shutdown('SIGTERM'))
}

main().catch((err) => {
  log.error('fatal error on startup', { error: String(err), stack: (err as Error)?.stack })
  process.exit(1)
})
