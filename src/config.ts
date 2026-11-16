/**
 * Node configuration, resolved from environment variables.
 *
 * Every participating PC runs the same image; a node's behaviour is controlled
 * entirely through these env vars (see deploy/docker-compose.yml).
 */

export type NodeRole = 'anchor' | 'ordinary'

export interface NodeConfig {
  /** Human-friendly name for logs (defaults to hostname). */
  name: string
  /** Role: `anchor` nodes are the always-available seed/primary/snapshot hosts. */
  role: NodeRole
  /** libp2p TCP listen port. 0 = random free port (fine for LAN/mDNS testing). */
  listenPort: number
  /** Bootstrap peer multiaddrs (used in internet/VPN mode; empty for pure LAN/mDNS). */
  bootstrapPeers: string[]
  /** Multiaddrs to announce instead of auto-detected NIC addrs (e.g. the mesh-VPN overlay IP). Empty = auto-detect. */
  announceMultiaddrs: string[]
  /** Enable mDNS LAN discovery. Disable to force bootstrap-only discovery (mesh-VPN / networking tests). */
  mdnsEnabled: boolean
  /** Content store soft quota in bytes (default ~20 GB). */
  storageQuotaBytes: number
  /** Desired replication factor (copies per drive). */
  replicationFactor: number
  /** Heartbeat interval in milliseconds. */
  heartbeatIntervalMs: number
  /** Health detector sweep interval (ms). */
  healthDetectIntervalMs: number
  /** Peer stale beyond this => suspect (ms). */
  healthSuspectMs: number
  /** Peer stale beyond this (ping failed) => down (ms). */
  healthDownMs: number
  /** Reconciler sweep interval (ms); 0 disables it. */
  reconcileIntervalMs: number
  /** Max chunks repaired per reconcile sweep. */
  reconcileBatch: number
  /** Election: settle delay before computing a winner (ms). */
  electionSettleMs: number
  /** Election: retry if no healthy Primary emerges within this window (ms). */
  electionTimeoutMs: number
  /** Rebalance: enable the capacity-driven offload sweep. */
  rebalanceEnabled: boolean
  /** Rebalance: start offloading above this fraction of quota (T1). */
  rebalanceHigh: number
  /** Rebalance: stop offloading below this fraction of quota (T2). */
  rebalanceLow: number
  /** Rebalance sweep interval (ms); 0 disables it. */
  rebalanceIntervalMs: number
  /** Rotation (§8.4): proactive passive→active swap sweep interval (ms); 0 disables (default). */
  rotationIntervalMs: number
  /** Rotation: max chunks swapped per sweep. */
  rotationBatch: number
  /** Whether this node is the metadata Primary (single writer). Election is Phase 3. */
  isPrimary: boolean
  /** Fixed metadata voting roster. Empty explicitly selects legacy, non-quorum mode. */
  metadataVoters: string[]
  /** Primary push sweep interval in milliseconds. */
  pushIntervalMs: number
  /** Max ops shipped per push/pull batch. */
  batchSize: number
  /** Admin/debug HTTP API port. */
  adminPort: number
  /** Admin: raw user table gating the admin API `user:pass:group1|group2,other:pass`. Empty = open (dev mode). */
  adminUsers: string
  /** Admin: HTTP Basic auth realm for the admin API. */
  adminRealm: string
  /** WebDAV: enable the mountable WebDAV endpoint. */
  webdavEnabled: boolean
  /** WebDAV: dedicated HTTP port for the WebDAV adapter. */
  webdavPort: number
  /** WebDAV: HTTP Basic auth realm shown to clients. */
  webdavRealm: string
  /** WebDAV: raw user table `user:pass:group1|group2,other:pass`. Parsed by auth/basic.ts. */
  webdavUsers: string
  /** WebDAV: allow unauthenticated access when the target ACL grants `*`. */
  webdavAllowAnon: boolean
  /** MCP: enable the AI-agent MCP endpoint. */
  mcpEnabled: boolean
  /** MCP: dedicated HTTP port for the MCP JSON-RPC adapter. */
  mcpPort: number
  /** MCP: HTTP Basic auth realm shown to clients. */
  mcpRealm: string
  /** MCP: allow unauthenticated access when the target ACL grants `*`. */
  mcpAllowAnon: boolean
  /** MCP: raw user table (defaults to WEBDAV_USERS — same cluster identities). */
  mcpUsers: string
  /** Root data directory (metadata db + content chunks). */
  dataDir: string
  /** Resolved SQLite metadata db path (dataDir/meta/meta.db). */
  dbPath: string
  /** Content chunk store directory (dataDir/content). */
  contentDir: string
  /** Fixed chunk size in bytes (env-overridable for tests). */
  chunkSizeBytes: number
  /** AES-256-GCM encryption key for chunks at rest (null = unencrypted). */
  storageKey: Buffer | null
  /** Expose a Prometheus /metrics endpoint (opt-in). */
  metricsEnabled: boolean
  /** HTTP port for the Prometheus metrics endpoint. */
  metricsPort: number
}

function num(value: string | undefined, fallback: number): number {
  if (value === undefined || value.trim() === '') return fallback
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function bool(value: string | undefined, fallback = false): boolean {
  if (value === undefined || value.trim() === '') return fallback
  return /^(1|true|yes|on)$/i.test(value.trim())
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): NodeConfig {
  const role: NodeRole = env.NODE_ROLE === 'anchor' ? 'anchor' : 'ordinary'
  const dataDir = env.DATA_DIR?.trim() || './data'
  const heartbeatIntervalMs = num(env.HEARTBEAT_INTERVAL_MS, 5000)
  const name = env.NODE_NAME?.trim() || env.HOSTNAME?.trim() || 'node'
  const metadataVoters = (env.METADATA_VOTERS ?? '').split(',').map((v) => v.trim()).filter(Boolean)
  if (metadataVoters.length > 0 &&
      (metadataVoters.length !== 3 || new Set(metadataVoters).size !== 3 || !metadataVoters.includes(name))) {
    throw new Error('METADATA_VOTERS must list exactly three distinct NODE_NAME values, including this node')
  }
  if (metadataVoters.length > 0 && num(env.REPLICATION_FACTOR, 2) !== 2) {
    throw new Error('Quorum mode requires REPLICATION_FACTOR=2')
  }
  return {
    name,
    metadataVoters,
    role,
    listenPort: num(env.LISTEN_PORT, 0),
    bootstrapPeers: (env.BOOTSTRAP_PEERS ?? '')
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0),
    announceMultiaddrs: (env.ANNOUNCE_MULTIADDRS ?? '')
      .split(',')
      .map((s) => s.trim())
      .filter((s) => s.length > 0),
    mdnsEnabled: bool(env.MDNS_ENABLED, true),
    storageQuotaBytes:
      env.STORAGE_QUOTA_BYTES && env.STORAGE_QUOTA_BYTES.trim() !== ''
        ? num(env.STORAGE_QUOTA_BYTES, 20 * 1024 * 1024 * 1024)
        : num(env.STORAGE_QUOTA_GB, 20) * 1024 * 1024 * 1024,
    replicationFactor: num(env.REPLICATION_FACTOR, 2),
    heartbeatIntervalMs,
    // Defaults scale with the heartbeat: suspect ~1.5 beats, down ~3 beats
    // (so the prod 5s heartbeat gives the design's ~7.5s/15s thresholds).
    healthDetectIntervalMs: num(env.HEALTH_DETECT_INTERVAL_MS, heartbeatIntervalMs),
    healthSuspectMs: num(env.HEALTH_SUSPECT_MS, Math.round(heartbeatIntervalMs * 1.5)),
    healthDownMs: num(env.HEALTH_DOWN_MS, heartbeatIntervalMs * 3),
    reconcileIntervalMs: num(env.RECONCILE_INTERVAL_MS, heartbeatIntervalMs),
    reconcileBatch: num(env.RECONCILE_BATCH, 32),
    electionSettleMs: num(env.ELECTION_SETTLE_MS, heartbeatIntervalMs),
    electionTimeoutMs: num(env.ELECTION_TIMEOUT_MS, heartbeatIntervalMs * 6),
    rebalanceEnabled: bool(env.REBALANCE_ENABLED, true),
    rebalanceHigh: num(env.REBALANCE_HIGH, 0.85),
    rebalanceLow: num(env.REBALANCE_LOW, 0.7),
    rebalanceIntervalMs: num(env.REBALANCE_INTERVAL_MS, heartbeatIntervalMs * 2),
    rotationIntervalMs: num(env.ROTATION_INTERVAL_MS, 0),
    rotationBatch: num(env.ROTATION_BATCH, 8),
    isPrimary: bool(env.IS_PRIMARY, false),
    pushIntervalMs: num(env.PUSH_INTERVAL_MS, 1000),
    batchSize: num(env.BATCH_SIZE, 500),
    adminPort: num(env.ADMIN_PORT, 8080),
    adminUsers: env.ADMIN_USERS ?? '',
    adminRealm: env.ADMIN_REALM?.trim() || 'p2p-admin',
    webdavEnabled: bool(env.WEBDAV_ENABLED, false),
    webdavPort: num(env.WEBDAV_PORT, 8090),
    webdavRealm: env.WEBDAV_REALM?.trim() || 'p2p-drive',
    webdavUsers: env.WEBDAV_USERS ?? '',
    webdavAllowAnon: bool(env.WEBDAV_ALLOW_ANON, true),
    mcpEnabled: bool(env.MCP_ENABLED, false),
    mcpPort: num(env.MCP_PORT, 8100),
    mcpRealm: env.MCP_REALM?.trim() || env.WEBDAV_REALM?.trim() || 'p2p-drive',
    mcpAllowAnon: bool(env.MCP_ALLOW_ANON, bool(env.WEBDAV_ALLOW_ANON, true)),
    mcpUsers: env.MCP_USERS ?? env.WEBDAV_USERS ?? '',
    dataDir,
    dbPath: `${dataDir.replace(/[/\\]+$/, '')}/meta/meta.db`,
    contentDir: `${dataDir.replace(/[/\\]+$/, '')}/content`,
    chunkSizeBytes: num(env.CHUNK_SIZE_BYTES, 4 * 1024 * 1024),
    storageKey: (() => {
      const raw = env.STORAGE_KEY?.trim()
      if (!raw) return null
      if (!/^[0-9a-fA-F]{64}$/.test(raw)) {
        throw new Error('STORAGE_KEY must be exactly 64 hex characters (32 bytes)')
      }
      return Buffer.from(raw, 'hex')
    })(),
    metricsEnabled: bool(env.METRICS_ENABLED, false),
    metricsPort: num(env.METRICS_PORT, 9090)
  }
}
