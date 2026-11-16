/**
 * Node configuration, resolved from a configuration file and/or environment variables.
 *
 * Every participating PC runs the same image; a node's behaviour is controlled
 * through these settings (via config.json or environment variables).
 */

import { existsSync, readFileSync } from 'node:fs'
import { resolve } from 'node:path'

export type NodeRole = 'anchor' | 'ordinary'
export type TierRole = 'relay' | 'leaf' | 'auto'

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

  // ---- Phase 8: Tiered Scaling & Checkpointing -----------------------------
  /** Maximum number of containers/nodes supported in the cluster (guards registration/gossip). */
  maxContainers: number
  /** Enable tiered fanout dissemination (relays fan out to leaf nodes). */
  tierEnabled: boolean
  /** Tier role: 'relay' receives from Primary and pushes to leaves; 'leaf' receives from relays; 'auto' assigns based on role. */
  tierRole: TierRole
  /** Node threshold where tiered mode auto-activates if tierRole is auto. */
  tierScaleThreshold: number
  /** Explicit roster of relay node names. If empty, anchors or designated nodes act as relays. */
  relayRoster: string[]
  /** Checkpoint generation interval in hours (default 24). */
  checkpointIntervalHours: number
  /** Days to retain historical content-addressed checkpoints (default 7). */
  checkpointRetentionDays: number
  /** Number of transaction operations retained in meta_log after checkpoint (default 10,000). */
  deltaLogRetentionOps: number
}

function num(value: unknown, fallback: number): number {
  if (value === undefined || value === null || String(value).trim() === '') return fallback
  const n = Number(value)
  return Number.isFinite(n) ? n : fallback
}

function bool(value: unknown, fallback = false): boolean {
  if (value === undefined || value === null || String(value).trim() === '') return fallback
  return /^(1|true|yes|on)$/i.test(String(value).trim())
}

function str(value: unknown, fallback = ''): string {
  if (value === undefined || value === null) return fallback
  return String(value).trim()
}

function strArray(value: unknown, fallback: string[] = []): string[] {
  if (Array.isArray(value)) return value.map((s) => String(s).trim()).filter(Boolean)
  if (typeof value === 'string') {
    return value.split(',').map((s) => s.trim()).filter(Boolean)
  }
  return fallback
}

export function loadConfigFile(filePath?: string): Record<string, unknown> {
  const target = filePath || (existsSync('config.json') ? 'config.json' : undefined)
  if (!target) return {}
  try {
    const fullPath = resolve(target)
    if (!existsSync(fullPath)) return {}
    const raw = readFileSync(fullPath, 'utf8')
    return JSON.parse(raw) as Record<string, unknown>
  } catch (err) {
    throw new Error(`Failed to parse configuration file "${target}": ${String(err)}`)
  }
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env, configFilePath?: string): NodeConfig {
  const file = loadConfigFile(configFilePath || env.CONFIG_FILE)

  const getVal = (camel: string, upper: string): unknown => {
    if (env[upper] !== undefined && env[upper] !== '') return env[upper]
    if (file[camel] !== undefined) return file[camel]
    if (file[upper] !== undefined) return file[upper]
    return undefined
  }

  const roleRaw = str(getVal('role', 'NODE_ROLE'), 'ordinary')
  const role: NodeRole = roleRaw === 'anchor' ? 'anchor' : 'ordinary'
  const dataDir = str(getVal('dataDir', 'DATA_DIR'), './data')
  const heartbeatIntervalMs = num(getVal('heartbeatIntervalMs', 'HEARTBEAT_INTERVAL_MS'), 5000)
  const name = str(getVal('name', 'NODE_NAME'), env.HOSTNAME?.trim() || 'node')

  const metadataVoters = strArray(getVal('metadataVoters', 'METADATA_VOTERS'), [])
  if (metadataVoters.length > 0 &&
      (metadataVoters.length !== 3 || new Set(metadataVoters).size !== 3 || !metadataVoters.includes(name))) {
    throw new Error('METADATA_VOTERS must list exactly three distinct NODE_NAME values, including this node')
  }
  const replicationFactor = num(getVal('replicationFactor', 'REPLICATION_FACTOR'), 2)
  if (metadataVoters.length > 0 && replicationFactor !== 2) {
    throw new Error('Quorum mode requires REPLICATION_FACTOR=2')
  }

  // Quota handling: check storageQuotaBytes or storageQuotaGb
  let quotaBytes = 20 * 1024 * 1024 * 1024
  const qBytesVal = getVal('storageQuotaBytes', 'STORAGE_QUOTA_BYTES')
  const qGbVal = getVal('storageQuotaGb', 'STORAGE_QUOTA_GB')
  if (qBytesVal !== undefined && String(qBytesVal).trim() !== '') {
    quotaBytes = num(qBytesVal, quotaBytes)
  } else if (qGbVal !== undefined && String(qGbVal).trim() !== '') {
    quotaBytes = num(qGbVal, 20) * 1024 * 1024 * 1024
  }

  const tierRoleRaw = str(getVal('tierRole', 'TIER_ROLE'), 'auto')
  const tierRole: TierRole = tierRoleRaw === 'relay' || tierRoleRaw === 'leaf' ? tierRoleRaw : 'auto'

  return {
    name,
    metadataVoters,
    role,
    listenPort: num(getVal('listenPort', 'LISTEN_PORT'), 0),
    bootstrapPeers: strArray(getVal('bootstrapPeers', 'BOOTSTRAP_PEERS'), []),
    announceMultiaddrs: strArray(getVal('announceMultiaddrs', 'ANNOUNCE_MULTIADDRS'), []),
    mdnsEnabled: bool(getVal('mdnsEnabled', 'MDNS_ENABLED'), true),
    storageQuotaBytes: quotaBytes,
    replicationFactor,
    heartbeatIntervalMs,
    healthDetectIntervalMs: num(getVal('healthDetectIntervalMs', 'HEALTH_DETECT_INTERVAL_MS'), heartbeatIntervalMs),
    healthSuspectMs: num(getVal('healthSuspectMs', 'HEALTH_SUSPECT_MS'), Math.round(heartbeatIntervalMs * 1.5)),
    healthDownMs: num(getVal('healthDownMs', 'HEALTH_DOWN_MS'), heartbeatIntervalMs * 3),
    reconcileIntervalMs: num(getVal('reconcileIntervalMs', 'RECONCILE_INTERVAL_MS'), heartbeatIntervalMs),
    reconcileBatch: num(getVal('reconcileBatch', 'RECONCILE_BATCH'), 32),
    electionSettleMs: num(getVal('electionSettleMs', 'ELECTION_SETTLE_MS'), heartbeatIntervalMs),
    electionTimeoutMs: num(getVal('electionTimeoutMs', 'ELECTION_TIMEOUT_MS'), heartbeatIntervalMs * 6),
    rebalanceEnabled: bool(getVal('rebalanceEnabled', 'REBALANCE_ENABLED'), true),
    rebalanceHigh: num(getVal('rebalanceHigh', 'REBALANCE_HIGH'), 0.85),
    rebalanceLow: num(getVal('rebalanceLow', 'REBALANCE_LOW'), 0.7),
    rebalanceIntervalMs: num(getVal('rebalanceIntervalMs', 'REBALANCE_INTERVAL_MS'), heartbeatIntervalMs * 2),
    rotationIntervalMs: num(getVal('rotationIntervalMs', 'ROTATION_INTERVAL_MS'), 0),
    rotationBatch: num(getVal('rotationBatch', 'ROTATION_BATCH'), 8),
    isPrimary: bool(getVal('isPrimary', 'IS_PRIMARY'), false),
    pushIntervalMs: num(getVal('pushIntervalMs', 'PUSH_INTERVAL_MS'), 1000),
    batchSize: num(getVal('batchSize', 'BATCH_SIZE'), 500),
    adminPort: num(getVal('adminPort', 'ADMIN_PORT'), 8080),
    adminUsers: str(getVal('adminUsers', 'ADMIN_USERS'), ''),
    adminRealm: str(getVal('adminRealm', 'ADMIN_REALM'), 'p2p-admin'),
    webdavEnabled: bool(getVal('webdavEnabled', 'WEBDAV_ENABLED'), false),
    webdavPort: num(getVal('webdavPort', 'WEBDAV_PORT'), 8090),
    webdavRealm: str(getVal('webdavRealm', 'WEBDAV_REALM'), 'p2p-drive'),
    webdavUsers: str(getVal('webdavUsers', 'WEBDAV_USERS'), ''),
    webdavAllowAnon: bool(getVal('webdavAllowAnon', 'WEBDAV_ALLOW_ANON'), true),
    mcpEnabled: bool(getVal('mcpEnabled', 'MCP_ENABLED'), false),
    mcpPort: num(getVal('mcpPort', 'MCP_PORT'), 8100),
    mcpRealm: str(getVal('mcpRealm', 'MCP_REALM'), 'p2p-drive'),
    mcpAllowAnon: bool(getVal('mcpAllowAnon', 'MCP_ALLOW_ANON'), true),
    mcpUsers: str(getVal('mcpUsers', 'MCP_USERS'), str(getVal('webdavUsers', 'WEBDAV_USERS'), '')),
    dataDir,
    dbPath: `${dataDir.replace(/[/\\]+$/, '')}/meta/meta.db`,
    contentDir: `${dataDir.replace(/[/\\]+$/, '')}/content`,
    chunkSizeBytes: num(getVal('chunkSizeBytes', 'CHUNK_SIZE_BYTES'), 4 * 1024 * 1024),
    storageKey: (() => {
      const raw = str(getVal('storageKey', 'STORAGE_KEY'), '')
      if (!raw) return null
      if (!/^[0-9a-fA-F]{64}$/.test(raw)) {
        throw new Error('STORAGE_KEY must be exactly 64 hex characters (32 bytes)')
      }
      return Buffer.from(raw, 'hex')
    })(),
    metricsEnabled: bool(getVal('metricsEnabled', 'METRICS_ENABLED'), false),
    metricsPort: num(getVal('metricsPort', 'METRICS_PORT'), 9090),

    // Phase 8:
    maxContainers: num(getVal('maxContainers', 'MAX_CONTAINERS'), 1000),
    tierEnabled: bool(getVal('tierEnabled', 'TIER_ENABLED'), false),
    tierRole,
    tierScaleThreshold: num(getVal('tierScaleThreshold', 'TIER_SCALE_THRESHOLD'), 100),
    relayRoster: strArray(getVal('relayRoster', 'RELAY_ROSTER'), []),
    checkpointIntervalHours: num(getVal('checkpointIntervalHours', 'CHECKPOINT_INTERVAL_HOURS'), 24),
    checkpointRetentionDays: num(getVal('checkpointRetentionDays', 'CHECKPOINT_RETENTION_DAYS'), 7),
    deltaLogRetentionOps: num(getVal('deltaLogRetentionOps', 'DELTA_LOG_RETENTION_OPS'), 10000)
  }
}
