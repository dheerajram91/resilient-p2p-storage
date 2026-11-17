/**
 * Admin / debug HTTP API on :8080 — the seed of the eventual WebDAV/web-UI surface.
 *
 * Endpoints (JSON unless noted):
 *   GET  /admin/status   -> { peerId, name, role, isPrimary, mode, lsn, sha256, peers, dialAddrs }
 *   GET  /admin/tree     -> recursive folder/file tree
 *   POST /admin/mkcol    { path }  -> create folder path (routes to Primary)
 *   POST /admin/rmdir    { path }  -> delete empty folder (routes to Primary)
 *   POST /admin/acl      { path, rules[] } -> set effective ACL on a folder/file (Phase 5)
 *   POST /admin/corrupt  -> TEST ONLY: diverge local metadata to exercise fallback
 *   PUT  /admin/file?path=/a/b   (raw body = bytes)  -> chunk + store + replicate
 *   GET  /admin/file?path=/a/b   -> raw file bytes (read-repairs on the fly)
 *   GET  /admin/chunks   -> { peerId, count, hashes[] } of locally-held chunks
 *   GET  /admin/locations-> chunk location map (active/backup/state)
 *   GET  /admin/capacity -> per-node used/quota + cluster free + RF headroom (Phase 4)
 *   GET  /admin/health   -> per-peer up/suspect/down liveness (Phase 3)
 *   POST /admin/reconcile-> force a reconcile sweep (Phase 3 test hook)
 *   POST /admin/rebalance-> force a capacity offload sweep (Phase 4 test hook)
 *   POST /admin/corrupt-chunk?hash=... -> TEST ONLY: corrupt a local chunk on disk
 */

import http from 'node:http'
import type { NodeHost } from '../p2p/host.js'
import type { MetadataStore } from '../metadata/store.js'
import type { ReplicationService } from '../replication/service.js'
import type { ClusterView } from '../p2p/announce.js'
import type { HealthDetector } from '../p2p/health.js'
import type { ContentService } from '../content/service.js'
import type { RebalanceService } from '../rebalance/service.js'
import type { AclRule } from '../metadata/types.js'
import { canAccess, type Principal } from '../acl/acl.js'
import type { BasicAuth } from '../auth/basic.js'
import { log } from '../logger.js'

export interface AdminDeps {
  port: number
  node: NodeHost
  store: MetadataStore
  replication: ReplicationService
  cluster: ClusterView
  content: ContentService
  detector: HealthDetector
  rebalance: RebalanceService
  name: string
  role: string
  isPrimary: boolean
  /** Optional BasicAuth (ADMIN_USERS). When set, EVERY admin endpoint requires valid credentials and
   *  path-scoped operations additionally require the 'admin' right. When unset the API is open (dev mode). */
  auth?: BasicAuth
}

export class AdminApi {
  private server?: http.Server

  constructor(private deps: AdminDeps) {}

  start(): void {
    this.server = http.createServer((req, res) => this.handle(req, res))
    this.server.requestTimeout = 0
    this.server.headersTimeout = 0
    this.server.keepAliveTimeout = 60000
    this.server.setTimeout(0)
    this.server.listen(this.deps.port, () => {
      log.info('admin api listening', { port: this.deps.port })
      if (!this.deps.auth) log.warn('admin api is UNAUTHENTICATED — set ADMIN_USERS to require Basic auth')
    })
  }

  stop(): void {
    this.server?.close()
  }

  private status(): object {
    const d = this.deps
    return {
      peerId: d.node.peerId.toString(),
      name: d.name,
      role: d.role,
      isPrimary: d.replication.isPrimary,
      quorum: d.replication.quorumStatus(),
      mode: d.store.mode,
      lsn: d.store.lsn,
      sha256: d.store.sha256(),
      connections: d.node.getConnections().length,
      // Full announced multiaddrs (incl. /p2p/<peerId>) — copy one into another
      // node's BOOTSTRAP_PEERS to dial this node across a network / mesh VPN.
      dialAddrs: d.node.getMultiaddrs().map((m) => m.toString()),
      usedBytes: d.content.usedBytes(),
      maxContainers: d.cluster.maxContainers,
      isRelay: d.replication.isRelay,
      peers: d.cluster.all().map((p) => ({
        name: p.name,
        peerId: p.peerId.slice(-8),
        isPrimary: p.isPrimary,
        lsn: p.lsn,
        usedBytes: p.usedBytes,
        quotaBytes: p.quotaBytes
      }))
    }
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const url = new URL(req.url ?? '/', 'http://localhost')
    const send = (code: number, body: unknown): void => {
      const json = JSON.stringify(body)
      res.writeHead(code, { 'content-type': 'application/json' })
      res.end(json)
    }
    try {
      // Phase 7: with ADMIN_USERS configured, every endpoint requires valid Basic
      // credentials; path-scoped operations additionally require the 'admin' right
      // (requireAdminRight below). Without ADMIN_USERS the API stays open (dev mode).
      let principal: Principal | null = null
      if (this.deps.auth) {
        const header = req.headers['authorization']
        const p = this.deps.auth.authenticate(typeof header === 'string' ? header : undefined)
        if (p === null || p.anonymous) {
          res.writeHead(401, {
            'content-type': 'application/json',
            'www-authenticate': this.deps.auth.challenge()
          })
          res.end(JSON.stringify({ error: 'authentication required' }))
          return
        }
        principal = p
      }
      const requireAdminRight = (path: string): boolean =>
        !this.deps.auth || canAccess(this.deps.store, path, principal as Principal, 'admin')

      if (req.method === 'GET' && url.pathname === '/admin/status') return send(200, this.status())
      if (req.method === 'GET' && url.pathname === '/admin/tree') return send(200, this.deps.store.tree())

      if (req.method === 'POST' && url.pathname === '/admin/mkcol') {
        const body = await readJson(req)
        const path = String((body as { path?: string }).path ?? '')
        if (!path) return send(400, { error: 'path required' })
        if (!requireAdminRight(path)) return send(403, { error: 'forbidden: admin right required' })
        const result = await this.deps.replication.mkpath(path)
        return send('error' in result ? (result.code === 'EUNAVAILABLE' ? 503 : 409) : 200, result)
      }

      if (req.method === 'POST' && url.pathname === '/admin/rmdir') {
        const body = await readJson(req)
        const path = String((body as { path?: string }).path ?? '')
        if (!path) return send(400, { error: 'path required' })
        if (!requireAdminRight(path)) return send(403, { error: 'forbidden: admin right required' })
        const result = await this.deps.replication.rmfolder(path)
        return send('error' in result ? (result.code === 'EUNAVAILABLE' ? 503 : 409) : 200, result)
      }

      if (req.method === 'POST' && url.pathname === '/admin/acl') {
        const body = (await readJson(req)) as { path?: string; rules?: unknown }
        const path = String(body.path ?? '')
        if (!path) return send(400, { error: 'path required' })
        if (!requireAdminRight(path)) return send(403, { error: 'forbidden: admin right required' })
        if (!Array.isArray(body.rules)) return send(400, { error: 'rules[] required' })
        const result = await this.deps.replication.setAcl(path, body.rules as AclRule[])
        return send('error' in result ? (result.code === 'EUNAVAILABLE' ? 503 : 409) : 200, result)
      }

      if (req.method === 'POST' && url.pathname === '/admin/corrupt') {
        // Directly diverge local metadata, bypassing the log — simulates corruption.
        this.deps.store.db
          .prepare('INSERT INTO folders (id, parent_id, name, acl_id, ctime, mtime) VALUES (?, 0, ?, 0, 0, 0)')
          .run(999000 + Math.floor(Math.random() * 1000), `__corrupt_${Date.now()}`)
        return send(200, { ok: true, sha256: this.deps.store.sha256() })
      }

      if (req.method === 'PUT' && url.pathname === '/admin/file') {
        const path = url.searchParams.get('path') ?? ''
        if (!path) return send(400, { error: 'path query param required' })
        if (!requireAdminRight(path)) return send(403, { error: 'forbidden: admin right required' })
        const bytes = await readRaw(req)
        const contentType = req.headers['content-type']
        const result = await this.deps.content.writeFile(path, bytes, typeof contentType === 'string' ? contentType : undefined)
        if ('error' in result) {
          return send(result.code === 'ENOSPC' ? 507 : result.code === 'EUNAVAILABLE' ? 503
            : result.code === 'EIO' ? 500 : 409, result)
        }
        return send(200, result)
      }

      if (req.method === 'GET' && url.pathname === '/admin/file') {
        const path = url.searchParams.get('path') ?? ''
        if (!path) return send(400, { error: 'path query param required' })
        if (!requireAdminRight(path)) return send(403, { error: 'forbidden: admin right required' })
        const file = await this.deps.content.readFile(path)
        if ('error' in file) return send(404, file)
        res.writeHead(200, {
          'content-type': file.contentType || 'application/octet-stream',
          'content-length': String(file.bytes.length)
        })
        res.end(file.bytes)
        return
      }

      if (req.method === 'GET' && url.pathname === '/admin/chunks') {
        const hashes = await this.deps.content.localChunks()
        return send(200, {
          peerId: this.deps.node.peerId.toString(),
          count: hashes.length,
          hashes
        })
      }

      if (req.method === 'GET' && url.pathname === '/admin/locations') {
        return send(200, { locations: this.deps.store.allLocations() })
      }

      if (req.method === 'GET' && url.pathname === '/admin/capacity') {
        return send(200, this.deps.content.capacity())
      }

      if (req.method === 'GET' && url.pathname === '/admin/health') {
        return send(200, {
          peerId: this.deps.node.peerId.toString(),
          isPrimary: this.deps.replication.isPrimary,
          peers: this.deps.detector.snapshot()
        })
      }

      if (req.method === 'POST' && url.pathname === '/admin/reconcile') {
        const repaired = await this.deps.content.reconcileOnce()
        return send(200, { ok: true, repaired })
      }

      if (req.method === 'POST' && url.pathname === '/admin/rebalance') {
        const moved = await this.deps.rebalance.sweepOnce()
        return send(200, { ok: true, moved })
      }

      if (req.method === 'POST' && url.pathname === '/admin/corrupt-chunk') {
        const hash = url.searchParams.get('hash') ?? ''
        if (!hash) return send(400, { error: 'hash query param required' })
        const ok = await this.deps.content.corruptLocalForTest(hash)
        return send(ok ? 200 : 404, { ok })
      }

      send(404, { error: 'not found' })
    } catch (err) {
      send(500, { error: String(err) })
    }
  }
}

function readRaw(req: http.IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    req.on('data', (c: Buffer) => chunks.push(c))
    req.on('end', () => resolve(Buffer.concat(chunks)))
    req.on('error', reject)
  })
}

function readJson(req: http.IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    req.on('data', (c: Buffer) => chunks.push(c))
    req.on('end', () => {
      const raw = Buffer.concat(chunks).toString('utf8').trim()
      if (!raw) return resolve({})
      try {
        resolve(JSON.parse(raw))
      } catch (e) {
        reject(e)
      }
    })
    req.on('error', reject)
  })
}
