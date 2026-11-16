/**
 * WebDAV protocol adapter (DETAILED_DESIGN §13) — the mountable surface of the
 * drive. Runs its own HTTP server on WEBDAV_PORT so a normal OS WebDAV client
 * (Windows Explorer, macOS Finder, Linux davfs/gio) can map the global
 * namespace as a network drive and read/write files with folder hierarchy.
 *
 * Every request is authenticated (HTTP Basic, auth/basic.ts) and authorized
 * against the effective per-folder ACL (acl/acl.ts) before any metadata or
 * content is touched. Metadata mutations route to the Primary automatically via
 * the replication write-intent path, so the adapter can run on any node.
 *
 * Methods: OPTIONS, PROPFIND (Depth 0/1), GET/HEAD, PUT, MKCOL, DELETE
 * (recursive), MOVE, COPY (recursive), PROPPATCH (best-effort), LOCK/UNLOCK
 * (in-memory advisory locks — enough to satisfy class-2 clients like Windows).
 */

import http from 'node:http'
import { randomUUID } from 'node:crypto'
import { Buffer } from 'node:buffer'
import type { MetadataStore } from '../metadata/store.js'
import type { ContentService } from '../content/service.js'
import type { ReplicationService } from '../replication/service.js'
import type { Folder, FileRec, Right } from '../metadata/types.js'
import type { BasicAuth } from '../auth/basic.js'
import { resolveEffectiveAcl, can, type Principal } from '../acl/acl.js'
import { log } from '../logger.js'

export interface WebdavDeps {
  port: number
  realm: string
  allowAnon: boolean
  store: MetadataStore
  content: ContentService
  replication: ReplicationService
  auth: BasicAuth
}

const ALLOW = 'OPTIONS, GET, HEAD, PUT, DELETE, PROPFIND, PROPPATCH, MKCOL, MOVE, COPY, LOCK, UNLOCK'
const SUPPORTED_LOCK =
  '<D:lockentry><D:lockscope><D:exclusive/></D:lockscope><D:locktype><D:write/></D:locktype></D:lockentry>'
const LOCK_TIMEOUT_MS = 3600_000

interface LockRec {
  token: string
  owner: string
  expires: number
}

export class WebdavApi {
  private server?: http.Server
  private readonly locks = new Map<string, LockRec>()

  constructor(private deps: WebdavDeps) {}

  start(): void {
    this.server = http.createServer((req, res) => {
      this.handle(req, res).catch((err) => {
        log.error('webdav handler error', { error: String(err) })
        if (!res.headersSent) res.writeHead(500)
        res.end()
      })
    })
    this.server.listen(this.deps.port, () => log.info('webdav listening', { port: this.deps.port }))
  }

  stop(): void {
    this.server?.close()
  }

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const method = (req.method ?? 'GET').toUpperCase()
    const url = new URL(req.url ?? '/', 'http://localhost')
    const path = internalPath(url.pathname)

    switch (method) {
      case 'OPTIONS':
        return this.options(res)
      case 'PROPFIND':
        return this.propfind(req, res, path)
      case 'GET':
      case 'HEAD':
        return this.get(req, res, path, method === 'HEAD')
      case 'PUT':
        return this.put(req, res, path)
      case 'MKCOL':
        return this.mkcol(req, res, path)
      case 'DELETE':
        return this.del(req, res, path)
      case 'MOVE':
        return this.move(req, res, path)
      case 'COPY':
        return this.copy(req, res, path)
      case 'PROPPATCH':
        return this.proppatch(req, res, path)
      case 'LOCK':
        return this.lock(req, res, path)
      case 'UNLOCK':
        return this.unlock(req, res, path)
      default:
        res.writeHead(405, { Allow: ALLOW, 'Content-Length': '0' })
        res.end()
    }
  }

  // ---- auth / acl -----------------------------------------------------------

  private authenticate(req: http.IncomingMessage, res: http.ServerResponse): Principal | null {
    const principal = this.deps.auth.authenticate(req.headers['authorization'])
    if (principal === null) {
      this.challenge(res)
      return null
    }
    if (principal.anonymous && !this.deps.allowAnon) {
      this.challenge(res)
      return null
    }
    return principal
  }

  private allowed(principal: Principal, path: string, right: Right): boolean {
    return can(resolveEffectiveAcl(this.deps.store, path), principal, right)
  }

  private authorize(
    req: http.IncomingMessage,
    res: http.ServerResponse,
    path: string,
    right: Right
  ): Principal | null {
    const principal = this.authenticate(req, res)
    if (!principal) return null
    if (!this.allowed(principal, path, right)) {
      this.deny(res, principal)
      return null
    }
    return principal
  }

  /** Anonymous callers get a 401 challenge (so they can authenticate); users get 403. */
  private deny(res: http.ServerResponse, principal: Principal): void {
    if (principal.anonymous) this.challenge(res)
    else {
      res.writeHead(403, { 'Content-Length': '0' })
      res.end()
    }
  }

  private challenge(res: http.ServerResponse): void {
    res.writeHead(401, { 'WWW-Authenticate': this.deps.auth.challenge(), 'Content-Length': '0' })
    res.end()
  }

  // ---- methods --------------------------------------------------------------

  private options(res: http.ServerResponse): void {
    res.writeHead(200, {
      DAV: '1, 2',
      Allow: ALLOW,
      'MS-Author-Via': 'DAV',
      'Content-Length': '0'
    })
    res.end()
  }

  private propfind(req: http.IncomingMessage, res: http.ServerResponse, path: string): void {
    if (!this.authorize(req, res, path, 'read')) return
    const target = this.deps.store.resolvePath(path)
    if (target.kind === 'none' || target.id === undefined) return notFound(res)

    const depthHeader = String(req.headers['depth'] ?? 'infinity').toLowerCase()
    const depth = depthHeader === '0' ? 0 : 1
    const responses: string[] = []

    if (target.kind === 'file') {
      responses.push(this.fileXml(encodeHref(path, false), this.deps.store.getFile(target.id)!))
    } else {
      responses.push(this.folderXml(encodeHref(path, true), this.deps.store.getFolder(target.id)!))
      if (depth >= 1) {
        const { folders, files } = this.deps.store.listFolder(target.id)
        for (const f of folders) responses.push(this.folderXml(encodeHref(joinPath(path, f.name), true), f))
        for (const f of files) responses.push(this.fileXml(encodeHref(joinPath(path, f.name), false), f))
      }
    }

    const body = `<?xml version="1.0" encoding="utf-8"?>\n<D:multistatus xmlns:D="DAV:">${responses.join('')}</D:multistatus>`
    res.writeHead(207, { 'Content-Type': 'application/xml; charset=utf-8', 'Content-Length': Buffer.byteLength(body) })
    res.end(body)
  }

  private fileXml(href: string, file: FileRec): string {
    return (
      `<D:response><D:href>${xml(href)}</D:href><D:propstat><D:prop>` +
      `<D:displayname>${xml(file.name)}</D:displayname>` +
      `<D:resourcetype/>` +
      `<D:getcontentlength>${file.size}</D:getcontentlength>` +
      `<D:getcontenttype>${xml(file.content_type || 'application/octet-stream')}</D:getcontenttype>` +
      `<D:getlastmodified>${xml(httpDate(file.mtime))}</D:getlastmodified>` +
      `<D:creationdate>${xml(isoDate(file.ctime))}</D:creationdate>` +
      `<D:getetag>${xml(`"${file.id}-${file.mtime}"`)}</D:getetag>` +
      `<D:supportedlock>${SUPPORTED_LOCK}</D:supportedlock>` +
      `<D:lockdiscovery/>` +
      `</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`
    )
  }

  private folderXml(href: string, folder: Folder): string {
    const name = folder.id === 0 ? '/' : folder.name
    return (
      `<D:response><D:href>${xml(href)}</D:href><D:propstat><D:prop>` +
      `<D:displayname>${xml(name)}</D:displayname>` +
      `<D:resourcetype><D:collection/></D:resourcetype>` +
      `<D:getlastmodified>${xml(httpDate(folder.mtime))}</D:getlastmodified>` +
      `<D:creationdate>${xml(isoDate(folder.ctime))}</D:creationdate>` +
      `<D:supportedlock>${SUPPORTED_LOCK}</D:supportedlock>` +
      `<D:lockdiscovery/>` +
      `</D:prop><D:status>HTTP/1.1 200 OK</D:status></D:propstat></D:response>`
    )
  }

  private async get(
    req: http.IncomingMessage,
    res: http.ServerResponse,
    path: string,
    headOnly: boolean
  ): Promise<void> {
    if (!this.authorize(req, res, path, 'read')) return
    const target = this.deps.store.resolvePath(path)
    if (target.kind === 'none' || target.id === undefined) return notFound(res)

    if (target.kind === 'folder') return this.listingHtml(res, path, target.id, headOnly)

    const file = await this.deps.content.readFile(path)
    if ('error' in file) return notFound(res)
    const headers: Record<string, string> = {
      'Content-Type': file.contentType || 'application/octet-stream',
      'Content-Length': String(file.bytes.length),
      'Accept-Ranges': 'none'
    }
    if (headOnly) {
      res.writeHead(200, headers)
      res.end()
      return
    }
    res.writeHead(200, headers)
    res.end(file.bytes)
  }

  private listingHtml(res: http.ServerResponse, path: string, folderId: number, headOnly: boolean): void {
    const { folders, files } = this.deps.store.listFolder(folderId)
    const rows = [
      ...(path !== '/' ? ['<li><a href="../">../</a></li>'] : []),
      ...folders.map((f) => `<li><a href="${xml(encodeURIComponent(f.name))}/">${xml(f.name)}/</a></li>`),
      ...files.map((f) => `<li><a href="${xml(encodeURIComponent(f.name))}">${xml(f.name)}</a> (${f.size} bytes)</li>`)
    ].join('\n')
    const html =
      `<!doctype html><html><head><meta charset="utf-8"><title>Index of ${xml(path)}</title></head>` +
      `<body><h1>Index of ${xml(path)}</h1><ul>${rows}</ul></body></html>`
    const headers = { 'Content-Type': 'text/html; charset=utf-8', 'Content-Length': String(Buffer.byteLength(html)) }
    if (headOnly) {
      res.writeHead(200, headers)
      res.end()
      return
    }
    res.writeHead(200, headers)
    res.end(html)
  }

  private async put(req: http.IncomingMessage, res: http.ServerResponse, path: string): Promise<void> {
    if (!this.authorize(req, res, path, 'write')) return
    const existing = this.deps.store.resolvePath(path)
    if (existing.kind === 'folder') {
      res.writeHead(405, { Allow: ALLOW, 'Content-Length': '0' })
      res.end()
      return
    }
    const bytes = await readRaw(req)
    const ct = req.headers['content-type']
    const result = await this.deps.content.writeFile(path, bytes, typeof ct === 'string' ? ct : undefined)
    if ('error' in result) {
      const code = storageErrorStatus(result.code)
      return sendJson(res, code, result)
    }
    res.writeHead(result.replaced ? 204 : 201, { 'Content-Length': '0' })
    res.end()
  }

  private async mkcol(req: http.IncomingMessage, res: http.ServerResponse, path: string): Promise<void> {
    if (!this.authorize(req, res, path, 'write')) return
    if (path === '/') {
      res.writeHead(405, { Allow: ALLOW, 'Content-Length': '0' })
      res.end()
      return
    }
    if (this.deps.store.resolvePath(path).kind !== 'none') {
      res.writeHead(405, { Allow: ALLOW, 'Content-Length': '0' })
      res.end()
      return
    }
    // MKCOL with a body is unsupported (RFC 4918 §9.3.1).
    if ((await readRaw(req)).length > 0) {
      res.writeHead(415, { 'Content-Length': '0' })
      res.end()
      return
    }
    // mkpath is authoritative on the Primary and creates any missing parents
    // (a replica's local view may lag, so we do not pre-reject here).
    const result = await this.deps.replication.mkpath(path)
    if ('error' in result) return sendJson(res, storageErrorStatus(result.code), result)
    res.writeHead(201, { 'Content-Length': '0' })
    res.end()
  }

  private async del(req: http.IncomingMessage, res: http.ServerResponse, path: string): Promise<void> {
    if (!this.authorize(req, res, path, 'write')) return
    if (path === '/') {
      res.writeHead(403, { 'Content-Length': '0' })
      res.end()
      return
    }
    // Deletion is decided on the Primary (authoritative), so 404/403 come from it.
    const result = await this.deps.replication.deleteEntry(path, true)
    if ('error' in result) {
      const code = result.code === 'EUNAVAILABLE' ? 503
        : result.error.includes('not found') ? 404 : result.error.includes('root') ? 403 : 409
      return sendJson(res, code, result)
    }
    this.locks.delete(path)
    res.writeHead(204, { 'Content-Length': '0' })
    res.end()
  }

  private async move(req: http.IncomingMessage, res: http.ServerResponse, path: string): Promise<void> {
    if (path === '/') {
      res.writeHead(403, { 'Content-Length': '0' })
      res.end()
      return
    }
    const principal = this.authorize(req, res, path, 'write')
    if (!principal) return
    const dst = this.destPath(req)
    if (!dst) return badRequest(res)
    if (!this.allowed(principal, dst, 'write')) return this.deny(res, principal)

    const overwrite = String(req.headers['overwrite'] ?? 'T').toUpperCase() !== 'F'
    const dstExisted = this.deps.store.resolvePath(dst).kind !== 'none'
    if (dstExisted && !overwrite) return preconditionFailed(res)

    const result = await this.deps.replication.moveEntry(path, dst, overwrite)
    if ('error' in result) return sendJson(res, result.code === 'EUNAVAILABLE' ? 503 : moveErrorCode(result.error), result)
    this.locks.delete(path)
    res.writeHead(dstExisted ? 204 : 201, { 'Content-Length': '0' })
    res.end()
  }

  private async copy(req: http.IncomingMessage, res: http.ServerResponse, path: string): Promise<void> {
    const principal = this.authorize(req, res, path, 'read')
    if (!principal) return
    const dst = this.destPath(req)
    if (!dst) return badRequest(res)
    if (!this.allowed(principal, dst, 'write')) return this.deny(res, principal)

    const src = this.deps.store.resolvePath(path)
    if (src.kind === 'none') return notFound(res)
    const overwrite = String(req.headers['overwrite'] ?? 'T').toUpperCase() !== 'F'
    const dstExisted = this.deps.store.resolvePath(dst).kind !== 'none'
    if (dstExisted && !overwrite) return preconditionFailed(res)

    try {
      if (src.kind === 'file') await this.copyFile(path, dst)
      else await this.copyFolder(path, dst)
    } catch (err) {
      const code = err instanceof Error && 'code' in err && typeof err.code === 'string' ? err.code : undefined
      return sendJson(res, storageErrorStatus(code), { error: String(err), code })
    }
    res.writeHead(dstExisted ? 204 : 201, { 'Content-Length': '0' })
    res.end()
  }

  private async copyFile(srcPath: string, dstPath: string): Promise<void> {
    const data = await this.deps.content.readFile(srcPath)
    if ('error' in data) throw new Error(data.error)
    const w = await this.deps.content.writeFile(dstPath, data.bytes, data.contentType ?? undefined)
    if ('error' in w) throw Object.assign(new Error(w.error), { code: w.code })
  }

  private async copyFolder(srcPath: string, dstPath: string): Promise<void> {
    const r = this.deps.store.resolvePath(srcPath)
    if (r.kind !== 'folder' || r.id === undefined) throw new Error('source not a folder')
    const created = await this.deps.replication.mkpath(dstPath)
    if ('error' in created) throw Object.assign(new Error(created.error), { code: created.code })
    const { folders, files } = this.deps.store.listFolder(r.id)
    for (const f of files) await this.copyFile(joinPath(srcPath, f.name), joinPath(dstPath, f.name))
    for (const sub of folders) await this.copyFolder(joinPath(srcPath, sub.name), joinPath(dstPath, sub.name))
  }

  private async proppatch(req: http.IncomingMessage, res: http.ServerResponse, path: string): Promise<void> {
    if (!this.authorize(req, res, path, 'write')) return
    const target = this.deps.store.resolvePath(path)
    if (target.kind === 'none') return notFound(res)
    await readRaw(req) // drain (we do not persist dead properties in v1)
    const href = encodeHref(path, target.kind === 'folder')
    const body =
      `<?xml version="1.0" encoding="utf-8"?>\n<D:multistatus xmlns:D="DAV:"><D:response>` +
      `<D:href>${xml(href)}</D:href><D:propstat><D:prop/><D:status>HTTP/1.1 200 OK</D:status></D:propstat>` +
      `</D:response></D:multistatus>`
    res.writeHead(207, { 'Content-Type': 'application/xml; charset=utf-8', 'Content-Length': Buffer.byteLength(body) })
    res.end(body)
  }

  private async lock(req: http.IncomingMessage, res: http.ServerResponse, path: string): Promise<void> {
    if (!this.authorize(req, res, path, 'write')) return
    const body = await readRaw(req)
    const existing = this.deps.store.resolvePath(path)
    const isRefresh = body.length === 0 && typeof req.headers['if'] === 'string'

    let rec = this.locks.get(path)
    if (rec && isRefresh) {
      rec.expires = Date.now() + LOCK_TIMEOUT_MS
    } else {
      // Create a lock-null resource so Windows' LOCK-then-PUT flow finds a target.
      if (existing.kind === 'none' && this.deps.store.resolvePath(parentPath(path)).kind === 'folder') {
        try {
          await this.deps.content.writeFile(path, Buffer.alloc(0))
        } catch {
          /* best-effort; PUT will create it */
        }
      }
      rec = { token: `opaquelocktoken:${randomUUID()}`, owner: 'webdav', expires: Date.now() + LOCK_TIMEOUT_MS }
      this.locks.set(path, rec)
    }

    const href = encodeHref(path, this.deps.store.resolvePath(path).kind === 'folder')
    const bodyXml =
      `<?xml version="1.0" encoding="utf-8"?>\n<D:prop xmlns:D="DAV:"><D:lockdiscovery><D:activelock>` +
      `<D:locktype><D:write/></D:locktype><D:lockscope><D:exclusive/></D:lockscope>` +
      `<D:depth>infinity</D:depth><D:owner>${xml(rec.owner)}</D:owner>` +
      `<D:timeout>Second-3600</D:timeout>` +
      `<D:locktoken><D:href>${xml(rec.token)}</D:href></D:locktoken>` +
      `<D:lockroot><D:href>${xml(href)}</D:href></D:lockroot>` +
      `</D:activelock></D:lockdiscovery></D:prop>`
    res.writeHead(200, {
      'Content-Type': 'application/xml; charset=utf-8',
      'Lock-Token': `<${rec.token}>`,
      'Content-Length': Buffer.byteLength(bodyXml)
    })
    res.end(bodyXml)
  }

  private unlock(req: http.IncomingMessage, res: http.ServerResponse, path: string): void {
    if (!this.authorize(req, res, path, 'write')) return
    this.locks.delete(path)
    res.writeHead(204, { 'Content-Length': '0' })
    res.end()
  }

  private destPath(req: http.IncomingMessage): string | null {
    const raw = req.headers['destination']
    if (typeof raw !== 'string' || raw.length === 0) return null
    try {
      const u = raw.startsWith('/') ? new URL('http://localhost' + raw) : new URL(raw)
      return internalPath(u.pathname)
    } catch {
      return null
    }
  }
}

// ---- path + xml helpers -----------------------------------------------------

/** Normalize a request URL path to an internal drive path ('/A/B', root = '/'). */
function internalPath(pathname: string): string {
  let p: string
  try {
    p = decodeURIComponent(pathname)
  } catch {
    p = pathname
  }
  p = p.replace(/\/+/g, '/')
  if (p.length > 1 && p.endsWith('/')) p = p.slice(0, -1)
  return p === '' ? '/' : p
}

/** Percent-encode an internal path back into an href, adding a trailing slash for collections. */
function encodeHref(internal: string, collection: boolean): string {
  const parts = internal
    .split('/')
    .filter((s) => s.length > 0)
    .map((s) => encodeURIComponent(s))
  let href = '/' + parts.join('/')
  if (collection && href !== '/') href += '/'
  return href
}

function parentPath(path: string): string {
  const parts = path.split('/').filter((s) => s.length > 0)
  return parts.length <= 1 ? '/' : '/' + parts.slice(0, -1).join('/')
}

function joinPath(base: string, name: string): string {
  return base === '/' ? '/' + name : base + '/' + name
}

function moveErrorCode(err: string): number {
  if (err.includes('exists')) return 412
  if (err.includes('not found')) return 404
  return 409
}

function storageErrorStatus(code?: string): number {
  if (code === 'ENOSPC') return 507
  if (code === 'EUNAVAILABLE') return 503
  if (code === 'EIO') return 500
  return 409
}

const XML_ESCAPES: Record<string, string> = { '<': '&lt;', '>': '&gt;', '&': '&amp;', "'": '&apos;', '"': '&quot;' }
const xml = (s: string): string => s.replace(/[<>&'"]/g, (c) => XML_ESCAPES[c])
const httpDate = (ms: number): string => new Date(ms).toUTCString()
const isoDate = (ms: number): string => new Date(ms).toISOString()

function notFound(res: http.ServerResponse): void {
  res.writeHead(404, { 'Content-Length': '0' })
  res.end()
}
function badRequest(res: http.ServerResponse): void {
  res.writeHead(400, { 'Content-Length': '0' })
  res.end()
}
function preconditionFailed(res: http.ServerResponse): void {
  res.writeHead(412, { 'Content-Length': '0' })
  res.end()
}
function sendJson(res: http.ServerResponse, code: number, body: unknown): void {
  const json = JSON.stringify(body)
  res.writeHead(code, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(json) })
  res.end(json)
}

function readRaw(req: http.IncomingMessage): Promise<Buffer> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    req.on('data', (c: Buffer) => chunks.push(c))
    req.on('end', () => resolve(Buffer.concat(chunks)))
    req.on('error', reject)
  })
}
