/**
 * MCP (Model Context Protocol) endpoint (DETAILED_DESIGN §14) — the AI-agent
 * surface of the drive. Runs its own HTTP server on MCP_PORT and speaks the MCP
 * JSON-RPC 2.0 wire protocol (Streamable-HTTP transport: one JSON-RPC request
 * per HTTP POST, JSON response). Any MCP client (Claude Desktop, the MCP
 * Inspector, an in-house agent) can connect and call tools.
 *
 * Tools (§14): metadata tools answer *locally* from the replica, content tools
 * *resolve + fetch* via the content service, and writes *route to the Primary*
 * through the replication write-intent path — exactly the same routing/ACL path
 * WebDAV uses, so AI is a first-class client and the adapter can run on any node.
 *
 *   list_folder   read   list child folders + files of a path
 *   stat_file     read   file/folder metadata (size, times, content type, chunks)
 *   read_file     read   file bytes (utf8 or base64), read-repaired on the fly
 *   search        read   find files whose path matches a query (ACL-filtered)
 *   create_folder write  create a folder path (routes to Primary)
 *   write_file    write  create/overwrite a file (chunk + RF=2 replicate)
 *
 * Every call is authenticated (HTTP Basic, auth/basic.ts) and authorized against
 * the effective per-folder ACL (acl/acl.ts) before any metadata or content is
 * touched — identical enforcement to the WebDAV surface.
 */

import http from 'node:http'
import { Buffer } from 'node:buffer'
import type { MetadataStore } from '../metadata/store.js'
import type { ContentService } from '../content/service.js'
import type { ReplicationService } from '../replication/service.js'
import type { Folder, FileRec, Right } from '../metadata/types.js'
import type { BasicAuth } from '../auth/basic.js'
import { resolveEffectiveAcl, can, type Principal } from '../acl/acl.js'
import { log } from '../logger.js'

export interface McpDeps {
  port: number
  realm: string
  allowAnon: boolean
  /** Server name/version advertised to clients in `initialize`. */
  serverName: string
  serverVersion: string
  store: MetadataStore
  content: ContentService
  replication: ReplicationService
  auth: BasicAuth
}

/** Protocol revision we implement; we also echo back a client's requested one. */
const PROTOCOL_VERSION = '2025-06-18'
const MAX_READ_BYTES = 8 * 1024 * 1024
const MAX_SEARCH_RESULTS = 200

/** JSON-RPC 2.0 request. `id` absent => notification (no response). */
interface RpcRequest {
  jsonrpc?: string
  id?: string | number | null
  method?: string
  params?: unknown
}

interface ToolDef {
  name: string
  description: string
  inputSchema: object
}

/** MCP tool-call content payload. */
type ToolContent = { type: 'text'; text: string }
interface ToolResult {
  content: ToolContent[]
  isError?: boolean
}

const RPC_PARSE_ERROR = -32700
const RPC_INVALID_REQUEST = -32600
const RPC_METHOD_NOT_FOUND = -32601
const RPC_INVALID_PARAMS = -32602
const RPC_INTERNAL_ERROR = -32603

export class McpApi {
  private server?: http.Server
  private readonly tools: ToolDef[]

  constructor(private deps: McpDeps) {
    this.tools = buildToolDefs()
  }

  start(): void {
    this.server = http.createServer((req, res) => {
      this.handle(req, res).catch((err) => {
        log.error('mcp handler error', { error: String(err) })
        if (!res.headersSent) res.writeHead(500)
        res.end()
      })
    })
    this.server.listen(this.deps.port, () => log.info('mcp listening', { port: this.deps.port }))
  }

  stop(): void {
    this.server?.close()
  }

  // ---- transport ------------------------------------------------------------

  private async handle(req: http.IncomingMessage, res: http.ServerResponse): Promise<void> {
    const method = (req.method ?? 'GET').toUpperCase()

    // The Streamable-HTTP transport uses GET only to open a server->client SSE
    // stream. We are request/response only, so we advertise that politely.
    if (method === 'GET') {
      res.writeHead(405, { Allow: 'POST', 'Content-Length': '0' })
      res.end()
      return
    }
    if (method === 'DELETE') {
      // Session teardown — we are stateless, so just acknowledge.
      res.writeHead(200, { 'Content-Length': '0' })
      res.end()
      return
    }
    if (method !== 'POST') {
      res.writeHead(405, { Allow: 'POST', 'Content-Length': '0' })
      res.end()
      return
    }

    // Authenticate once per HTTP request (Basic). Anonymous is allowed only when
    // MCP_ALLOW_ANON and the target ACL grants `*`; the per-tool ACL check is the
    // real gate. A bad/wrong credential is challenged with 401.
    const principal = this.deps.auth.authenticate(req.headers['authorization'])
    if (principal === null || (principal.anonymous && !this.deps.allowAnon)) {
      this.challenge(res)
      return
    }

    const raw = await readBody(req)
    let payload: unknown
    try {
      payload = raw.trim() === '' ? undefined : JSON.parse(raw)
    } catch {
      return this.sendRpc(res, rpcError(null, RPC_PARSE_ERROR, 'parse error'))
    }

    // JSON-RPC batches (array) are supported for robustness across MCP revisions.
    if (Array.isArray(payload)) {
      const responses = (
        await Promise.all(payload.map((m) => this.dispatch(m as RpcRequest, principal)))
      ).filter((r): r is object => r !== null)
      if (responses.length === 0) {
        res.writeHead(202, { 'Content-Length': '0' })
        res.end()
        return
      }
      return this.sendRpc(res, responses)
    }

    const response = await this.dispatch(payload as RpcRequest, principal)
    if (response === null) {
      // Notification — nothing to return.
      res.writeHead(202, { 'Content-Length': '0' })
      res.end()
      return
    }
    return this.sendRpc(res, response)
  }

  private challenge(res: http.ServerResponse): void {
    res.writeHead(401, {
      'WWW-Authenticate': this.deps.auth.challenge(),
      'Content-Type': 'application/json'
    })
    res.end(JSON.stringify(rpcError(null, RPC_INVALID_REQUEST, 'authentication required')))
  }

  private sendRpc(res: http.ServerResponse, body: unknown): void {
    const json = JSON.stringify(body)
    res.writeHead(200, { 'Content-Type': 'application/json' })
    res.end(json)
  }

  // ---- JSON-RPC dispatch ----------------------------------------------------

  /** Returns the JSON-RPC response object, or null for a notification. */
  private async dispatch(msg: RpcRequest, principal: Principal): Promise<object | null> {
    if (!msg || msg.jsonrpc !== '2.0' || typeof msg.method !== 'string') {
      const id = msg && msg.id !== undefined ? msg.id : null
      return rpcError(id ?? null, RPC_INVALID_REQUEST, 'invalid request')
    }
    const isNotification = msg.id === undefined || msg.id === null
    const id = msg.id ?? null

    try {
      switch (msg.method) {
        case 'initialize':
          return rpcOk(id, this.initialize(msg.params))
        case 'ping':
          return rpcOk(id, {})
        case 'tools/list':
          return rpcOk(id, { tools: this.tools })
        case 'tools/call':
          return rpcOk(id, await this.callTool(msg.params, principal))
        default:
          // Client notifications (notifications/initialized, notifications/*)
          // require no response and no error.
          if (isNotification || msg.method.startsWith('notifications/')) return null
          return rpcError(id, RPC_METHOD_NOT_FOUND, `method not found: ${msg.method}`)
      }
    } catch (err) {
      if (err instanceof RpcInvalidParams) return rpcError(id, RPC_INVALID_PARAMS, err.message)
      log.error('mcp dispatch error', { method: msg.method, error: String(err) })
      if (isNotification) return null
      return rpcError(id, RPC_INTERNAL_ERROR, String(err))
    }
  }

  private initialize(params: unknown): object {
    const requested = asObject(params)?.protocolVersion
    return {
      protocolVersion: typeof requested === 'string' ? requested : PROTOCOL_VERSION,
      capabilities: { tools: { listChanged: false } },
      serverInfo: { name: this.deps.serverName, version: this.deps.serverVersion }
    }
  }

  // ---- tools ----------------------------------------------------------------

  private async callTool(params: unknown, principal: Principal): Promise<ToolResult> {
    const p = asObject(params)
    const name = p?.name
    if (typeof name !== 'string') throw new RpcInvalidParams('tools/call requires a string "name"')
    const args = asObject(p?.arguments) ?? {}

    switch (name) {
      case 'list_folder':
        return this.toolListFolder(args, principal)
      case 'stat_file':
        return this.toolStat(args, principal)
      case 'read_file':
        return this.toolReadFile(args, principal)
      case 'search':
        return this.toolSearch(args, principal)
      case 'create_folder':
        return this.toolCreateFolder(args, principal)
      case 'write_file':
        return this.toolWriteFile(args, principal)
      default:
        throw new RpcInvalidParams(`unknown tool: ${name}`)
    }
  }

  private allowed(principal: Principal, path: string, right: Right): boolean {
    return can(resolveEffectiveAcl(this.deps.store, path), principal, right)
  }

  private toolListFolder(args: Record<string, unknown>, principal: Principal): ToolResult {
    const path = normPath(reqString(args, 'path'))
    const res = this.deps.store.resolvePath(path)
    if (res.kind !== 'folder' || res.id === undefined) return toolError(`not a folder: ${path}`)
    if (!this.allowed(principal, path, 'read')) return toolError(`access denied: ${path}`)

    const { folders, files } = this.deps.store.listFolder(res.id)
    return toolJson({
      path,
      folders: folders.map((f) => ({ name: f.name, path: joinPath(path, f.name) })),
      files: files.map((f) => ({
        name: f.name,
        path: joinPath(path, f.name),
        size: f.size,
        contentType: f.content_type ?? null,
        mtime: f.mtime
      }))
    })
  }

  private toolStat(args: Record<string, unknown>, principal: Principal): ToolResult {
    const path = normPath(reqString(args, 'path'))
    const res = this.deps.store.resolvePath(path)
    if (res.kind === 'none' || res.id === undefined) return toolError(`not found: ${path}`)
    if (!this.allowed(principal, path, 'read')) return toolError(`access denied: ${path}`)

    if (res.kind === 'folder') {
      const folder = this.deps.store.getFolder(res.id) as Folder
      return toolJson({ path, kind: 'folder', name: folder.name, ctime: folder.ctime, mtime: folder.mtime })
    }
    const file = this.deps.store.getFile(res.id) as FileRec
    return toolJson({
      path,
      kind: 'file',
      name: file.name,
      size: file.size,
      contentType: file.content_type ?? null,
      ctime: file.ctime,
      mtime: file.mtime
    })
  }

  private async toolReadFile(args: Record<string, unknown>, principal: Principal): Promise<ToolResult> {
    const path = normPath(reqString(args, 'path'))
    const encoding = optEnum(args, 'encoding', ['utf8', 'base64']) ?? 'utf8'
    const maxBytes = optNumber(args, 'maxBytes') ?? MAX_READ_BYTES

    const res = this.deps.store.resolvePath(path)
    if (res.kind !== 'file') return toolError(`not a file: ${path}`)
    if (!this.allowed(principal, path, 'read')) return toolError(`access denied: ${path}`)

    const file = await this.deps.content.readFile(path)
    if ('error' in file) return toolError(file.error)
    if (file.bytes.length > maxBytes) {
      return toolError(`file is ${file.bytes.length} bytes, exceeds maxBytes=${maxBytes}`)
    }
    const text = encoding === 'base64' ? file.bytes.toString('base64') : file.bytes.toString('utf8')
    return {
      content: [{ type: 'text', text }],
      isError: false
    }
  }

  private toolSearch(args: Record<string, unknown>, principal: Principal): ToolResult {
    const query = reqString(args, 'query').toLowerCase()
    const root = normPath(optString(args, 'path') ?? '/')
    const res = this.deps.store.resolvePath(root)
    if (res.kind !== 'folder' || res.id === undefined) return toolError(`not a folder: ${root}`)

    const matches: Array<{ path: string; size: number; contentType: string | null }> = []
    const walk = (folderId: number, prefix: string): void => {
      if (matches.length >= MAX_SEARCH_RESULTS) return
      const { folders, files } = this.deps.store.listFolder(folderId)
      for (const f of files) {
        if (matches.length >= MAX_SEARCH_RESULTS) return
        const full = joinPath(prefix, f.name)
        if (!full.toLowerCase().includes(query)) continue
        // Only surface files the caller may read.
        if (!this.allowed(principal, full, 'read')) continue
        matches.push({ path: full, size: f.size, contentType: f.content_type ?? null })
      }
      for (const sub of folders) {
        if (matches.length >= MAX_SEARCH_RESULTS) return
        walk(sub.id, joinPath(prefix, sub.name))
      }
    }
    walk(res.id, root === '/' ? '' : root)
    return toolJson({ query, root, count: matches.length, results: matches })
  }

  private async toolCreateFolder(args: Record<string, unknown>, principal: Principal): Promise<ToolResult> {
    const path = normPath(reqString(args, 'path'))
    if (path === '/') return toolError('cannot create root')
    // Create is governed by the folder it would be created in (nearest ancestor).
    if (!this.allowed(principal, path, 'write')) return toolError(`access denied: ${path}`)

    const result = await this.deps.replication.mkpath(path)
    if ('error' in result) return toolError(result.error)
    return toolJson({ ok: true, path, lsn: result.lsn })
  }

  private async toolWriteFile(args: Record<string, unknown>, principal: Principal): Promise<ToolResult> {
    const path = normPath(reqString(args, 'path'))
    if (path === '/' || path.endsWith('/')) return toolError(`invalid file path: ${path}`)
    const encoding = optEnum(args, 'encoding', ['utf8', 'base64']) ?? 'utf8'
    const contentType = optString(args, 'contentType')
    const content = reqString(args, 'content')
    if (!this.allowed(principal, path, 'write')) return toolError(`access denied: ${path}`)

    let bytes: Buffer
    try {
      bytes = Buffer.from(content, encoding === 'base64' ? 'base64' : 'utf8')
    } catch {
      return toolError('invalid content encoding')
    }
    const result = await this.deps.content.writeFile(path, bytes, contentType)
    if ('error' in result) {
      return toolError(result.code === 'ENOSPC' ? `no space: ${result.error}` : result.error)
    }
    return toolJson({ ok: true, path, size: bytes.length, chunks: result.chunks, lsn: result.lsn })
  }
}

// ---- tool schema ------------------------------------------------------------

function buildToolDefs(): ToolDef[] {
  const pathProp = { type: 'string', description: 'Absolute drive path, e.g. "/docs/report.txt"' }
  return [
    {
      name: 'list_folder',
      description: 'List the child folders and files of a folder in the shared drive.',
      inputSchema: {
        type: 'object',
        properties: { path: pathProp },
        required: ['path'],
        additionalProperties: false
      }
    },
    {
      name: 'stat_file',
      description: 'Get metadata (size, content type, timestamps) for a file or folder.',
      inputSchema: {
        type: 'object',
        properties: { path: pathProp },
        required: ['path'],
        additionalProperties: false
      }
    },
    {
      name: 'read_file',
      description: 'Read the contents of a file. Text is returned utf8 by default, or base64 for binary.',
      inputSchema: {
        type: 'object',
        properties: {
          path: pathProp,
          encoding: { type: 'string', enum: ['utf8', 'base64'], description: 'Output encoding (default utf8).' },
          maxBytes: { type: 'number', description: 'Refuse to read files larger than this.' }
        },
        required: ['path'],
        additionalProperties: false
      }
    },
    {
      name: 'search',
      description: 'Find files whose path contains a query string. Results are limited to files the caller may read.',
      inputSchema: {
        type: 'object',
        properties: {
          query: { type: 'string', description: 'Case-insensitive substring to match against file paths.' },
          path: { type: 'string', description: 'Folder to search under (default "/").' }
        },
        required: ['query'],
        additionalProperties: false
      }
    },
    {
      name: 'create_folder',
      description: 'Create a folder (and any missing parents). Requires write access to the parent folder.',
      inputSchema: {
        type: 'object',
        properties: { path: pathProp },
        required: ['path'],
        additionalProperties: false
      }
    },
    {
      name: 'write_file',
      description: 'Create or overwrite a file with the given content. Requires write access.',
      inputSchema: {
        type: 'object',
        properties: {
          path: pathProp,
          content: { type: 'string', description: 'File content (utf8 text or base64 bytes).' },
          encoding: { type: 'string', enum: ['utf8', 'base64'], description: 'Encoding of "content" (default utf8).' },
          contentType: { type: 'string', description: 'Optional MIME type to record.' }
        },
        required: ['path', 'content'],
        additionalProperties: false
      }
    }
  ]
}

// ---- JSON-RPC + tool helpers ------------------------------------------------

class RpcInvalidParams extends Error {}

function rpcOk(id: string | number | null, result: unknown): object {
  return { jsonrpc: '2.0', id, result }
}

function rpcError(id: string | number | null, code: number, message: string): object {
  return { jsonrpc: '2.0', id, error: { code, message } }
}

function toolJson(value: unknown): ToolResult {
  return { content: [{ type: 'text', text: JSON.stringify(value, null, 2) }], isError: false }
}

function toolError(message: string): ToolResult {
  return { content: [{ type: 'text', text: message }], isError: true }
}

function asObject(value: unknown): Record<string, unknown> | undefined {
  return value && typeof value === 'object' && !Array.isArray(value) ? (value as Record<string, unknown>) : undefined
}

function reqString(args: Record<string, unknown>, key: string): string {
  const v = args[key]
  if (typeof v !== 'string' || v.length === 0) throw new RpcInvalidParams(`"${key}" (string) is required`)
  return v
}

function optString(args: Record<string, unknown>, key: string): string | undefined {
  const v = args[key]
  if (v === undefined || v === null) return undefined
  if (typeof v !== 'string') throw new RpcInvalidParams(`"${key}" must be a string`)
  return v
}

function optNumber(args: Record<string, unknown>, key: string): number | undefined {
  const v = args[key]
  if (v === undefined || v === null) return undefined
  if (typeof v !== 'number' || !Number.isFinite(v)) throw new RpcInvalidParams(`"${key}" must be a number`)
  return v
}

function optEnum<T extends string>(args: Record<string, unknown>, key: string, allowed: T[]): T | undefined {
  const v = args[key]
  if (v === undefined || v === null) return undefined
  if (typeof v !== 'string' || !allowed.includes(v as T)) {
    throw new RpcInvalidParams(`"${key}" must be one of: ${allowed.join(', ')}`)
  }
  return v as T
}

/** Normalise to a leading-slash, no-trailing-slash absolute path ('/' for root). */
function normPath(path: string): string {
  const parts = path.split('/').filter((p) => p.length > 0 && p !== '.')
  return parts.length === 0 ? '/' : '/' + parts.join('/')
}

function joinPath(prefix: string, name: string): string {
  const base = prefix === '/' ? '' : prefix.replace(/\/$/, '')
  return `${base}/${name}`
}

function readBody(req: http.IncomingMessage): Promise<string> {
  return new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    req.on('data', (c: Buffer) => chunks.push(c))
    req.on('end', () => resolve(Buffer.concat(chunks).toString('utf8')))
    req.on('error', reject)
  })
}
