/**
 * Phase 6 fast unit harness — exercises the MCP endpoint (api/mcp.ts) directly,
 * WITHOUT libp2p, so it runs in seconds even where the full networked cluster
 * harness (run.mjs) is too slow to load the p2p stack.
 *
 * It wires the REAL McpApi + MetadataStore + BasicAuth to lightweight fakes for
 * the content and replication services (which the MCP layer only calls through
 * narrow interfaces), then drives raw MCP JSON-RPC 2.0 over HTTP — proving the
 * protocol handshake, tool dispatch, content round-trips and ACL enforcement.
 *
 * Usage:  node test/phase6/mcp-unit.mjs   (after `npm run build`)
 */

import { fileURLToPath, pathToFileURL } from 'node:url'
import { dirname, join } from 'node:path'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'

const __dirname = dirname(fileURLToPath(import.meta.url))
const DIST = join(__dirname, '..', '..', 'dist')
const imp = (p) => import(pathToFileURL(join(DIST, ...p)).href)

const { McpApi } = await imp(['api', 'mcp.js'])
const { MetadataStore } = await imp(['metadata', 'store.js'])
const { BasicAuth } = await imp(['auth', 'basic.js'])

let failed = false
const pass = (m) => console.log(`  ok ${m}`)
const fail = (m) => { failed = true; console.error(`\n  x FAIL: ${m}\n`) }
const basic = (creds) => 'Basic ' + Buffer.from(creds).toString('base64')

const CHUNK = 1024
const now = () => Date.now()

// ---- store-backed fakes for the content + replication services --------------

/** Minimal replication service: single writer (Primary), mkpath via store.apply. */
function makeReplication(store) {
  return {
    isPrimary: true,
    async mkpath(path) {
      const parts = path.split('/').filter((p) => p.length > 0)
      let parent = 0
      for (const name of parts) {
        let child = store.childFolder(parent, name)
        if (!child) {
          const id = store.nextId('folders')
          store.apply({ op: 'folder.create', id, parentId: parent, name, aclId: 0, ctime: now(), mtime: now() })
          store.setLsn(store.lsn + 1)
          child = { id }
        }
        parent = child.id
      }
      return { ok: true, lsn: store.lsn }
    }
  }
}

/** Minimal content service: chunk-count + a path->bytes map; drives the store. */
function makeContent(store, repl) {
  const blobs = new Map()
  return {
    usedBytes: () => [...blobs.values()].reduce((n, b) => n + b.length, 0),
    async writeFile(path, bytes, contentType) {
      const parts = path.split('/').filter((p) => p.length > 0)
      const name = parts.pop()
      const parentPath = '/' + parts.join('/')
      await repl.mkpath(parentPath === '/' ? '/' : parentPath)
      const res = store.resolvePath(parentPath)
      const folderId = res.kind === 'folder' ? res.id : 0
      const existing = store.childFile(folderId, name)
      const chunks = Math.max(1, Math.ceil(bytes.length / CHUNK))
      const manifestId = store.nextId('manifests')
      if (existing) {
        store.apply({ op: 'file.update', id: existing.id, size: bytes.length, manifestId, mtime: now() })
      } else {
        const id = store.nextId('files')
        const ext = name.includes('.') ? name.slice(name.lastIndexOf('.') + 1) : null
        store.apply({ op: 'file.create', id, folderId, name, ext, size: bytes.length, contentType: contentType ?? null, manifestId, aclId: null, ctime: now(), mtime: now() })
      }
      store.apply({ op: 'manifest.set', id: manifestId, fileId: 0, totalSize: bytes.length, chunkHashes: [] })
      store.setLsn(store.lsn + 1)
      blobs.set(path, Buffer.from(bytes))
      return { ok: true, lsn: store.lsn, chunks }
    },
    async readFile(path) {
      const res = store.resolvePath(path)
      if (res.kind !== 'file') return { error: 'not found' }
      const bytes = blobs.get(path)
      if (!bytes) return { error: 'no content' }
      const file = store.getFile(res.id)
      return { bytes, contentType: file?.content_type ?? null }
    }
  }
}

/** Seed a non-`*` ACL on a folder (mirrors an admin /acl call), via store.apply. */
function setAcl(store, path, rules) {
  const res = store.resolvePath(path)
  const aclId = store.nextId('acls')
  store.apply({ op: 'acl.set', aclId, rules })
  if (res.kind === 'folder') store.apply({ op: 'folder.setacl', id: res.id, aclId, mtime: now() })
  else store.apply({ op: 'file.setacl', id: res.id, aclId, mtime: now() })
  store.setLsn(store.lsn + 1)
}

// ---- JSON-RPC client --------------------------------------------------------

let rpcId = 0
async function rpc(port, method, params, auth) {
  const headers = { 'content-type': 'application/json' }
  if (auth) headers['authorization'] = basic(auth)
  const res = await fetch(`http://127.0.0.1:${port}/`, {
    method: 'POST', headers, body: JSON.stringify({ jsonrpc: '2.0', id: ++rpcId, method, params })
  })
  const text = await res.text()
  let body = null
  if (text) { try { body = JSON.parse(text) } catch { body = text } }
  return { httpStatus: res.status, body }
}
async function callTool(port, name, args, auth) {
  const { httpStatus, body } = await rpc(port, 'tools/call', { name, arguments: args }, auth)
  const result = body && body.result
  const text = result && Array.isArray(result.content) ? result.content.map((c) => c.text).join('') : ''
  let json = null
  try { json = JSON.parse(text) } catch { /* not json */ }
  return { httpStatus, isError: !!(result && result.isError), text, json }
}

// ---- main -------------------------------------------------------------------

const tmp = mkdtempSync(join(tmpdir(), 'mcp-unit-'))
const store = new MetadataStore(join(tmp, 'meta.db'))
const repl = makeReplication(store)
const content = makeContent(store, repl)
const auth = new BasicAuth('alice:secret:staff,bob:hunter2,carol:pw:staff', 'p2p-drive')
const PORT = 8199
const mcp = new McpApi({
  port: PORT, realm: 'p2p-drive', allowAnon: true, serverName: 'p2p-storage', serverVersion: '0.1.0',
  store, content, replication: repl, auth
})
mcp.start()
await new Promise((r) => setTimeout(r, 300))

try {
  // ---- Part A: protocol ----
  console.log('\nPART A: MCP protocol handshake\n')
  const init = await rpc(PORT, 'initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 't', version: '0' } })
  if (init.httpStatus !== 200 || !init.body?.result?.serverInfo?.name) fail(`initialize: ${JSON.stringify(init.body)}`)
  else pass(`initialize -> ${init.body.result.serverInfo.name} @ ${init.body.result.protocolVersion}`)

  const notif = await fetch(`http://127.0.0.1:${PORT}/`, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }) })
  if (notif.status !== 202) fail(`notifications/initialized expected 202, got ${notif.status}`)
  else pass('notifications/initialized -> 202')

  const list = await rpc(PORT, 'tools/list', {})
  const names = (list.body?.result?.tools ?? []).map((t) => t.name).sort()
  const want = ['create_folder', 'list_folder', 'read_file', 'search', 'stat_file', 'write_file']
  if (JSON.stringify(names) !== JSON.stringify(want)) fail(`tools/list: ${JSON.stringify(names)}`)
  else pass(`tools/list -> ${names.join(', ')}`)

  const badMethod = await rpc(PORT, 'no/such', {})
  if (badMethod.body?.error?.code !== -32601) fail(`unknown method expected -32601, got ${JSON.stringify(badMethod.body)}`)
  else pass('unknown method -> -32601')

  // ---- Part B: tools ----
  console.log('\nPART B: create / write / list / stat / read / search\n')
  const mk = await callTool(PORT, 'create_folder', { path: '/agent' })
  if (mk.isError || !mk.json?.ok) fail(`create_folder: ${mk.text}`); else pass('create_folder /agent -> ok')

  const body = 'hello from an AI agent\n'
  const wr = await callTool(PORT, 'write_file', { path: '/agent/note.txt', content: body, contentType: 'text/plain' })
  if (wr.isError || !wr.json?.ok) fail(`write_file: ${wr.text}`); else pass(`write_file /agent/note.txt -> ok (${wr.json.size}B, ${wr.json.chunks} chunk(s))`)

  const ls = await callTool(PORT, 'list_folder', { path: '/agent' })
  if (ls.isError || !ls.json?.files?.some((f) => f.name === 'note.txt')) fail(`list_folder: ${ls.text}`); else pass('list_folder /agent lists note.txt')

  const st = await callTool(PORT, 'stat_file', { path: '/agent/note.txt' })
  if (st.isError || st.json?.kind !== 'file' || st.json?.size !== Buffer.byteLength(body)) fail(`stat_file: ${st.text}`); else pass(`stat_file -> file size=${st.json.size}`)

  const rd = await callTool(PORT, 'read_file', { path: '/agent/note.txt' })
  if (rd.isError || rd.text !== body) fail(`read_file: ${JSON.stringify(rd.text)}`); else pass('read_file -> byte-for-byte (utf8)')

  const big = Buffer.from('x'.repeat(CHUNK * 2 + 5))
  const wrB = await callTool(PORT, 'write_file', { path: '/agent/blob.bin', content: big.toString('base64'), encoding: 'base64' })
  const rdB = await callTool(PORT, 'read_file', { path: '/agent/blob.bin', encoding: 'base64' })
  if (wrB.json?.chunks !== 3) fail(`expected 3 chunks, got ${wrB.json?.chunks}`); else pass('write_file base64 multi-chunk -> 3 chunks')
  if (rdB.isError || Buffer.from(rdB.text, 'base64').toString() !== big.toString()) fail('base64 read mismatch'); else pass('read_file base64 -> byte-for-byte')

  const sr = await callTool(PORT, 'search', { query: 'note' })
  if (sr.isError || !sr.json?.results?.some((r) => r.path === '/agent/note.txt')) fail(`search: ${sr.text}`); else pass('search "note" finds /agent/note.txt')

  const miss = await callTool(PORT, 'read_file', { path: '/agent/nope.txt' })
  if (!miss.isError) fail('missing read should be tool error'); else pass('read_file missing -> isError')

  const badParams = await rpc(PORT, 'tools/call', { name: 'read_file', arguments: {} })
  if (badParams.body?.error?.code !== -32602) fail(`missing path expected -32602, got ${JSON.stringify(badParams.body)}`); else pass('read_file without path -> -32602')

  // ---- Part C: ACL enforcement ----
  console.log('\nPART C: ACL enforcement\n')
  await callTool(PORT, 'create_folder', { path: '/vault' })
  setAcl(store, '/vault', [{ principal: 'alice', rights: ['read', 'write', 'admin'] }, { principal: 'staff', rights: ['read'] }])
  pass('ACL on /vault (alice=rwx, staff=read)')

  const aw = await callTool(PORT, 'write_file', { path: '/vault/secret.txt', content: 'top secret' }, 'alice:secret')
  if (aw.isError) fail(`alice write: ${aw.text}`); else pass('alice write_file /vault/secret.txt -> ok')

  const anon = await callTool(PORT, 'read_file', { path: '/vault/secret.txt' })
  if (!anon.isError && anon.httpStatus !== 401) fail(`anon read expected denial, got ${anon.httpStatus}`); else pass('anonymous read -> denied')

  const bob = await callTool(PORT, 'read_file', { path: '/vault/secret.txt' }, 'bob:hunter2')
  if (!bob.isError) fail('bob (out of ACL) read should be denied'); else pass('bob (out of ACL) read -> isError')

  const cr = await callTool(PORT, 'read_file', { path: '/vault/secret.txt' }, 'carol:pw')
  const cw = await callTool(PORT, 'write_file', { path: '/vault/nope.txt', content: 'x' }, 'carol:pw')
  if (cr.isError || cr.text !== 'top secret') fail(`carol read: ${cr.text}`); else pass('carol (staff) read -> ok')
  if (!cw.isError) fail('carol write should be denied'); else pass('carol (staff) write -> denied')

  const bobSearch = await callTool(PORT, 'search', { query: 'secret.txt' }, 'bob:hunter2')
  if ((bobSearch.json?.results ?? []).some((r) => r.path === '/vault/secret.txt')) fail('search leaked secret.txt to bob'); else pass('search as bob hides /vault/secret.txt (ACL-filtered)')

  const aliceSearch = await callTool(PORT, 'search', { query: 'secret.txt' }, 'alice:secret')
  if (!(aliceSearch.json?.results ?? []).some((r) => r.path === '/vault/secret.txt')) fail('search should surface secret.txt to alice'); else pass('search as alice surfaces /vault/secret.txt')

  const bad = await rpc(PORT, 'tools/list', {}, 'alice:wrong')
  if (bad.httpStatus !== 401) fail(`bad password expected 401, got ${bad.httpStatus}`); else pass('wrong password -> 401')
} catch (err) {
  fail(`harness error: ${err?.stack ?? err}`)
} finally {
  mcp.stop()
  store.close()
  try { rmSync(tmp, { recursive: true, force: true }) } catch { /* ignore */ }
  console.log(`\n${failed ? 'MCP UNIT: FAIL x' : 'MCP UNIT: PASS ok'}\n`)
  process.exit(failed ? 1 : 0)
}
