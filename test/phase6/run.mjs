/**
 * Phase 6 acceptance harness (DETAILED_DESIGN §14) — MCP endpoint for AI agents.
 *
 * Spawns a local 3-node cluster with the MCP surface enabled and drives it with
 * raw MCP JSON-RPC 2.0 over HTTP — exactly what an MCP client (Claude Desktop,
 * the MCP Inspector, an in-house agent) sends.
 *
 * Part A — protocol handshake + discovery (via node2):
 *   initialize returns a protocolVersion + serverInfo; tools/list advertises the
 *   six tools; ping answers.
 *
 * Part B — metadata + content tools (create/list/stat/read/search/write):
 *   create_folder + write_file, then list_folder / stat_file / read_file round-trip
 *   byte-for-byte, and search finds the file by path.
 *
 * Part C — RF=2 + cross-node visibility:
 *   a multi-chunk write reaches RF=2 and read_file on node3 returns the same bytes.
 *
 * Part D — ACL enforcement (per-folder, replicated):
 *   with a non-`*` ACL, anonymous => 401, an out-of-ACL user => tool error,
 *   the owner => ok, a read-only group member => read ok / write denied; search
 *   hides files the caller may not read; enforcement holds on a second node.
 *
 * Usage:  node test/phase6/run.mjs   (after `npm run build`)
 */

import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { rmSync, mkdirSync } from 'node:fs'
import { randomBytes, createHash } from 'node:crypto'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = join(__dirname, '..', '..')
const ENTRY = join(ROOT, 'dist', 'index.js')
const TEST_DATA = join(ROOT, '.test-data-p6')

const CHUNK_SIZE = 65536
const USERS = 'alice:secret:staff,bob:hunter2,carol:pw:staff'

const children = new Map()
let failed = false

const log = (...a) => console.log('[harness]', ...a)
const fail = (m) => { failed = true; console.error(`\n  x FAIL: ${m}\n`) }
const pass = (m) => console.log(`  ok ${m}`)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const sha256 = (buf) => createHash('sha256').update(buf).digest('hex')
const basic = (creds) => 'Basic ' + Buffer.from(creds).toString('base64')

// ---- admin (control plane) --------------------------------------------------

async function getStatus(port) {
  const res = await fetch(`http://127.0.0.1:${port}/admin/status`)
  if (!res.ok) throw new Error(`status ${res.status}`)
  return res.json()
}
async function setAcl(adminPort, path, rules) {
  const res = await fetch(`http://127.0.0.1:${adminPort}/admin/acl`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ path, rules })
  })
  return { status: res.status, body: await res.json() }
}
async function getJson(port, path) {
  const res = await fetch(`http://127.0.0.1:${port}${path}`)
  return res.json()
}

// ---- mcp (data plane) -------------------------------------------------------

let rpcId = 0

/** One MCP JSON-RPC request over HTTP POST. Returns { httpStatus, body }. */
async function rpc(port, method, params, { auth } = {}) {
  const headers = { 'content-type': 'application/json', accept: 'application/json' }
  if (auth) headers['authorization'] = basic(auth)
  const res = await fetch(`http://127.0.0.1:${port}/`, {
    method: 'POST',
    headers,
    body: JSON.stringify({ jsonrpc: '2.0', id: ++rpcId, method, params })
  })
  let body = null
  const text = await res.text()
  if (text) { try { body = JSON.parse(text) } catch { body = text } }
  return { httpStatus: res.status, body }
}

/** Call a tool; returns { httpStatus, rpcError, isError, text, json }. */
async function callTool(port, name, args, opts) {
  const { httpStatus, body } = await rpc(port, 'tools/call', { name, arguments: args }, opts)
  const result = body && body.result
  const text = result && Array.isArray(result.content) ? result.content.map((c) => c.text).join('') : ''
  let json = null
  try { json = JSON.parse(text) } catch { /* not json */ }
  return { httpStatus, rpcError: body && body.error, isError: !!(result && result.isError), text, json }
}

// ---- cluster lifecycle ------------------------------------------------------

function spawnNode({ name, adminPort, mcpPort, listenPort, primary }) {
  const dataDir = join(TEST_DATA, name)
  const env = {
    ...process.env,
    NODE_NO_WARNINGS: '1',
    NODE_NAME: name,
    NODE_ROLE: primary ? 'anchor' : 'ordinary',
    IS_PRIMARY: primary ? 'true' : 'false',
    LISTEN_PORT: String(listenPort),
    ADMIN_PORT: String(adminPort),
    MCP_ENABLED: 'true',
    MCP_PORT: String(mcpPort),
    MCP_USERS: USERS,
    MCP_ALLOW_ANON: 'true',
    DATA_DIR: dataDir,
    CHUNK_SIZE_BYTES: String(CHUNK_SIZE),
    REBALANCE_ENABLED: 'false',
    PUSH_INTERVAL_MS: '400',
    HEARTBEAT_INTERVAL_MS: '1500'
  }
  const child = spawn(process.execPath, [ENTRY], { env, stdio: ['ignore', 'pipe', 'pipe'] })
  const tag = `[${name}]`
  child.stdout.on('data', (d) => process.stdout.write(prefix(tag, d)))
  child.stderr.on('data', (d) => process.stderr.write(prefix(tag, d)))
  child.on('exit', (code) => log(`${name} exited (${code})`))
  children.set(name, { child, adminPort, mcpPort })
  return { name, adminPort, mcpPort }
}

function prefix(tag, buf) {
  return String(buf).split('\n').filter((l) => l.length > 0).map((l) => `${tag} ${l}\n`).join('')
}

async function waitReachable(ports, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try { await Promise.all(ports.map(getStatus)); return true } catch { await sleep(500) }
  }
  return false
}
async function waitPrimaryKnown(port, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const s = await getStatus(port)
      if (s.isPrimary || (s.peers ?? []).some((p) => p.isPrimary)) return true
    } catch { /* starting */ }
    await sleep(300)
  }
  return false
}
async function waitConverged(ports, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  let last = {}
  while (Date.now() < deadline) {
    try {
      const statuses = await Promise.all(ports.map(getStatus))
      const shas = statuses.map((s) => s.sha256)
      last = Object.fromEntries(statuses.map((s) => [s.name, `${s.sha256.slice(0, 8)}@lsn${s.lsn}`]))
      if (shas.every((s) => s && s === shas[0])) return { ok: true, sha: shas[0], lsn: statuses[0].lsn }
    } catch { /* still starting */ }
    await sleep(400)
  }
  return { ok: false, last }
}
async function waitLocationsOk(ports, minCount, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  let last = {}
  while (Date.now() < deadline) {
    try {
      const all = await Promise.all(ports.map((p) => getJson(p, '/admin/locations')))
      const healthy = (locs) =>
        locs.length >= minCount &&
        locs.every((l) => l.state === 'ok' && l.backup_peer && l.active_peer && l.active_peer !== l.backup_peer)
      last = Object.fromEntries(ports.map((p, i) => [p, `${all[i].locations.length} locs`]))
      if (all.every((r) => healthy(r.locations))) return { ok: true }
    } catch { /* propagating */ }
    await sleep(500)
  }
  return { ok: false, last }
}

async function main() {
  rmSync(TEST_DATA, { recursive: true, force: true })
  mkdirSync(TEST_DATA, { recursive: true })
  log('spawning local 3-node cluster with MCP enabled...')
  spawnNode({ name: 'anchor1', adminPort: 8161, mcpPort: 8171, listenPort: 4361, primary: true })
  spawnNode({ name: 'node2', adminPort: 8162, mcpPort: 8172, listenPort: 4362, primary: false })
  spawnNode({ name: 'node3', adminPort: 8163, mcpPort: 8173, listenPort: 4363, primary: false })
  const admins = [8161, 8162, 8163]
  const MCP2 = 8172 // node2 MCP
  const MCP3 = 8173 // node3 MCP

  log('waiting for nodes to become reachable...')
  if (!(await waitReachable(admins, 30000))) return fail('nodes did not become reachable')
  const c0 = await waitConverged(admins, 30000)
  if (!c0.ok) return fail(`initial convergence failed: ${JSON.stringify(c0.last)}`)
  pass(`cluster converged: sha=${c0.sha.slice(0, 8)} lsn=${c0.lsn}`)
  if (!(await waitPrimaryKnown(8162, 15000))) return fail('node2 never learned the Primary')

  // ==== PART A: MCP handshake + discovery ===================================
  log('\nPART A: MCP protocol handshake (via node2:8172)\n')

  const init = await rpc(MCP2, 'initialize', { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'harness', version: '0' } })
  if (init.httpStatus !== 200 || !init.body?.result?.serverInfo?.name || !init.body?.result?.protocolVersion) {
    return fail(`initialize wrong: ${JSON.stringify(init.body)}`)
  }
  pass(`initialize -> serverInfo=${init.body.result.serverInfo.name} protocol=${init.body.result.protocolVersion}`)

  // notifications/initialized has no id and must get no JSON-RPC body.
  const notif = await fetch(`http://127.0.0.1:${MCP2}/`, {
    method: 'POST', headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' })
  })
  if (notif.status !== 202) return fail(`notifications/initialized expected 202, got ${notif.status}`)
  pass('notifications/initialized -> 202 (no response body)')

  const list = await rpc(MCP2, 'tools/list', {})
  const toolNames = (list.body?.result?.tools ?? []).map((t) => t.name).sort()
  const expected = ['create_folder', 'list_folder', 'read_file', 'search', 'stat_file', 'write_file']
  if (JSON.stringify(toolNames) !== JSON.stringify(expected)) {
    return fail(`tools/list mismatch: got ${JSON.stringify(toolNames)}`)
  }
  pass(`tools/list advertises: ${toolNames.join(', ')}`)

  const ping = await rpc(MCP2, 'ping', {})
  if (ping.httpStatus !== 200 || ping.body?.error) return fail(`ping failed: ${JSON.stringify(ping.body)}`)
  pass('ping -> ok')

  // ==== PART B: metadata + content tools ====================================
  log('\nPART B: create / write / list / stat / read / search\n')

  const mk = await callTool(MCP2, 'create_folder', { path: '/agent' })
  if (mk.isError || !mk.json?.ok) return fail(`create_folder failed: ${mk.text}`)
  pass('create_folder /agent -> ok')

  const text = 'hello from an AI agent\n'
  const wr = await callTool(MCP2, 'write_file', { path: '/agent/note.txt', content: text, contentType: 'text/plain' })
  if (wr.isError || !wr.json?.ok) return fail(`write_file failed: ${wr.text}`)
  pass(`write_file /agent/note.txt -> ok (${wr.json.size}B, ${wr.json.chunks} chunk(s))`)

  const ls = await callTool(MCP2, 'list_folder', { path: '/agent' })
  if (ls.isError || !ls.json?.files?.some((f) => f.name === 'note.txt')) return fail(`list_folder missing note.txt: ${ls.text}`)
  pass('list_folder /agent lists note.txt')

  const st = await callTool(MCP2, 'stat_file', { path: '/agent/note.txt' })
  if (st.isError || st.json?.kind !== 'file' || st.json?.size !== Buffer.byteLength(text)) return fail(`stat_file wrong: ${st.text}`)
  pass(`stat_file /agent/note.txt -> file, size=${st.json.size}`)

  const rd = await callTool(MCP2, 'read_file', { path: '/agent/note.txt' })
  if (rd.isError || rd.text !== text) return fail(`read_file mismatch: ${JSON.stringify(rd.text)}`)
  pass('read_file /agent/note.txt -> byte-for-byte')

  const sr = await callTool(MCP2, 'search', { query: 'note.txt' })
  if (sr.isError || !sr.json?.results?.some((r) => r.path === '/agent/note.txt')) return fail(`search missed note.txt: ${sr.text}`)
  pass('search "note.txt" finds /agent/note.txt')

  // read_file on a missing file is a tool error, not a transport error.
  const miss = await callTool(MCP2, 'read_file', { path: '/agent/nope.txt' })
  if (!miss.isError) return fail('read_file of missing file should be a tool error')
  pass('read_file missing file -> isError (graceful)')

  // ==== PART C: RF=2 + cross-node visibility ================================
  log('\nPART C: RF=2 + read from another node\n')

  const bin = randomBytes(CHUNK_SIZE * 3 + 77) // 4 chunks
  const shaBin = sha256(bin)
  const wrBin = await callTool(MCP2, 'write_file', { path: '/agent/blob.bin', content: bin.toString('base64'), encoding: 'base64' })
  if (wrBin.isError || !wrBin.json?.ok) return fail(`write_file (base64) failed: ${wrBin.text}`)
  pass(`write_file /agent/blob.bin (${bin.length}B base64) -> ok`)

  const okC = await waitLocationsOk(admins, 4, 30000)
  if (!okC.ok) return fail(`blob did not reach RF=2: ${JSON.stringify(okC.last)}`)
  pass('blob at RF=2: every chunk state=ok, active!=backup (>=4 locations)')

  if (!(await waitConverged(admins, 15000)).ok) return fail('metadata did not converge after write')
  const rd3 = await callTool(MCP3, 'read_file', { path: '/agent/blob.bin', encoding: 'base64' })
  if (rd3.isError || sha256(Buffer.from(rd3.text, 'base64')) !== shaBin) return fail(`cross-node read on node3 wrong: ${rd3.text.slice(0, 60)}`)
  pass('cross-node read_file /agent/blob.bin on node3 -> byte-for-byte')

  const ls3 = await callTool(MCP3, 'list_folder', { path: '/agent' })
  if (ls3.isError || !ls3.json?.files?.some((f) => f.name === 'blob.bin')) return fail(`node3 list_folder missing blob.bin: ${ls3.text}`)
  pass('cross-node list_folder /agent on node3 lists blob.bin')

  // ==== PART D: ACL enforcement (replicated) ================================
  log('\nPART D: ACL enforcement (per-folder, replicated)\n')

  const mkP = await callTool(MCP2, 'create_folder', { path: '/vault' }, { auth: 'alice:secret' })
  if (mkP.isError) return fail(`create_folder /vault failed: ${mkP.text}`)
  const acl = await setAcl(8161, '/vault', [
    { principal: 'alice', rights: ['read', 'write', 'admin'] },
    { principal: 'staff', rights: ['read'] }
  ])
  if (acl.status !== 200) return fail(`setAcl failed (${acl.status}): ${JSON.stringify(acl.body)}`)
  if (!(await waitConverged(admins, 15000)).ok) return fail('ACL did not converge')
  pass('ACL on /vault (alice=rwx, staff=read) converged')

  // alice can write.
  const aw = await callTool(MCP2, 'write_file', { path: '/vault/secret.txt', content: 'top secret', encoding: 'utf8' }, { auth: 'alice:secret' })
  if (aw.isError) return fail(`alice write denied: ${aw.text}`)
  pass('alice write_file /vault/secret.txt -> ok')

  // anonymous is blocked: MCP_ALLOW_ANON only lets anonymous *reach* the ACL; a
  // non-`*` ACL then blocks the read (tool error, or 401 at the transport).
  const anon = await callTool(MCP2, 'read_file', { path: '/vault/secret.txt' })
  if (!anon.isError && anon.httpStatus !== 401) return fail(`anonymous read expected denial, got ${anon.httpStatus}/${anon.text}`)
  pass('anonymous read_file /vault/secret.txt -> denied')

  // bob authenticates but is out of the ACL => tool error.
  const bob = await callTool(MCP2, 'read_file', { path: '/vault/secret.txt' }, { auth: 'bob:hunter2' })
  if (!bob.isError) return fail('bob (out of ACL) read should be denied')
  pass('bob (out of ACL) read_file -> isError (access denied)')

  // carol is group staff => read yes, write no.
  const cr = await callTool(MCP2, 'read_file', { path: '/vault/secret.txt' }, { auth: 'carol:pw' })
  const cw = await callTool(MCP2, 'write_file', { path: '/vault/nope.txt', content: 'x' }, { auth: 'carol:pw' })
  if (cr.isError || cr.text !== 'top secret') return fail(`carol read expected ok, got ${cr.text}`)
  if (!cw.isError) return fail('carol write should be denied')
  pass('carol (group staff) read -> ok, write -> denied')

  // search hides files carol/bob may not read but shows those they can.
  const bobSearch = await callTool(MCP2, 'search', { query: 'secret.txt' }, { auth: 'bob:hunter2' })
  if (bobSearch.isError || (bobSearch.json?.results ?? []).some((r) => r.path === '/vault/secret.txt')) {
    return fail(`search leaked /vault/secret.txt to bob: ${bobSearch.text}`)
  }
  pass('search as bob does NOT surface /vault/secret.txt (ACL-filtered)')

  // Bad password rejected at transport (401).
  const bad = await rpc(MCP2, 'tools/list', {}, { auth: 'alice:wrong' })
  if (bad.httpStatus !== 401) return fail(`bad password expected 401, got ${bad.httpStatus}`)
  pass('wrong password -> 401')

  // Enforcement holds on node3 too (ACL replicated).
  const anon3 = await callTool(MCP3, 'read_file', { path: '/vault/secret.txt' })
  const alice3 = await callTool(MCP3, 'read_file', { path: '/vault/secret.txt' }, { auth: 'alice:secret' })
  if (!anon3.isError && anon3.httpStatus !== 401) return fail(`node3 anonymous read expected denial, got ${anon3.httpStatus}`)
  if (alice3.isError || alice3.text !== 'top secret') return fail(`node3 alice read expected ok, got ${alice3.text}`)
  pass('ACL enforced on node3 too: anon -> denied, alice -> ok')
}

async function teardown() {
  log('tearing down...')
  for (const { child } of children.values()) {
    try { child.kill() } catch { /* ignore */ }
  }
  await sleep(1000)
  try { rmSync(TEST_DATA, { recursive: true, force: true }) } catch { /* ignore */ }
}

main()
  .catch((err) => fail(`harness error: ${err?.stack ?? err}`))
  .finally(async () => {
    await teardown()
    console.log(`\n${failed ? 'PHASE 6: FAIL x' : 'PHASE 6: PASS ok'}\n`)
    process.exit(failed ? 1 : 0)
  })
