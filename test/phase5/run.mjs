/**
 * Phase 5 acceptance harness (DETAILED_DESIGN §12/§13) — WebDAV mount + ACL.
 *
 * Spawns a local 3-node cluster with the WebDAV surface enabled and drives it
 * with raw WebDAV verbs over HTTP (exactly what an OS mount client sends).
 *
 * Part A — protocol (on the world-writable default drive, via node2):
 *   OPTIONS advertises DAV 1,2; MKCOL; PUT (201 create / 204 overwrite);
 *   GET + HEAD byte-for-byte; PROPFIND Depth-1 multistatus; MOVE (rename);
 *   COPY; DELETE; and recursive DELETE of a non-empty folder.
 *
 * Part B — replication (RF=2 + cross-node):
 *   A multi-chunk PUT on node2 reaches RF=2, and the bytes + metadata are
 *   readable through node3's WebDAV port (content fetch + metadata replication).
 *
 * Part C — ACL enforcement (per-folder, replicated):
 *   With a non-`*` ACL on /private: anonymous => 401, an out-of-ACL user => 403,
 *   the owner => 200, a group member => read yes / write 403. Enforcement holds
 *   on a second node too (ACL replicates).
 *
 * Usage:  node test/phase5/run.mjs
 */

import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { rmSync, mkdirSync } from 'node:fs'
import { randomBytes, createHash } from 'node:crypto'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = join(__dirname, '..', '..')
const ENTRY = join(ROOT, 'dist', 'index.js')
const TEST_DATA = join(ROOT, '.test-data-p5')

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
async function getJson(port, path) {
  const res = await fetch(`http://127.0.0.1:${port}${path}`)
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

// ---- webdav (data plane) ----------------------------------------------------

/** One WebDAV request; returns status, headers and the body as buffer + text. */
async function dav(port, method, path, { headers = {}, body, auth, dest, overwrite, depth } = {}) {
  const h = { ...headers }
  if (auth) h['Authorization'] = basic(auth)
  if (dest !== undefined) h['Destination'] = `http://127.0.0.1:${port}${dest}`
  if (overwrite !== undefined) h['Overwrite'] = overwrite
  if (depth !== undefined) h['Depth'] = String(depth)
  const res = await fetch(`http://127.0.0.1:${port}${path}`, { method, headers: h, body })
  const buf = Buffer.from(await res.arrayBuffer())
  return { status: res.status, headers: res.headers, bytes: buf, text: buf.toString('utf8') }
}

// ---- cluster lifecycle ------------------------------------------------------

function spawnNode({ name, adminPort, webdavPort, listenPort, primary }) {
  const dataDir = join(TEST_DATA, name)
  const env = {
    ...process.env,
    NODE_NO_WARNINGS: '1',
    NODE_NAME: name,
    NODE_ROLE: primary ? 'anchor' : 'ordinary',
    IS_PRIMARY: primary ? 'true' : 'false',
    LISTEN_PORT: String(listenPort),
    ADMIN_PORT: String(adminPort),
    WEBDAV_PORT: String(webdavPort),
    WEBDAV_ENABLED: 'true',
    WEBDAV_USERS: USERS,
    WEBDAV_ALLOW_ANON: 'true',
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
  children.set(name, { child, adminPort, webdavPort })
  return { name, adminPort, webdavPort }
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
  log('spawning local 3-node cluster with WebDAV enabled...')
  spawnNode({ name: 'anchor1', adminPort: 8121, webdavPort: 8131, listenPort: 4321, primary: true })
  spawnNode({ name: 'node2', adminPort: 8122, webdavPort: 8132, listenPort: 4322, primary: false })
  spawnNode({ name: 'node3', adminPort: 8123, webdavPort: 8133, listenPort: 4323, primary: false })
  const admins = [8121, 8122, 8123]
  const DAV2 = 8132 // node2 WebDAV
  const DAV3 = 8133 // node3 WebDAV

  log('waiting for nodes to become reachable...')
  if (!(await waitReachable(admins, 30000))) return fail('nodes did not become reachable')
  const c0 = await waitConverged(admins, 30000)
  if (!c0.ok) return fail(`initial convergence failed: ${JSON.stringify(c0.last)}`)
  pass(`cluster converged: sha=${c0.sha.slice(0, 8)} lsn=${c0.lsn}`)
  if (!(await waitPrimaryKnown(DAV2 === 8132 ? 8122 : 8122, 15000))) return fail('node2 never learned the Primary')

  // ==== PART A: WebDAV protocol on the world-writable drive ==================
  log('\nPART A: WebDAV protocol (via node2:8132)\n')

  const opt = await dav(DAV2, 'OPTIONS', '/')
  const davHdr = opt.headers.get('dav') ?? ''
  if (opt.status !== 200 || !davHdr.includes('1') || !davHdr.includes('2')) {
    return fail(`OPTIONS did not advertise DAV 1,2 (status ${opt.status}, DAV="${davHdr}")`)
  }
  pass(`OPTIONS advertises DAV: ${davHdr}`)

  const mk = await dav(DAV2, 'MKCOL', '/pub')
  if (mk.status !== 201) return fail(`MKCOL /pub expected 201, got ${mk.status}`)
  pass('MKCOL /pub -> 201')

  const body1 = Buffer.from('hello webdav\n', 'utf8')
  const put1 = await dav(DAV2, 'PUT', '/pub/hello.txt', { body: body1, headers: { 'content-type': 'text/plain' } })
  if (put1.status !== 201) return fail(`PUT new file expected 201, got ${put1.status}`)
  pass('PUT /pub/hello.txt (new) -> 201')

  const put1b = await dav(DAV2, 'PUT', '/pub/hello.txt', { body: Buffer.from('hello again\n') })
  if (put1b.status !== 204) return fail(`PUT overwrite expected 204, got ${put1b.status}`)
  pass('PUT /pub/hello.txt (overwrite) -> 204')

  const get1 = await dav(DAV2, 'GET', '/pub/hello.txt')
  if (get1.status !== 200 || get1.text !== 'hello again\n') return fail(`GET body mismatch (status ${get1.status}, body ${JSON.stringify(get1.text)})`)
  pass('GET /pub/hello.txt -> 200 byte-for-byte')

  const head1 = await dav(DAV2, 'HEAD', '/pub/hello.txt')
  if (head1.status !== 200 || head1.headers.get('content-length') !== String(Buffer.byteLength('hello again\n'))) {
    return fail(`HEAD content-length wrong (status ${head1.status}, len ${head1.headers.get('content-length')})`)
  }
  pass('HEAD /pub/hello.txt -> 200 with correct Content-Length')

  const pf = await dav(DAV2, 'PROPFIND', '/pub', { depth: 1 })
  if (pf.status !== 207 || !pf.text.includes('hello.txt') || !pf.text.includes('<D:collection/>')) {
    return fail(`PROPFIND Depth-1 missing expected content (status ${pf.status})`)
  }
  pass('PROPFIND /pub Depth-1 -> 207 multistatus lists hello.txt + collection')

  const mv = await dav(DAV2, 'MOVE', '/pub/hello.txt', { dest: '/pub/renamed.txt' })
  if (mv.status !== 201) return fail(`MOVE expected 201, got ${mv.status}`)
  const gone = await dav(DAV2, 'GET', '/pub/hello.txt')
  const moved = await dav(DAV2, 'GET', '/pub/renamed.txt')
  if (gone.status !== 404 || moved.status !== 200 || moved.text !== 'hello again\n') {
    return fail(`MOVE result wrong (old ${gone.status}, new ${moved.status})`)
  }
  pass('MOVE /pub/hello.txt -> /pub/renamed.txt (old 404, new 200)')

  const cp = await dav(DAV2, 'COPY', '/pub/renamed.txt', { dest: '/pub/copy.txt' })
  const cpGet = await dav(DAV2, 'GET', '/pub/copy.txt')
  if (cp.status !== 201 || cpGet.text !== 'hello again\n') return fail(`COPY result wrong (status ${cp.status}, body ${JSON.stringify(cpGet.text)})`)
  pass('COPY /pub/renamed.txt -> /pub/copy.txt (both readable)')

  const del = await dav(DAV2, 'DELETE', '/pub/copy.txt')
  const delGet = await dav(DAV2, 'GET', '/pub/copy.txt')
  if (del.status !== 204 || delGet.status !== 404) return fail(`DELETE result wrong (del ${del.status}, get ${delGet.status})`)
  pass('DELETE /pub/copy.txt -> 204 (then 404)')

  // Recursive DELETE of a non-empty folder.
  await dav(DAV2, 'MKCOL', '/pub/sub')
  await dav(DAV2, 'PUT', '/pub/sub/a.txt', { body: Buffer.from('a') })
  const rdel = await dav(DAV2, 'DELETE', '/pub/sub')
  const rSub = await dav(DAV2, 'PROPFIND', '/pub/sub', { depth: 0 })
  const rChild = await dav(DAV2, 'GET', '/pub/sub/a.txt')
  if (rdel.status !== 204 || rSub.status !== 404 || rChild.status !== 404) {
    return fail(`recursive DELETE wrong (del ${rdel.status}, folder ${rSub.status}, child ${rChild.status})`)
  }
  pass('recursive DELETE /pub/sub (non-empty) -> 204 (folder + child gone)')

  // ==== PART B: RF=2 + cross-node visibility =================================
  log('\nPART B: replication (RF=2 + read/list from another node)\n')

  const bytesB = randomBytes(CHUNK_SIZE * 3 + 123) // 4 chunks
  const shaB = sha256(bytesB)
  await dav(DAV2, 'MKCOL', '/shared')
  const putB = await dav(DAV2, 'PUT', '/shared/blob.bin', { body: bytesB })
  if (putB.status !== 201) return fail(`PUT /shared/blob.bin expected 201, got ${putB.status}`)
  pass(`PUT /shared/blob.bin (${bytesB.length}B) on node2 -> 201`)

  const okB = await waitLocationsOk(admins, 4, 30000)
  if (!okB.ok) return fail(`blob did not reach RF=2: ${JSON.stringify(okB.last)}`)
  pass('blob at RF=2: every chunk state=ok, active!=backup (>=4 locations)')

  if (!(await waitConverged(admins, 15000)).ok) return fail('metadata did not converge after PUT')
  const getB3 = await dav(DAV3, 'GET', '/shared/blob.bin')
  if (getB3.status !== 200 || sha256(getB3.bytes) !== shaB) {
    return fail(`cross-node GET on node3 wrong (status ${getB3.status}, sha ${getB3.status === 200 ? sha256(getB3.bytes).slice(0, 8) : 'n/a'})`)
  }
  pass('cross-node GET /shared/blob.bin on node3 -> 200 byte-for-byte')

  const pfB3 = await dav(DAV3, 'PROPFIND', '/shared', { depth: 1 })
  if (pfB3.status !== 207 || !pfB3.text.includes('blob.bin')) return fail(`node3 PROPFIND missing blob.bin (status ${pfB3.status})`)
  pass('cross-node PROPFIND /shared on node3 lists blob.bin')

  // ==== PART C: ACL enforcement (replicated) ================================
  log('\nPART C: ACL enforcement (per-folder, replicated)\n')

  const mkP = await dav(DAV2, 'MKCOL', '/private', { auth: 'alice:secret' })
  if (mkP.status !== 201) return fail(`MKCOL /private expected 201, got ${mkP.status}`)
  // alice: full; staff group: read-only. No '*' => anonymous locked out.
  const acl = await setAcl(8121, '/private', [
    { principal: 'alice', rights: ['read', 'write', 'admin'] },
    { principal: 'staff', rights: ['read'] }
  ])
  if (acl.status !== 200) return fail(`setAcl failed (${acl.status}): ${JSON.stringify(acl.body)}`)
  pass('POST /admin/acl on /private (alice=rwx, staff=read)')

  // ACL is metadata — let it replicate to every node.
  if (!(await waitConverged(admins, 15000)).ok) return fail('ACL did not converge')

  // alice can write.
  const aPut = await dav(DAV2, 'PUT', '/private/secret.txt', { body: Buffer.from('top secret'), auth: 'alice:secret' })
  if (aPut.status !== 201) return fail(`alice PUT expected 201, got ${aPut.status}`)
  pass('alice PUT /private/secret.txt -> 201')

  // anonymous is challenged (401).
  const anon = await dav(DAV2, 'GET', '/private/secret.txt')
  if (anon.status !== 401 || !(anon.headers.get('www-authenticate') ?? '').includes('Basic')) {
    return fail(`anonymous GET expected 401+challenge, got ${anon.status}`)
  }
  pass('anonymous GET /private/secret.txt -> 401 (WWW-Authenticate)')

  // bob is authenticated but not in the ACL => 403.
  const bob = await dav(DAV2, 'GET', '/private/secret.txt', { auth: 'bob:hunter2' })
  if (bob.status !== 403) return fail(`bob GET expected 403, got ${bob.status}`)
  pass('bob (out of ACL) GET /private/secret.txt -> 403')

  // carol is in group staff => can read, cannot write.
  const carolGet = await dav(DAV2, 'GET', '/private/secret.txt', { auth: 'carol:pw' })
  const carolPut = await dav(DAV2, 'PUT', '/private/nope.txt', { body: Buffer.from('x'), auth: 'carol:pw' })
  if (carolGet.status !== 200 || carolGet.text !== 'top secret') return fail(`carol read expected 200, got ${carolGet.status}`)
  if (carolPut.status !== 403) return fail(`carol write expected 403, got ${carolPut.status}`)
  pass('carol (group staff) GET -> 200, PUT -> 403 (read-only)')

  // Enforcement holds on another node too (ACL replicated).
  const anon3 = await dav(DAV3, 'GET', '/private/secret.txt')
  const alice3 = await dav(DAV3, 'GET', '/private/secret.txt', { auth: 'alice:secret' })
  if (anon3.status !== 401) return fail(`node3 anonymous GET expected 401, got ${anon3.status}`)
  if (alice3.status !== 200 || alice3.text !== 'top secret') return fail(`node3 alice GET expected 200, got ${alice3.status}`)
  pass('ACL enforced on node3 too: anon -> 401, alice -> 200')

  // Bad password is rejected outright.
  const badpw = await dav(DAV2, 'GET', '/private/secret.txt', { auth: 'alice:wrong' })
  if (badpw.status !== 401) return fail(`bad password expected 401, got ${badpw.status}`)
  pass('wrong password -> 401')
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
    console.log(`\n${failed ? 'PHASE 5: FAIL x' : 'PHASE 5: PASS ok'}\n`)
    process.exit(failed ? 1 : 0)
  })
