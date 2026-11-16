/**
 * Phase 2 acceptance harness (DETAILED_DESIGN §17) — content storage @ RF=2.
 *
 * Proves the content invariants end to end:
 *   A. A file written to a node is chunked, stored, and each chunk replicated to
 *      a SECOND node (active + backup, active != backup, state='ok'). RF=2.
 *   B. Read-repair: corrupting a chunk on the node that holds it does NOT corrupt
 *      the read — the good replica is fetched and the local copy is repaired.
 *   C. No data loss on a single-PC failure: killing the ACTIVE node still lets a
 *      surviving node return the exact original bytes (served from the backup).
 *
 * Modes:
 *   - default: spawns local `node dist/index.js` processes (build first) with a
 *     small CHUNK_SIZE so a modest file yields several chunks.
 *   - external: set P2P_ADMIN_PORTS="8081,8082,8083" P2P_NO_SPAWN=1 to run the
 *     assertions against an already-running cluster (e.g. docker compose). Test C
 *     (which kills a node) is skipped in external mode.
 *
 * Usage:  node test/phase2/run.mjs
 */

import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { rmSync, mkdirSync } from 'node:fs'
import { randomBytes, createHash } from 'node:crypto'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = join(__dirname, '..', '..')
const ENTRY = join(ROOT, 'dist', 'index.js')
const TEST_DATA = join(ROOT, '.test-data-p2')

const NO_SPAWN = process.env.P2P_NO_SPAWN === '1'
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE_BYTES ?? 65536)
const children = new Map() // name -> { child, adminPort }
let failed = false

const log = (...a) => console.log('[harness]', ...a)
const fail = (m) => { failed = true; console.error(`\n  x FAIL: ${m}\n`) }
const pass = (m) => console.log(`  ok ${m}`)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const sha256 = (buf) => createHash('sha256').update(buf).digest('hex')

async function getStatus(port) {
  const res = await fetch(`http://127.0.0.1:${port}/admin/status`)
  if (!res.ok) throw new Error(`status ${res.status}`)
  return res.json()
}
async function getJson(port, path) {
  const res = await fetch(`http://127.0.0.1:${port}${path}`)
  return res.json()
}
async function putFile(port, path, buf, contentType = 'application/octet-stream') {
  const res = await fetch(`http://127.0.0.1:${port}/admin/file?path=${encodeURIComponent(path)}`, {
    method: 'PUT',
    headers: { 'content-type': contentType },
    body: buf
  })
  return res.json()
}
async function getFile(port, path) {
  const res = await fetch(`http://127.0.0.1:${port}/admin/file?path=${encodeURIComponent(path)}`)
  if (!res.ok) return { ok: false, status: res.status }
  const ab = await res.arrayBuffer()
  return { ok: true, status: res.status, bytes: Buffer.from(ab), contentType: res.headers.get('content-type') }
}
async function postCorruptChunk(port, hash) {
  const res = await fetch(`http://127.0.0.1:${port}/admin/corrupt-chunk?hash=${hash}`, { method: 'POST' })
  return res.json()
}

function spawnNode({ name, adminPort, listenPort, primary }) {
  const dataDir = join(TEST_DATA, name)
  const env = {
    ...process.env,
    NODE_NO_WARNINGS: '1',
    NODE_NAME: name,
    NODE_ROLE: primary ? 'anchor' : 'ordinary',
    IS_PRIMARY: primary ? 'true' : 'false',
    LISTEN_PORT: String(listenPort),
    ADMIN_PORT: String(adminPort),
    DATA_DIR: dataDir,
    CHUNK_SIZE_BYTES: String(CHUNK_SIZE),
    PUSH_INTERVAL_MS: '500',
    HEARTBEAT_INTERVAL_MS: '2000'
  }
  const child = spawn(process.execPath, [ENTRY], { env, stdio: ['ignore', 'pipe', 'pipe'] })
  const tag = `[${name}]`
  child.stdout.on('data', (d) => process.stdout.write(prefix(tag, d)))
  child.stderr.on('data', (d) => process.stderr.write(prefix(tag, d)))
  child.on('exit', (code) => log(`${name} exited (${code})`))
  children.set(name, { child, adminPort })
  return { name, adminPort }
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

/** Poll until `port` knows who the Primary is (itself or a peer via gossip). */
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

/** Poll until every port reports an equal, non-empty sha256 (metadata converged). */
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
    await sleep(500)
  }
  return { ok: false, last }
}

/**
 * Poll every port's /admin/locations until all agree there are `expected`
 * chunk locations and each is fully replicated: state='ok', backup set, and
 * active != backup. Returns the location list from the first port.
 */
async function waitLocationsOk(ports, expected, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  let last = {}
  while (Date.now() < deadline) {
    try {
      const all = await Promise.all(ports.map((p) => getJson(p, '/admin/locations')))
      const healthy = (locs) =>
        locs.length === expected &&
        locs.every((l) => l.state === 'ok' && l.backup_peer && l.active_peer && l.active_peer !== l.backup_peer)
      last = Object.fromEntries(
        ports.map((p, i) => [p, `${all[i].locations.length} locs, ok=${all[i].locations.filter((l) => l.state === 'ok').length}`])
      )
      if (all.every((r) => healthy(r.locations))) return { ok: true, locations: all[0].locations }
    } catch { /* metadata still propagating */ }
    await sleep(500)
  }
  return { ok: false, last }
}

async function main() {
  let ports, primaryPort, writerPort
  const filePath = '/docs/report.bin'
  // Sized to yield several chunks at the (small) test CHUNK_SIZE. In external
  // mode the running cluster decides the real chunk count, so we treat the
  // server's reported `chunks` as the source of truth after the write.
  const original = randomBytes(Math.max(CHUNK_SIZE * 3 + 12345, 200000))
  const originalSha = sha256(original)
  const guessChunks = Math.ceil(original.length / CHUNK_SIZE)

  if (NO_SPAWN) {
    ports = (process.env.P2P_ADMIN_PORTS ?? '8081,8082,8083').split(',').map((s) => Number(s.trim()))
    primaryPort = ports[0]
    writerPort = ports[1] ?? ports[0] // write to a replica if we have one
    log('external mode - testing running cluster on ports', ports.join(', '))
  } else {
    rmSync(TEST_DATA, { recursive: true, force: true })
    mkdirSync(TEST_DATA, { recursive: true })
    log(`spawning local 3-node cluster (chunk size ${CHUNK_SIZE}B, file ${original.length}B -> ~${guessChunks} chunks)...`)
    spawnNode({ name: 'anchor1', adminPort: 8091, listenPort: 4201, primary: true })
    spawnNode({ name: 'node2', adminPort: 8092, listenPort: 4202, primary: false })
    spawnNode({ name: 'node3', adminPort: 8093, listenPort: 4203, primary: false })
    ports = [8091, 8092, 8093]
    primaryPort = 8091
    writerPort = 8092 // node2, a replica -> becomes the ACTIVE holder
  }

  log('waiting for nodes to become reachable...')
  if (!(await waitReachable(ports, 30000))) return fail('nodes did not become reachable')

  log('waiting for cluster to form + metadata converge...')
  const c0 = await waitConverged(ports, 30000)
  if (!c0.ok) return fail(`initial convergence failed: ${JSON.stringify(c0.last)}`)
  pass(`cluster converged: sha=${c0.sha.slice(0, 8)} lsn=${c0.lsn}`)

  // Map admin port <-> full peerId so we can resolve location peers to nodes.
  const statuses = await Promise.all(ports.map(getStatus))
  const peerIdByPort = Object.fromEntries(ports.map((p, i) => [p, statuses[i].peerId]))
  const portByPeerId = Object.fromEntries(ports.map((p, i) => [statuses[i].peerId, p]))
  const nameByPort = Object.fromEntries(ports.map((p, i) => [p, statuses[i].name]))
  const writerPeerId = peerIdByPort[writerPort]

  // The writer forwards its createFile intent to the Primary, so it must first
  // learn who the Primary is via cluster gossip (a heartbeat cycle or two).
  log('waiting for writer to learn the Primary via gossip...')
  if (!(await waitPrimaryKnown(writerPort, 15000))) return fail('writer never learned the Primary')

  // ---- Test A: write -> chunk -> RF=2 replication ---------------------------
  log(`TEST A: PUT ${filePath} (${original.length}B) to ${nameByPort[writerPort]} -> RF=2`)
  const put = await putFile(writerPort, filePath, original)
  if (put.error) return fail(`write failed: ${put.error}`)
  pass(`write acked: chunks=${put.chunks} fileId=${put.fileId} lsn=${put.lsn}`)
  const expectedChunks = put.chunks // server is the source of truth
  if (!(expectedChunks >= 1)) return fail(`write produced no chunks`)

  log('waiting for backup replication (1-copy window to close)...')
  const locOk = await waitLocationsOk(ports, expectedChunks, 30000)
  if (!locOk.ok) return fail(`chunks did not reach RF=2: ${JSON.stringify(locOk.last)}`)
  pass(`all ${expectedChunks} chunks at RF=2 (state=ok, active!=backup) on every node's view`)

  const locations = locOk.locations
  // active should be the writer; backup should be a different, real node.
  const activeIsWriter = locations.every((l) => l.active_peer === writerPeerId)
  if (!activeIsWriter) fail('active_peer is not the writer node for all chunks')
  else pass(`active_peer == writer (${nameByPort[writerPort]}) for all chunks`)

  const backups = new Set(locations.map((l) => l.backup_peer))
  const backupPorts = [...backups].map((b) => nameByPort[portByPeerId[b]] ?? b.slice(-8))
  pass(`backup copies placed on: ${backupPorts.join(', ')}`)

  // Physical cross-check: each chunk hash is present on >= 2 distinct nodes.
  const chunksByPort = {}
  for (const p of ports) chunksByPort[p] = new Set((await getJson(p, '/admin/chunks')).hashes)
  let rf2Physical = true
  for (const l of locations) {
    const holders = ports.filter((p) => chunksByPort[p].has(l.chunk_hash))
    if (holders.length < 2) { rf2Physical = false; fail(`chunk ${l.chunk_hash.slice(0, 12)} on only ${holders.length} node(s)`) }
  }
  if (rf2Physical) pass(`physical check: every chunk present on >= 2 nodes on disk`)

  // ---- Test B: read-repair on a corrupted local chunk -----------------------
  log('TEST B: corrupt a chunk on the active node -> read-repairs from backup')
  const victimHash = locations[0].chunk_hash
  const corrupt = await postCorruptChunk(writerPort, victimHash)
  if (!corrupt.ok) fail(`corrupt-chunk failed for ${victimHash.slice(0, 12)}`)
  else pass(`corrupted chunk ${victimHash.slice(0, 12)} on ${nameByPort[writerPort]}`)

  const repaired = await getFile(writerPort, filePath)
  if (!repaired.ok) fail(`read after corruption failed (status ${repaired.status})`)
  else if (sha256(repaired.bytes) !== originalSha) fail('read-repair returned WRONG bytes')
  else pass(`read-repair OK: ${nameByPort[writerPort]} returned correct ${repaired.bytes.length}B (sha matches)`)

  // ---- Test C: no data loss when the ACTIVE node dies -----------------------
  if (NO_SPAWN) {
    log('TEST C skipped in external mode (cannot kill a container from here)')
  } else {
    log(`TEST C: kill ACTIVE node (${nameByPort[writerPort]}) -> read from a survivor (no data loss)`)
    // Sanity: a survivor can read it while everyone is alive.
    const survivorPort = primaryPort // anchor1, always alive, fetches from backup
    const pre = await getFile(survivorPort, filePath)
    if (!pre.ok || sha256(pre.bytes) !== originalSha) fail('survivor could not read the file before kill')
    else pass(`survivor ${nameByPort[survivorPort]} reads correct bytes pre-kill`)

    const victim = children.get(nameByPort[writerPort])
    victim.child.kill('SIGKILL')
    log(`killed ${nameByPort[writerPort]} (active holder); waiting for disconnect...`)
    await sleep(4000)

    const after = await getFile(survivorPort, filePath)
    if (!after.ok) fail(`read after active-death failed (status ${after.status}) -> DATA LOSS`)
    else if (sha256(after.bytes) !== originalSha) fail('read after active-death returned WRONG bytes -> DATA LOSS')
    else pass(`NO DATA LOSS: ${nameByPort[survivorPort]} served correct ${after.bytes.length}B from backup after active died`)
  }
}

async function teardown() {
  if (NO_SPAWN) return
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
    console.log(`\n${failed ? 'PHASE 2: FAIL x' : 'PHASE 2: PASS ok'}\n`)
    process.exit(failed ? 1 : 0)
  })
