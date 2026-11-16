/**
 * Phase 3 acceptance harness (DETAILED_DESIGN §17) — availability & self-healing.
 *
 * Part 1 (self-heal, always runs):
 *   - 4-node cluster, a file written to a replica reaches RF=2.
 *   - Kill the ACTIVE (non-Primary) holder. Prove:
 *       (a) NO DATA LOSS — a survivor still returns the exact original bytes;
 *       (b) PROMOTION    — the surviving backup becomes the new active;
 *       (c) RECONCILE    — RF=2 is rebuilt on a fresh healthy node, with no
 *                          location still pointing at the dead peer.
 *
 * Part 2 (leader election, set P2P_PHASE3_ELECTION!=0):
 *   - Kill the Primary. Prove a new Primary is elected, the cluster converges,
 *     and it still accepts a new write and serves both old + new files.
 *
 * Modes:
 *   - default: spawns local `node dist/index.js` processes (build first).
 *   - external: P2P_NO_SPAWN=1 P2P_ADMIN_PORTS="8081,8082,8083" runs the
 *     read-only baseline checks against a running cluster (kills are skipped).
 *
 * Usage:  node test/phase3/run.mjs
 */

import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { rmSync, mkdirSync } from 'node:fs'
import { randomBytes, createHash } from 'node:crypto'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = join(__dirname, '..', '..')
const ENTRY = join(ROOT, 'dist', 'index.js')
const TEST_DATA = join(ROOT, '.test-data-p3')

const NO_SPAWN = process.env.P2P_NO_SPAWN === '1'
const RUN_ELECTION = process.env.P2P_PHASE3_ELECTION !== '0'
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
    PUSH_INTERVAL_MS: '400',
    HEARTBEAT_INTERVAL_MS: '1500',
    RECONCILE_INTERVAL_MS: '1500'
    // health thresholds derive from the heartbeat: suspect ~2.25s, down ~4.5s
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
    await sleep(500)
  }
  return { ok: false, last }
}

/** All ports agree there are `expected` fully-replicated locations (state=ok, active!=backup). */
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
    } catch { /* propagating */ }
    await sleep(500)
  }
  return { ok: false, last }
}

/**
 * All ALIVE ports agree there are `expected` locations back at RF=2 with NO
 * reference to `deadPeerId` (proves promotion + reconcile removed the dead node).
 */
async function waitLocationsHealed(ports, expected, deadPeerId, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  let last = {}
  while (Date.now() < deadline) {
    try {
      const all = await Promise.all(ports.map((p) => getJson(p, '/admin/locations')))
      const healed = (locs) =>
        locs.length === expected &&
        locs.every(
          (l) =>
            l.state === 'ok' &&
            l.backup_peer &&
            l.active_peer &&
            l.active_peer !== l.backup_peer &&
            l.active_peer !== deadPeerId &&
            l.backup_peer !== deadPeerId
        )
      last = Object.fromEntries(
        ports.map((p, i) => {
          const locs = all[i].locations
          const bad = locs.filter((l) => l.active_peer === deadPeerId || l.backup_peer === deadPeerId).length
          return [p, `${locs.length} locs, ok=${locs.filter((l) => l.state === 'ok').length}, refDead=${bad}`]
        })
      )
      if (all.every((r) => healed(r.locations))) return { ok: true, locations: all[0].locations }
    } catch { /* propagating */ }
    await sleep(500)
  }
  return { ok: false, last }
}

/** Poll observer /admin/health until it classifies `peerId` as down. */
async function waitPeerDown(observerPort, peerId, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const h = await getJson(observerPort, '/admin/health')
      if ((h.peers ?? []).some((p) => p.peerId === peerId && p.state === 'down')) return true
    } catch { /* starting */ }
    await sleep(400)
  }
  return false
}

/** Read `path` from `port`, retrying briefly, and check the sha matches. */
async function readCorrect(port, path, expectedSha, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  let lastStatus = 0
  while (Date.now() < deadline) {
    const r = await getFile(port, path)
    if (r.ok && sha256(r.bytes) === expectedSha) return { ok: true, bytes: r.bytes }
    lastStatus = r.status ?? 0
    await sleep(500)
  }
  return { ok: false, status: lastStatus }
}

/** Poll alive ports until a NEW Primary (peerId != oldPrimaryId) is agreed and metadata converges. */
async function waitNewPrimary(ports, oldPrimaryId, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  let last = {}
  while (Date.now() < deadline) {
    try {
      const statuses = await Promise.all(ports.map(getStatus))
      const primaries = statuses.filter((s) => s.isPrimary).map((s) => s.peerId)
      const uniquePrimary = new Set(primaries)
      const shas = statuses.map((s) => s.sha256)
      last = Object.fromEntries(statuses.map((s) => [s.name, `${s.isPrimary ? 'PRIMARY ' : ''}${s.sha256.slice(0, 8)}@lsn${s.lsn}`]))
      const newP = [...uniquePrimary].find((p) => p && p !== oldPrimaryId)
      const everyoneAgrees = statuses.every(
        (s) => s.isPrimary || (s.peers ?? []).some((p) => p.isPrimary && p.peerId?.slice(-8) === newP?.slice(-8))
      )
      if (uniquePrimary.size === 1 && newP && shas.every((s) => s && s === shas[0]) && everyoneAgrees) {
        return { ok: true, primaryId: newP, lsn: statuses[0].lsn }
      }
    } catch { /* electing */ }
    await sleep(500)
  }
  return { ok: false, last }
}

async function main() {
  const filePath = '/data/blob.bin'
  const original = randomBytes(Math.max(CHUNK_SIZE * 3 + 12345, 200000))
  const originalSha = sha256(original)

  let ports, primaryPort, writerPort
  if (NO_SPAWN) {
    ports = (process.env.P2P_ADMIN_PORTS ?? '8081,8082,8083').split(',').map((s) => Number(s.trim()))
    primaryPort = ports[0]
    writerPort = ports[1] ?? ports[0]
    log('external mode - baseline checks only (kills skipped) on ports', ports.join(', '))
  } else {
    rmSync(TEST_DATA, { recursive: true, force: true })
    mkdirSync(TEST_DATA, { recursive: true })
    log(`spawning local 4-node cluster (chunk ${CHUNK_SIZE}B, file ${original.length}B)...`)
    spawnNode({ name: 'anchor1', adminPort: 8101, listenPort: 4301, primary: true })
    spawnNode({ name: 'node2', adminPort: 8102, listenPort: 4302, primary: false })
    spawnNode({ name: 'node3', adminPort: 8103, listenPort: 4303, primary: false })
    spawnNode({ name: 'node4', adminPort: 8104, listenPort: 4304, primary: false })
    ports = [8101, 8102, 8103, 8104]
    primaryPort = 8101
    writerPort = 8102 // node2, a replica -> becomes the ACTIVE holder
  }

  log('waiting for nodes to become reachable...')
  if (!(await waitReachable(ports, 30000))) return fail('nodes did not become reachable')

  log('waiting for cluster to form + metadata converge...')
  const c0 = await waitConverged(ports, 30000)
  if (!c0.ok) return fail(`initial convergence failed: ${JSON.stringify(c0.last)}`)
  pass(`cluster converged: sha=${c0.sha.slice(0, 8)} lsn=${c0.lsn}`)

  const statuses = await Promise.all(ports.map(getStatus))
  const peerIdByPort = Object.fromEntries(ports.map((p, i) => [p, statuses[i].peerId]))
  const portByPeerId = Object.fromEntries(ports.map((p, i) => [statuses[i].peerId, p]))
  const nameByPort = Object.fromEntries(ports.map((p, i) => [p, statuses[i].name]))
  const nameByPeerId = (id) => nameByPort[portByPeerId[id]] ?? id.slice(-8)
  const writerPeerId = peerIdByPort[writerPort]
  const primaryPeerId = peerIdByPort[primaryPort]

  log('waiting for writer to learn the Primary via gossip...')
  if (!(await waitPrimaryKnown(writerPort, 15000))) return fail('writer never learned the Primary')

  // ---- Baseline: write -> RF=2 ---------------------------------------------
  log(`BASELINE: PUT ${filePath} (${original.length}B) to ${nameByPort[writerPort]} -> RF=2`)
  const put = await putFile(writerPort, filePath, original)
  if (put.error) return fail(`write failed: ${put.error}`)
  const expectedChunks = put.chunks
  pass(`write acked: chunks=${expectedChunks} fileId=${put.fileId} lsn=${put.lsn}`)

  const locOk = await waitLocationsOk(ports, expectedChunks, 30000)
  if (!locOk.ok) return fail(`baseline did not reach RF=2: ${JSON.stringify(locOk.last)}`)
  pass(`baseline RF=2: all ${expectedChunks} chunks state=ok, active!=backup`)

  const baselineBackupByChunk = Object.fromEntries(locOk.locations.map((l) => [l.chunk_hash, l.backup_peer]))
  const activeAllWriter = locOk.locations.every((l) => l.active_peer === writerPeerId)
  if (!activeAllWriter) return fail('baseline active_peer is not the writer for all chunks')
  pass(
    `baseline active == ${nameByPort[writerPort]} for all chunks; backups on ${[...new Set(Object.values(baselineBackupByChunk))]
      .map(nameByPeerId)
      .join(', ')}`
  )

  if (NO_SPAWN) {
    log('external mode: skipping node-kill tests (Parts 1 & 2)')
    return
  }

  // ---- PART 1: kill the ACTIVE (non-Primary) holder -> self-heal ------------
  log(`\nPART 1: kill ACTIVE holder ${nameByPort[writerPort]} -> no data loss + promote + reconcile\n`)

  const survivorPort = primaryPort // anchor1 stays alive
  const pre = await getFile(survivorPort, filePath)
  if (!pre.ok || sha256(pre.bytes) !== originalSha) return fail('survivor could not read the file before kill')
  pass(`pre-kill: ${nameByPort[survivorPort]} reads correct ${pre.bytes.length}B`)

  children.get(nameByPort[writerPort]).child.kill('SIGKILL')
  log(`killed ${nameByPort[writerPort]} (active holder for all chunks)`)

  const alivePorts = ports.filter((p) => p !== writerPort)

  // (a) NO DATA LOSS — survivor still serves the exact bytes (from the backup).
  const noLoss = await readCorrect(survivorPort, filePath, originalSha, 20000)
  if (!noLoss.ok) return fail(`DATA LOSS: survivor read failed (status ${noLoss.status})`)
  pass(`(a) NO DATA LOSS: ${nameByPort[survivorPort]} served correct ${noLoss.bytes.length}B after active died`)

  // Detection: at least the survivor classifies the dead node as down.
  if (!(await waitPeerDown(survivorPort, writerPeerId, 20000))) return fail('health detector never marked the dead node down')
  pass(`detector: ${nameByPort[survivorPort]} classified ${nameByPort[writerPort]} as DOWN`)

  // (b)+(c) PROMOTION + RECONCILE — RF=2 rebuilt, no location references the dead node.
  const healed = await waitLocationsHealed(alivePorts, expectedChunks, writerPeerId, 45000)
  if (!healed.ok) return fail(`self-heal did not restore RF=2: ${JSON.stringify(healed.last)}`)
  pass(`(c) RECONCILE: all ${expectedChunks} chunks back at RF=2 with no reference to the dead node`)

  // Promotion check: the new active is the former backup for each chunk.
  const promotedOk = healed.locations.every((l) => l.active_peer === baselineBackupByChunk[l.chunk_hash])
  if (!promotedOk) fail('(b) PROMOTION: new active is not the former backup for some chunk')
  else pass(`(b) PROMOTION: former backup is now active for all chunks`)

  // Physical: each chunk present on >= 2 ALIVE nodes.
  const chunksByPort = {}
  for (const p of alivePorts) chunksByPort[p] = new Set((await getJson(p, '/admin/chunks')).hashes)
  let physOk = true
  for (const l of healed.locations) {
    const holders = alivePorts.filter((p) => chunksByPort[p].has(l.chunk_hash))
    if (holders.length < 2) { physOk = false; fail(`chunk ${l.chunk_hash.slice(0, 12)} physically on only ${holders.length} alive node(s)`) }
  }
  if (physOk) pass(`physical: every chunk present on >= 2 ALIVE nodes on disk`)

  // Still readable after heal.
  const postHealRead = await readCorrect(survivorPort, filePath, originalSha, 10000)
  if (!postHealRead.ok) fail('read after self-heal failed')
  else pass(`post-heal: ${nameByPort[survivorPort]} still serves correct bytes`)

  // ---- PART 2: kill the Primary -> election --------------------------------
  if (!RUN_ELECTION) {
    log('\nPART 2 (election) skipped (P2P_PHASE3_ELECTION=0)\n')
    return
  }
  log(`\nPART 2: kill the PRIMARY ${nameByPort[primaryPort]} -> elect a new Primary\n`)

  const electorPorts = alivePorts.filter((p) => p !== primaryPort)
  children.get(nameByPort[primaryPort]).child.kill('SIGKILL')
  log(`killed ${nameByPort[primaryPort]} (the Primary)`)

  const elected = await waitNewPrimary(electorPorts, primaryPeerId, 45000)
  if (!elected.ok) return fail(`no new Primary elected: ${JSON.stringify(elected.last)}`)
  pass(`election: new Primary is ${nameByPeerId(elected.primaryId)}; cluster converged at lsn=${elected.lsn}`)

  // New Primary must accept writes and serve them.
  const newPrimaryPort = portByPeerId[elected.primaryId] ?? electorPorts[0]
  const writePort2 = electorPorts.find((p) => p !== newPrimaryPort) ?? newPrimaryPort
  if (!(await waitPrimaryKnown(writePort2, 15000))) return fail('surviving writer never learned the new Primary')
  const afterPath = '/data/after-election.txt'
  const afterBytes = Buffer.from('written under the newly elected primary\n')
  const afterSha = sha256(afterBytes)
  const put2 = await putFile(writePort2, afterPath, afterBytes, 'text/plain')
  if (put2.error) return fail(`write under new Primary failed: ${put2.error}`)
  pass(`write under new Primary acked: lsn=${put2.lsn}`)

  const read2 = await readCorrect(writePort2, afterPath, afterSha, 15000)
  if (!read2.ok) fail('could not read the file written under the new Primary')
  else pass(`read-back OK under new Primary`)

  const oldStill = await readCorrect(writePort2, filePath, originalSha, 10000)
  if (!oldStill.ok) fail('original file no longer readable after election')
  else pass(`original file still served correctly after two node failures`)
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
    console.log(`\n${failed ? 'PHASE 3: FAIL x' : 'PHASE 3: PASS ok'}\n`)
    process.exit(failed ? 1 : 0)
  })
