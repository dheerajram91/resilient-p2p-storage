/**
 * Phase 4 acceptance harness (DETAILED_DESIGN §10) — capacity management.
 *
 * Part A (rebalance, two-threshold hysteresis):
 *   - 3-node cluster, each with a small equal quota (16 chunks). REBALANCE is
 *     disabled so the sweep is driven deterministically via POST /admin/rebalance.
 *   - Write a file that pushes the writer above the high-water mark (T1).
 *   - Trigger a rebalance sweep and prove:
 *       (a) the writer drops below the low-water mark (T2);
 *       (b) RF is preserved — every chunk is still at RF=2 (state=ok, active!=backup);
 *       (c) NO DATA LOSS — the file still reads back byte-for-byte;
 *       (d) NO OSCILLATION — a second sweep moves nothing (used is under T1).
 *
 * Part B (admission control):
 *   - An over-quota write is rejected up-front with HTTP 507 / code ENOSPC.
 *   - Filling the cluster eventually makes admission reject with 507 as well,
 *     and the Part-A file still reads correctly under a full cluster.
 *
 * Usage:  node test/phase4/run.mjs
 */

import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { rmSync, mkdirSync } from 'node:fs'
import { randomBytes, createHash } from 'node:crypto'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = join(__dirname, '..', '..')
const ENTRY = join(ROOT, 'dist', 'index.js')
const TEST_DATA = join(ROOT, '.test-data-p4')

const CHUNK_SIZE = Number(process.env.CHUNK_SIZE_BYTES ?? 65536)
const QUOTA_CHUNKS = 16
const QUOTA_BYTES = QUOTA_CHUNKS * CHUNK_SIZE
const HIGH = 0.85
const LOW = 0.7
const T1 = QUOTA_BYTES * HIGH // start offloading above this
const T2 = QUOTA_BYTES * LOW // stop offloading below this

const children = new Map() // name -> { child, adminPort }
let failed = false

const log = (...a) => console.log('[harness]', ...a)
const fail = (m) => { failed = true; console.error(`\n  x FAIL: ${m}\n`) }
const pass = (m) => console.log(`  ok ${m}`)
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const sha256 = (buf) => createHash('sha256').update(buf).digest('hex')
const chunksToBytes = (n) => n * CHUNK_SIZE

async function getStatus(port) {
  const res = await fetch(`http://127.0.0.1:${port}/admin/status`)
  if (!res.ok) throw new Error(`status ${res.status}`)
  return res.json()
}
async function getJson(port, path) {
  const res = await fetch(`http://127.0.0.1:${port}${path}`)
  return res.json()
}
async function getCapacity(port) {
  return getJson(port, '/admin/capacity')
}
/** PUT that returns BOTH the HTTP status and the parsed body (needed to see 507). */
async function putFileRaw(port, path, buf, contentType = 'application/octet-stream') {
  const res = await fetch(`http://127.0.0.1:${port}/admin/file?path=${encodeURIComponent(path)}`, {
    method: 'PUT',
    headers: { 'content-type': contentType },
    body: buf
  })
  let body = {}
  try { body = await res.json() } catch { /* ignore */ }
  return { status: res.status, body }
}
async function post(port, path) {
  const res = await fetch(`http://127.0.0.1:${port}${path}`, { method: 'POST' })
  return { status: res.status, body: await res.json() }
}
async function getFile(port, path) {
  const res = await fetch(`http://127.0.0.1:${port}/admin/file?path=${encodeURIComponent(path)}`)
  if (!res.ok) return { ok: false, status: res.status }
  const ab = await res.arrayBuffer()
  return { ok: true, status: res.status, bytes: Buffer.from(ab) }
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
    STORAGE_QUOTA_BYTES: String(QUOTA_BYTES),
    REBALANCE_ENABLED: 'false', // sweep is driven manually for determinism
    REBALANCE_HIGH: String(HIGH),
    REBALANCE_LOW: String(LOW),
    PUSH_INTERVAL_MS: '400',
    HEARTBEAT_INTERVAL_MS: '1500',
    RECONCILE_INTERVAL_MS: '0' // capacity-only test: no reconciler churn
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

/** This node's own used/free bytes, from its capacity report (self is nodes[0]). */
async function selfCapacity(port) {
  const cap = await getCapacity(port)
  return cap.nodes[0] // { peerId, used, quota, free, pct }
}

async function main() {
  rmSync(TEST_DATA, { recursive: true, force: true })
  mkdirSync(TEST_DATA, { recursive: true })
  log(
    `spawning local 3-node cluster (chunk ${CHUNK_SIZE}B, quota ${QUOTA_CHUNKS} chunks=${QUOTA_BYTES}B/node, ` +
    `T1=${Math.round(T1)}B T2=${Math.round(T2)}B)...`
  )
  spawnNode({ name: 'anchor1', adminPort: 8111, listenPort: 4311, primary: true })
  spawnNode({ name: 'node2', adminPort: 8112, listenPort: 4312, primary: false })
  spawnNode({ name: 'node3', adminPort: 8113, listenPort: 4313, primary: false })
  const ports = [8111, 8112, 8113]
  const writerPort = 8112 // node2 — a replica that becomes the ACTIVE holder

  log('waiting for nodes to become reachable...')
  if (!(await waitReachable(ports, 30000))) return fail('nodes did not become reachable')

  log('waiting for cluster to form + metadata converge...')
  const c0 = await waitConverged(ports, 30000)
  if (!c0.ok) return fail(`initial convergence failed: ${JSON.stringify(c0.last)}`)
  pass(`cluster converged: sha=${c0.sha.slice(0, 8)} lsn=${c0.lsn}`)

  const statuses = await Promise.all(ports.map(getStatus))
  const nameByPort = Object.fromEntries(ports.map((p, i) => [p, statuses[i].name]))

  log('waiting for writer to learn the Primary via gossip...')
  if (!(await waitPrimaryKnown(writerPort, 15000))) return fail('writer never learned the Primary')

  // ==== PART A: rebalance =====================================================
  log(`\nPART A: rebalance (hysteresis T1=${Math.round(HIGH * 100)}% / T2=${Math.round(LOW * 100)}%)\n`)

  // A file of 15 chunks: fits the writer's 16-chunk quota (admission passes) but
  // pushes it to ~94% — over the 85% high-water mark.
  const fileA = '/capacity/big.bin'
  const bytesA = randomBytes(chunksToBytes(15))
  const shaA = sha256(bytesA)
  log(`PUT ${fileA} (15 chunks=${bytesA.length}B) to ${nameByPort[writerPort]}`)
  const putA = await putFileRaw(writerPort, fileA, bytesA)
  if (putA.status !== 200) return fail(`write A rejected (${putA.status}): ${JSON.stringify(putA.body)}`)
  const chunksA = putA.body.chunks
  pass(`write A acked: chunks=${chunksA} lsn=${putA.body.lsn}`)

  const okA = await waitLocationsOk(ports, chunksA, 30000)
  if (!okA.ok) return fail(`file A did not reach RF=2: ${JSON.stringify(okA.last)}`)
  pass(`file A at RF=2: all ${chunksA} chunks state=ok, active!=backup`)

  // (pre) writer is over the high-water mark.
  const preCap = await selfCapacity(writerPort)
  if (preCap.used <= T1) return fail(`writer not over T1 before rebalance (used ${preCap.used} <= T1 ${Math.round(T1)})`)
  pass(`(pre) ${nameByPort[writerPort]} over high-water: used=${preCap.used}B (${preCap.pct}%) > T1=${Math.round(T1)}B`)

  // Drive one deterministic offload sweep.
  log(`POST /admin/rebalance -> ${nameByPort[writerPort]}`)
  const sweep1 = await post(writerPort, '/admin/rebalance')
  if (sweep1.status !== 200) return fail(`rebalance sweep failed (${sweep1.status})`)
  if (!(sweep1.body.moved >= 1)) return fail(`rebalance moved nothing (moved=${sweep1.body.moved})`)
  pass(`sweep moved ${sweep1.body.moved} chunk(s) off ${nameByPort[writerPort]}`)

  // (a) writer now under the low-water mark.
  const postCap = await selfCapacity(writerPort)
  if (postCap.used > T2) return fail(`(a) writer still over T2 after rebalance (used ${postCap.used} > T2 ${Math.round(T2)})`)
  pass(`(a) ${nameByPort[writerPort]} under low-water: used=${postCap.used}B (${postCap.pct}%) <= T2=${Math.round(T2)}B`)

  // (b) RF preserved — still exactly `chunksA` fully-replicated locations.
  const okA2 = await waitLocationsOk(ports, chunksA, 30000)
  if (!okA2.ok) return fail(`(b) RF not preserved after rebalance: ${JSON.stringify(okA2.last)}`)
  const movedOffWriter = okA2.locations.filter(
    (l) => l.active_peer !== preCap.peerId && l.backup_peer !== preCap.peerId
  ).length
  pass(`(b) RF=2 preserved: all ${chunksA} chunks state=ok, active!=backup (${movedOffWriter} now fully off the writer)`)

  // (c) no data loss.
  const readA = await readCorrect(writerPort, fileA, shaA, 15000)
  if (!readA.ok) return fail(`(c) DATA LOSS: file A unreadable after rebalance (status ${readA.status})`)
  pass(`(c) NO DATA LOSS: file A reads back ${readA.bytes.length}B, sha matches`)

  // (d) no oscillation — a second sweep, now under T1, moves nothing.
  const sweep2 = await post(writerPort, '/admin/rebalance')
  if (sweep2.body.moved !== 0) return fail(`(d) OSCILLATION: second sweep moved ${sweep2.body.moved} (expected 0)`)
  pass(`(d) NO OSCILLATION: second sweep moved 0 (writer under T1)`)

  // ==== PART B: admission control ============================================
  log(`\nPART B: admission control (over-quota + full-cluster -> 507 ENOSPC)\n`)

  // (1) A single write bigger than one node's quota is rejected up-front.
  const tooBig = randomBytes(chunksToBytes(QUOTA_CHUNKS + 4)) // 20 chunks > 16-chunk quota
  const rej = await putFileRaw(writerPort, '/capacity/toobig.bin', tooBig)
  if (rej.status !== 507 || rej.body.code !== 'ENOSPC') {
    return fail(`(1) over-quota write not rejected with 507/ENOSPC (got ${rej.status} ${JSON.stringify(rej.body)})`)
  }
  pass(`(1) over-quota write rejected: HTTP ${rej.status} code=${rej.body.code}`)

  // (2) Fill the cluster with small files until admission trips 507 somewhere.
  let got507 = false
  let reason = ''
  for (let i = 0; i < 10 && !got507; i++) {
    // Pick the emptiest node (by its own free bytes) as the writer.
    const caps = await Promise.all(ports.map(async (p) => ({ port: p, free: (await selfCapacity(p)).free })))
    caps.sort((a, b) => b.free - a.free)
    const p = caps[0].port
    const r = await putFileRaw(p, `/fill/f${i}.bin`, randomBytes(chunksToBytes(4)))
    log(`fill#${i}: PUT 4 chunks -> ${nameByPort[p]} (free=${caps[0].free}B) => HTTP ${r.status}`)
    if (r.status === 507 && r.body.code === 'ENOSPC') { got507 = true; reason = r.body.error }
    else if (r.status !== 200) return fail(`fill#${i} unexpected status ${r.status}: ${JSON.stringify(r.body)}`)
    await sleep(1800) // let eager backups + gossip usedBytes propagate before the next admission check
  }
  if (!got507) return fail('(2) cluster never reached ENOSPC after fill writes')
  pass(`(2) full cluster rejects write: 507 ENOSPC ("${reason}")`)

  // (3) data is intact under a full cluster.
  const readAFull = await readCorrect(writerPort, fileA, shaA, 15000)
  if (!readAFull.ok) return fail(`(3) file A unreadable under full cluster (status ${readAFull.status})`)
  pass(`(3) NO DATA LOSS: file A still reads back correctly under a full cluster`)
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
    console.log(`\n${failed ? 'PHASE 4: FAIL x' : 'PHASE 4: PASS ok'}\n`)
    process.exit(failed ? 1 : 0)
  })
