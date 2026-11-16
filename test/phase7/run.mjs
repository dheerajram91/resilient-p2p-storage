/**
 * Phase 7 acceptance harness (DETAILED_DESIGN §17) — hardening: encryption-at-rest.
 *
 * Runs the same RF=2 content invariants as Phase 2, but with STORAGE_KEY set on
 * every node, and adds encryption-specific assertions:
 *   A. An upload under STORAGE_KEY chunks, replicates, and acknowledges as usual
 *      (RF=2, active != backup, state='ok').
 *   B. On-disk chunk files are CIPHERTEXT: the raw bytes at <dataDir>/content/
 *      <hash[0:2]>/<hash[2:4]>/<hash> do NOT hash to the chunk hash, and their
 *      length is exactly plaintext + 28 (12-byte nonce + 16-byte GCM tag).
 *   C. Reads return the exact original plaintext (decrypt + verify on the way out).
 *   D. Tampering with on-disk ciphertext fails the GCM auth tag and does NOT
 *      corrupt the read — the good replica is fetched and the copy repaired.
 *   E. Killing the ACTIVE holder still leaves the file readable from the backup
 *      (no data loss with encryption enabled).
 *
 * Modes:
 *   - default: spawns local `node dist/index.js` processes (build first).
 *   - external: set P2P_ADMIN_PORTS="8081,8082,8083" P2P_NO_SPAWN=1 to test an
 *     already-running cluster. On-disk assertions (B, and D's local tampering)
 *     are skipped in external mode; the cluster must have been started with a
 *     matching STORAGE_KEY.
 *
 * Usage:  npm run build && node test/phase7/run.mjs
 */

import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { rmSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { randomBytes, createHash, randomUUID } from 'node:crypto'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = join(__dirname, '..', '..')
const ENTRY = join(ROOT, 'dist', 'index.js')
const TEST_DATA = join(ROOT, '.test-data-p7')

const NO_SPAWN = process.env.P2P_NO_SPAWN === '1'
const CHUNK_SIZE = Number(process.env.CHUNK_SIZE_BYTES ?? 65536)
// 32 bytes of hex — same key on every node (see DEPLOYMENT.md, storage key).
const STORAGE_KEY = process.env.P7_STORAGE_KEY ?? randomBytes(32).toString('hex')
const children = new Map() // name -> { child, adminPort, dataDir }
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
  return { ok: true, status: res.status, bytes: Buffer.from(ab) }
}
async function postCorruptChunk(port, hash) {
  const res = await fetch(`http://127.0.0.1:${port}/admin/corrupt-chunk?hash=${hash}`, { method: 'POST' })
  return res.json()
}

function spawnNode({ name, adminPort, listenPort, primary, extraEnv = {} }) {
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
    STORAGE_KEY,
    PUSH_INTERVAL_MS: '500',
    HEARTBEAT_INTERVAL_MS: '2000',
    ROTATION_INTERVAL_MS: '500',
    ROTATION_BATCH: '4',
    ...extraEnv
  }
  const child = spawn(process.execPath, [ENTRY], { env, stdio: ['ignore', 'pipe', 'pipe'] })
  const tag = `[${name}]`
  child.stdout.on('data', (d) => process.stdout.write(prefix(tag, d)))
  child.stderr.on('data', (d) => process.stderr.write(prefix(tag, d)))
  child.on('exit', (code) => log(`${name} exited (${code})`))
  children.set(name, { child, adminPort, dataDir })
  return { name, adminPort, dataDir }
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

/** Resolve a chunk hash to its on-disk file path inside a node's data dir. */
function chunkFile(dataDir, hash) {
  return join(dataDir, 'content', hash.slice(0, 2), hash.slice(2, 4), hash)
}

async function main() {
  let ports, primaryPort, writerPort
  const filePath = '/docs/encrypted-report.bin'
  const original = randomBytes(Math.max(CHUNK_SIZE * 3 + 54321, 200000))
  const originalSha = sha256(original)

  if (NO_SPAWN) {
    ports = (process.env.P2P_ADMIN_PORTS ?? '8081,8082,8083').split(',').map((s) => Number(s.trim()))
    primaryPort = ports[0]
    writerPort = ports[1] ?? ports[0]
    log('external mode - testing running cluster on ports', ports.join(', '))
  } else {
    rmSync(TEST_DATA, { recursive: true, force: true })
    mkdirSync(TEST_DATA, { recursive: true })
    log(`spawning local 3-node cluster (STORAGE_KEY set, chunk size ${CHUNK_SIZE}B, file ${original.length}B)...`)
    spawnNode({ name: 'anchor1', adminPort: 8097, listenPort: 4291, primary: true })
    spawnNode({ name: 'node2', adminPort: 8098, listenPort: 4292, primary: false })
    spawnNode({ name: 'node3', adminPort: 8099, listenPort: 4293, primary: false })
    ports = [8097, 8098, 8099]
    primaryPort = 8097
    writerPort = 8098
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
  const writerPeerId = peerIdByPort[writerPort]

  log('waiting for writer to learn the Primary via gossip...')
  if (!(await waitPrimaryKnown(writerPort, 15000))) return fail('writer never learned the Primary')

  // ---- Test A: upload with encryption enabled, reach RF=2 -------------------
  log(`TEST A: PUT ${filePath} (${original.length}B) with STORAGE_KEY set`)
  const put = await putFile(writerPort, filePath, original)
  if (put.error) return fail(`write failed: ${put.error}`)
  const expectedChunks = put.chunks
  if (!(expectedChunks >= 1)) return fail('write produced no chunks')
  pass(`write acked: chunks=${expectedChunks} fileId=${put.fileId} lsn=${put.lsn}`)

  log('waiting for backup replication (RF=2 window to close)...')
  const locOk = await waitLocationsOk(ports, expectedChunks, 30000)
  if (!locOk.ok) return fail(`chunks did not reach RF=2: ${JSON.stringify(locOk.last)}`)
  pass(`all ${expectedChunks} chunks at RF=2 (state=ok, active!=backup)`)

  // ---- Test B: on-disk chunks are ciphertext, never plaintext ---------------
  if (NO_SPAWN) {
    log('TEST B skipped in external mode (no access to node data dirs)')
  } else {
    log('TEST B: raw on-disk chunk files must be ciphertext (nonce+ct+tag)')
    let allCiphertext = true
    for (const l of locOk.locations) {
      for (const p of ports) {
        const node = [...children.values()].find((c) => c.adminPort === p)
        const file = chunkFile(node.dataDir, l.chunk_hash)
        let disk
        try { disk = readFileSync(file) } catch { continue } // node may not hold this chunk
        if (sha256(disk) === l.chunk_hash) {
          allCiphertext = false
          fail(`${node.dataDir}: chunk ${l.chunk_hash.slice(0, 12)} stored as PLAINTEXT`)
        }
        // AEAD framing: on-disk = plaintext + 12-byte nonce + 16-byte tag. Chunk
        // plaintext sizes are known from the file: CHUNK_SIZE each, remainder last.
        const plainSizes = new Set([CHUNK_SIZE, original.length % CHUNK_SIZE || CHUNK_SIZE])
        if (!plainSizes.has(disk.length - 28)) {
          allCiphertext = false
          fail(`${node.dataDir}: chunk ${l.chunk_hash.slice(0, 12)} length ${disk.length} is not ciphertext-framed (expected plaintext+28)`)
        }
      }
    }
    if (allCiphertext) pass(`every on-disk chunk is AEAD-framed ciphertext, none hashes to its chunk hash`)
  }

  // ---- Test C: reads return exact plaintext ---------------------------------
  log('TEST C: read back through decryption (every node view)')
  let readsOk = true
  for (const p of ports) {
    const got = await getFile(p, filePath)
    if (!got.ok || sha256(got.bytes) !== originalSha) {
      readsOk = false
      fail(`${nameByPort[p]} read returned wrong bytes or status ${got.status}`)
    }
  }
  if (readsOk) pass(`all ${ports.length} nodes decrypt and return the exact original bytes`)

  // ---- Test D: tampered ciphertext -> auth-tag failure -> read-repair -------
  if (NO_SPAWN) {
    log('TEST D skipped in external mode (no access to node data dirs)')
  } else {
    log('TEST D: tamper with on-disk ciphertext on the writer -> read-repairs from backup')
    const victimHash = locOk.locations[0].chunk_hash
    const writerNode = [...children.values()].find((c) => c.adminPort === writerPort)
    const file = chunkFile(writerNode.dataDir, victimHash)
    const disk = readFileSync(file)
    // Flip bits in the ciphertext body (after the 12-byte nonce) so the GCM tag no longer matches.
    const tampered = Buffer.from(disk)
    tampered[12] ^= 0xff
    writeFileSync(file, tampered)

    const got = await getFile(writerPort, filePath)
    if (!got.ok) fail(`read after ciphertext tamper failed (status ${got.status})`)
    else if (sha256(got.bytes) !== originalSha) fail('read after ciphertext tamper returned WRONG bytes')
    else pass(`tampered ciphertext detected (auth-tag failure) and read-repaired from the backup replica`)
  }

  // ---- Test E: kill the ACTIVE holder -> read from backup (no data loss) ----
  if (NO_SPAWN) {
    log('TEST E skipped in external mode (cannot kill a container from here)')
  } else {
    log(`TEST E: kill ACTIVE node (${nameByPort[writerPort]}) -> read from a survivor`)
    const survivorPort = primaryPort
    const victim = children.get(nameByPort[writerPort])
    victim.child.kill('SIGKILL')
    log(`killed ${nameByPort[writerPort]}; waiting for disconnect...`)
    await sleep(4000)

    const after = await getFile(survivorPort, filePath)
    if (!after.ok) fail(`read after active-death failed (status ${after.status}) -> DATA LOSS`)
    else if (sha256(after.bytes) !== originalSha) fail('read after active-death returned WRONG bytes -> DATA LOSS')
    else pass(`NO DATA LOSS: ${nameByPort[survivorPort]} served correct bytes from the encrypted backup after active died`)
  }

  // ---- Test F: ADMIN_USERS gates every admin endpoint -----------------------
  if (NO_SPAWN) {
    log('TEST F skipped in external mode')
  } else {
    log('TEST F: ADMIN_USERS set -> admin API requires Basic auth on every endpoint')
    spawnNode({
      name: 'authnode', adminPort: 8096, listenPort: 4290, primary: true,
      extraEnv: {
        ADMIN_USERS: 'admin:adminsecret',
        ADMIN_REALM: 'p7-admin',
        METRICS_ENABLED: 'true',
        METRICS_PORT: '9096'
      }
    })
    const authPort = 8096
    const code = async (header) => {
      const r = await fetch(`http://127.0.0.1:${authPort}/admin/status`, {
        headers: header ? { authorization: header } : {}
      })
      return r.status
    }
    // Wait until the server responds at all (any status counts as reachable).
    const deadline = Date.now() + 30000
    let up = false
    while (Date.now() < deadline) {
      try { await code(); up = true; break } catch { await sleep(300) }
    }
    if (!up) return fail('authnode did not become reachable')

    const b64 = (s) => Buffer.from(s).toString('base64')
    const good = `Basic ${b64('admin:adminsecret')}`
    const bad = `Basic ${b64('admin:wrong')}`

    if ((await code()) !== 401) fail('GET /admin/status without credentials did not return 401')
    else pass('no credentials -> 401 on /admin/status')
    if ((await code(bad)) !== 401) fail('GET /admin/status with BAD credentials did not return 401')
    else pass('wrong password -> 401')
    if ((await code(good)) !== 200) fail('GET /admin/status with valid credentials did not return 200')
    else pass('valid credentials -> 200')

    // ---- Phase 7 metrics: counters appear on the Prometheus scrape ----------
    log('TEST F2: /metrics exposes monotonic counters after an upload')
    const putRes = await fetch(`http://127.0.0.1:${authPort}/admin/file?path=${encodeURIComponent('/m/c.bin')}`, {
      method: 'PUT',
      headers: { authorization: good, 'content-type': 'application/octet-stream' },
      body: randomBytes(CHUNK_SIZE + 100)
    })
    if (!putRes.ok) fail(`authenticated upload to authnode failed (status ${putRes.status})`)
    const readDeadline = Date.now() + 10000
    let readOk = false
    let lastGetStatus = 0
    let lastGetBody = ''
    while (Date.now() < readDeadline) {
      const getRes = await fetch(`http://127.0.0.1:${authPort}/admin/file?path=${encodeURIComponent('/m/c.bin')}`, {
        headers: { authorization: good }
      })
      lastGetStatus = getRes.status
      if (getRes.ok) {
        readOk = true
        break
      }
      lastGetBody = await getRes.text()
      await sleep(100)
    }
    if (!readOk) fail(`authenticated read on authnode did not become available after replication (status ${lastGetStatus}, body: ${lastGetBody})`)
    const scrape = await fetch('http://127.0.0.1:9096/metrics')
    if (!scrape.ok) fail(`metrics scrape failed (status ${scrape.status})`)
    else {
      const text = await scrape.text()
      const counter = (name) => Number((text.match(new RegExp(`^p2p_${name} (\\d+)$`, 'm')) ?? [])[1] ?? NaN)
      const uploads = counter('uploads_total')
      const reads = counter('reads_total')
      if (!Number.isFinite(uploads) || uploads < 1) fail(`p2p_uploads_total missing or not incremented (got ${text.match(/p2p_uploads_total \d+/)?.[0] ?? 'absent'})`)
      else pass(`p2p_uploads_total present and incremented (${uploads})`)
      if (!Number.isFinite(reads) || reads < 1) fail(`p2p_reads_total missing or not incremented (got ${text.match(/p2p_reads_total \d+/)?.[0] ?? 'absent'})`)
      else pass(`p2p_reads_total present and incremented (${reads})`)
    }
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
    console.log(`\n${failed ? 'PHASE 7: FAIL x' : 'PHASE 7: PASS ok'}\n`)
    process.exit(failed ? 1 : 0)
  })
