/**
 * Phase 1 acceptance harness (DETAILED_DESIGN §17).
 *
 * Proves the metadata-replication invariants end to end:
 *   A. A write on the Primary propagates to every replica (identical sha256).
 *   B. A node that joins AFTER writes pull-syncs to the same sha256.
 *   C. A corrupted replica detects the divergence on the next push, falls back
 *      to PULL, and re-converges (identical sha256 again).
 *
 * Modes:
 *   - default: spawns local `node dist/index.js` processes (build first).
 *   - external: set P2P_ADMIN_PORTS="8081,8082,8083" P2P_NO_SPAWN=1 to run the
 *     assertions against an already-running cluster (e.g. docker compose).
 *
 * Usage:  node test/phase1/run.mjs
 */

import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { rmSync, mkdirSync } from 'node:fs'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = join(__dirname, '..', '..')
const ENTRY = join(ROOT, 'dist', 'index.js')
const TEST_DATA = join(ROOT, '.test-data')

const NO_SPAWN = process.env.P2P_NO_SPAWN === '1'
const children = []
let failed = false

function log(...a) {
  console.log(`[harness]`, ...a)
}

function fail(msg) {
  failed = true
  console.error(`\n  x FAIL: ${msg}\n`)
}

function pass(msg) {
  console.log(`  ok ${msg}`)
}

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms))
}

async function getStatus(port) {
  const res = await fetch(`http://127.0.0.1:${port}/admin/status`)
  if (!res.ok) throw new Error(`status ${res.status}`)
  return res.json()
}

async function post(port, path, body) {
  const res = await fetch(`http://127.0.0.1:${port}${path}`, {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body ?? {})
  })
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
    PUSH_INTERVAL_MS: '500',
    HEARTBEAT_INTERVAL_MS: '2000'
  }
  const child = spawn(process.execPath, [ENTRY], { env, stdio: ['ignore', 'pipe', 'pipe'] })
  const tag = `[${name}]`
  child.stdout.on('data', (d) => process.stdout.write(prefix(tag, d)))
  child.stderr.on('data', (d) => process.stderr.write(prefix(tag, d)))
  child.on('exit', (code) => log(`${name} exited (${code})`))
  children.push(child)
  return { name, adminPort }
}

function prefix(tag, buf) {
  return String(buf)
    .split('\n')
    .filter((l) => l.length > 0)
    .map((l) => `${tag} ${l}\n`)
    .join('')
}

/** Poll until every port reports an equal, non-empty sha256, or timeout. */
async function waitConverged(ports, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs
  let last = {}
  while (Date.now() < deadline) {
    try {
      const statuses = await Promise.all(ports.map(getStatus))
      const shas = statuses.map((s) => s.sha256)
      last = Object.fromEntries(statuses.map((s) => [s.name, `${s.sha256.slice(0, 8)}@lsn${s.lsn}/${s.mode}`]))
      if (shas.every((s) => s && s === shas[0])) {
        return { ok: true, sha: shas[0], lsn: statuses[0].lsn, statuses }
      }
    } catch {
      /* nodes may still be starting */
    }
    await sleep(500)
  }
  return { ok: false, last, label }
}

async function waitReachable(ports, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      await Promise.all(ports.map(getStatus))
      return true
    } catch {
      await sleep(500)
    }
  }
  return false
}

async function main() {
  let ports
  let primaryPort

  if (NO_SPAWN) {
    ports = (process.env.P2P_ADMIN_PORTS ?? '8081,8082,8083').split(',').map((s) => Number(s.trim()))
    primaryPort = ports[0]
    log('external mode - testing running cluster on ports', ports.join(', '))
  } else {
    rmSync(TEST_DATA, { recursive: true, force: true })
    mkdirSync(TEST_DATA, { recursive: true })
    log('spawning local 3-node cluster...')
    spawnNode({ name: 'anchor1', adminPort: 8091, listenPort: 4101, primary: true })
    spawnNode({ name: 'node2', adminPort: 8092, listenPort: 4102, primary: false })
    spawnNode({ name: 'node3', adminPort: 8093, listenPort: 4103, primary: false })
    ports = [8091, 8092, 8093]
    primaryPort = 8091
  }

  log('waiting for nodes to become reachable...')
  if (!(await waitReachable(ports, 30000))) {
    fail('nodes did not become reachable')
    return
  }

  // Give mDNS a moment to form the mesh so replicas learn the Primary.
  log('waiting for cluster to form + initial convergence...')
  const c0 = await waitConverged(ports, 30000, 'initial')
  if (!c0.ok) fail(`initial convergence failed: ${JSON.stringify(c0.last)}`)
  else pass(`initial convergence: sha=${c0.sha.slice(0, 8)} lsn=${c0.lsn}`)

  // ---- Test A: write on Primary propagates ----------------------------------
  log('TEST A: mkcol on Primary -> propagation')
  const mk1 = await post(primaryPort, '/admin/mkcol', { path: '/alpha/beta' })
  if (mk1.error) fail(`mkcol failed: ${mk1.error}`)
  else log('mkcol /alpha/beta ->', JSON.stringify(mk1))
  const cA = await waitConverged(ports, 20000, 'after-write')
  if (!cA.ok) fail(`propagation failed: ${JSON.stringify(cA.last)}`)
  else pass(`propagated to all ${ports.length} nodes: sha=${cA.sha.slice(0, 8)} lsn=${cA.lsn}`)

  // ---- Test B: late joiner pull-syncs (local mode only) ---------------------
  if (!NO_SPAWN) {
    log('TEST B: late joiner pull-syncs after writes')
    await post(primaryPort, '/admin/mkcol', { path: '/gamma' })
    spawnNode({ name: 'node4', adminPort: 8094, listenPort: 4104, primary: false })
    const allPorts = [...ports, 8094]
    if (!(await waitReachable([8094], 30000))) fail('node4 did not become reachable')
    const cB = await waitConverged(allPorts, 30000, 'late-join')
    if (!cB.ok) fail(`late joiner did not converge: ${JSON.stringify(cB.last)}`)
    else pass(`late joiner synced: sha=${cB.sha.slice(0, 8)} lsn=${cB.lsn}`)
    ports = allPorts
  }

  // ---- Test C: corrupt a replica -> detect -> pull -> reconverge -------------
  log('TEST C: corrupt a replica -> fallback pull -> reconverge')
  const victimPort = ports[1] // a replica, not the primary
  const before = await getStatus(victimPort)
  const corrupt = await post(victimPort, '/admin/corrupt', {})
  log(`corrupted ${before.name}: sha ${before.sha256.slice(0, 8)} -> ${String(corrupt.sha256).slice(0, 8)}`)
  const cCorrupt = await getStatus(victimPort)
  if (cCorrupt.sha256 === before.sha256) fail('corrupt did not change local sha256')
  else pass(`replica diverged locally (sha ${cCorrupt.sha256.slice(0, 8)})`)

  // A subsequent Primary write triggers a push that the victim can't apply
  // contiguously (its sha won't match), forcing the fallback pull.
  await post(primaryPort, '/admin/mkcol', { path: '/delta' })
  const cC = await waitConverged(ports, 30000, 'post-corrupt')
  if (!cC.ok) fail(`did not reconverge after corruption: ${JSON.stringify(cC.last)}`)
  else pass(`reconverged after corruption: sha=${cC.sha.slice(0, 8)} lsn=${cC.lsn}`)

  // ---- Final: all identical -------------------------------------------------
  const finals = await Promise.all(ports.map(getStatus))
  const shas = new Set(finals.map((s) => s.sha256))
  if (shas.size === 1) pass(`FINAL: all ${finals.length} nodes identical (sha=${[...shas][0].slice(0, 8)})`)
  else fail(`FINAL divergence: ${JSON.stringify(finals.map((s) => [s.name, s.sha256.slice(0, 8)]))}`)
}

async function teardown() {
  if (NO_SPAWN) return
  log('tearing down...')
  for (const c of children) {
    try {
      c.kill()
    } catch {
      /* ignore */
    }
  }
  await sleep(1000)
  try {
    rmSync(TEST_DATA, { recursive: true, force: true })
  } catch {
    /* ignore */
  }
}

main()
  .catch((err) => {
    fail(`harness error: ${err?.stack ?? err}`)
  })
  .finally(async () => {
    await teardown()
    console.log(`\n${failed ? 'PHASE 1: FAIL x' : 'PHASE 1: PASS ok'}\n`)
    process.exit(failed ? 1 : 0)
  })
