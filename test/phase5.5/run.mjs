/**
 * Phase 5.5 acceptance harness — cross-network networking (mesh-VPN enablement).
 *
 * Proves the two invariants that unlock Path B (mesh VPN) in DEPLOYMENT.md:
 *   A. PERSISTENT IDENTITY — a node keeps the same peerId across restarts when it
 *      reuses its DATA_DIR (so static BOOTSTRAP_PEERS `/p2p/<id>` stays valid).
 *   B. BOOTSTRAP DISCOVERY WITHOUT mDNS — with MDNS_ENABLED=false on both nodes,
 *      a replica finds the Primary purely via BOOTSTRAP_PEERS and the cluster
 *      converges (identical sha256). This simulates a VPN where multicast/mDNS
 *      does not reach. Also checks the ANNOUNCE_MULTIADDRS override surfaces on
 *      /admin/status.dialAddrs, and that a subsequent write propagates.
 *
 * Native only (single host, loopback). Usage:  node test/phase5.5/run.mjs
 */

import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { rmSync, mkdirSync } from 'node:fs'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = join(__dirname, '..', '..')
const ENTRY = join(ROOT, 'dist', 'index.js')
const TEST_DATA = join(ROOT, '.test-data-net')

const children = new Set()
let failed = false

const log = (...a) => console.log('[harness]', ...a)
const pass = (m) => console.log(`  ok ${m}`)
const fail = (m) => {
  failed = true
  console.error(`\n  x FAIL: ${m}\n`)
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

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

function prefix(tag, buf) {
  return String(buf)
    .split('\n')
    .filter((l) => l.length > 0)
    .map((l) => `${tag} ${l}\n`)
    .join('')
}

/** Spawn a node-agent process with the given env overrides. */
function spawnNode({ name, adminPort, listenPort, primary, extraEnv }) {
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
    HEARTBEAT_INTERVAL_MS: '2000',
    ...extraEnv
  }
  const child = spawn(process.execPath, [ENTRY], { env, stdio: ['ignore', 'pipe', 'pipe'] })
  const tag = `[${name}]`
  child.stdout.on('data', (d) => process.stdout.write(prefix(tag, d)))
  child.stderr.on('data', (d) => process.stderr.write(prefix(tag, d)))
  children.add(child)
  return { name, adminPort, listenPort, dataDir, child }
}

/** Terminate a node and resolve once it has actually exited (frees its ports). */
function kill(child) {
  return new Promise((resolve) => {
    if (child.exitCode !== null || child.signalCode !== null) {
      children.delete(child)
      return resolve()
    }
    const done = () => {
      children.delete(child)
      resolve()
    }
    child.once('exit', done)
    try {
      child.kill()
    } catch {
      done()
    }
    setTimeout(done, 6000)
  })
}

async function waitReachable(ports, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      await Promise.all(ports.map(getStatus))
      return true
    } catch {
      await sleep(400)
    }
  }
  return false
}

/** Poll until every port reports an equal, non-empty sha256, or timeout. */
async function waitConverged(ports, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  let last = {}
  while (Date.now() < deadline) {
    try {
      const statuses = await Promise.all(ports.map(getStatus))
      const shas = statuses.map((s) => s.sha256)
      last = Object.fromEntries(statuses.map((s) => [s.name, `${s.sha256.slice(0, 8)}@lsn${s.lsn}/${s.mode}/conns${s.connections}`]))
      if (shas.every((s) => s && s === shas[0])) {
        return { ok: true, sha: shas[0], lsn: statuses[0].lsn, statuses }
      }
    } catch {
      /* nodes may still be starting */
    }
    await sleep(500)
  }
  return { ok: false, last }
}

/** Poll until a node reports at least `min` live connections, or timeout. */
async function waitForConnections(port, min, timeoutMs) {
  const deadline = Date.now() + timeoutMs
  while (Date.now() < deadline) {
    try {
      const s = await getStatus(port)
      if ((s.connections ?? 0) >= min) return true
    } catch {
      /* starting */
    }
    await sleep(400)
  }
  return false
}

// ---- Test A: persistent identity across restart ----------------------------
async function testIdentity() {
  log('TEST A: persistent identity across restart (same DATA_DIR -> same peerId)')
  const port = 8190
  let n = spawnNode({
    name: 'idnode',
    adminPort: port,
    listenPort: 4300,
    primary: true,
    extraEnv: { MDNS_ENABLED: 'false' }
  })
  if (!(await waitReachable([port], 30000))) return fail('idnode did not become reachable')
  const peer1 = (await getStatus(port)).peerId
  log(`idnode first boot peerId=...${peer1.slice(-12)}`)

  await kill(n.child)
  await sleep(1500)

  n = spawnNode({
    name: 'idnode',
    adminPort: port,
    listenPort: 4300,
    primary: true,
    extraEnv: { MDNS_ENABLED: 'false' }
  })
  if (!(await waitReachable([port], 30000))) return fail('idnode did not come back after restart')
  const peer2 = (await getStatus(port)).peerId
  log(`idnode second boot peerId=...${peer2.slice(-12)}`)

  if (peer1 === peer2) pass(`peerId stable across restart (...${peer1.slice(-12)})`)
  else fail(`peerId changed across restart: ${peer1} != ${peer2}`)

  await kill(n.child)
  await sleep(1000)
}

// ---- Test B: bootstrap discovery with mDNS disabled ------------------------
async function testBootstrapNoMdns() {
  log('TEST B: bootstrap-only discovery (mDNS disabled on both nodes)')
  const P_ADMIN = 8191
  const P_LISTEN = 4301
  const R_ADMIN = 8192
  const R_LISTEN = 4302
  const pAnnounce = `/ip4/127.0.0.1/tcp/${P_LISTEN}`

  const primary = spawnNode({
    name: 'net1',
    adminPort: P_ADMIN,
    listenPort: P_LISTEN,
    primary: true,
    extraEnv: { MDNS_ENABLED: 'false', ANNOUNCE_MULTIADDRS: pAnnounce }
  })
  if (!(await waitReachable([P_ADMIN], 30000))) return fail('net1 (primary) did not become reachable')

  const pStatus = await getStatus(P_ADMIN)
  const primaryPeerId = pStatus.peerId
  log(`net1 peerId=...${primaryPeerId.slice(-12)} dialAddrs=${JSON.stringify(pStatus.dialAddrs)}`)

  // WI-3: the announce override should surface on /admin/status.dialAddrs.
  const announced = (pStatus.dialAddrs ?? []).some((a) => a.includes(`127.0.0.1/tcp/${P_LISTEN}`))
  if (announced) pass('ANNOUNCE_MULTIADDRS surfaced in dialAddrs')
  else fail(`announce override not in dialAddrs: ${JSON.stringify(pStatus.dialAddrs)}`)

  // WI-2: replica discovers the Primary purely via BOOTSTRAP_PEERS (no mDNS).
  const bootstrapAddr = `/ip4/127.0.0.1/tcp/${P_LISTEN}/p2p/${primaryPeerId}`
  const replica = spawnNode({
    name: 'net2',
    adminPort: R_ADMIN,
    listenPort: R_LISTEN,
    primary: false,
    extraEnv: {
      MDNS_ENABLED: 'false',
      ANNOUNCE_MULTIADDRS: `/ip4/127.0.0.1/tcp/${R_LISTEN}`,
      BOOTSTRAP_PEERS: bootstrapAddr
    }
  })
  if (!(await waitReachable([R_ADMIN], 30000))) return fail('net2 (replica) did not become reachable')

  // WI-2 headline: with mDNS off, the ONLY way net2 can reach net1 is by dialing
  // the bootstrap multiaddr. Wait for that link to form. (Both nodes start with
  // identical empty metadata, so a plain sha convergence check would pass
  // trivially before any connection — hence we assert the connection directly.)
  const linked = await waitForConnections(R_ADMIN, 1, 30000)
  if (!linked) {
    fail('replica did not connect to the Primary via BOOTSTRAP_PEERS (mDNS off)')
  } else {
    pass('replica connected to Primary purely via BOOTSTRAP_PEERS (no mDNS)')

    // Prove metadata actually flows over that bootstrap-formed link.
    await post(P_ADMIN, '/admin/mkcol', { path: '/net-test' })
    const conv = await waitConverged([P_ADMIN, R_ADMIN], 30000)
    if (conv.ok && conv.lsn >= 1) pass(`write propagated over bootstrap link: sha=${conv.sha.slice(0, 8)} lsn=${conv.lsn}`)
    else fail(`write did not propagate: ${JSON.stringify(conv.last)}`)
  }

  await kill(primary.child)
  await kill(replica.child)
  await sleep(1000)
}

async function main() {
  rmSync(TEST_DATA, { recursive: true, force: true })
  mkdirSync(TEST_DATA, { recursive: true })
  await testIdentity()
  await testBootstrapNoMdns()
}

async function teardown() {
  for (const c of [...children]) await kill(c)
  try {
    rmSync(TEST_DATA, { recursive: true, force: true })
  } catch {
    /* ignore */
  }
}

main()
  .catch((err) => fail(`harness error: ${err?.stack ?? err}`))
  .finally(async () => {
    await teardown()
    console.log(`\n${failed ? 'PHASE 5.5: FAIL x' : 'PHASE 5.5: PASS ok'}\n`)
    process.exit(failed ? 1 : 0)
  })
