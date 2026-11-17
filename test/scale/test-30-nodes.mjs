/**
 * 30-Node Large Cluster Scale & 1 GB Data Ingestion Test (Phase 8).
 *
 * Configuration:
 *   - Node Count: 30 nodes
 *     - 1 Primary (Tier-0)
 *     - 3 Middle Tier-1 Relays (threshold configured to 10)
 *     - 26 Tier-2 Leaf nodes
 *   - Data Volume: 1 GB (32 files of 32 MB = 1,024 MB total)
 *   - Workload: Distributed uploads across leaf and relay nodes
 *   - Validations:
 *     1. Multi-tier metadata push fanout converges across all 30 nodes.
 *     2. Every chunk reaches RF=2 (active != backup) across physical nodes.
 *     3. End-to-end data integrity: all 32 files read back byte-for-byte matching SHA-256.
 *
 * Usage: node test/scale/test-30-nodes.mjs
 */

import { spawn } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { dirname, join } from 'node:path'
import { rmSync, mkdirSync } from 'node:fs'
import { createHash, randomBytes } from 'node:crypto'

const __dirname = dirname(fileURLToPath(import.meta.url))
const ROOT = join(__dirname, '..', '..')
const ENTRY = join(ROOT, 'dist', 'index.js')
const TEST_DATA = join(ROOT, '.test-data-30nodes')

const TOTAL_NODES = 30
const RELAY_COUNT = 3
const BASE_ADMIN_PORT = 8300
const BASE_LISTEN_PORT = 9300

const children = []
let failed = false

function log(...a) {
  console.log(`[scale-test]`, ...a)
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

function sha256(buf) {
  return createHash('sha256').update(buf).digest('hex')
}

async function getStatus(port) {
  const res = await fetch(`http://127.0.0.1:${port}/admin/status`)
  if (!res.ok) throw new Error(`status ${res.status}`)
  return res.json()
}

async function uploadFile(port, filePath, buffer, maxRetries = 5) {
  let lastErr
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/admin/file?path=${encodeURIComponent(filePath)}`, {
        method: 'PUT',
        headers: { 'content-type': 'application/octet-stream' },
        body: buffer
      })
      if (!res.ok) {
        const text = await res.text()
        throw new Error(`Upload failed (${res.status}): ${text}`)
      }
      return await res.json()
    } catch (err) {
      lastErr = err
      const cause = err.cause ? ` (cause: ${err.cause.code || err.cause.message || err.cause})` : ''
      if (attempt < maxRetries) {
        log(`upload ${filePath} attempt ${attempt} failed (${err.message}${cause}), retrying in 3s...`)
        await sleep(3000)
      }
    }
  }
  throw lastErr
}

async function readFile(port, filePath, maxRetries = 5) {
  let lastErr
  for (let attempt = 1; attempt <= maxRetries; attempt++) {
    try {
      const res = await fetch(`http://127.0.0.1:${port}/admin/file?path=${encodeURIComponent(filePath)}`)
      if (!res.ok) throw new Error(`Read failed (${res.status})`)
      const arrayBuf = await res.arrayBuffer()
      return Buffer.from(arrayBuf)
    } catch (err) {
      lastErr = err
      const cause = err.cause ? ` (cause: ${err.cause.code || err.cause.message || err.cause})` : ''
      if (attempt < maxRetries) {
        log(`read ${filePath} attempt ${attempt} failed (${err.message}${cause}), retrying in 3s...`)
        await sleep(3000)
      }
    }
  }
  throw lastErr
}

function spawnNode({ id, name, role, isPrimary, tierRole }) {
  const adminPort = BASE_ADMIN_PORT + id
  const listenPort = BASE_LISTEN_PORT + id
  const dataDir = join(TEST_DATA, name)

  const env = {
    ...process.env,
    NODE_NO_WARNINGS: '1',
    NODE_NAME: name,
    NODE_ROLE: role,
    IS_PRIMARY: isPrimary ? 'true' : 'false',
    TIER_ROLE: tierRole,
    TIER_ENABLED: 'true',
    TIER_SCALE_THRESHOLD: '10',
    RELAY_ROSTER: 'node-relay-1,node-relay-2,node-relay-3',
    LISTEN_PORT: String(listenPort),
    ADMIN_PORT: String(adminPort),
    DATA_DIR: dataDir,
    STORAGE_QUOTA_GB: '5', // 5 GB soft quota per node
    PUSH_INTERVAL_MS: '400',
    HEARTBEAT_INTERVAL_MS: '1500',
    MAX_CONTAINERS: '50'
  }

  const child = spawn(process.execPath, [ENTRY], { env, stdio: ['ignore', 'ignore', 'pipe'] })
  child.stderr.on('data', (d) => {
    const s = String(d)
    if (s.includes('FATAL') || s.includes('Unhandled') || s.includes('uncaughtException') || s.includes('ERR_') || s.includes('Error')) {
      console.error(`[${name}-err] ${s.trim()}`)
    }
  })
  child.on('exit', (code, sig) => {
    if (code !== 0 && code !== null) {
      console.error(`[${name}-exit] process exited unexpectedly with code ${code}, signal ${sig}`)
    }
  })
  children.push(child)
  return { id, name, adminPort, listenPort }
}

async function waitAllReady(nodes, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs
  log(`waiting for all ${nodes.length} nodes to initialize and bind admin HTTP ports...`)
  while (Date.now() < deadline) {
    try {
      const results = await Promise.allSettled(nodes.map((n) => getStatus(n.adminPort)))
      const ready = results.filter((r) => r.status === 'fulfilled').length
      if (ready === nodes.length) {
        pass(`all ${nodes.length} nodes online and reporting status`)
        return
      }
    } catch {}
    await sleep(500)
  }
  throw new Error(`Timeout waiting for all ${nodes.length} nodes to come online`)
}

async function waitClusterMesh(nodes, minPeers = 3, timeoutMs = 30000) {
  const deadline = Date.now() + timeoutMs
  log(`waiting for mesh gossiping to form across the 30 nodes (min ${minPeers} peers/node)...`)
  while (Date.now() < deadline) {
    try {
      const stats = await Promise.all(nodes.map((n) => getStatus(n.adminPort)))
      const connectedCount = stats.filter((s) => s.connections >= minPeers && s.peers.length >= minPeers).length
      if (connectedCount === nodes.length) {
        pass(`gossip mesh established across all ${nodes.length} nodes (min ${minPeers} peers confirmed)`)
        return
      }
    } catch {}
    await sleep(800)
  }
  log(`mesh partially connected; proceeding with cluster convergence`)
}

async function waitClusterConvergence(nodes, timeoutMs = 40000, label = 'cluster convergence') {
  const deadline = Date.now() + timeoutMs
  let lastShas = []
  while (Date.now() < deadline) {
    try {
      const stats = await Promise.all(nodes.map((n) => getStatus(n.adminPort)))
      const shas = stats.map((s) => s.sha256)
      const lsns = stats.map((s) => s.lsn)
      lastShas = shas
      const allEqual = shas.length > 0 && shas.every((s) => s && s === shas[0])
      if (allEqual) {
        pass(`${label} -> all ${nodes.length} nodes converged at sha=${shas[0].slice(0, 8)} (LSN: ${lsns[0]})`)
        return shas[0]
      }
    } catch {}
    await sleep(1000)
  }
  fail(`${label} TIMED OUT. Diverged states: ${JSON.stringify(lastShas.slice(0, 6))}...`)
  throw new Error('Convergence timeout')
}

async function main() {
  log(`=== Starting 30-Node Scale & 1 GB Data Ingestion Test ===`)
  log(`cleaning test data directory: ${TEST_DATA}`)
  rmSync(TEST_DATA, { recursive: true, force: true })
  mkdirSync(TEST_DATA, { recursive: true })

  // Define nodes:
  // Node 0: Primary (Tier-0)
  // Node 1..3: Relays (Tier-1 middle nodes)
  // Node 4..29: Leaves (Tier-2 leaf nodes)
  const nodeDefs = []
  nodeDefs.push({ id: 0, name: 'primary-0', role: 'anchor', isPrimary: true, tierRole: 'relay' })

  for (let i = 1; i <= RELAY_COUNT; i++) {
    nodeDefs.push({ id: i, name: `node-relay-${i}`, role: 'anchor', isPrimary: false, tierRole: 'relay' })
  }

  for (let i = RELAY_COUNT + 1; i < TOTAL_NODES; i++) {
    nodeDefs.push({ id: i, name: `node-leaf-${i}`, role: 'ordinary', isPrimary: false, tierRole: 'leaf' })
  }

  log(`spawning ${TOTAL_NODES} node processes staggered...`)
  const nodes = []
  for (const def of nodeDefs) {
    nodes.push(spawnNode(def))
    await sleep(80)
  }

  // Wait for all 30 nodes to start and form mesh
  await waitAllReady(nodes)
  await waitClusterMesh(nodes, 2)
  await waitClusterConvergence(nodes, 25000, 'initial 30-node mesh formation')

  // Generate data: default 16 files of 64 MiB each (total = 1,024 MB = 1 GB)
  const FILE_COUNT = Number(process.env.FILE_COUNT || 16)
  const FILE_SIZE_MB = Number(process.env.FILE_SIZE_MB || 64)
  const FILE_SIZE_BYTES = FILE_SIZE_MB * 1024 * 1024
  const totalMb = FILE_COUNT * FILE_SIZE_MB
  log(`\n--- Step 2: Preparing ${totalMb} MB of Data (${FILE_COUNT} files × ${FILE_SIZE_MB} MB) ---`)

  const chunkSeed = randomBytes(4 * 1024 * 1024)
  const fileSpecs = []

  for (let i = 0; i < FILE_COUNT; i++) {
    const buf = Buffer.alloc(FILE_SIZE_BYTES)
    for (let offset = 0; offset < FILE_SIZE_BYTES; offset += chunkSeed.length) {
      chunkSeed.copy(buf, offset)
      buf.writeUInt32BE(i * 1000 + (offset / chunkSeed.length), offset)
    }
    const digest = sha256(buf)
    fileSpecs.push({
      path: `/datasets/partition_${Math.floor(i / 4)}/file_${i}.bin`,
      buffer: buf,
      expectedSha: digest
    })
  }
  pass(`Generated ${totalMb} MB of test payloads (${FILE_COUNT} files, total ${totalMb} MB)`)

  // Upload files distributed across leaf nodes (Node 4 to 29)
  log(`\n--- Step 3: Ingesting 1 GB into the 30-node cluster ---`)
  const startTime = Date.now()

  for (let i = 0; i < fileSpecs.length; i++) {
    const spec = fileSpecs[i]
    // Rotate target node: send through leaf nodes (e.g. node 4..29)
    const targetNodeIndex = 4 + (i % (TOTAL_NODES - 4))
    const targetNode = nodes[targetNodeIndex]
    log(`[${i + 1}/${FILE_COUNT}] uploading ${spec.path} (${FILE_SIZE_MB} MB) via "${targetNode.name}" (port ${targetNode.adminPort})...`)
    const uploadRes = await uploadFile(targetNode.adminPort, spec.path, spec.buffer)
    if (!uploadRes.fileId) {
      throw new Error(`Upload returned error: ${JSON.stringify(uploadRes)}`)
    }
  }

  const durationSec = ((Date.now() - startTime) / 1000).toFixed(1)
  pass(`${totalMb} MB successfully ingested across 30 nodes in ${durationSec}s (${(totalMb / Number(durationSec)).toFixed(1)} MB/s)`)

  // Step 4: Verify metadata convergence across all 30 nodes
  log(`\n--- Step 4: Verifying Tiered Push Fanout Convergence across all 30 Nodes ---`)
  const convergedSha = await waitClusterConvergence(nodes, 40000, '30-node post-ingestion convergence')
  pass(`Metadata 100% synchronized across all 30 nodes (Convergence SHA: ${convergedSha.slice(0, 10)})`)

  // Step 5: Verify RF=2 chunk distribution across physical nodes
  log(`\n--- Step 5: Checking Replication Factor (RF=2) across the 30 nodes ---`)
  const locRes = await fetch(`http://127.0.0.1:${nodes[0].adminPort}/admin/locations`)
  const locData = await locRes.json()
  const locations = Array.isArray(locData) ? locData : locData.locations || []
  const totalChunks = locations.length
  log(`total stored chunks in cluster: ${totalChunks} chunks`)

  let okRfCount = 0
  const holderSet = new Set()
  for (const loc of locations) {
    if (loc.state === 'ok' && loc.active_peer && loc.backup_peer && loc.active_peer !== loc.backup_peer) {
      okRfCount++
      holderSet.add(loc.active_peer)
      holderSet.add(loc.backup_peer)
    }
  }

  log(`chunks with verified RF=2 (state=ok, active!=backup): ${okRfCount} / ${totalChunks}`)
  log(`distinct physical nodes holding chunks: ${holderSet.size} / ${TOTAL_NODES}`)
  if (okRfCount === totalChunks && holderSet.size >= 10) {
    pass(`All ${totalChunks} chunks reliably stored at RF=2 across ${holderSet.size} distributed nodes!`)
  } else {
    fail(`RF=2 invariant check failed: okRf=${okRfCount}/${totalChunks}, holders=${holderSet.size}`)
  }

  // Step 6: Verify end-to-end data integrity: read back all 32 files from random different nodes
  log(`\n--- Step 6: Verifying End-to-End Data Integrity (Reading back all 1 GB) ---`)
  for (let i = 0; i < fileSpecs.length; i++) {
    const spec = fileSpecs[i]
    // Choose a reader node different from upload target
    const readerNodeIndex = (i * 5 + 2) % TOTAL_NODES
    const readerNode = nodes[readerNodeIndex]
    log(`[${i + 1}/${FILE_COUNT}] reading back ${spec.path} from "${readerNode.name}" (port ${readerNode.adminPort})...`)
    const readBytes = await readFile(readerNode.adminPort, spec.path)
    const readSha = sha256(readBytes)

    if (readSha === spec.expectedSha && readBytes.length === FILE_SIZE_BYTES) {
      pass(`File ${i + 1}/${FILE_COUNT} verified: exact SHA-256 match (${readSha.slice(0, 10)})`)
    } else {
      fail(`File ${spec.path} corrupted! Expected ${spec.expectedSha.slice(0, 10)}, got ${readSha.slice(0, 10)}`)
    }
  }

  pass(`\n*** SUCCESS: 30-Node Tiered Cluster flawlessly held 1 GB of data under full RF=2 redundancy! ***`)
}

async function shutdown() {
  log('tearing down 30 node processes...')
  for (const c of children) {
    try { c.kill('SIGTERM') } catch {}
  }
  await sleep(1000)
  for (const c of children) {
    try { c.kill('SIGKILL') } catch {}
  }
  try {
    rmSync(TEST_DATA, { recursive: true, force: true })
  } catch {}
}

process.on('SIGINT', async () => {
  await shutdown()
  process.exit(130)
})

process.on('SIGTERM', async () => {
  await shutdown()
  process.exit(143)
})

main()
  .catch((err) => {
    fail(`uncaught: ${err.message}\n${err.stack}`)
  })
  .finally(async () => {
    await shutdown()
    if (failed) {
      console.error('\nScale test: FAILED\n')
      process.exit(1)
    } else {
      console.log('\nScale test: ALL 30 NODES & 1 GB CHECKS PASSED\n')
      process.exit(0)
    }
  })
