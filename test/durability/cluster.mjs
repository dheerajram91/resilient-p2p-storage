import test from 'node:test'
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { fileURLToPath } from 'node:url'
import net from 'node:net'

const entry = fileURLToPath(new URL('../../dist/index.js', import.meta.url))
const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms))

async function unusedPort() {
  const server = net.createServer()
  await new Promise((resolve, reject) => {
    server.once('error', reject)
    server.listen(0, '127.0.0.1', resolve)
  })
  const port = server.address().port
  await new Promise((resolve) => server.close(resolve))
  return port
}

test('acknowledged files survive immediate Primary loss, stale restart, and minority write rejection',
  { timeout: 180_000 }, async () => {
    const root = await mkdtemp(join(tmpdir(), 'p2p-quorum-cluster-'))
    const nodes = []
    const names = ['durable-a', 'durable-b', 'durable-c']
    const addresses = []
    for (const name of names) {
      nodes.push({ name, port: await unusedPort(), listenPort: await unusedPort(), process: null, logs: '' })
    }

    async function request(node, path, init) {
      return fetch(`http://127.0.0.1:${node.port}${path}`,
        { ...init, signal: AbortSignal.timeout(20_000) })
    }

    async function status(node) {
      return (await request(node, '/admin/status')).json()
    }

    async function until(fn, timeout = 30_000) {
      const deadline = Date.now() + timeout
      let error
      while (Date.now() < deadline) {
        try {
          const result = await fn()
          if (result) return result
        } catch (err) {
          error = err
        }
        await sleep(100)
      }
      throw new Error(`condition timed out: ${error ?? 'not satisfied'}\n${nodes.map((n) => n.logs.slice(-6000)).join('\n')}`)
    }

    async function start(node) {
      node.process = spawn(process.execPath, [entry], {
        env: {
          ...process.env,
          NODE_NAME: node.name,
          METADATA_VOTERS: names.join(','),
          IS_PRIMARY: 'true', // Must never override the persisted quorum election.
          NODE_ROLE: 'ordinary',
          DATA_DIR: join(root, node.name),
          LISTEN_PORT: String(node.listenPort),
          ADMIN_PORT: String(node.port),
          MDNS_ENABLED: 'false',
          BOOTSTRAP_PEERS: addresses.filter((a) => !a.endsWith(node.peerId ?? '__none__')).join(','),
          ANNOUNCE_MULTIADDRS: `/ip4/127.0.0.1/tcp/${node.listenPort}`,
          HEARTBEAT_INTERVAL_MS: '300',
          HEALTH_DETECT_INTERVAL_MS: '300',
          HEALTH_SUSPECT_MS: '1500',
          HEALTH_DOWN_MS: '2500',
          RECONCILE_INTERVAL_MS: '300',
          REBALANCE_ENABLED: 'false',
          CHUNK_SIZE_BYTES: '4096',
          STORAGE_QUOTA_BYTES: String(16 * 1024 * 1024),
          WEBDAV_ENABLED: 'false',
          MCP_ENABLED: 'false',
          NODE_NO_WARNINGS: '1'
        },
        stdio: ['ignore', 'pipe', 'pipe']
      })
      node.process.stdout.on('data', (b) => { node.logs += b.toString() })
      node.process.stderr.on('data', (b) => { node.logs += b.toString() })
      const info = await until(async () => {
        if (node.process.exitCode !== null) throw new Error(`${node.name} exited: ${node.logs}`)
        return status(node)
      })
      node.peerId = info.peerId
      const addr = `/ip4/127.0.0.1/tcp/${node.listenPort}/p2p/${info.peerId}`
      if (!addresses.includes(addr)) addresses.push(addr)
    }

    async function kill(node) {
      const child = node.process
      if (!child || child.exitCode !== null) return
      const exited = new Promise((resolve) => child.once('exit', resolve))
      child.kill('SIGKILL')
      await exited
      node.process = null
    }

    async function leader(live = nodes.filter((n) => n.process)) {
      return until(async () => {
        const info = await Promise.all(live.map(status))
        const leaders = info.flatMap((s, i) => s.isPrimary ? [live[i]] : [])
        return leaders.length === 1 ? leaders[0] : undefined
      })
    }

    async function upload(node, path, bytes) {
      const response = await request(node, `/admin/file?path=${encodeURIComponent(path)}`, {
        method: 'PUT', body: bytes
      })
      const body = await response.json()
      assert.equal(response.status, 200, JSON.stringify(body))
      assert.equal(body.ok, true)
      return body
    }

    async function read(node, path, bytes) {
      return until(async () => {
        const response = await request(node, `/admin/file?path=${encodeURIComponent(path)}`)
        if (response.status !== 200) return false
        const actual = Buffer.from(await response.arrayBuffer())
        assert.deepEqual(actual, bytes)
        return true
      })
    }

    try {
      for (const node of nodes) await start(node)
      await until(async () => {
        const info = await Promise.all(nodes.map(status))
        return info.every((s) => s.peers.length === 2)
      })
      const first = await leader()
      const original = Buffer.from('acknowledged upload must outlive the Primary\n'.repeat(500))
      await upload(first, '/durable/original.txt', original)
      // No wait for background replication: crash immediately after the successful HTTP response.
      await kill(first)
      const survivors = nodes.filter((n) => n.process)
      const second = await leader(survivors)
      for (const node of survivors) await read(node, '/durable/original.txt', original)

      const later = Buffer.from('written with one voter offline\n'.repeat(200))
      await upload(second, '/durable/later.txt', later)
      await start(first)
      await until(async () => {
        const info = await Promise.all(nodes.map(status))
        return info.every((s) => s.sha256 === info[0].sha256 && s.lsn === info[0].lsn)
      })
      for (const node of nodes) await read(node, '/durable/later.txt', later)

      const activeLeader = await leader()
      const writer = nodes.find((n) => n !== activeLeader)
      const nonPrimaryBytes = Buffer.from('acknowledged on a non-Primary holder\n'.repeat(250))
      await upload(writer, '/durable/non-primary.txt', nonPrimaryBytes)
      await kill(writer)
      for (const node of nodes.filter((n) => n.process)) {
        await read(node, '/durable/non-primary.txt', nonPrimaryBytes)
      }
      await start(writer)
      await leader()
      await until(async () => {
        const info = await Promise.all(nodes.map(status))
        return info.every((s) => s.sha256 === info[0].sha256 && s.peers.length === 2)
      })

      const parallel = nodes.map((node, i) => ({
        node, path: `/concurrent/file-${i}.txt`, bytes: Buffer.from(`concurrent-${i}\n`.repeat(50))
      }))
      await Promise.all(parallel.map(({ node, path, bytes }) => upload(node, path, bytes)))
      for (const { path, bytes } of parallel) await read(activeLeader, path, bytes)

      const current = await leader()
      const others = nodes.filter((n) => n !== current)
      for (const node of others) await kill(node)
      // A stale local Primary flag must not allow a successful overwrite without a quorum.
      const response = await request(current, '/admin/file?path=/durable/original.txt', {
        method: 'PUT', body: Buffer.from('must not replace acknowledged bytes')
      })
      const failed = await response.json()
      assert.notEqual(response.status, 200, JSON.stringify(failed))
      assert.ok(failed.error)
      const metaResponse = await request(current, '/admin/mkcol', {
        method: 'POST', body: JSON.stringify({ path: '/minority-must-not-commit' })
      })
      assert.notEqual(metaResponse.status, 200)

      for (const node of others) await start(node)
      await leader()
      await until(async () => {
        const info = await Promise.all(nodes.map(status))
        return info.every((s) => s.sha256 === info[0].sha256 && s.peers.length === 2)
      })
      for (const node of nodes) await read(node, '/durable/original.txt', original)
      // Reconstruct the entire cluster from the same on-disk databases and identities.
      await Promise.all(nodes.map(kill))
      for (const node of nodes) await start(node)
      await leader()
      for (const node of nodes) {
        await read(node, '/durable/original.txt', original)
        await read(node, '/durable/later.txt', later)
        await read(node, '/durable/non-primary.txt', nonPrimaryBytes)
      }
    } finally {
      await Promise.all(nodes.map(kill))
      await rm(root, { recursive: true, force: true })
    }
  })
