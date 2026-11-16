/**
 * Fast content durability harness with real temporary files and in-memory peers.
 * Run after npm run build: node test/durability/content-unit.mjs
 */
import assert from 'node:assert/strict'
import { promises as fs } from 'node:fs'
import { tmpdir } from 'node:os'
import { dirname, join } from 'node:path'
import { fileURLToPath, pathToFileURL } from 'node:url'
import * as lp from 'it-length-prefixed'
import { pipe } from 'it-pipe'

const dist = join(dirname(fileURLToPath(import.meta.url)), '..', '..', 'dist')
const { ContentStore, CorruptChunkError } = await import(pathToFileURL(join(dist, 'content', 'store.js')).href)
const { ContentService } = await import(pathToFileURL(join(dist, 'content', 'service.js')).href)
const { ContentTransfer } = await import(pathToFileURL(join(dist, 'content', 'transfer.js')).href)
const { RebalanceService } = await import(pathToFileURL(join(dist, 'rebalance', 'service.js')).href)
const { hashBytes } = await import(pathToFileURL(join(dist, 'content', 'chunker.js')).href)
const { PROTOCOLS } = await import(pathToFileURL(join(dist, 'p2p', 'protocols.js')).href)

const root = await fs.mkdtemp(join(tmpdir(), 'p2p-content-unit-'))
const tests = []
const test = (name, fn) => tests.push({ name, fn })
const wait = (ms) => new Promise((resolve) => setTimeout(resolve, ms))
const deferred = () => {
  let resolve
  const promise = new Promise((r) => { resolve = r })
  return { promise, resolve }
}
let sequence = 0
async function store() {
  const dir = join(root, String(++sequence), 'nested', 'content')
  const content = new ContentStore(dir)
  await content.whenReady()
  return { content, dir }
}
const chunkPath = (dir, hash) => join(dir, hash.slice(0, 2), hash.slice(2, 4), hash)
const peer = (id) => ({ toString: () => id })

function fixture(content, overrides = {}) {
  const events = []
  const locations = new Map()
  const files = new Map()
  const manifests = new Map()
  const peers = [{ peerId: 'backup', peer: peer('backup'), usedBytes: 0, quotaBytes: 1024 }]
  const meta = {
    mode: 'push', lsn: 0,
    getLocation: (hash) => locations.get(hash),
    resolveFile: (path) => files.get(path),
    getManifest: (id) => manifests.get(id),
    degradedActiveLocations: () => []
  }
  const transfer = {
    registerHandlers() {},
    async putChunk(dest, hash, bytes, token) {
      assert.equal(hashBytes(bytes), hash)
      assert.ok(token)
      events.push('backup')
      return true
    },
    async releaseChunk() { events.push('release') },
    async getChunk() { return null },
    ...overrides.transfer
  }
  const replication = {
    async createFile(args) {
      events.push('metadata')
      for (const c of args.chunks) {
        assert.ok(c.backupPeer)
        assert.notEqual(c.backupPeer, args.activePeer)
        assert.equal(hashBytes(await content.get(c.hash)), c.hash)
        locations.set(c.hash, { active_peer: args.activePeer, backup_peer: c.backupPeer, state: 'ok' })
      }
      meta.lsn++
      manifests.set(meta.lsn, { chunkHashes: args.chunks.map((c) => c.hash) })
      files.set(args.path, { manifest_id: meta.lsn, content_type: args.contentType })
      return { ok: true, lsn: meta.lsn, fileId: 1 }
    },
    async setBackup() { return { ok: true, lsn: ++meta.lsn } },
    ...overrides.replication
  }
  const opts = {
    chunkSize: 4, quotaBytes: 1024, replicationFactor: 2,
    warmCacheMaxBytes: 512, reconcileIntervalMs: 0, reconcileBatch: 10,
    isHealthy: () => true,
    ...overrides.opts
  }
  const service = new ContentService(
    { peerId: peer('self') }, content, transfer, meta, replication,
    { all: () => peers, get: (id) => peers.find((p) => p.peerId === id) }, opts
  )
  return { service, meta, transfer, replication, events, locations, files, manifests, peers, opts }
}

test('real files: verified concurrent dedup, corruption replacement and exact accounting', async () => {
  const { content, dir } = await store()
  const bytes = Buffer.from('durable chunk')
  const hash = hashBytes(bytes)
  assert.equal(content.generation(hash), 0)
  await Promise.all(Array.from({ length: 20 }, () => content.put(hash, bytes)))
  assert.equal(content.generation(hash), 20)
  assert.equal(content.usedBytes(), bytes.length)
  assert.deepEqual(await content.get(hash), bytes)
  const reopened = new ContentStore(dir)
  await reopened.whenReady()
  assert.equal(reopened.usedBytes(), bytes.length)
  await fs.writeFile(chunkPath(dir, hash), Buffer.from('x'))
  await assert.rejects(content.get(hash), CorruptChunkError)
  await Promise.all(Array.from({ length: 10 }, () => content.put(hash, bytes)))
  assert.equal(content.generation(hash), 30)
  assert.deepEqual(await content.get(hash), bytes)
  assert.equal(content.usedBytes(), bytes.length)
  assert.deepEqual(await fs.readdir(dirname(chunkPath(dir, hash))), [hash])
  await content.corruptForTest(hash)
  assert.equal(content.usedBytes(), (await fs.stat(chunkPath(dir, hash))).size)
  await content.put(hash, bytes)
  assert.equal(content.usedBytes(), bytes.length)
  const generation = content.generation(hash)
  await assert.rejects(content.put(hash, Buffer.from('invalid')), CorruptChunkError)
  assert.equal(content.generation(hash), generation)
  await content.delete(hash)
  assert.equal(content.usedBytes(), 0)
})

test('file flushes happen on new writes and existing dedup; sync failures propagate and clean temp files', async () => {
  const { content, dir } = await store()
  const bytes = Buffer.from('flush')
  const hash = hashBytes(bytes)
  const original = fs.open
  const events = []
  let fail = true
  fs.open = async (path, flags, ...args) => {
    const handle = await original(path, flags, ...args)
    return new Proxy(handle, {
      get(target, key) {
        if (key === 'sync') return async () => {
          events.push({ path: String(path), flags })
          if (fail && String(path).includes('.tmp-')) throw new Error('injected sync failure')
          await target.sync()
        }
        const value = Reflect.get(target, key, target)
        return typeof value === 'function' ? value.bind(target) : value
      }
    })
  }
  try {
    await assert.rejects(content.put(hash, bytes), /injected sync failure/)
    assert.equal(content.generation(hash), 0)
    assert.equal(await content.has(hash), false)
    assert.equal(content.usedBytes(), 0)
    assert.deepEqual(await fs.readdir(dirname(chunkPath(dir, hash))), [])
    fail = false
    await content.put(hash, bytes)
    assert.equal(content.generation(hash), 1)
    events.length = 0
    await content.put(hash, bytes)
    assert.equal(content.generation(hash), 2)
    assert.ok(events.some((e) => e.path === chunkPath(dir, hash) && e.flags === 'r+'))
    fs.open = async (path, flags, ...args) => {
      const handle = await original(path, flags, ...args)
      if (String(path) !== chunkPath(dir, hash)) return handle
      return new Proxy(handle, {
        get(target, key) {
          if (key === 'sync') return async () => { throw new Error('dedup flush failure') }
          const value = Reflect.get(target, key, target)
          return typeof value === 'function' ? value.bind(target) : value
        }
      })
    }
    await assert.rejects(content.put(hash, bytes), /dedup flush failure/)
    assert.equal(content.generation(hash), 2)
    assert.deepEqual(await content.get(hash), bytes)
  } finally {
    fs.open = original
  }
})

test('platform branches: POSIX parent directories flushed, Windows uses only file flushes', async () => {
  const originalOpen = fs.open
  const originalPlatform = Object.getOwnPropertyDescriptor(process, 'platform')
  const calls = []
  const fileFlushes = []
  let failDir = false
  fs.open = async (path, flags, ...args) => {
    if (flags === 'r') {
      calls.push(String(path))
      return {
        async sync() { if (failDir) throw new Error('directory flush failure') },
        async close() {}
      }
    }
    const handle = await originalOpen(path, flags, ...args)
    return new Proxy(handle, {
      get(target, key) {
        if (key === 'sync') return async () => {
          fileFlushes.push(String(path))
          await target.sync()
        }
        const value = Reflect.get(target, key, target)
        return typeof value === 'function' ? value.bind(target) : value
      }
    })
  }
  try {
    Object.defineProperty(process, 'platform', { value: 'linux' })
    const { content, dir } = await store()
    assert.ok(calls.includes(dir))
    assert.ok(calls.includes(dirname(dir)))
    calls.length = 0
    const bytes = Buffer.from('platform')
    const hash = hashBytes(bytes)
    await content.put(hash, bytes)
    assert.deepEqual(calls, [dirname(chunkPath(dir, hash)), dirname(dirname(chunkPath(dir, hash))), dir])
    calls.length = 0
    await content.pin(hash, 'posix-pin')
    assert.deepEqual(calls, [join(dir, '.upload-pins')])
    failDir = true
    await assert.rejects(content.put(hash, bytes), /directory flush failure/)
    await assert.rejects(content.pin(hash, 'failed-posix-pin'), /directory flush failure/)
    failDir = false
    await content.unpin(hash, 'posix-pin')
    await content.unpin(hash, 'failed-posix-pin')
    Object.defineProperty(process, 'platform', { value: 'win32' })
    calls.length = 0
    await content.put(hash, bytes)
    assert.equal(calls.length, 0)
    fileFlushes.length = 0
    const windows = Buffer.from('windows')
    const windowsHash = hashBytes(windows)
    await content.put(windowsHash, windows)
    assert.equal(calls.length, 0)
    assert.equal(fileFlushes.length, 2)
    assert.ok(fileFlushes[0].includes('.tmp-'))
    assert.equal(fileFlushes[1], chunkPath(dir, windowsHash))
  } finally {
    fs.open = originalOpen
    Object.defineProperty(process, 'platform', originalPlatform)
  }
})

test('scan errors are not acknowledged as an empty usable store', async () => {
  const path = join(root, 'not-a-directory')
  await fs.writeFile(path, 'x')
  const content = new ContentStore(path)
  await assert.rejects(content.whenReady())
  await assert.rejects(content.put(hashBytes(Buffer.from('x')), Buffer.from('x')))
})

test('backup failure never submits metadata or removes an acknowledged overwrite target', async () => {
  const { content } = await store()
  const f = fixture(content)
  assert.equal((await f.service.writeFile('/file', Buffer.from('old'))).ok, true)
  const old = f.files.get('/file')
  f.events.length = 0
  f.transfer.putChunk = async () => false
  const result = await f.service.writeFile('/file', Buffer.from('new'))
  assert.equal(result.code, 'EUNAVAILABLE')
  assert.equal(f.files.get('/file'), old)
  assert.equal(f.events.includes('metadata'), false)
  assert.equal((await f.service.readFile('/file')).bytes.toString(), 'old')
})

test('exhausted backup capacity across multiple chunks fails before metadata submission', async () => {
  const { content } = await store()
  const f = fixture(content)
  f.peers[0].quotaBytes = 6
  const result = await f.service.writeFile('/file', Buffer.from('abcdefgh'))
  assert.equal(result.code, 'ENOSPC')
  assert.equal(f.events.filter((event) => event === 'backup').length, 1)
  assert.equal(f.events.includes('metadata'), false)
  assert.equal(f.files.has('/file'), false)
})

test('a backup becoming unhealthy during transfer cannot acknowledge the write', async () => {
  const { content } = await store()
  const f = fixture(content, { transfer: {
    async putChunk() {
      f.opts.isHealthy = (id) => id !== 'backup'
      return true
    }
  } })
  assert.equal((await f.service.writeFile('/file', Buffer.from('test'))).code, 'EUNAVAILABLE')
  assert.equal(f.events.includes('metadata'), false)
})

test('success awaits verified backup before metadata; cached location never bypasses backup', async () => {
  const { content } = await store()
  const gate = deferred()
  const entered = deferred()
  const f = fixture(content, { transfer: {
    async putChunk() {
      f.events.push('backup-start')
      entered.resolve()
      await gate.promise
      f.events.push('backup-done')
      return true
    }
  } })
  const bytes = Buffer.from('same')
  const hash = hashBytes(bytes)
  await content.put(hash, bytes)
  f.locations.set(hash, { active_peer: 'elsewhere', backup_peer: 'backup', state: 'ok' })
  const write = f.service.writeFile('/file', bytes)
  await entered.promise
  assert.equal(f.events.includes('metadata'), false)
  assert.equal(f.service.isPinned(hash), true)
  assert.equal(await f.service.deleteLocalChunk(hash), false)
  gate.resolve()
  assert.equal((await write).ok, true)
  assert.deepEqual(f.events.slice(0, 3), ['backup-start', 'backup-done', 'metadata'])
  assert.equal(f.service.isPinned(hash), false)
})

test('dedup writers increment generations so an older relocation cannot delete confirmed content', async () => {
  const { content } = await store()
  const f = fixture(content)
  const bytes = Buffer.from('race')
  const hash = hashBytes(bytes)
  await content.put(hash, bytes)
  const generation = f.service.localGeneration(hash)
  await Promise.all([f.service.writeFile('/a', bytes), f.service.writeFile('/b', bytes)])
  f.locations.set(hash, { active_peer: 'moved', backup_peer: 'backup', state: 'ok' })
  assert.equal(await f.service.deleteLocalChunk(hash, generation), false)
  assert.equal(await content.has(hash), true)
  assert.equal(await f.service.deleteLocalChunk(hash, f.service.localGeneration(hash)), true)
})

test('store generation-conditional deletion preserves a concurrent put in both serialization orderings', async () => {
  const { content } = await store()
  const bytes = Buffer.from('orders')
  const hash = hashBytes(bytes)
  await content.put(hash, bytes)
  const old = content.generation(hash)
  const firstPut = content.put(hash, bytes)
  const staleDelete = content.delete(hash, old)
  await firstPut
  assert.equal(await staleDelete, false)
  assert.equal(content.generation(hash), old + 1)
  assert.deepEqual(await content.get(hash), bytes)

  const current = content.generation(hash)
  const firstDelete = content.delete(hash, current)
  const restoringPut = content.put(hash, bytes)
  assert.equal(await firstDelete, true)
  await restoringPut
  assert.equal(content.generation(hash), current + 1)
  assert.deepEqual(await content.get(hash), bytes)
  assert.equal(content.usedBytes(), bytes.length)
  assert.equal(await content.delete(hash, current), false)
  assert.equal(await content.delete(hash, content.generation(hash)), true)
  assert.equal(await content.delete(hash), false)
})

test('rebalance drops 94% usage below 70% even when committed location moves precede local metadata apply', async () => {
  const { content } = await store()
  const committed = new Map()
  const f = fixture(content, {
    transfer: { async putChunk() { return true } },
    replication: {
      async relocateActive(hash, active, backup, state, expected) {
        assert.deepEqual(expected, { active: 'self', backup: 'backup' })
        committed.set(hash, { active, backup, state })
        // Location intents are not read-your-writes operations: the source can
        // still see its old holder rows while it catches up via push or pull.
        f.meta.mode = 'pull'
        return { ok: true, lsn: ++f.meta.lsn }
      }
    }
  })
  const rows = []
  for (let i = 0; i < 15; i++) {
    const bytes = Buffer.alloc(4, i)
    const hash = hashBytes(bytes)
    await content.put(hash, bytes)
    const row = { chunk_hash: hash, size: 4, active_peer: 'self', backup_peer: 'backup', state: 'ok' }
    rows.push(row)
    f.locations.set(hash, row)
  }
  f.meta.heldChunksBySize = () => rows
  f.peers.push({ peerId: 'other', peer: peer('other'), usedBytes: 0, quotaBytes: 1024 })
  const rebalance = new RebalanceService(
    { peerId: peer('self') }, f.service, f.transfer, f.meta, f.replication,
    { quotaBytes: 64, high: 0.85, low: 0.7, intervalMs: 0, isHealthy: () => true }
  )
  assert.equal(content.usedBytes(), 60)
  assert.equal(await rebalance.sweepOnce(), 4)
  assert.equal(content.usedBytes(), 44)
  assert.ok(content.usedBytes() <= 64 * 0.7)
  assert.equal(committed.size, 4)
  for (const [hash, location] of committed) {
    assert.equal(location.active, 'other')
    assert.equal(location.backup, 'backup')
    assert.equal(f.meta.getLocation(hash).active_peer, 'self')
    assert.equal(await content.has(hash), false)
  }
})

test('protection acquired during deletion IO prevents unlink, including dedup writes', async () => {
  const { content, dir } = await store()
  const bytes = Buffer.from('race')
  const hash = hashBytes(bytes)
  await content.put(hash, bytes)
  const entered = deferred()
  const gate = deferred()
  const original = fs.stat
  fs.stat = async (path, ...args) => {
    const result = await original(path, ...args)
    if (String(path) === chunkPath(dir, hash)) {
      entered.resolve()
      await gate.promise
    }
    return result
  }
  try {
    const deletion = content.delete(hash)
    await entered.promise
    const pinned = content.pin(hash, 'during-delete')
    const put = content.put(hash, bytes)
    gate.resolve()
    assert.equal(await deletion, false)
    await pinned
    await put
    await content.unpin(hash, 'during-delete')
    assert.deepEqual(await content.get(hash), bytes)
    assert.equal(content.usedBytes(), bytes.length)
  } finally {
    gate.resolve()
    fs.stat = original
  }
})

test('resolved metadata rejection releases upload pins without deleting stored copies', async () => {
  const { content } = await store()
  const f = fixture(content, { replication: { async createFile() { return { error: 'invalid file path' } } } })
  const bytes = Buffer.from('pins')
  const hash = hashBytes(bytes)
  const result = await f.service.writeFile('/file', bytes)
  assert.equal(result.error, 'invalid file path')
  assert.equal(content.isPinned(hash), false)
  assert.equal(f.events.includes('release'), true)
  assert.deepEqual(await content.get(hash), bytes)
})

test('metadata EUNAVAILABLE propagates unchanged and retains pins for a potentially late commit', async () => {
  const { content } = await store()
  const unavailable = { error: 'metadata quorum timed out', code: 'EUNAVAILABLE' }
  const f = fixture(content, { replication: { async createFile() { return unavailable } } })
  const bytes = Buffer.from('pins')
  assert.deepEqual(await f.service.writeFile('/file', bytes), unavailable)
  assert.equal(content.isPinned(hashBytes(bytes)), true)
  assert.equal(f.events.includes('release'), false)
  assert.deepEqual(await content.get(hashBytes(bytes)), bytes)
})

test('thrown metadata outcomes retain pins because a late commit is still possible', async () => {
  const { content } = await store()
  const f = fixture(content, { replication: { async createFile() { throw new Error('unknown commit outcome') } } })
  const bytes = Buffer.from('pins')
  const result = await f.service.writeFile('/file', bytes)
  assert.equal(result.code, 'EIO')
  assert.equal(content.isPinned(hashBytes(bytes)), true)
  assert.equal(f.events.includes('release'), false)
})

test('named upload pins are immediately visible, persisted, idempotent and token-isolated', async () => {
  const { content } = await store()
  const bytes = Buffer.from('pins')
  const hash = hashBytes(bytes)
  await content.put(hash, bytes)
  const generation = content.generation(hash)
  const first = content.pin(hash, 'first')
  assert.equal(content.isPinned(hash), true)
  await first
  await content.pin(hash, 'first')
  await content.pin(hash, 'second')
  assert.equal(content.isPinned(hash), true)
  assert.equal(await content.delete(hash, generation), false)
  await content.unpin(hash, 'unknown')
  await content.unpin(hash, 'first')
  assert.equal(content.isPinned(hash), true)
  await content.unpin(hash, 'second')
  assert.equal(content.isPinned(hash), false)
  await content.pin(hash, 'repin')
  const release = content.unpin(hash, 'repin')
  const repin = content.pin(hash, 'repin')
  await Promise.all([release, repin])
  assert.equal(content.isPinned(hash), true)
  assert.equal(await content.delete(hash, generation), false)
  await content.unpin(hash, 'repin')
  assert.equal(content.generation(hash), generation)
  assert.equal(await content.delete(hash, generation), true)
})

test('durable upload pins survive store restart and persist release without affecting chunk accounting', async () => {
  const { content, dir } = await store()
  const bytes = Buffer.from('restart')
  const hash = hashBytes(bytes)
  await content.put(hash, bytes)
  await content.pin(hash, 'survivor')
  const restarted = new ContentStore(dir)
  await restarted.whenReady()
  assert.equal(restarted.isPinned(hash), true)
  assert.equal(restarted.generation(hash), 0)
  assert.equal(await restarted.delete(hash, 0), false)
  assert.equal(restarted.usedBytes(), bytes.length)
  assert.deepEqual(await restarted.list(), [hash])
  await restarted.unpin(hash, 'survivor')
  const released = new ContentStore(dir)
  await released.whenReady()
  assert.equal(released.isPinned(hash), false)
  assert.equal(await released.delete(hash, 0), true)
})

test('pin fsync failure prevents local placement and metadata publication and removes its precise temp', async () => {
  const { content, dir } = await store()
  const original = fs.open
  fs.open = async (path, flags, ...args) => {
    const handle = await original(path, flags, ...args)
    if (!String(path).includes('.upload-pins') || !String(path).includes('.tmp-')) return handle
    return new Proxy(handle, {
      get(target, key) {
        if (key === 'sync') return async () => { throw new Error('injected pin fsync failure') }
        const value = Reflect.get(target, key, target)
        return typeof value === 'function' ? value.bind(target) : value
      }
    })
  }
  try {
    const f = fixture(content)
    const bytes = Buffer.from('pins')
    assert.equal((await f.service.writeFile('/file', bytes)).code, 'EIO')
    assert.equal(f.events.includes('metadata'), false)
    assert.equal(f.events.includes('backup'), false)
    assert.equal(await content.has(hashBytes(bytes)), false)
    assert.deepEqual(await fs.readdir(join(dir, '.upload-pins')), [])
    const remote = await network()
    assert.equal(await remote.sender.putChunk(peer('backup'), hashBytes(bytes), bytes, 'failed-pin'), false)
    assert.equal(await remote.content.has(hashBytes(bytes)), false)
    assert.deepEqual(await fs.readdir(join(remote.dir, '.upload-pins')), [])
  } finally {
    fs.open = original
  }
})

test('corrupt persisted pins fail startup instead of silently enabling relocation deletion', async () => {
  const { content, dir } = await store()
  const bytes = Buffer.from('pins')
  const hash = hashBytes(bytes)
  await content.put(hash, bytes)
  await content.pin(hash, 'corrupted-pin')
  const [name] = await fs.readdir(join(dir, '.upload-pins'))
  await fs.writeFile(join(dir, '.upload-pins', name), 'invalid pin record')
  const restarted = new ContentStore(dir)
  await assert.rejects(restarted.whenReady(), SyntaxError)
  await assert.rejects(restarted.delete(hash), SyntaxError)
})

test('disk ENOSPC and other local IO failures return explicit errors without metadata submission', async () => {
  const { content } = await store()
  const f = fixture(content)
  content.put = async () => { throw Object.assign(new Error('disk full'), { code: 'ENOSPC' }) }
  assert.equal((await f.service.writeFile('/file', Buffer.from('test'))).code, 'ENOSPC')
  content.put = async () => { throw new Error('fsync failure') }
  assert.equal((await f.service.writeFile('/file', Buffer.from('test'))).code, 'EIO')
  assert.equal(f.events.includes('metadata'), false)
})

test('empty files have no chunks but require two healthy dialable peers; quota admission stays ENOSPC', async () => {
  const { content } = await store()
  const f = fixture(content)
  f.peers.length = 0
  assert.equal((await f.service.writeFile('/empty', Buffer.alloc(0))).code, 'ENOSPC')
  f.peers.push({ peerId: 'backup', usedBytes: 0, quotaBytes: 1024 })
  assert.equal((await f.service.writeFile('/empty', Buffer.alloc(0))).code, 'ENOSPC')
  f.peers[0].peer = peer('backup')
  f.opts.isHealthy = (id) => id !== 'backup'
  assert.equal((await f.service.writeFile('/empty', Buffer.alloc(0))).code, 'ENOSPC')
  f.opts.isHealthy = () => true
  const result = await f.service.writeFile('/empty', Buffer.alloc(0))
  assert.equal(result.ok, true)
  assert.equal(result.chunks, 0)
  assert.equal(f.events.includes('backup'), false)
  assert.equal((await f.service.readFile('/empty')).bytes.length, 0)
  f.opts.quotaBytes = 1
  assert.equal((await f.service.writeFile('/big', Buffer.from('large'))).code, 'ENOSPC')
})

test('failed peer is bounded by candidate count and a healthy alternative can succeed', async () => {
  const { content } = await store()
  const tried = []
  const f = fixture(content, { transfer: {
    async putChunk(dest) {
      tried.push(dest.toString())
      if (dest.toString() === 'backup') throw new Error('offline')
      return true
    }
  } })
  f.peers.push({ peerId: 'other', peer: peer('other'), usedBytes: 1, quotaBytes: 1024 })
  assert.equal((await f.service.writeFile('/file', Buffer.from('test'))).ok, true)
  assert.deepEqual(tried, ['backup', 'other'])
})

test('reconciliation submits the pre-transfer location pair and treats stale CAS rejection as failure', async () => {
  const { content } = await store()
  const bytes = Buffer.from('heal')
  const hash = hashBytes(bytes)
  await content.put(hash, bytes)
  const location = {
    chunk_hash: hash, active_peer: 'self', backup_peer: 'old-backup', state: 'degraded', size: bytes.length
  }
  let submitted = false
  const f = fixture(content, {
    transfer: {
      async putChunk() {
        location.active_peer = 'backup'
        location.backup_peer = 'other'
        location.state = 'ok'
        return true
      }
    },
    replication: {
      async setBackup(chunk, backup, state, expected) {
        submitted = true
        assert.equal(chunk, hash)
        assert.equal(backup, 'backup')
        assert.equal(state, 'ok')
        assert.deepEqual(expected, { active: 'self', backup: 'old-backup' })
        return { error: 'stale location' }
      }
    }
  })
  f.locations.set(hash, location)
  f.meta.degradedActiveLocations = () => [location]
  assert.equal(await f.service.reconcileOnce(), 0)
  assert.equal(submitted, true)
  assert.equal(f.locations.get(hash).active_peer, 'backup')
  assert.equal(f.locations.get(hash).backup_peer, 'other')
})

test('warm caching respects low-water threshold and corrupt read repair replaces atomically', async () => {
  const { content } = await store()
  const bytes = Buffer.from('read')
  const hash = hashBytes(bytes)
  const f = fixture(content, { transfer: { async getChunk() { return bytes } } })
  f.locations.set(hash, { active_peer: 'backup', backup_peer: null })
  f.manifests.set(1, { chunkHashes: [hash] })
  f.files.set('/file', { manifest_id: 1, content_type: null })
  f.opts.warmCacheMaxBytes = 0
  assert.deepEqual((await f.service.readFile('/file')).bytes, bytes)
  assert.equal(await content.has(hash), false)
  f.opts.warmCacheMaxBytes = 10
  await f.service.readFile('/file')
  assert.equal(await content.has(hash), true)
  await content.corruptForTest(hash)
  const unpin = await content.protect(hash)
  assert.deepEqual((await f.service.readFile('/file')).bytes, bytes)
  await unpin()
  assert.deepEqual(await content.get(hash), bytes)
})

function channel() {
  const values = []
  let notify
  let ended = false
  let error
  return {
    push(value) { values.push(value); notify?.() },
    end(err) { error = err; ended = true; notify?.() },
    async *[Symbol.asyncIterator]() {
      while (true) {
        while (values.length) yield values.shift()
        if (ended) { if (error) throw error; return }
        await new Promise((resolve) => { notify = resolve })
      }
    }
  }
}

function streamPair() {
  const a = channel()
  const b = channel()
  let aborts = 0
  const endpoint = (source, target) => ({
    source,
    async sink(input) {
      try {
        for await (const frame of input) target.push(frame)
        target.end()
      } catch (err) { target.end(err); throw err }
    },
    async close() {},
    abort(err) { aborts++; a.end(err); b.end(err) }
  })
  return { client: endpoint(a, b), server: endpoint(b, a), aborts: () => aborts }
}

async function network(directory) {
  const local = directory ? { content: new ContentStore(directory), dir: directory } : await store()
  const { content, dir } = local
  await content.whenReady()
  const handlers = new Map()
  const receiver = new ContentTransfer({ async handle(protocol, fn) { handlers.set(protocol, fn) } }, content, 250)
  let lsn = 0
  receiver.registerHandlers((committed) => lsn >= committed)
  const senderFor = (owner) => new ContentTransfer({
    async dialProtocol(dest, protocol, options) {
      assert.ok(options.signal)
      const pair = streamPair()
      handlers.get(protocol)({ stream: pair.server, connection: { remotePeer: peer(owner) } })
      return pair.client
    }
  }, content, 250)
  return { content, dir, sender: senderFor('sender'), senderFor, receiver, apply: (next) => { lsn = next } }
}

test('receiver restart preserves pins before publication and only the original sender can release them', async () => {
  const original = await network()
  const bytes = Buffer.from('restart-backup')
  const hash = hashBytes(bytes)
  assert.equal(await original.sender.putChunk(peer('backup'), hash, bytes, 'surviving-upload'), true)
  const restarted = await network(original.dir)
  assert.equal(restarted.content.isPinned(hash), true)
  assert.equal(await restarted.content.delete(hash, restarted.content.generation(hash)), false)
  await restarted.senderFor('stranger').releaseChunk(peer('backup'), hash, 'surviving-upload')
  assert.equal(restarted.content.isPinned(hash), true)
  restarted.apply(7)
  await restarted.sender.releaseChunk(peer('backup'), hash, 'surviving-upload', 7)
  const afterRelease = new ContentStore(original.dir)
  await afterRelease.whenReady()
  assert.equal(afterRelease.isPinned(hash), false)
  assert.deepEqual(await afterRelease.get(hash), bytes)
})

test('both holders stay upload-pinned through metadata resolution, using one token across chunks', async () => {
  const { content } = await store()
  const remote = await network()
  const gate = deferred()
  const entered = deferred()
  const tokens = []
  const hashes = []
  const f = fixture(content, {
    transfer: {
      async putChunk(dest, hash, bytes, token) {
        tokens.push(token)
        hashes.push(hash)
        return remote.sender.putChunk(dest, hash, bytes, token)
      },
      releaseChunk: remote.sender.releaseChunk.bind(remote.sender)
    },
    replication: {
      async createFile(args) {
        assert.equal(args.chunks.length, 2)
        entered.resolve()
        await gate.promise
        remote.apply(7)
        return { ok: true, lsn: 7 }
      }
    }
  })
  const write = f.service.writeFile('/file', Buffer.from('abcdwxyz'))
  await entered.promise
  assert.equal(new Set(tokens).size, 1)
  for (const hash of hashes) {
    assert.equal(f.service.isPinned(hash), true)
    assert.equal(remote.content.isPinned(hash), true)
    assert.equal(await content.delete(hash, content.generation(hash)), false)
    assert.equal(await remote.content.delete(hash, remote.content.generation(hash)), false)
  }
  gate.resolve()
  assert.equal((await write).ok, true)
  for (const hash of hashes) {
    assert.equal(content.isPinned(hash), false)
    assert.equal(remote.content.isPinned(hash), false)
    assert.equal(await content.has(hash), true)
    assert.equal(await remote.content.has(hash), true)
  }
})

test('remote release is scoped to authenticated caller, hash and upload token', async () => {
  const { content, sender, senderFor } = await network()
  const bytes = Buffer.from('owners')
  const hash = hashBytes(bytes)
  assert.equal(await sender.putChunk(peer('backup'), hash, bytes, 'shared-token'), true)
  const foreign = senderFor('other-sender')
  await foreign.releaseChunk(peer('backup'), hash, 'shared-token')
  assert.equal(content.isPinned(hash), true)
  await sender.releaseChunk(peer('backup'), hashBytes(Buffer.from('other-hash')), 'shared-token')
  assert.equal(content.isPinned(hash), true)
  assert.equal(await foreign.putChunk(peer('backup'), hash, bytes, 'shared-token'), true)
  await sender.releaseChunk(peer('backup'), hash, 'shared-token')
  assert.equal(content.isPinned(hash), true)
  await foreign.releaseChunk(peer('backup'), hash, 'shared-token')
  assert.equal(content.isPinned(hash), false)
})

test('real transfer handler acks flushed verified content, verifies gets and pins until remote metadata applies', async () => {
  const { content, sender, apply } = await network()
  const bytes = Buffer.from('network')
  const hash = hashBytes(bytes)
  assert.equal(await sender.putChunk(peer('backup'), hash, bytes, 'lease'), true)
  const generation = content.generation(hash)
  assert.equal(await sender.putChunk(peer('backup'), hash, bytes), true)
  assert.equal(content.generation(hash), generation + 1)
  assert.equal(await content.delete(hash, generation), false)
  assert.deepEqual(await sender.getChunk(peer('backup'), hash), bytes)
  assert.equal(await content.delete(hash), false)
  await sender.releaseChunk(peer('backup'), hash, 'lease', 7)
  assert.equal(await content.delete(hash), false)
  apply(7)
  await wait(130)
  await content.delete(hash)
  assert.equal(await content.has(hash), false)
})

test('remote sync failure cannot produce durable success', async () => {
  const { content, sender } = await network()
  content.put = async () => { throw new Error('injected remote fsync failure') }
  const bytes = Buffer.from('remote')
  assert.equal(await sender.putChunk(peer('backup'), hashBytes(bytes), bytes, 'failed'), false)
  assert.equal(content.isProtected(hashBytes(bytes)), false)
})

test('full transfer deadlines bound dial, hung body and hung close and abort acquired streams', async () => {
  const { content } = await store()
  const keepAlive = setInterval(() => {}, 100)
  try {
    const dial = new ContentTransfer({ dialProtocol: () => new Promise(() => {}) }, content, 30)
    const bytes = Buffer.from('timeout')
    const hash = hashBytes(bytes)
    const before = Date.now()
    await assert.rejects(dial.putChunk(peer('backup'), hash, bytes), /deadline/)
    assert.ok(Date.now() - before < 1000)
    let aborted = false
    const hung = {
      source: { async *[Symbol.asyncIterator]() { await new Promise(() => {}) } },
      async sink(input) { for await (const frame of input) void frame },
      async close() {},
      abort() { aborted = true }
    }
    const transfer = new ContentTransfer({ async dialProtocol() { return hung } }, content, 30)
    await assert.rejects(transfer.getChunk(peer('backup'), hash), /deadline/)
    assert.equal(aborted, true)
    const pair = streamPair()
    pair.client.close = () => new Promise(() => {})
    void pipe(
      pair.server,
      (s) => lp.decode(s),
      async function* (source) {
        for await (const frame of source) { void frame; break }
        yield Buffer.from(JSON.stringify({ ok: true, durable: true, hash, size: bytes.length }))
      },
      (s) => lp.encode(s),
      pair.server
    ).catch(() => {})
    const close = new ContentTransfer({ async dialProtocol() { return pair.client } }, content, 30)
    await assert.rejects(close.putChunk(peer('backup'), hash, bytes), /deadline/)
    assert.ok(pair.aborts() > 0)
  } finally {
    clearInterval(keepAlive)
  }
})

test('unverified legacy acknowledgments and malformed remote bytes cannot count as a second copy', async () => {
  const { content } = await store()
  const bytes = Buffer.from('valid')
  const hash = hashBytes(bytes)
  const make = (frames) => new ContentTransfer({
    async dialProtocol() {
      const encoded = []
      for await (const frame of lp.encode(frames)) encoded.push(frame)
      return {
        source: encoded,
        async sink(source) { for await (const frame of source) void frame },
        async close() {},
        abort() {}
      }
    }
  }, content, 100)
  assert.equal(await make([Buffer.from('{"ok":true}')]).putChunk(peer('backup'), hash, bytes), false)
  const bad = make([Buffer.from('{"ok":true,"size":5}'), Buffer.from('wrong')])
  await assert.rejects(bad.getChunk(peer('backup'), hash), /integrity/)
})

let failures = 0
try {
  for (const { name, fn } of tests) {
    try {
      await fn()
      console.log(`  ok ${name}`)
    } catch (err) {
      failures++
      console.error(`  FAIL ${name}`, err)
    }
  }
} finally {
  await fs.rm(root, { recursive: true, force: true })
}
console.log(`\n${tests.length - failures}/${tests.length} content durability tests passed`)
if (failures) process.exitCode = 1
