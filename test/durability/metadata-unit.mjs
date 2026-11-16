import test from 'node:test'
import assert from 'node:assert/strict'
import { MetadataStore } from '../../dist/metadata/store.js'
import { ReplicationService } from '../../dist/replication/service.js'
import { MetaLog } from '../../dist/metadata/log.js'

const options = { isPrimary: false, pushIntervalMs: 1000, batchSize: 500, name: 'a', voters: ['a', 'b', 'c'] }
const node = { peerId: { toString: () => 'peer-a' } }
const cluster = { all: () => [], primaryPeer: () => undefined }

function fixture() {
  const store = new MetadataStore(':memory:')
  const replication = new ReplicationService(node, store, cluster, new MetaLog(store.db), options)
  let commits = 0
  const control = {
    reject: false,
    beforeApply: undefined
  }
  // Isolate materialization/publication from the separately tested Raft transport.
  replication.quorum = {
    isLeader: true,
    ready: true,
    term: 1,
    async propose(command) {
      control.beforeApply?.(command)
      if (control.reject) throw new Error('injected majority loss')
      store.tx(() => {
        for (const op of command.ops) {
          store.apply(op)
          store.setLsn(store.lsn + 1)
        }
      })
      commits++
    }
  }
  return { store, replication, control, commits: () => commits }
}

const file = (hash, backupPeer = 'peer-b') => ({
  path: '/docs/file.txt', size: 3, ext: 'txt', contentType: 'text/plain',
  activePeer: 'peer-a', chunks: [{ hash, size: 3, backupPeer }]
})

test('complete file registration is staged without exposing partial metadata', async () => {
  const f = fixture()
  try {
    f.control.beforeApply = (command) => {
      assert.equal(f.store.resolvePath('/docs/file.txt').kind, 'none')
      assert.equal(f.store.lsn, 0)
      assert.equal(command.ops.at(-1).op, 'file.create')
      const location = command.ops.find((op) => op.op === 'location.set')
      assert.equal(location.active, 'peer-a')
      assert.equal(location.backup, 'peer-b')
      assert.equal(location.state, 'ok')
    }
    const result = await f.replication.createFile(file('a'.repeat(64)))
    assert.equal(result.ok, true)
    assert.equal(f.commits(), 1)
    assert.equal(f.store.resolvePath('/docs/file.txt').kind, 'file')
  } finally {
    f.store.close()
  }
})

test('failed quorum overwrite retains the previously committed file and manifest', async () => {
  const f = fixture()
  try {
    await f.replication.createFile(file('a'.repeat(64)))
    const before = f.store.sha256()
    f.control.reject = true
    const result = await f.replication.createFile(file('b'.repeat(64)))
    assert.equal(result.code, 'EUNAVAILABLE')
    assert.equal(f.store.sha256(), before)
    assert.equal(f.store.getLocation('b'.repeat(64)), undefined)
  } finally {
    f.store.close()
  }
})

test('invalid replica confirmation rolls back the entire staged overwrite', async () => {
  const f = fixture()
  try {
    await f.replication.createFile(file('a'.repeat(64)))
    const before = f.store.sha256()
    for (const backup of [undefined, 'peer-a']) {
      const invalid = file('b'.repeat(64))
      invalid.chunks[0].backupPeer = backup
      const result = await f.replication.createFile(invalid)
      assert.ok(result.error)
      assert.equal(f.store.sha256(), before)
    }
    assert.equal(f.commits(), 1)
  } finally {
    f.store.close()
  }
})

test('stale relocation and co-located backup updates are rejected', async () => {
  const f = fixture()
  try {
    const hash = 'a'.repeat(64)
    await f.replication.createFile(file(hash))
    const before = f.store.sha256()
    const stale = await f.replication.relocateActive(hash, 'peer-c', 'peer-b', 'ok',
      { active: 'not-current', backup: 'peer-b' })
    assert.match(stale.error, /changed/)
    const colocated = await f.replication.setBackup(hash, 'peer-a', 'ok')
    assert.match(colocated.error, /distinct/)
    assert.equal(f.store.sha256(), before)
  } finally {
    f.store.close()
  }
})

test('quorum refuses implicit legacy adoption and unsafe configuration removal', () => {
  const legacy = new MetadataStore(':memory:')
  const fresh = new MetadataStore(':memory:')
  try {
    legacy.setLsn(1)
    assert.throws(() => new ReplicationService(node, legacy, cluster, null, options), /legacy metadata/)
    new ReplicationService(node, fresh, cluster, null, options)
    assert.throws(() => new ReplicationService(node, fresh, cluster, null,
      { ...options, voters: [] }), /must not be removed/)
  } finally {
    legacy.close()
    fresh.close()
  }
})
