import test from 'node:test'
import assert from 'node:assert/strict'
import { promises as fs } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { MetadataStore } from '../../dist/metadata/store.js'
import { MetaLog } from '../../dist/metadata/log.js'
import { ContentStore } from '../../dist/content/store.js'
import { CheckpointService } from '../../dist/metadata/checkpoint.js'

test('CheckpointService creates content-addressed snapshot, stores chunk, and restores cleanly', async () => {
  const dir = join(tmpdir(), `p2p-checkpoint-test-${Date.now()}`)
  await fs.mkdir(dir, { recursive: true })

  try {
    const dbPath = join(dir, 'meta.db')
    const contentDir = join(dir, 'content')
    const store = new MetadataStore(dbPath)
    const metaLog = new MetaLog(store.db)
    const contentStore = new ContentStore(contentDir)
    await contentStore.whenReady()

    const checkpointSvc = new CheckpointService(store, contentStore, metaLog, {
      intervalHours: 24,
      retentionDays: 7,
      deltaLogRetentionOps: 100
    })

    // 1. Populate metadata with folders and files
    store.tx(() => {
      store.apply({ op: 'folder.create', id: 1, parentId: 0, name: 'docs', aclId: 0, ctime: 100, mtime: 100 })
      metaLog.append(1, { op: 'folder.create', id: 1, parentId: 0, name: 'docs', aclId: 0, ctime: 100, mtime: 100 }, 100)
      store.setLsn(1)
    })
    store.tx(() => {
      store.apply({
        op: 'file.create',
        id: 1,
        folderId: 1,
        name: 'spec.txt',
        ext: 'txt',
        size: 1024,
        contentType: 'text/plain',
        manifestId: 1,
        aclId: 0,
        ctime: 101,
        mtime: 101
      })
      metaLog.append(2, {
        op: 'file.create',
        id: 1,
        folderId: 1,
        name: 'spec.txt',
        ext: 'txt',
        size: 1024,
        contentType: 'text/plain',
        manifestId: 1,
        aclId: 0,
        ctime: 101,
        mtime: 101
      }, 101)
      store.setLsn(2)
    })

    assert.equal(store.lsn, 2)
    assert.equal(metaLog.latestLsn(), 2)

    // 2. Create checkpoint
    const cp1 = await checkpointSvc.createCheckpoint()
    assert.equal(cp1.lsn, 2)
    assert.ok(cp1.chunkHash)
    assert.ok(cp1.size > 0)

    // Verify chunk exists in content store
    assert.equal(await contentStore.has(cp1.chunkHash), true)

    // Verify latest checkpoint
    const latest = checkpointSvc.getLatestCheckpoint()
    assert.equal(latest?.lsn, 2)
    assert.equal(latest?.chunkHash, cp1.chunkHash)

    // 3. Add more operations to advance LSN
    store.tx(() => {
      store.apply({ op: 'folder.create', id: 2, parentId: 0, name: 'archive', aclId: 0, ctime: 200, mtime: 200 })
      metaLog.append(3, { op: 'folder.create', id: 2, parentId: 0, name: 'archive', aclId: 0, ctime: 200, mtime: 200 }, 200)
      store.setLsn(3)
    })
    assert.equal(store.lsn, 3)

    // 4. Create second checkpoint at LSN 3
    const cp2 = await checkpointSvc.createCheckpoint()
    assert.equal(cp2.lsn, 3)

    // Verify meta_log purge
    const remaining = metaLog.read(0, 100)
    // entries before minLsn should be purged or remaining >= minLsn
    assert.ok(remaining.every((r) => r.lsn >= cp1.lsn))

    // 5. Restore into a brand new store
    const restoreDbPath = join(dir, 'restore.db')
    const restoreStore = new MetadataStore(restoreDbPath)
    assert.equal(restoreStore.lsn, 0)
    assert.equal(restoreStore.resolvePath('/docs/spec.txt').kind, 'none')

    await checkpointSvc.restoreFromCheckpoint(cp1)
    // cp1 was taken at lsn 2 (has /docs/spec.txt but not /archive)
    // Note: checkpointSvc.restoreFromCheckpoint restores into checkpointSvc's store
    // Let's create a restore checkpointSvc for the new store
    const restoreCheckpointSvc = new CheckpointService(restoreStore, contentStore, null)
    await restoreCheckpointSvc.restoreFromCheckpoint(cp1)

    assert.equal(restoreStore.lsn, 2)
    assert.equal(restoreStore.resolvePath('/docs/spec.txt').kind, 'file')
    assert.equal(restoreStore.resolvePath('/archive').kind, 'none')

    store.close()
    restoreStore.close()
  } finally {
    await fs.rm(dir, { recursive: true, force: true })
  }
})
