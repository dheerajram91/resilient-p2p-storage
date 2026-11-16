import test from 'node:test'
import assert from 'node:assert/strict'
import { loadConfig } from '../../dist/config.js'
import { MetadataStore } from '../../dist/metadata/store.js'

test('quorum configuration requires a fixed three-voter roster including self and RF2', () => {
  const config = loadConfig({ NODE_NAME: 'a', METADATA_VOTERS: 'a,b,c' })
  assert.deepEqual(config.metadataVoters, ['a', 'b', 'c'])
  assert.equal(config.replicationFactor, 2)
  for (const voters of ['a,b', 'a,b,c,d', 'a,a,b', 'b,c,d']) {
    assert.throws(() => loadConfig({ NODE_NAME: 'a', METADATA_VOTERS: voters }), /three distinct/)
  }
  assert.throws(() => loadConfig({
    NODE_NAME: 'a', METADATA_VOTERS: 'a,b,c', REPLICATION_FACTOR: '3'
  }), /REPLICATION_FACTOR=2/)
})

test('legacy selection is explicit in the resolved configuration', () => {
  assert.deepEqual(loadConfig({ NODE_NAME: 'demo' }).metadataVoters, [])
})

test('metadata transactions explicitly use FULL synchronization', () => {
  const store = new MetadataStore(':memory:')
  try {
    assert.equal(store.db.prepare('PRAGMA synchronous').get().synchronous, 2)
  } finally {
    store.close()
  }
})
