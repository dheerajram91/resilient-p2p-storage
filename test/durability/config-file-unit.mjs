import test from 'node:test'
import assert from 'node:assert/strict'
import { writeFileSync, unlinkSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { loadConfig } from '../../dist/config.js'

test('config file loading merges file defaults with environment overrides', () => {
  const tmpConfig = join(tmpdir(), `p2p-test-config-${Date.now()}.json`)
  const fileContent = {
    name: 'file-node',
    role: 'anchor',
    storageQuotaGb: 40,
    maxContainers: 500,
    tierEnabled: true,
    tierRole: 'relay',
    tierScaleThreshold: 50,
    relayRoster: ['file-node', 'relay-2'],
    checkpointIntervalHours: 12,
    checkpointRetentionDays: 3,
    deltaLogRetentionOps: 5000
  }
  writeFileSync(tmpConfig, JSON.stringify(fileContent), 'utf8')

  try {
    // 1. Load directly from file without env overrides
    const config1 = loadConfig({}, tmpConfig)
    assert.equal(config1.name, 'file-node')
    assert.equal(config1.role, 'anchor')
    assert.equal(config1.storageQuotaBytes, 40 * 1024 * 1024 * 1024)
    assert.equal(config1.maxContainers, 500)
    assert.equal(config1.tierEnabled, true)
    assert.equal(config1.tierRole, 'relay')
    assert.equal(config1.tierScaleThreshold, 50)
    assert.deepEqual(config1.relayRoster, ['file-node', 'relay-2'])
    assert.equal(config1.checkpointIntervalHours, 12)
    assert.equal(config1.checkpointRetentionDays, 3)
    assert.equal(config1.deltaLogRetentionOps, 5000)

    // 2. Env overrides file values
    const config2 = loadConfig({
      NODE_NAME: 'env-override-node',
      MAX_CONTAINERS: '250',
      TIER_ROLE: 'leaf'
    }, tmpConfig)
    assert.equal(config2.name, 'env-override-node')
    assert.equal(config2.maxContainers, 250)
    assert.equal(config2.tierRole, 'leaf')
    // Untouched values stay as file defaults
    assert.equal(config2.checkpointIntervalHours, 12)
  } finally {
    try { unlinkSync(tmpConfig) } catch {}
  }
})

test('default maxContainers is 1000 and auto tier settings resolve cleanly', () => {
  const config = loadConfig({ NODE_NAME: 'default-test' })
  assert.equal(config.maxContainers, 1000)
  assert.equal(config.tierEnabled, false)
  assert.equal(config.tierRole, 'auto')
  assert.equal(config.tierScaleThreshold, 100)
  assert.deepEqual(config.relayRoster, [])
  assert.equal(config.checkpointIntervalHours, 24)
  assert.equal(config.checkpointRetentionDays, 7)
  assert.equal(config.deltaLogRetentionOps, 10000)
})
