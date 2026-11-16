/**
 * Persistent libp2p node identity.
 *
 * A node's peerId is derived from its Ed25519 private key. Phase 0 generated a
 * fresh key on every start, so the peerId changed on each restart. That breaks
 * two things we now need for cross-network operation:
 *   - static `BOOTSTRAP_PEERS` (the `/p2p/<peerId>` suffix goes stale), and
 *   - stable Primary identity across restarts / elections.
 *
 * We persist the key (protobuf-encoded) under `${DATA_DIR}/identity/node.key`
 * with owner-only permissions and load it on subsequent starts, so the peerId is
 * stable for the life of the data directory.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { generateKeyPair, privateKeyFromProtobuf, privateKeyToProtobuf } from '@libp2p/crypto/keys'
import type { PrivateKey } from '@libp2p/interface'
import { log } from '../logger.js'

/** Absolute path of the persisted identity key for a data directory. */
export function identityPath(dataDir: string): string {
  return join(dataDir.replace(/[/\\]+$/, ''), 'identity', 'node.key')
}

/**
 * Load the node's persistent Ed25519 identity from DATA_DIR, generating and
 * persisting a new one on first run. A malformed existing key is surfaced (not
 * silently regenerated) so we never accidentally change a node's peerId.
 */
export async function loadOrCreateIdentity(dataDir: string): Promise<PrivateKey> {
  const path = identityPath(dataDir)
  if (existsSync(path)) {
    // The cast bridges a benign @libp2p/interface version skew: @libp2p/crypto
    // pulls interface v3 while libp2p uses interface v2 (only a Uint8ArrayList
    // generic differs). The key object is exactly what createLibp2p consumes at
    // runtime.
    const key = privateKeyFromProtobuf(readFileSync(path)) as unknown as PrivateKey
    log.info('loaded persistent identity', { path })
    return key
  }
  const generated = await generateKeyPair('Ed25519')
  mkdirSync(dirname(path), { recursive: true })
  writeFileSync(path, privateKeyToProtobuf(generated), { mode: 0o600 })
  log.info('generated new persistent identity', { path })
  return generated as unknown as PrivateKey
}
