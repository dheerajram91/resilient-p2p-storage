import test from 'node:test'
import assert from 'node:assert/strict'
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { createHash, randomBytes } from 'node:crypto'
import { ContentStore, CorruptChunkError } from '../../dist/content/store.js'

const sha256 = (buf) => createHash('sha256').update(buf).digest('hex')

test('ContentStore encryption-at-rest with AES-256-GCM', async () => {
  const root = await mkdtemp(join(tmpdir(), 'p2p-enc-unit-'))
  const key = randomBytes(32)
  const wrongKey = randomBytes(32)

  try {
    const encStore = new ContentStore(root, key)
    const plaintext = Buffer.from('hello encrypted world! '.repeat(200))
    const hash = sha256(plaintext)

    // Put encrypted chunk
    await encStore.put(hash, plaintext)
    assert.equal(await encStore.has(hash), true)

    // Read back through encStore -> exact plaintext
    const decrypted = await encStore.get(hash)
    assert.deepEqual(decrypted, plaintext)

    // Verify on-disk file is ciphertext: nonce(12) + ciphertext + tag(16) = len + 28
    const diskPath = join(root, hash.slice(0, 2), hash.slice(2, 4), hash)
    const diskBytes = await readFile(diskPath)
    assert.equal(diskBytes.length, plaintext.length + 28)
    assert.notEqual(sha256(diskBytes), hash)

    // Reading with wrong key throws CorruptChunkError
    const wrongStore = new ContentStore(root, wrongKey)
    await assert.rejects(async () => {
      await wrongStore.get(hash)
    }, CorruptChunkError)

    // Reading without key (plain store) throws CorruptChunkError (sha mismatch or corrupt)
    const plainStore = new ContentStore(root, null)
    await assert.rejects(async () => {
      await plainStore.get(hash)
    }, CorruptChunkError)

    // Tampering with ciphertext fails GCM auth tag
    const tampered = Buffer.from(diskBytes)
    tampered[14] ^= 0x01
    await writeFile(diskPath, tampered)

    await assert.rejects(async () => {
      await encStore.get(hash)
    }, CorruptChunkError)

    // Plain store without key puts unencrypted chunks
    const plainRoot = await mkdtemp(join(tmpdir(), 'p2p-plain-unit-'))
    try {
      const pStore = new ContentStore(plainRoot, null)
      const data = Buffer.from('plain data content')
      const pHash = sha256(data)
      await pStore.put(pHash, data)
      const pDiskPath = join(plainRoot, pHash.slice(0, 2), pHash.slice(2, 4), pHash)
      const pDiskBytes = await readFile(pDiskPath)
      assert.deepEqual(pDiskBytes, data)
      assert.equal(sha256(pDiskBytes), pHash)
    } finally {
      await rm(plainRoot, { recursive: true, force: true })
    }
  } finally {
    await rm(root, { recursive: true, force: true })
  }
})
