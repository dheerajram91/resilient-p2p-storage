/**
 * On-disk content store (DETAILED_DESIGN §4).
 *
 * Chunks are content-addressed and laid out with a 2-level fan-out:
 *   <root>/<hash[0:2]>/<hash[2:4]>/<hash>
 *
 * Integrity is enforced on both write and read: put() and get() verify that
 * sha256(bytes) === hash. A read that fails verification throws so the caller
 * can read-repair from the other replica (§4, §11).
 *
 * usedBytes() powers quota / placement decisions (§5, §10). It is tracked in
 * memory, seeded by a one-time scan of the store at startup.
 */

import { createHash, createCipheriv, createDecipheriv, randomBytes, randomUUID } from 'node:crypto'
import { promises as fs } from 'node:fs'
import { dirname, join, parse, resolve } from 'node:path'
import { log } from '../logger.js'

function sha256(bytes: Buffer): string {
  return createHash('sha256').update(bytes).digest('hex')
}

export class CorruptChunkError extends Error {
  constructor(
    public readonly hash: string,
    public readonly actual: string
  ) {
    super(`chunk ${hash.slice(0, 12)} failed integrity check (got ${actual.slice(0, 12)})`)
    this.name = 'CorruptChunkError'
  }
}

export class ContentStore {
  private used = 0
  private ready: Promise<void>
  private sizes = new Map<string, number>()
  private operations = new Map<string, Promise<unknown>>()
  private protections = new Map<string, Set<string>>()
  private generations = new Map<string, number>()
  private readonly pinDirectory: string

  constructor(private root: string, private encryptionKey: Buffer | null = null) {
    this.root = resolve(root)
    this.pinDirectory = join(this.root, '.upload-pins')
    this.ready = this.scan()
  }

  private async syncDirectory(dir: string): Promise<void> {
    // Node cannot open/fsync directory handles on Windows. File data is flushed
    // before and after rename there, but namespace survival on power loss is NOT
    // guaranteed. POSIX acknowledges only after the directory fsync succeeds.
    if (process.platform === 'win32') return
    const handle = await fs.open(dir, 'r')
    try {
      await handle.sync()
    } finally {
      await handle.close()
    }
  }

  private async syncParents(dir: string, stop: string): Promise<void> {
    for (let current = dir; ; current = dirname(current)) {
      await this.syncDirectory(current)
      if (current === stop || current === dirname(current)) break
    }
  }

  private account(file: string, size: number): void {
    this.used += size - (this.sizes.get(file) ?? 0)
    if (size === 0) this.sizes.delete(file)
    else this.sizes.set(file, size)
  }

  /** AES-256-GCM encrypt. Format: [12 nonce][N ciphertext][16 tag]. */
  private encrypt(plaintext: Buffer): Buffer {
    if (!this.encryptionKey) return plaintext
    const nonce = randomBytes(12)
    const cipher = createCipheriv('aes-256-gcm', this.encryptionKey, nonce)
    const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()])
    const tag = cipher.getAuthTag()
    return Buffer.concat([nonce, ciphertext, tag])
  }

  /** AES-256-GCM decrypt. Throws CorruptChunkError on auth-tag failure. */
  private decrypt(data: Buffer, hash: string): Buffer {
    if (!this.encryptionKey) return data
    if (data.length < 12 + 16) throw new CorruptChunkError(hash, 'too-short')
    const nonce = data.subarray(0, 12)
    const tag = data.subarray(data.length - 16)
    const ciphertext = data.subarray(12, data.length - 16)
    try {
      const decipher = createDecipheriv('aes-256-gcm', this.encryptionKey, nonce)
      decipher.setAuthTag(tag)
      return Buffer.concat([decipher.update(ciphertext), decipher.final()])
    } catch {
      throw new CorruptChunkError(hash, 'bad-auth-tag')
    }
  }

  private async serialized<T>(hash: string, fn: () => Promise<T>): Promise<T> {
    const previous = this.operations.get(hash) ?? Promise.resolve()
    const next = previous.then(fn, fn)
    this.operations.set(hash, next)
    try {
      return await next
    } finally {
      if (this.operations.get(hash) === next) this.operations.delete(hash)
    }
  }

  private pinPath(hash: string, token: string): string {
    this.pathFor(hash)
    if (!token) throw new Error('upload pin token is required')
    return join(this.pinDirectory, `${hash}.${sha256(Buffer.from(token))}.pin`)
  }

  private rememberPin(hash: string, token: string): void {
    const pins = this.protections.get(hash) ?? new Set<string>()
    pins.add(token)
    this.protections.set(hash, pins)
  }

  /** Persist before placement/ack. Abandoned pins require explicit manual recovery. */
  async pin(hash: string, token: string): Promise<void> {
    const file = this.pinPath(hash, token)
    this.rememberPin(hash, token)
    await this.ready
    await this.serialized(hash, async () => {
      this.rememberPin(hash, token)
      await this.writeAtomic(file, Buffer.from(JSON.stringify({ hash, token })), this.pinDirectory)
    })
  }

  async unpin(hash: string, token: string): Promise<void> {
    const file = this.pinPath(hash, token)
    await this.ready
    await this.serialized(hash, async () => {
      await fs.rm(file, { force: true })
      await this.syncDirectory(this.pinDirectory)
      const pins = this.protections.get(hash)
      if (!pins) return
      pins.delete(token)
      if (pins.size === 0) this.protections.delete(hash)
    })
  }

  isPinned(hash: string): boolean {
    return this.protections.has(hash)
  }

  /** Prevent relocation deletion until the caller has committed its location. */
  async protect(hash: string): Promise<() => Promise<void>> {
    const token = randomUUID()
    await this.pin(hash, token)
    return () => this.unpin(hash, token)
  }

  isProtected(hash: string): boolean {
    return this.isPinned(hash)
  }

  generation(hash: string): number {
    return this.generations.get(hash) ?? 0
  }

  /** One-time startup scan to seed usedBytes and warm the directory tree. */
  private async scan(): Promise<void> {
    try {
      await fs.mkdir(this.root, { recursive: true })
      await fs.mkdir(this.pinDirectory, { recursive: true })
      await this.syncParents(this.pinDirectory, parse(this.root).root)
      for (const name of await fs.readdir(this.pinDirectory)) {
        // A crash before publishing a pin can leave an unacknowledged temp file.
        if (/^[0-9a-f]{64}\.[0-9a-f]{64}\.pin\.tmp-[0-9a-f-]+$/.test(name)) continue
        const record = JSON.parse(await fs.readFile(join(this.pinDirectory, name), 'utf8')) as {
          hash?: unknown; token?: unknown
        }
        if (typeof record.hash !== 'string' || typeof record.token !== 'string' ||
          this.pinPath(record.hash, record.token) !== join(this.pinDirectory, name)) {
          throw new Error(`invalid durable upload pin ${name}`)
        }
        this.rememberPin(record.hash, record.token)
      }
      let total = 0
      const l1 = await fs.readdir(this.root)
      for (const a of l1) {
        if (a === '.upload-pins') continue
        const p1 = join(this.root, a)
        const l2 = await fs.readdir(p1)
        for (const b of l2) {
          const p2 = join(p1, b)
          const files = await fs.readdir(p2)
          for (const f of files) {
            const file = join(p2, f)
            const st = await fs.stat(file)
            if (st.isFile()) {
              total += st.size
              this.sizes.set(file, st.size)
            }
          }
        }
      }
      this.used = total
      log.info('content store ready', { root: this.root, usedBytes: total })
    } catch (err) {
      log.error('content store scan failed', { error: String(err) })
      throw err
    }
  }

  whenReady(): Promise<void> {
    return this.ready
  }

  private pathFor(hash: string): { dir: string; file: string } {
    if (!/^[0-9a-f]{64}$/.test(hash)) throw new Error('invalid chunk hash')
    const dir = join(this.root, hash.slice(0, 2), hash.slice(2, 4))
    return { dir, file: join(dir, hash) }
  }

  private async writeAtomic(file: string, bytes: Buffer, stop: string, onRename: () => void = () => {}): Promise<void> {
    const tmp = `${file}.tmp-${randomUUID()}`
    try {
      const handle = await fs.open(tmp, 'wx')
      try {
        await handle.writeFile(bytes)
        await handle.sync()
      } finally {
        await handle.close()
      }
      await fs.rename(tmp, file)
      onRename()
      if (process.platform === 'win32') {
        const renamed = await fs.open(file, 'r+')
        try {
          await renamed.sync()
        } finally {
          await renamed.close()
        }
      }
      await this.syncParents(dirname(file), stop)
    } finally {
      await fs.rm(tmp, { force: true })
    }
  }

  async has(hash: string): Promise<boolean> {
    await this.ready
    try {
      await fs.access(this.pathFor(hash).file)
      return true
    } catch (err) {
      if ((err as NodeJS.ErrnoException).code === 'ENOENT') return false
      throw err
    }
  }

  /** Store bytes under `hash`, verifying integrity. Idempotent (dedup-safe). */
  async put(hash: string, bytes: Buffer): Promise<void> {
    await this.ready
    const actual = sha256(bytes)
    if (actual !== hash) throw new CorruptChunkError(hash, actual)
    const { dir, file } = this.pathFor(hash)
    await this.serialized(hash, async () => {
      await fs.mkdir(dir, { recursive: true })
      let existing
      try {
        existing = await fs.open(file, 'r+')
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err
        this.account(file, 0)
      }
      if (existing) {
        let verified = false
        try {
          const current = await existing.readFile()
          this.account(file, current.length)
          try {
            const plaintext = this.decrypt(current, hash)
            verified = sha256(plaintext) === hash
          } catch {
            verified = false
          }
          if (verified) await existing.sync()
        } finally {
          await existing.close()
        }
        if (verified) {
          await this.syncParents(dir, this.root)
          this.generations.set(hash, this.generation(hash) + 1)
          return
        }
      }
      const diskBytes = this.encrypt(bytes)
      await this.writeAtomic(file, diskBytes, this.root, () => this.account(file, diskBytes.length))
      this.generations.set(hash, this.generation(hash) + 1)
    })
  }

  /** Read bytes for `hash`, verifying integrity. Throws CorruptChunkError on mismatch. */
  async get(hash: string): Promise<Buffer> {
    await this.ready
    const data = await fs.readFile(this.pathFor(hash).file)
    const bytes = this.decrypt(data, hash)
    const actual = sha256(bytes)
    if (actual !== hash) throw new CorruptChunkError(hash, actual)
    return bytes
  }

  /** Remove only the observed generation, serialized with all puts for this hash. */
  async delete(hash: string, expectedGeneration?: number): Promise<boolean> {
    await this.ready
    const { file } = this.pathFor(hash)
    return this.serialized(hash, async () => {
      if (expectedGeneration !== undefined && this.generation(hash) !== expectedGeneration) return false
      try {
        const st = await fs.stat(file)
        this.account(file, st.size)
      } catch (err) {
        if ((err as NodeJS.ErrnoException).code !== 'ENOENT') throw err
        this.account(file, 0)
        return false
      }
      if ((expectedGeneration !== undefined && this.generation(hash) !== expectedGeneration) ||
        this.isPinned(hash)) return false
      await fs.rm(file)
      this.account(file, 0)
      await this.syncDirectory(dirname(file))
      return true
    })
  }

  usedBytes(): number {
    return this.used
  }

  /** List all locally stored chunk hashes (debug / admin). */
  async list(): Promise<string[]> {
    await this.ready
    const out: string[] = []
    const l1 = await fs.readdir(this.root)
    for (const a of l1) {
      if (a === '.upload-pins') continue
      const p1 = join(this.root, a)
      const l2 = await fs.readdir(p1)
      for (const b of l2) {
        const files = await fs.readdir(join(p1, b))
        for (const f of files) if (/^[0-9a-f]{64}$/.test(f)) out.push(f)
      }
    }
    return out
  }

  /**
   * TEST ONLY: overwrite a chunk's on-disk bytes with garbage to simulate
   * bit-rot / corruption, so read-repair can be exercised.
   */
  async corruptForTest(hash: string): Promise<boolean> {
    await this.ready
    const { file } = this.pathFor(hash)
    return this.serialized(hash, async () => {
      if (!(await this.has(hash))) return false
      const bytes = Buffer.from(`corrupt-${Date.now()}`)
      await fs.writeFile(file, bytes)
      this.account(file, bytes.length)
      return true
    })
  }
}
