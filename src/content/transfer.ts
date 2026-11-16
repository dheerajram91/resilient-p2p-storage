/**
 * Content transfer protocols (DETAILED_DESIGN §6): binary chunk push/pull.
 *
 *   content-put: client sends [ header{hash,size,protection?} , rawBytes ] ->
 *                server verifies/flushes then replies [{ok,durable,hash,size,
 *                protection?}]. Durability follows ContentStore's platform rules.
 *                A [{hash,release:protection,lsn?}] request releases a pin after
 *                the backup has applied that committed metadata lsn. Pin keys
 *                include the authenticated sender; another peer cannot release
 *                them. Pins survive restart; sender-crash/lost-release pins
 *                have no TTL and require explicit manual recovery.
 *   content-get: client sends [ header{hash} ] -> server replies
 *                [ {ok,size} , rawBytes ]  (or [ {ok:false} ] if it lacks it).
 *
 * These are length-prefixed frames like the JSON RPC in protocols.ts, but the
 * payload frames are raw binary (it-length-prefixed is binary-safe), so we avoid
 * base64 overhead. Chunks are <= CHUNK_SIZE and fit comfortably in memory.
 */

import { pipe } from 'it-pipe'
import * as lp from 'it-length-prefixed'
import type { PeerId, Stream } from '@libp2p/interface'
import type { NodeHost } from '../p2p/host.js'
import { PROTOCOLS } from '../p2p/protocols.js'
import type { ContentStore } from './store.js'
import { log } from '../logger.js'
import { hashBytes } from './chunker.js'

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyPipe = (...args: any[]) => any
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyIter = any

function jsonFrame(obj: unknown): Uint8Array {
  return Buffer.from(JSON.stringify(obj), 'utf8')
}

function toBuffer(frame: { subarray(): Uint8Array }): Buffer {
  return Buffer.from(frame.subarray())
}

export class ContentTransfer {
  private protections = new Map<string, { hash: string; owner: string; release: () => Promise<void>; lsn?: number }>()
  private releaseTimer?: NodeJS.Timeout

  constructor(
    private node: NodeHost,
    private store: ContentStore,
    private timeoutMs = 15_000
  ) {}

  registerHandlers(isApplied: (lsn: number) => boolean = () => false): void {
    // content-put: receive a chunk and store it (verifying integrity).
    this.node
      .handle(PROTOCOLS.contentPut, ({ stream, connection }) => {
        const signal = AbortSignal.timeout(this.timeoutMs)
        const abort = () => stream.abort(new Error('content-put deadline exceeded'))
        signal.addEventListener('abort', abort, { once: true })
        void (pipe as AnyPipe)(
          stream,
          (s: AnyIter) => lp.decode(s),
          async function* (this: ContentTransfer, source: AnyIter) {
            let installed: string | undefined
            let stored = false
            try {
              const frames: Buffer[] = []
              for await (const msg of source) {
                frames.push(toBuffer(msg))
                if (frames.length === 1 && JSON.parse(frames[0].toString('utf8')).release) break
                if (frames.length === 2) break
              }
              const header = JSON.parse(frames[0].toString('utf8')) as {
                hash: string; size: number; protection?: string; release?: string; lsn?: number
              }
              const owner = connection.remotePeer.toString()
              if (header.release) {
                const key = JSON.stringify([owner, header.hash, header.release])
                if (typeof header.release !== 'string') throw new Error('invalid protection release token')
                if (header.lsn !== undefined && (!Number.isSafeInteger(header.lsn) || header.lsn < 0)) {
                  throw new Error('invalid protection release lsn')
                }
                if (header.lsn === undefined || isApplied(header.lsn)) {
                  // Derive the key from authenticated identity even after restart,
                  // when only the store's durable sidecar knows about this pin.
                  await this.store.unpin(header.hash, key)
                  this.protections.delete(key)
                } else {
                  this.protections.set(key, {
                    hash: header.hash,
                    owner,
                    release: () => this.store.unpin(header.hash, key),
                    lsn: header.lsn
                  })
                  this.scheduleRelease(isApplied)
                }
                yield jsonFrame({ ok: true })
                return
              }
              const payload = frames[1] ?? Buffer.alloc(0)
              if (frames.length !== 2 || header.size !== payload.length) throw new Error('content-put size mismatch')
              if (header.protection) {
                const key = JSON.stringify([owner, header.hash, header.protection])
                if (typeof header.protection !== 'string' || this.protections.has(key)) {
                  throw new Error('invalid or duplicate content protection')
                }
                this.protections.set(key, {
                  hash: header.hash,
                  owner,
                  release: () => this.store.unpin(header.hash, key)
                })
                installed = key
                await this.store.pin(header.hash, key)
              }
              await this.store.put(header.hash, payload)
              stored = true
              yield jsonFrame({ ok: true, durable: true, hash: header.hash, size: payload.length, protection: header.protection })
            } catch (err) {
              if (installed && !stored) {
                try {
                  await this.protections.get(installed)?.release()
                  this.protections.delete(installed)
                } catch (releaseError) {
                  log.warn('failed content-put retains durable pin', { error: String(releaseError) })
                }
              }
              log.warn('content-put failed', { error: String(err) })
              yield jsonFrame({ ok: false, error: String(err) })
            }
          }.bind(this),
          (s: AnyIter) => lp.encode(s),
          stream
        ).catch((err: unknown) => log.debug('content-put stream closed', { error: String(err) }))
          .finally(() => signal.removeEventListener('abort', abort))
      })
      .catch((err) => log.error('failed to register content-put', { error: String(err) }))

    // content-get: serve a chunk we hold locally.
    this.node
      .handle(PROTOCOLS.contentGet, ({ stream }) => {
        const signal = AbortSignal.timeout(this.timeoutMs)
        const abort = () => stream.abort(new Error('content-get deadline exceeded'))
        signal.addEventListener('abort', abort, { once: true })
        void (pipe as AnyPipe)(
          stream,
          (s: AnyIter) => lp.decode(s),
          async function* (this: ContentTransfer, source: AnyIter) {
            let header: { hash: string } | undefined
            for await (const msg of source) {
              header = JSON.parse(toBuffer(msg).toString('utf8')) as { hash: string }
              break
            }
            if (!header) return
            try {
              const bytes = await this.store.get(header.hash)
              yield jsonFrame({ ok: true, size: bytes.length })
              yield bytes
            } catch (err) {
              log.debug('content-get miss', { hash: header.hash.slice(0, 12), error: String(err) })
              yield jsonFrame({ ok: false, error: String(err) })
            }
          }.bind(this),
          (s: AnyIter) => lp.encode(s),
          stream
        ).catch((err: unknown) => log.debug('content-get stream closed', { error: String(err) }))
          .finally(() => signal.removeEventListener('abort', abort))
      })
      .catch((err) => log.error('failed to register content-get', { error: String(err) }))
  }

  private scheduleRelease(isApplied: (lsn: number) => boolean): void {
    if (this.releaseTimer) return
    this.releaseTimer = setTimeout(() => {
      void this.releaseApplied(isApplied)
    }, 100)
    this.releaseTimer.unref()
  }

  private async releaseApplied(isApplied: (lsn: number) => boolean): Promise<void> {
    let pending = false
    try {
      for (const [token, pin] of this.protections) {
        if (pin.lsn === undefined) continue
        if (!isApplied(pin.lsn)) {
          pending = true
          continue
        }
        try {
          await pin.release()
          this.protections.delete(token)
        } catch (err) {
          pending = true
          log.warn('durable backup pin release failed', { hash: pin.hash.slice(0, 12), error: String(err) })
        }
      }
    } finally {
      this.releaseTimer = undefined
      if (pending) this.scheduleRelease(isApplied)
    }
  }

  private async withStream<T>(peer: PeerId, protocol: string, fn: (stream: Stream) => Promise<T>): Promise<T> {
    const signal = AbortSignal.timeout(this.timeoutMs)
    let stream: Stream | undefined
    let abort: () => void = () => {}
    const deadline = new Promise<never>((_, reject) => {
      abort = () => {
        const error = new Error(`${protocol} deadline exceeded`)
        reject(error)
        stream?.abort(error)
      }
      signal.addEventListener('abort', abort, { once: true })
    })
    const operation = async () => {
      stream = await this.node.dialProtocol(peer, protocol, { signal })
      if (signal.aborted) {
        stream.abort(new Error(`${protocol} deadline exceeded`))
        signal.throwIfAborted()
      }
      try {
        const result = await fn(stream)
        await stream.close({ signal })
        return result
      } catch (err) {
        stream.abort(err instanceof Error ? err : new Error(String(err)))
        throw err
      }
    }
    try {
      return await Promise.race([operation(), deadline])
    } finally {
      signal.removeEventListener('abort', abort)
    }
  }

  /** Push a chunk; acknowledge only a matching verified/flushed response. */
  async putChunk(peer: PeerId, hash: string, bytes: Buffer, protection?: string): Promise<boolean> {
    if (hashBytes(bytes) !== hash) throw new Error('content-put hash mismatch')
    return this.withStream(peer, PROTOCOLS.contentPut, async (stream) => {
      const resp: { ok?: boolean; durable?: boolean; hash?: string; size?: number; protection?: string } = await (pipe as AnyPipe)(
        [jsonFrame({ hash, size: bytes.length, protection }), bytes],
        (s: AnyIter) => lp.encode(s),
        stream,
        (s: AnyIter) => lp.decode(s),
        async (source: AnyIter) => {
          for await (const msg of source) return JSON.parse(toBuffer(msg).toString('utf8'))
          throw new Error('no content-put response')
        }
      )
      return resp.ok === true && resp.durable === true && resp.hash === hash &&
        resp.size === bytes.length && resp.protection === protection
    })
  }

  /** Release after the backup has applied the committed location, not before. */
  async releaseChunk(peer: PeerId, hash: string, protection: string, lsn?: number): Promise<void> {
    await this.withStream(peer, PROTOCOLS.contentPut, async (stream) => {
      const ok = await (pipe as AnyPipe)(
        [jsonFrame({ hash, release: protection, lsn })],
        (s: AnyIter) => lp.encode(s),
        stream,
        (s: AnyIter) => lp.decode(s),
        async (source: AnyIter) => {
          for await (const msg of source) return JSON.parse(toBuffer(msg).toString('utf8')).ok === true
          return false
        }
      )
      if (!ok) throw new Error('content protection release failed')
    })
  }

  /** Fetch a chunk from `peer`. Returns the verified bytes, or null if absent. */
  async getChunk(peer: PeerId, hash: string): Promise<Buffer | null> {
    return this.withStream(peer, PROTOCOLS.contentGet, async (stream) => {
      const result: Buffer | null = await (pipe as AnyPipe)(
        [jsonFrame({ hash })],
        (s: AnyIter) => lp.encode(s),
        stream,
        (s: AnyIter) => lp.decode(s),
        async (source: AnyIter) => {
          let meta: { ok?: boolean; size?: number } | undefined
          const parts: Buffer[] = []
          for await (const msg of source) {
            if (!meta) {
              meta = JSON.parse(toBuffer(msg).toString('utf8'))
              if (!meta || meta.ok !== true) return null
            } else {
              parts.push(toBuffer(msg))
            }
          }
          if (!meta) throw new Error('no content-get response')
          const bytes = Buffer.concat(parts)
          if (meta.size !== bytes.length || hashBytes(bytes) !== hash) throw new Error('content-get integrity mismatch')
          return bytes
        }
      )
      return result
    })
  }
}
