/**
 * libp2p protocol IDs and a minimal one-shot request/response RPC over streams.
 *
 * Framing: length-prefixed JSON (it-length-prefixed). Each RPC opens a fresh
 * stream, writes one request frame, half-closes the write side, then reads one
 * response frame — the standard libp2p request/response pattern.
 */

import { pipe } from 'it-pipe'
import * as lp from 'it-length-prefixed'
import type { Stream, PeerId } from '@libp2p/interface'
import type { NodeHost } from './host.js'
import { log } from '../logger.js'

// it-length-prefixed / it-pipe generics don't compose cleanly with a mid-pipe
// duplex libp2p Stream. We confine the resulting `any` to these two aliases
// rather than sprinkling `as never` across the call sites.
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyPipe = (...args: any[]) => any
// eslint-disable-next-line @typescript-eslint/no-explicit-any
type AnyIter = any

export const PROTOCOLS = {
  announce: '/p2pstore/announce/1',
  metaWrite: '/p2pstore/meta-write/1',
  quorum: '/p2pstore/quorum/1',
  metaPush: '/p2pstore/meta-push/1',
  metaPull: '/p2pstore/meta-pull/1',
  snapshot: '/p2pstore/snapshot/1',
  contentPut: '/p2pstore/content-put/1',
  contentGet: '/p2pstore/content-get/1'
} as const

function encodeObj(obj: unknown): Uint8Array {
  return Buffer.from(JSON.stringify(obj), 'utf8')
}

function decodeObj<T>(bytes: Uint8Array): T {
  return JSON.parse(Buffer.from(bytes).toString('utf8')) as T
}

/** Send one request object on an existing stream and read one response object. */
async function requestOnStream<Req, Res>(stream: Stream, req: Req): Promise<Res> {
  // it-pipe's generics don't infer cleanly through a mid-pipe duplex stream, so we
  // type the transform lambdas loosely. Framing is length-prefixed JSON; exhausting
  // the source half-closes our write side (the one-shot request/response idiom).
  const result = await (pipe as AnyPipe)(
    [encodeObj(req)],
    (source: AnyIter) => lp.encode(source),
    stream,
    (source: AnyIter) => lp.decode(source),
    async (source: AnyIter): Promise<Res> => {
      for await (const msg of source) {
        return decodeObj<Res>((msg as { subarray(): Uint8Array }).subarray())
      }
      throw new Error('no response frame received')
    }
  )
  return result as Res
}

/** Dial `peer`, perform a one-shot request/response on `protocol`, then close. */
export async function rpc<Req, Res>(
  node: NodeHost,
  peer: PeerId,
  protocol: string,
  req: Req,
  signal?: AbortSignal
): Promise<Res> {
  const stream = await node.dialProtocol(peer, protocol, signal ? { signal } : undefined)
  const abort = (): void => stream.abort(signal?.reason instanceof Error ? signal.reason : new Error('RPC aborted'))
  signal?.addEventListener('abort', abort, { once: true })
  if (signal?.aborted) abort()
  try {
    return await requestOnStream<Req, Res>(stream, req)
  } finally {
    signal?.removeEventListener('abort', abort)
    try {
      await stream.close()
    } catch {
      /* ignore */
    }
  }
}

export interface RpcContext {
  peer: PeerId
}

/** Register a request/response handler for `protocol`. */
export function handle<Req, Res>(
  node: NodeHost,
  protocol: string,
  fn: (req: Req, ctx: RpcContext) => Promise<Res>
): void {
  node
    .handle(protocol, ({ stream, connection }) => {
      void (pipe as AnyPipe)(
        stream,
        (source: AnyIter) => lp.decode(source),
        async function* (source: AnyIter) {
          for await (const msg of source) {
            const req = decodeObj<Req>((msg as { subarray(): Uint8Array }).subarray())
            let res: Res
            try {
              res = await fn(req, { peer: connection.remotePeer })
            } catch (err) {
              log.warn('rpc handler error', { protocol, error: String(err) })
              res = { error: String(err) } as unknown as Res
            }
            yield encodeObj(res)
          }
        },
        (source: AnyIter) => lp.encode(source),
        stream
      ).catch((err: unknown) => {
        log.debug('rpc stream closed', { protocol, error: String(err) })
      })
    })
    .catch((err) => log.error('failed to register protocol', { protocol, error: String(err) }))
}
