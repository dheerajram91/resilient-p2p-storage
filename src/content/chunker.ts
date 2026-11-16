/**
 * Content chunker (DETAILED_DESIGN §4).
 *
 * Fixed-size chunking for v1: a file's bytes are split into CHUNK_SIZE pieces,
 * each addressed by hex(sha256(bytes)). A file is the concatenation of its
 * chunks in order (the manifest). Identical chunks dedup automatically because
 * they share the same hash.
 *
 * FastCDC (content-defined) is a later upgrade for better dedup on edits.
 */

import { createHash } from 'node:crypto'

export const DEFAULT_CHUNK_SIZE = 4 * 1024 * 1024 // 4 MiB

export interface Chunk {
  hash: string
  bytes: Buffer
  size: number
}

export function hashBytes(bytes: Buffer | Uint8Array): string {
  return createHash('sha256').update(bytes).digest('hex')
}

/** Split a buffer into fixed-size chunks, each with its sha256 id. */
export function chunkBuffer(data: Buffer, chunkSize = DEFAULT_CHUNK_SIZE): Chunk[] {
  if (chunkSize <= 0) throw new Error('chunkSize must be > 0')
  const chunks: Chunk[] = []
  if (data.length === 0) {
    // Represent an empty file as a single empty chunk so it still has a manifest.
    const bytes = Buffer.alloc(0)
    return [{ hash: hashBytes(bytes), bytes, size: 0 }]
  }
  for (let off = 0; off < data.length; off += chunkSize) {
    const slice = data.subarray(off, Math.min(off + chunkSize, data.length))
    // subarray shares memory; copy so callers can retain/store safely.
    const bytes = Buffer.from(slice)
    chunks.push({ hash: hashBytes(bytes), bytes, size: bytes.length })
  }
  return chunks
}

/** Reassemble file bytes from ordered chunk buffers. */
export function joinChunks(parts: Buffer[]): Buffer {
  return Buffer.concat(parts)
}
