/**
 * Metadata store — the full hierarchy replica held by every node.
 *
 * Backed by the built-in `node:sqlite` (no native build). The set of tables
 * covered by the convergence hash (`sha256()`) is exactly the authoritative,
 * replicated state: folders, files, manifests, locations, acls. The `nodes`
 * membership table is deliberately NOT part of the hash — it is soft, per-node
 * gossip state (see p2p/announce.ts) and would otherwise prevent convergence.
 */

import { DatabaseSync } from 'node:sqlite'
import { createHash } from 'node:crypto'
import type { Op, Folder, FileRec, AclRule, LocationRec } from './types.js'

const SCHEMA = `
CREATE TABLE IF NOT EXISTS folders (
  id INTEGER PRIMARY KEY, parent_id INTEGER, name TEXT NOT NULL,
  acl_id INTEGER NOT NULL, ctime INTEGER NOT NULL, mtime INTEGER NOT NULL,
  UNIQUE(parent_id, name)
);
CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY, folder_id INTEGER NOT NULL, name TEXT NOT NULL,
  ext TEXT, size INTEGER NOT NULL, content_type TEXT, manifest_id INTEGER,
  acl_id INTEGER, ctime INTEGER NOT NULL, mtime INTEGER NOT NULL,
  UNIQUE(folder_id, name)
);
CREATE TABLE IF NOT EXISTS manifests (
  id INTEGER PRIMARY KEY, file_id INTEGER NOT NULL, total_size INTEGER NOT NULL,
  chunk_hashes TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS locations (
  chunk_hash TEXT PRIMARY KEY, size INTEGER NOT NULL, active_peer TEXT NOT NULL,
  backup_peer TEXT, state TEXT NOT NULL, refcount INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS acls (
  id INTEGER PRIMARY KEY, rules TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS sync_state (
  k TEXT PRIMARY KEY, v TEXT NOT NULL
);
`

/** Tables in the convergence hash, with canonical column order. */
const HASHED_TABLES: Array<[string, string[]]> = [
  ['folders', ['id', 'parent_id', 'name', 'acl_id', 'ctime', 'mtime']],
  ['files', ['id', 'folder_id', 'name', 'ext', 'size', 'content_type', 'manifest_id', 'acl_id', 'ctime', 'mtime']],
  ['manifests', ['id', 'file_id', 'total_size', 'chunk_hashes']],
  ['locations', ['chunk_hash', 'size', 'active_peer', 'backup_peer', 'state', 'refcount']],
  ['acls', ['id', 'rules']]
]

const PK: Record<string, string> = {
  folders: 'id', files: 'id', manifests: 'id', locations: 'chunk_hash', acls: 'id'
}

export interface SnapshotBlob {
  lsn: number
  tables: Record<string, Array<Record<string, unknown>>>
}

export class MetadataStore {
  readonly db: DatabaseSync

  constructor(dbPath: string) {
    this.db = new DatabaseSync(dbPath)
    this.db.exec('PRAGMA journal_mode = WAL;')
    this.db.exec('PRAGMA synchronous = FULL;')
    this.db.exec('PRAGMA foreign_keys = OFF;')
    this.db.exec(SCHEMA)
    this.seed()
  }

  /** Deterministic seed so every fresh node starts byte-identical (lsn 0). */
  private seed(): void {
    const acl = this.db.prepare('SELECT id FROM acls WHERE id = 0').get()
    if (!acl) {
      const rules: AclRule[] = [{ principal: '*', rights: ['read', 'write', 'admin'] }]
      this.db.prepare('INSERT INTO acls (id, rules) VALUES (0, ?)').run(JSON.stringify(rules))
    }
    const root = this.db.prepare('SELECT id FROM folders WHERE id = 0').get()
    if (!root) {
      this.db.prepare(
        'INSERT INTO folders (id, parent_id, name, acl_id, ctime, mtime) VALUES (0, NULL, ?, 0, 0, 0)'
      ).run('')
    }
    if (!this.getMeta('lsn')) this.setMeta('lsn', '0')
    if (!this.getMeta('mode')) this.setMeta('mode', 'pull')
  }

  close(): void {
    this.db.close()
  }

  // ---- sync_state accessors -------------------------------------------------

  getMeta(k: string): string | undefined {
    const row = this.db.prepare('SELECT v FROM sync_state WHERE k = ?').get(k) as { v: string } | undefined
    return row?.v
  }

  setMeta(k: string, v: string): void {
    this.db.prepare('INSERT INTO sync_state (k, v) VALUES (?, ?) ON CONFLICT(k) DO UPDATE SET v = excluded.v').run(k, v)
  }

  get lsn(): number {
    return Number(this.getMeta('lsn') ?? '0')
  }

  setLsn(n: number): void {
    this.setMeta('lsn', String(n))
  }

  get mode(): 'push' | 'pull' {
    return (this.getMeta('mode') as 'push' | 'pull') ?? 'pull'
  }

  setMode(m: 'push' | 'pull'): void {
    this.setMeta('mode', m)
  }

  // ---- transactions ---------------------------------------------------------

  /** Run fn inside a single SQLite transaction. Rolls back and rethrows on error. */
  tx<T>(fn: () => T): T {
    this.db.exec('BEGIN')
    try {
      const r = fn()
      this.db.exec('COMMIT')
      return r
    } catch (e) {
      try { this.db.exec('ROLLBACK') } catch { /* ignore */ }
      throw e
    }
  }

  // ---- op application -------------------------------------------------------

  /** Apply a single op to the tables. Pure state mutation — no lsn/log side effects. */
  apply(op: Op): void {
    const db = this.db
    switch (op.op) {
      case 'folder.create':
        db.prepare('INSERT INTO folders (id, parent_id, name, acl_id, ctime, mtime) VALUES (?, ?, ?, ?, ?, ?)')
          .run(op.id, op.parentId, op.name, op.aclId, op.ctime, op.mtime)
        break
      case 'folder.rename':
        db.prepare('UPDATE folders SET name = ?, mtime = ? WHERE id = ?').run(op.name, op.mtime, op.id)
        break
      case 'folder.move':
        db.prepare('UPDATE folders SET parent_id = ?, mtime = ? WHERE id = ?').run(op.newParentId, op.mtime, op.id)
        break
      case 'folder.delete':
        db.prepare('DELETE FROM folders WHERE id = ?').run(op.id)
        break
      case 'file.create':
        db.prepare('INSERT INTO files (id, folder_id, name, ext, size, content_type, manifest_id, acl_id, ctime, mtime) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)')
          .run(op.id, op.folderId, op.name, op.ext, op.size, op.contentType, op.manifestId, op.aclId, op.ctime, op.mtime)
        break
      case 'file.update':
        db.prepare('UPDATE files SET size = ?, manifest_id = ?, mtime = ? WHERE id = ?')
          .run(op.size, op.manifestId, op.mtime, op.id)
        break
      case 'file.rename':
        db.prepare('UPDATE files SET name = ?, mtime = ? WHERE id = ?').run(op.name, op.mtime, op.id)
        break
      case 'file.move':
        db.prepare('UPDATE files SET folder_id = ?, mtime = ? WHERE id = ?').run(op.newFolderId, op.mtime, op.id)
        break
      case 'file.delete':
        db.prepare('DELETE FROM files WHERE id = ?').run(op.id)
        break
      case 'manifest.set':
        db.prepare('INSERT INTO manifests (id, file_id, total_size, chunk_hashes) VALUES (?, ?, ?, ?) ON CONFLICT(id) DO UPDATE SET file_id = excluded.file_id, total_size = excluded.total_size, chunk_hashes = excluded.chunk_hashes')
          .run(op.id, op.fileId, op.totalSize, JSON.stringify(op.chunkHashes))
        break
      case 'acl.set':
        db.prepare('INSERT INTO acls (id, rules) VALUES (?, ?) ON CONFLICT(id) DO UPDATE SET rules = excluded.rules')
          .run(op.aclId, JSON.stringify(op.rules))
        break
      case 'folder.setacl':
        db.prepare('UPDATE folders SET acl_id = ?, mtime = ? WHERE id = ?').run(op.aclId, op.mtime, op.id)
        break
      case 'file.setacl':
        db.prepare('UPDATE files SET acl_id = ?, mtime = ? WHERE id = ?').run(op.aclId, op.mtime, op.id)
        break
      case 'location.set':
        db.prepare('INSERT INTO locations (chunk_hash, size, active_peer, backup_peer, state, refcount) VALUES (?, ?, ?, ?, ?, 1) ON CONFLICT(chunk_hash) DO UPDATE SET size = excluded.size, active_peer = excluded.active_peer, backup_peer = excluded.backup_peer, state = excluded.state')
          .run(op.chunkHash, op.size, op.active, op.backup, op.state)
        break
      case 'location.backup':
        db.prepare('UPDATE locations SET backup_peer = ?, state = ? WHERE chunk_hash = ?')
          .run(op.backup, op.state, op.chunkHash)
        break
      case 'location.promote':
        db.prepare('UPDATE locations SET active_peer = ?, backup_peer = ?, state = ? WHERE chunk_hash = ?')
          .run(op.newActive, op.newBackup, op.state, op.chunkHash)
        break
      default: {
        const _exhaustive: never = op
        throw new Error(`unknown op: ${JSON.stringify(_exhaustive)}`)
      }
    }
  }

  /**
   * Apply a batch atomically, advancing lsn to toLsn. If expectedSha is given it
   * is verified BEFORE commit; a mismatch rolls back and throws 'sha-mismatch'.
   */
  applyBatch(ops: Op[], toLsn: number, expectedSha?: string): void {
    this.tx(() => {
      for (const op of ops) this.apply(op)
      this.setLsn(toLsn)
      if (expectedSha !== undefined && this.sha256() !== expectedSha) {
        throw new Error('sha-mismatch')
      }
    })
  }

  // ---- convergence hash -----------------------------------------------------

  /** Deterministic SHA-256 over all replicated tables. Identical iff state identical. */
  sha256(): string {
    const h = createHash('sha256')
    for (const [table, cols] of HASHED_TABLES) {
      const rows = this.db.prepare(`SELECT ${cols.join(', ')} FROM ${table} ORDER BY ${PK[table]}`).all() as Array<Record<string, unknown>>
      h.update(`#${table}\n`)
      for (const row of rows) {
        for (const c of cols) h.update(`${c}=${row[c] === null ? '\u0000' : String(row[c])}\u001f`)
        h.update('\u001e')
      }
    }
    return h.digest('hex')
  }

  // ---- snapshot / load ------------------------------------------------------

  snapshot(): { lsn: number; blob: Buffer } {
    const tables: Record<string, Array<Record<string, unknown>>> = {}
    for (const [table, cols] of HASHED_TABLES) {
      tables[table] = this.db.prepare(`SELECT ${cols.join(', ')} FROM ${table} ORDER BY ${PK[table]}`).all() as Array<Record<string, unknown>>
    }
    const lsn = this.lsn
    const payload: SnapshotBlob = { lsn, tables }
    return { lsn, blob: Buffer.from(JSON.stringify(payload), 'utf8') }
  }

  /** Replace all replicated state from a snapshot blob and set lsn. */
  load(blob: Buffer): void {
    const payload = JSON.parse(blob.toString('utf8')) as SnapshotBlob
    this.tx(() => {
      for (const [table, cols] of HASHED_TABLES) {
        this.db.exec(`DELETE FROM ${table}`)
        const rows = payload.tables[table] ?? []
        if (rows.length === 0) continue
        const placeholders = cols.map(() => '?').join(', ')
        const stmt = this.db.prepare(`INSERT INTO ${table} (${cols.join(', ')}) VALUES (${placeholders})`)
        for (const row of rows) {
          stmt.run(...cols.map((c) => row[c] as never))
        }
      }
      this.setLsn(payload.lsn)
    })
  }

  // ---- typed reads ----------------------------------------------------------

  nextId(table: 'folders' | 'files' | 'manifests' | 'acls'): number {
    const row = this.db.prepare(`SELECT COALESCE(MAX(id), 0) AS m FROM ${table}`).get() as { m: number }
    return Number(row.m) + 1
  }

  getFolder(id: number): Folder | undefined {
    return this.db.prepare('SELECT * FROM folders WHERE id = ?').get(id) as Folder | undefined
  }

  getFile(id: number): FileRec | undefined {
    return this.db.prepare('SELECT * FROM files WHERE id = ?').get(id) as FileRec | undefined
  }

  /** ACL rules for an acl id (empty array if the id is unknown). */
  getAcl(id: number): AclRule[] {
    const row = this.db.prepare('SELECT rules FROM acls WHERE id = ?').get(id) as { rules: string } | undefined
    return row ? (JSON.parse(row.rules) as AclRule[]) : []
  }

  /** Parsed manifest (chunk_hashes deserialized to a string[]). */
  getManifest(id: number): { id: number; fileId: number; totalSize: number; chunkHashes: string[] } | undefined {
    const row = this.db.prepare('SELECT id, file_id, total_size, chunk_hashes FROM manifests WHERE id = ?').get(id) as
      | { id: number; file_id: number; total_size: number; chunk_hashes: string }
      | undefined
    if (!row) return undefined
    return {
      id: Number(row.id),
      fileId: Number(row.file_id),
      totalSize: Number(row.total_size),
      chunkHashes: JSON.parse(row.chunk_hashes) as string[]
    }
  }

  getLocation(hash: string): LocationRec | undefined {
    return this.db.prepare('SELECT * FROM locations WHERE chunk_hash = ?').get(hash) as LocationRec | undefined
  }

  allLocations(): LocationRec[] {
    return this.db.prepare('SELECT * FROM locations ORDER BY chunk_hash').all() as unknown as LocationRec[]
  }

  /** Chunks this node is the active holder of that are missing a healthy backup (reconciler input). */
  degradedActiveLocations(activePeer: string): LocationRec[] {
    return this.db
      .prepare("SELECT * FROM locations WHERE active_peer = ? AND state != 'ok' ORDER BY chunk_hash")
      .all(activePeer) as unknown as LocationRec[]
  }

  /** Every location where `peer` is the active or backup holder (promotion input). */
  locationsWithPeer(peer: string): LocationRec[] {
    return this.db
      .prepare('SELECT * FROM locations WHERE active_peer = ? OR backup_peer = ? ORDER BY chunk_hash')
      .all(peer, peer) as unknown as LocationRec[]
  }

  /** Chunks physically held by `peer` (active or backup), largest first (rebalance input). */
  heldChunksBySize(peer: string): LocationRec[] {
    return this.db
      .prepare('SELECT * FROM locations WHERE active_peer = ? OR backup_peer = ? ORDER BY size DESC, chunk_hash')
      .all(peer, peer) as unknown as LocationRec[]
  }

  /** Resolve a path directly to a file record, or undefined if it is not a file. */
  resolveFile(path: string): FileRec | undefined {
    const r = this.resolvePath(path)
    if (r.kind !== 'file' || r.id === undefined) return undefined
    return this.getFile(r.id)
  }

  listFolder(id: number): { folders: Folder[]; files: FileRec[] } {
    const folders = this.db.prepare('SELECT * FROM folders WHERE parent_id = ? ORDER BY name').all(id) as unknown as Folder[]
    const files = this.db.prepare('SELECT * FROM files WHERE folder_id = ? ORDER BY name').all(id) as unknown as FileRec[]
    return { folders, files }
  }

  /** Direct child folder of `parentId` named `name`, or undefined. */
  childFolder(parentId: number, name: string): Folder | undefined {
    return this.db.prepare('SELECT * FROM folders WHERE parent_id = ? AND name = ?').get(parentId, name) as Folder | undefined
  }

  /** Direct child file of `folderId` named `name`, or undefined. */
  childFile(folderId: number, name: string): FileRec | undefined {
    return this.db.prepare('SELECT * FROM files WHERE folder_id = ? AND name = ?').get(folderId, name) as FileRec | undefined
  }

  /** Resolve a POSIX-style path ('/A/B') to a folder or file id. */
  resolvePath(path: string): { kind: 'folder' | 'file' | 'none'; id?: number } {
    const parts = path.split('/').filter((p) => p.length > 0)
    let folderId = 0
    for (let i = 0; i < parts.length; i++) {
      const name = parts[i]
      const isLast = i === parts.length - 1
      const child = this.db.prepare('SELECT id FROM folders WHERE parent_id = ? AND name = ?').get(folderId, name) as { id: number } | undefined
      if (child) {
        folderId = Number(child.id)
        continue
      }
      if (isLast) {
        const file = this.db.prepare('SELECT id FROM files WHERE folder_id = ? AND name = ?').get(folderId, name) as { id: number } | undefined
        if (file) return { kind: 'file', id: Number(file.id) }
      }
      return { kind: 'none' }
    }
    return { kind: 'folder', id: folderId }
  }

  /** A compact recursive tree view for debugging / convergence checks. */
  tree(): unknown {
    const build = (id: number): unknown => {
      const { folders, files } = this.listFolder(id)
      return {
        folders: folders.map((f) => ({ name: f.name, ...(build(f.id) as object) })),
        files: files.map((f) => ({ name: f.name, size: f.size }))
      }
    }
    return build(0)
  }
}
