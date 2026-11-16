/**
 * Metadata operation types — the ONLY operations that replicate across nodes.
 *
 * Every op is applied verbatim on every node, so all value-bearing fields (ids,
 * timestamps) are set by the Primary and carried in the payload — never derived
 * from a node's local clock or state. This keeps every replica byte-identical,
 * which is what makes the SHA-256 convergence check meaningful.
 */

export type LocationState = 'ok' | 'degraded' | 'replicating'

export type Op =
  | { op: 'folder.create'; id: number; parentId: number | null; name: string; aclId: number; ctime: number; mtime: number }
  | { op: 'folder.rename'; id: number; name: string; mtime: number }
  | { op: 'folder.move'; id: number; newParentId: number; mtime: number }
  | { op: 'folder.delete'; id: number }
  | { op: 'file.create'; id: number; folderId: number; name: string; ext: string | null; size: number; contentType: string | null; manifestId: number | null; aclId: number | null; ctime: number; mtime: number }
  | { op: 'file.update'; id: number; size: number; manifestId: number | null; mtime: number }
  | { op: 'file.rename'; id: number; name: string; mtime: number }
  | { op: 'file.move'; id: number; newFolderId: number; mtime: number }
  | { op: 'file.delete'; id: number }
  | { op: 'manifest.set'; id: number; fileId: number; totalSize: number; chunkHashes: string[] }
  | { op: 'acl.set'; aclId: number; rules: AclRule[] }
  | { op: 'folder.setacl'; id: number; aclId: number; mtime: number }
  | { op: 'file.setacl'; id: number; aclId: number; mtime: number }
  | { op: 'location.set'; chunkHash: string; size: number; active: string; backup: string | null; state: LocationState }
  | { op: 'location.backup'; chunkHash: string; backup: string | null; state: LocationState }
  | { op: 'location.promote'; chunkHash: string; newActive: string; newBackup: string | null; state: LocationState }

export type Right = 'read' | 'write' | 'admin'
export interface AclRule {
  principal: string // user id / group id / '*'
  rights: Right[]
}

export interface Folder {
  id: number
  parent_id: number | null
  name: string
  acl_id: number
  ctime: number
  mtime: number
}

export interface FileRec {
  id: number
  folder_id: number
  name: string
  ext: string | null
  size: number
  content_type: string | null
  manifest_id: number | null
  acl_id: number | null
  ctime: number
  mtime: number
}

export interface LocationRec {
  chunk_hash: string
  size: number
  active_peer: string
  backup_peer: string | null
  state: LocationState
  refcount: number
}

/** A push/pull batch of ops shipped between nodes. */
export interface OpBatch {
  fromLsn: number
  toLsn: number
  ops: Op[]
  sha256?: string // present when toLsn == Primary's latest (verify point)
}
