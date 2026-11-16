/**
 * ACL resolution and enforcement (DETAILED_DESIGN §12).
 *
 * The drive is a global namespace with per-folder (and optionally per-file)
 * ACLs. Access is decided by the *effective* ACL of a path:
 *
 *   - A file uses its own acl_id when it is explicitly set (non-zero); otherwise
 *     it inherits from its containing folder.
 *   - A folder uses its own acl_id when explicitly set (non-zero); otherwise it
 *     inherits from its parent, walking up to the root.
 *   - acl_id 0 is the seeded world ACL (`*` = read+write+admin) and also serves
 *     as the "inherit" sentinel, since `setAcl` always allocates a fresh id >= 1.
 *
 * Rights map to WebDAV verbs as: read (GET/PROPFIND/HEAD/OPTIONS), write
 * (PUT/MKCOL/DELETE/MOVE/PROPPATCH/LOCK), admin (changing an ACL).
 */

import type { MetadataStore } from '../metadata/store.js'
import type { AclRule, Right } from '../metadata/types.js'

/** An authenticated (or anonymous) caller. */
export interface Principal {
  /** User id, or 'anonymous' for an unauthenticated caller. */
  principal: string
  /** Group ids this principal belongs to. */
  groups: string[]
  /** True when the caller did not authenticate. */
  anonymous: boolean
}

export const ANONYMOUS: Principal = { principal: 'anonymous', groups: [], anonymous: true }

/** Deepest existing ancestor folder of a (possibly non-existent) path. Root = 0. */
function nearestFolder(store: MetadataStore, path: string): number {
  const parts = path.split('/').filter((p) => p.length > 0)
  for (let i = parts.length - 1; i >= 0; i--) {
    const sub = '/' + parts.slice(0, i).join('/')
    const r = store.resolvePath(sub)
    if (r.kind === 'folder' && r.id !== undefined) return r.id
  }
  return 0
}

/**
 * The ACL rules governing `path`. For a non-existent path (e.g. the target of a
 * PUT or MKCOL) the nearest existing ancestor folder governs it, so creation is
 * controlled by the folder it would be created in.
 */
export function resolveEffectiveAcl(store: MetadataStore, path: string): AclRule[] {
  const res = store.resolvePath(path)

  let folderId: number
  if (res.kind === 'file' && res.id !== undefined) {
    const file = store.getFile(res.id)
    if (file && file.acl_id != null && file.acl_id !== 0) return store.getAcl(file.acl_id)
    folderId = file ? file.folder_id : 0
  } else if (res.kind === 'folder' && res.id !== undefined) {
    folderId = res.id
  } else {
    folderId = nearestFolder(store, path)
  }

  let cur: number | null = folderId
  while (cur !== null) {
    const f = store.getFolder(cur)
    if (!f) break
    // Explicit ACL, or the root (which has no parent to inherit from) => stop.
    if (f.acl_id !== 0 || f.id === 0) return store.getAcl(f.acl_id)
    cur = f.parent_id
  }
  return store.getAcl(0)
}

/** Whether `p` is granted `right` by `rules`. Anonymous callers match only `*`. */
export function can(rules: AclRule[], p: Principal, right: Right): boolean {
  const ids = new Set<string>(['*'])
  if (!p.anonymous) {
    ids.add(p.principal)
    for (const g of p.groups) ids.add(g)
  }
  for (const r of rules) {
    if (ids.has(r.principal) && r.rights.includes(right)) return true
  }
  return false
}

/** Convenience: resolve the effective ACL for `path` and test `right`. */
export function canAccess(store: MetadataStore, path: string, p: Principal, right: Right): boolean {
  return can(resolveEffectiveAcl(store, path), p, right)
}
