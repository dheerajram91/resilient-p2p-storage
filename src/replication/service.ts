/**
 * Metadata replication. Fixed-voter mode commits complete mutations through
 * QuorumLog; the push/pull implementation below is retained for legacy demos.
 *
 * Primary:
 *   - single writer: apply op + append log + advance lsn atomically, then push.
 *   - serves meta-pull (delta from a given lsn) and snapshot requests.
 *   - materializes client write intents (assigns ids/timestamps).
 * Replica:
 *   - PUSH mode (steady state): apply pushed batches, verifying lsn contiguity
 *     and SHA-256; any gap/mismatch => fall back to PULL.
 *   - PULL mode (join/recovery): load snapshot, then apply deltas until caught
 *     up, verify SHA-256, resume serving in PUSH mode.
 */

import type { NodeHost } from '../p2p/host.js'
import type { PeerId } from '@libp2p/interface'
import { PROTOCOLS, handle, rpc } from '../p2p/protocols.js'
import type { ClusterView } from '../p2p/announce.js'
import type { MetadataStore } from '../metadata/store.js'
import { MetaLog } from '../metadata/log.js'
import type { Op, LocationState, AclRule } from '../metadata/types.js'
import { log } from '../logger.js'
import { QuorumLog, type QuorumRequest, type QuorumResponse } from './quorum.js'
import type { Counters } from '../api/counters.js'

export interface ChunkRef {
  hash: string
  size: number
  backupPeer?: string
}

interface ExpectedLocation {
  active: string
  backup: string | null
}

export type WriteIntent =
  | { kind: 'mkpath'; path: string }
  | { kind: 'rmfolder'; path: string }
  | { kind: 'createFile'; path: string; size: number; ext: string | null; contentType: string | null; chunks: ChunkRef[]; activePeer: string }
  | { kind: 'setBackup'; chunkHash: string; backup: string; state: LocationState; expected?: ExpectedLocation }
  | { kind: 'relocateActive'; chunkHash: string; newActive: string; newBackup: string | null; state: LocationState; expected?: ExpectedLocation }
  | { kind: 'deleteEntry'; path: string; recursive: boolean }
  | { kind: 'moveEntry'; src: string; dst: string; overwrite: boolean }
  | { kind: 'setAcl'; path: string; rules: AclRule[] }

export type WriteResult =
  | { ok: true; lsn: number; fileId?: number; manifestId?: number; replaced?: boolean }
  | { error: string; code?: 'EUNAVAILABLE' }

/** Intents whose ack must wait for local apply (read-your-writes on the same node). */
const RYW_KINDS = new Set<WriteIntent['kind']>([
  'mkpath', 'rmfolder', 'createFile', 'deleteEntry', 'moveEntry', 'setAcl', 'setBackup', 'relocateActive'
])

export interface PromotionSummary {
  downPeer: string
  promoted: number // chunks whose active copy moved to the surviving backup
  backupsCleared: number // chunks that lost their backup copy
  atRisk: number // chunks with no healthy holder left (data unavailable)
}

interface PushMsg {
  fromLsn: number
  toLsn: number
  ops: Op[]
  sha256?: string
}
interface PushResp {
  status: 'ok' | 'needPull'
  lsn: number
}
interface PullReq {
  haveLsn: number
}
interface PullResp {
  ops: Op[]
  toLsn: number
  latest: boolean
  sha256?: string
  error?: string
}
interface SnapshotResp {
  lsn: number
  blob: string // base64
  error?: string
}

export interface ReplicationOpts {
  isPrimary: boolean
  pushIntervalMs: number
  batchSize: number
  name?: string
  voters?: string[]
  /** Phase 7: optional monotonic counters surfaced by the /metrics endpoint. */
  counters?: Counters
  /** Phase 8: Tiered replication options */
  tierEnabled?: boolean
  tierRole?: 'relay' | 'leaf' | 'auto'
  tierScaleThreshold?: number
  relayRoster?: string[]
}

interface MetadataCommand {
  ops: Op[]
  result: WriteResult
}

export class ReplicationService {
  private lastBroadcastLsn: number
  private pushing = false
  private pushPending = false
  private pulling = false
  private pullFull = false
  private pushTimer?: NodeJS.Timeout
  private pullTimer?: NodeJS.Timeout
  private _isPrimary: boolean
  private _epoch: number
  private quorum?: QuorumLog<MetadataCommand>
  private stagedOps?: Op[]
  private writeQueue: Promise<void> = Promise.resolve()

  constructor(
    private node: NodeHost,
    private store: MetadataStore,
    private cluster: ClusterView,
    private metaLog: MetaLog | null,
    private opts: ReplicationOpts
  ) {
    this.lastBroadcastLsn = store.lsn
    this._isPrimary = opts.isPrimary
    // The bootstrap Primary starts at term 1; a replica at 0, so any election
    // (which uses max(seen epoch)+1) always out-ranks the static configuration.
    this._epoch = opts.isPrimary ? 1 : 0
    const hasQuorum = store.db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='quorum_state'").get()
    if (opts.voters?.length) {
      if (!opts.name) throw new Error('Quorum mode requires a stable node name')
      if (!hasQuorum && store.lsn > 0) {
        throw new Error('Cannot adopt legacy metadata into quorum automatically; export files and import into a fresh three-voter cluster')
      }
      this.metaLog ??= new MetaLog(store.db)
      this.quorum = new QuorumLog<MetadataCommand>({
        id: opts.name,
        voters: opts.voters,
        db: store.db,
        counters: opts.counters,
        send: async (name, request) => {
          const peer = this.cluster.all().find((p) => p.name === name)?.peer
          if (!peer) throw new Error(`Metadata voter ${name} is not connected`)
          const response = await rpc<QuorumRequest<MetadataCommand>, QuorumResponse | { error: string }>(
            this.node, peer, PROTOCOLS.quorum, request, AbortSignal.timeout(3000))
          if ('error' in response) throw new Error(response.error)
          return response
        },
        apply: (command) => {
          for (const op of command.ops) {
            const lsn = store.lsn + 1
            store.apply(op)
            this.metaLog!.append(lsn, op, Date.now())
            store.setLsn(lsn)
          }
          store.setMode('push')
        }
      })
    } else if (hasQuorum) {
      throw new Error('This database belongs to a quorum cluster; METADATA_VOTERS must not be removed')
    }
    if (this.isRelay && !this.metaLog) {
      this.metaLog = new MetaLog(store.db)
    }
  }

  get isRelay(): boolean {
    if (this.opts.tierRole === 'relay') return true
    if (this.opts.tierRole === 'leaf') return false
    if (this.opts.relayRoster && this.opts.name && this.opts.relayRoster.includes(this.opts.name)) return true
    const tieredActive = this.opts.tierEnabled || this.cluster.size >= (this.opts.tierScaleThreshold ?? 100)
    if (!tieredActive) return false
    return this.cluster.get(this.selfId)?.role === 'anchor' || this.opts.name?.startsWith('anchor') === true || this.opts.name?.startsWith('relay') === true
  }

  get isPrimary(): boolean {
    return this.quorum ? this.quorum.isLeader && this.quorum.ready : this._isPrimary
  }

  get epoch(): number {
    return this.quorum?.term ?? this._epoch
  }

  get quorumEnabled(): boolean {
    return this.quorum !== undefined
  }

  quorumStatus(): object {
    return this.quorum
      ? { enabled: true, voters: this.opts.voters, leader: this.quorum.leaderId, term: this.quorum.term,
          ready: this.quorum.ready, commitIndex: this.quorum.commitIndex, lastIndex: this.quorum.lastIndex }
      : { enabled: false }
  }

  get selfId(): string {
    return this.node.peerId.toString()
  }

  /** Current committed lsn (used by the election coordinator to rank candidates). */
  get currentLsn(): number {
    return this.store.lsn
  }

  start(): void {
    this.registerHandlers()
    if (this.quorum) {
      this.quorum.start()
      return
    }
    log.warn('legacy metadata mode: no quorum durability; configure METADATA_VOTERS on a fresh three-node cluster')
    if (this.isPrimary) {
      this.store.setMode('push')
      this.pushTimer = setInterval(() => void this.broadcast(), this.opts.pushIntervalMs)
      log.info('replication: primary', { lsn: this.store.lsn, sha: this.store.sha256().slice(0, 8) })
    } else {
      this.store.setMode('pull')
      log.info('replication: replica (pull pending)')
      this.ensurePull()
    }
  }

  stop(): void {
    this.quorum?.stop()
    if (this.pushTimer) clearInterval(this.pushTimer)
    if (this.pullTimer) clearTimeout(this.pullTimer)
  }

  // ---- availability: runtime role changes (DETAILED_DESIGN §8.3) ------------

  /**
   * Win an election: assume the Primary role at `newEpoch`. Idempotent — a
   * higher/equal epoch than ours is required to (re)take the role. Creates the
   * meta-log over the existing DB (a replica never had one), switches to push
   * mode, and starts broadcasting from the current lsn.
   */
  becomePrimary(newEpoch: number): void {
    if (this.quorum) throw new Error('Quorum leadership can only be obtained through a majority election')
    if (this._isPrimary && newEpoch <= this._epoch) return
    this._isPrimary = true
    this._epoch = newEpoch
    if (!this.metaLog) this.metaLog = new MetaLog(this.store.db)
    if (this.pullTimer) {
      clearTimeout(this.pullTimer)
      this.pullTimer = undefined
    }
    this.store.setMode('push')
    this.lastBroadcastLsn = this.store.lsn
    if (!this.pushTimer) {
      this.pushTimer = setInterval(() => void this.broadcast(), this.opts.pushIntervalMs)
    }
    log.warn('replication: PROMOTED to primary', {
      epoch: this._epoch,
      lsn: this.store.lsn,
      sha: this.store.sha256().slice(0, 8)
    })
    void this.broadcast()
  }

  /** Lost a split-brain to a better-ranked Primary: revert to replica and resync. */
  stepDown(newEpoch: number): void {
    if (this.quorum) throw new Error('Quorum terms are managed by the consensus protocol')
    if (!this._isPrimary) return
    this._isPrimary = false
    this._epoch = Math.max(this._epoch, newEpoch)
    if (this.pushTimer) {
      clearInterval(this.pushTimer)
      this.pushTimer = undefined
    }
    this.store.setMode('pull')
    log.warn('replication: STEPPED DOWN to replica', { epoch: this._epoch })
    this.ensurePull(true)
  }

  // ---- public write API (used by admin/WebDAV) ------------------------------

  async mkpath(path: string): Promise<WriteResult> {
    return this.submitIntent({ kind: 'mkpath', path })
  }

  async rmfolder(path: string): Promise<WriteResult> {
    return this.submitIntent({ kind: 'rmfolder', path })
  }

  /** Register file + manifest + confirmed chunk holders as one quorum command. */
  async createFile(args: Omit<Extract<WriteIntent, { kind: 'createFile' }>, 'kind'>): Promise<WriteResult> {
    return this.submitIntent({ kind: 'createFile', ...args })
  }

  /** Record that a chunk's backup copy now exists on `backup` (closes the 1-copy window). */
  async setBackup(chunkHash: string, backup: string, state: LocationState, expected?: ExpectedLocation): Promise<WriteResult> {
    return this.submitIntent({ kind: 'setBackup', chunkHash, backup, state, expected })
  }

  /**
   * Move a chunk's ACTIVE copy to another node (capacity rebalance, §10). Keeps
   * the existing backup so RF is preserved. Routes to the Primary (single-writer).
   */
  async relocateActive(chunkHash: string, newActive: string, newBackup: string | null, state: LocationState, expected?: ExpectedLocation): Promise<WriteResult> {
    return this.submitIntent({ kind: 'relocateActive', chunkHash, newActive, newBackup, state, expected })
  }

  /** Delete a file, or a folder (which must be empty unless `recursive`). */
  async deleteEntry(path: string, recursive: boolean): Promise<WriteResult> {
    return this.submitIntent({ kind: 'deleteEntry', path, recursive })
  }

  /** Move/rename a file or folder to `dst` (fails if dst exists unless `overwrite`). */
  async moveEntry(src: string, dst: string, overwrite: boolean): Promise<WriteResult> {
    return this.submitIntent({ kind: 'moveEntry', src, dst, overwrite })
  }

  /** Set the ACL rules governing a folder or file (allocates a fresh acl id and repoints). */
  async setAcl(path: string, rules: AclRule[]): Promise<WriteResult> {
    return this.submitIntent({ kind: 'setAcl', path, rules })
  }

  private async submitIntent(intent: WriteIntent): Promise<WriteResult> {
    let result: WriteResult
    if (this.isPrimary) {
      result = this.quorum ? await this.commitIntent(intent) : this.performIntent(intent)
    } else {
      let primary = this.quorum
        ? this.cluster.all().find((p) => p.name === this.quorum!.leaderId)?.peer
        : this.cluster.primaryPeer()
      if (!primary && !this.isRelay) {
        primary = this.cluster.relayPeers(this.opts.relayRoster).find((r) => r.peer)?.peer
      }
      if (!primary) return { error: 'no primary known yet', code: 'EUNAVAILABLE' }
      try {
        result = await rpc<WriteIntent, WriteResult>(this.node, primary, PROTOCOLS.metaWrite, intent, AbortSignal.timeout(12000))
      } catch (err) {
        return { error: `forward to primary failed (outcome uncertain): ${String(err)}`, code: 'EUNAVAILABLE' }
      }
    }
    // Read-your-writes: don't ack a namespace mutation until THIS node's local
    // replica has applied it, so an immediate read on the same node is
    // consistent. On the Primary this returns instantly (lsn already advanced);
    // on a replica it waits for the Primary's push to bring the op back.
    if (!('error' in result) && RYW_KINDS.has(intent.kind)) {
      if (!(await this.waitForLocalLsn(result.lsn))) {
        return { error: 'Metadata committed but local catch-up timed out; write outcome is uncertain, read before retrying', code: 'EUNAVAILABLE' }
      }
    }
    return result
  }

  private enqueueWrite<T>(fn: () => Promise<T>): Promise<T> {
    const next = this.writeQueue.then(fn)
    this.writeQueue = next.then(() => undefined, () => undefined)
    return next
  }

  private commitIntent(intent: WriteIntent): Promise<WriteResult> {
    return this.enqueueWrite(async () => {
      if (!this.isPrimary) return { error: 'No ready metadata leader; a majority is required', code: 'EUNAVAILABLE' }
      const command = this.materialize(() => this.performIntent(intent))
      if ('error' in command.result) return command.result
      try {
        await this.quorum!.propose(command)
        return command.result
      } catch (err) {
        log.warn('metadata quorum commit failed', { error: String(err) })
        return { error: `Metadata commit not acknowledged (outcome uncertain): ${String(err)}`, code: 'EUNAVAILABLE' }
      }
    })
  }

  private materialize(fn: () => WriteResult): MetadataCommand {
    this.store.db.exec('SAVEPOINT materialize')
    this.stagedOps = []
    try {
      const result = fn()
      return { result, ops: this.stagedOps }
    } finally {
      this.stagedOps = undefined
      this.store.db.exec('ROLLBACK TO materialize; RELEASE materialize')
    }
  }

  /** Resolve once the local store has applied up to (at least) `lsn`, or on timeout. */
  async waitForLocalLsn(lsn: number, timeoutMs = 8000): Promise<boolean> {
    if (this.store.lsn >= lsn) return true
    const deadline = Date.now() + timeoutMs
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 20))
      if (this.store.lsn >= lsn) return true
    }
    return this.store.lsn >= lsn
  }

  // ---- primary: intent materialization --------------------------------------

  private performIntent(intent: WriteIntent): WriteResult {
    try {
      let result: WriteResult
      if (intent.kind === 'mkpath') result = this.doMkpath(intent.path)
      else if (intent.kind === 'rmfolder') result = this.doRmfolder(intent.path)
      else if (intent.kind === 'createFile') result = this.doCreateFile(intent)
      else if (intent.kind === 'setBackup') result = this.doSetBackup(intent)
      else if (intent.kind === 'relocateActive') result = this.doRelocateActive(intent)
      else if (intent.kind === 'deleteEntry') result = this.doDeleteEntry(intent.path, intent.recursive)
      else if (intent.kind === 'moveEntry') result = this.doMoveEntry(intent)
      else if (intent.kind === 'setAcl') result = this.doSetAcl(intent)
      else result = { error: 'unknown intent' }
      if (!this.quorum) void this.broadcast()
      return result
    } catch (err) {
      return { error: String(err) }
    }
  }

  private doMkpath(path: string): WriteResult {
    const parts = path.split('/').filter((p) => p.length > 0)
    if (parts.length === 0) return { error: 'empty path' }
    let parentId = 0
    let lastLsn = this.store.lsn
    for (const name of parts) {
      const existing = this.store.db
        .prepare('SELECT id FROM folders WHERE parent_id = ? AND name = ?')
        .get(parentId, name) as { id: number } | undefined
      if (existing) {
        parentId = Number(existing.id)
        continue
      }
      const id = this.store.nextId('folders')
      const now = Date.now()
      lastLsn = this.appendOp({ op: 'folder.create', id, parentId, name, aclId: 0, ctime: now, mtime: now })
      parentId = id
    }
    return { ok: true, lsn: lastLsn }
  }

  private doRmfolder(path: string): WriteResult {
    const res = this.store.resolvePath(path)
    if (res.kind !== 'folder' || res.id === undefined) return { error: 'not a folder' }
    if (res.id === 0) return { error: 'cannot delete root' }
    const { folders, files } = this.store.listFolder(res.id)
    if (folders.length > 0 || files.length > 0) return { error: 'folder not empty' }
    const lsn = this.appendOp({ op: 'folder.delete', id: res.id })
    return { ok: true, lsn }
  }

  private doCreateFile(i: Extract<WriteIntent, { kind: 'createFile' }>): WriteResult {
    const parts = i.path.split('/').filter((p) => p.length > 0)
    if (parts.length === 0) return { error: 'empty path' }
    const fileName = parts[parts.length - 1]
    const folderParts = parts.slice(0, -1)

    // Ensure parent folders exist (like mkpath).
    let parentId = 0
    for (const name of folderParts) {
      const existing = this.store.db
        .prepare('SELECT id FROM folders WHERE parent_id = ? AND name = ?')
        .get(parentId, name) as { id: number } | undefined
      if (existing) {
        parentId = Number(existing.id)
        continue
      }
      const id = this.store.nextId('folders')
      const now = Date.now()
      this.appendOp({ op: 'folder.create', id, parentId, name, aclId: 0, ctime: now, mtime: now })
      parentId = id
    }

    // Overwrite semantics (v1): if a file with this name exists, drop the old
    // record first. Old manifest/locations are left for a later GC pass.
    const existingFile = this.store.db
      .prepare('SELECT id FROM files WHERE folder_id = ? AND name = ?')
      .get(parentId, fileName) as { id: number } | undefined
    if (existingFile) this.appendOp({ op: 'file.delete', id: Number(existingFile.id) })
    const replaced = !!existingFile

    const now = Date.now()
    const manifestId = this.store.nextId('manifests')
    const fileId = this.store.nextId('files')
    const chunkHashes = i.chunks.map((c) => c.hash)

    // Locations and manifest precede the file record in the same quorum command.
    // last so the file only becomes visible once its manifest + locations exist.
    for (const c of i.chunks) {
      if (this.quorum && (!c.backupPeer || c.backupPeer === i.activePeer)) {
        throw new Error('Every uploaded chunk requires two confirmed distinct holders')
      }
      if (c.backupPeer || !this.store.getLocation(c.hash)) {
        this.appendOp({ op: 'location.set', chunkHash: c.hash, size: c.size, active: i.activePeer,
          backup: c.backupPeer ?? null, state: c.backupPeer ? 'ok' : 'degraded' })
      }
    }
    this.appendOp({ op: 'manifest.set', id: manifestId, fileId, totalSize: i.size, chunkHashes })
    const lsn = this.appendOp({
      op: 'file.create',
      id: fileId,
      folderId: parentId,
      name: fileName,
      ext: i.ext,
      size: i.size,
      contentType: i.contentType,
      manifestId,
      aclId: 0,
      ctime: now,
      mtime: now
    })
    return { ok: true, lsn, fileId, manifestId, replaced }
  }

  private doSetBackup(i: Extract<WriteIntent, { kind: 'setBackup' }>): WriteResult {
    const loc = this.store.getLocation(i.chunkHash)
    if (!loc) return { error: 'no such chunk location' }
    if (i.expected && (loc.active_peer !== i.expected.active || loc.backup_peer !== i.expected.backup)) {
      return { error: 'chunk location changed; retry from current metadata' }
    }
    if (i.backup === loc.active_peer) return { error: 'active and backup must be distinct' }
    const lsn = this.appendOp({ op: 'location.backup', chunkHash: i.chunkHash, backup: i.backup, state: i.state })
    return { ok: true, lsn }
  }

  private doRelocateActive(i: Extract<WriteIntent, { kind: 'relocateActive' }>): WriteResult {
    const loc = this.store.getLocation(i.chunkHash)
    if (!loc) return { error: 'no such chunk location' }
    if (i.expected && (loc.active_peer !== i.expected.active || loc.backup_peer !== i.expected.backup)) {
      return { error: 'chunk location changed; retry from current metadata' }
    }
    if (i.newActive === i.newBackup) return { error: 'active and backup must be distinct' }
    // Reuse location.promote: it rewrites (active, backup, state) atomically.
    const lsn = this.appendOp({ op: 'location.promote', chunkHash: i.chunkHash, newActive: i.newActive, newBackup: i.newBackup, state: i.state })
    return { ok: true, lsn }
  }

  // ---- namespace mutations (WebDAV: DELETE / MOVE / ACL) ---------------------

  private doDeleteEntry(path: string, recursive: boolean): WriteResult {
    const res = this.store.resolvePath(path)
    if (res.kind === 'none' || res.id === undefined) return { error: 'not found' }
    if (res.kind === 'file') {
      const lsn = this.appendOp({ op: 'file.delete', id: res.id })
      return { ok: true, lsn }
    }
    if (res.id === 0) return { error: 'cannot delete root' }
    const { folders, files } = this.store.listFolder(res.id)
    if ((folders.length > 0 || files.length > 0) && !recursive) return { error: 'folder not empty' }
    const lsn = recursive ? this.deleteSubtree(res.id) : this.appendOp({ op: 'folder.delete', id: res.id })
    return { ok: true, lsn }
  }

  /** Delete a folder and everything beneath it, children before parents. Returns the last lsn. */
  private deleteSubtree(folderId: number): number {
    const { folders, files } = this.store.listFolder(folderId)
    let lsn = this.store.lsn
    for (const f of files) lsn = this.appendOp({ op: 'file.delete', id: f.id })
    for (const sub of folders) lsn = this.deleteSubtree(sub.id)
    return this.appendOp({ op: 'folder.delete', id: folderId })
  }

  private doMoveEntry(i: Extract<WriteIntent, { kind: 'moveEntry' }>): WriteResult {
    const src = this.store.resolvePath(i.src)
    if (src.kind === 'none' || src.id === undefined) return { error: 'source not found' }
    if (src.kind === 'folder' && src.id === 0) return { error: 'cannot move root' }

    const dstParts = i.dst.split('/').filter((p) => p.length > 0)
    if (dstParts.length === 0) return { error: 'invalid destination' }
    const newName = dstParts[dstParts.length - 1]

    // Ensure destination parent folders exist (mkpath semantics) and get parentId.
    let parentId = 0
    for (const name of dstParts.slice(0, -1)) {
      const existing = this.store.childFolder(parentId, name)
      if (existing) {
        parentId = existing.id
        continue
      }
      if (this.store.childFile(parentId, name)) return { error: 'destination path invalid' }
      const id = this.store.nextId('folders')
      const t = Date.now()
      this.appendOp({ op: 'folder.create', id, parentId, name, aclId: 0, ctime: t, mtime: t })
      parentId = id
    }

    // A folder may not be moved into itself or one of its descendants.
    if (src.kind === 'folder' && this.isAncestorOrSelf(src.id, parentId)) {
      return { error: 'cannot move a folder into itself' }
    }

    // Resolve any existing entry at the destination.
    const dstFolder = this.store.childFolder(parentId, newName)
    const dstFile = this.store.childFile(parentId, newName)
    if (dstFolder || dstFile) {
      const ontoSelf =
        (src.kind === 'file' && dstFile?.id === src.id) || (src.kind === 'folder' && dstFolder?.id === src.id)
      if (ontoSelf) return { ok: true, lsn: this.store.lsn } // no-op
      if (!i.overwrite) return { error: 'destination exists' }
      if (dstFile) this.appendOp({ op: 'file.delete', id: dstFile.id })
      if (dstFolder) this.deleteSubtree(dstFolder.id)
    }

    const now = Date.now()
    let lsn = this.store.lsn
    if (src.kind === 'file') {
      const cur = this.store.getFile(src.id)!
      if (cur.folder_id !== parentId) lsn = this.appendOp({ op: 'file.move', id: src.id, newFolderId: parentId, mtime: now })
      if (cur.name !== newName) lsn = this.appendOp({ op: 'file.rename', id: src.id, name: newName, mtime: now })
    } else {
      const cur = this.store.getFolder(src.id)!
      if (cur.parent_id !== parentId) lsn = this.appendOp({ op: 'folder.move', id: src.id, newParentId: parentId, mtime: now })
      if (cur.name !== newName) lsn = this.appendOp({ op: 'folder.rename', id: src.id, name: newName, mtime: now })
    }
    return { ok: true, lsn }
  }

  /** True if folder `a` is `b` itself or an ancestor of `b` (walks b up to the root). */
  private isAncestorOrSelf(a: number, b: number): boolean {
    let cur: number | null = b
    while (cur !== null) {
      if (cur === a) return true
      if (cur === 0) break
      const f = this.store.getFolder(cur)
      cur = f ? f.parent_id : null
    }
    return false
  }

  private doSetAcl(i: Extract<WriteIntent, { kind: 'setAcl' }>): WriteResult {
    const res = this.store.resolvePath(i.path)
    if (res.kind === 'none' || res.id === undefined) return { error: 'not found' }
    const aclId = this.store.nextId('acls')
    const now = Date.now()
    this.appendOp({ op: 'acl.set', aclId, rules: i.rules })
    const lsn =
      res.kind === 'folder'
        ? this.appendOp({ op: 'folder.setacl', id: res.id, aclId, mtime: now })
        : this.appendOp({ op: 'file.setacl', id: res.id, aclId, mtime: now })
    return { ok: true, lsn }
  }

  // ---- availability: promotion (DETAILED_DESIGN §8.2) -----------------------

  /**
   * A peer went DOWN — repair the metadata so no chunk still points its only
   * live copy at a dead node. Primary-only (single-writer). For each affected
   * location:
   *   active == down, backup healthy  -> promote backup to active (state=degraded)
   *   backup == down                  -> clear the backup (state=degraded)
   * Both leave the chunk `degraded`, which is the reconciler's cue to rebuild
   * RF=2 elsewhere. Idempotent: once repaired, `down` no longer appears.
   */
  async applyPromotion(downPeer: string, isHealthy: (peerId: string) => boolean): Promise<PromotionSummary> {
    if (!this.quorum) return this.performPromotion(downPeer, isHealthy)
    return this.enqueueWrite(async () => {
      if (!this.isPrimary) throw new Error('Cannot promote content without a ready quorum leader')
      let summary: PromotionSummary | undefined
      const command = this.materialize(() => {
        summary = this.performPromotion(downPeer, isHealthy)
        return { ok: true, lsn: this.store.lsn }
      })
      if (command.ops.length > 0) await this.quorum!.propose(command)
      return summary!
    })
  }

  private performPromotion(downPeer: string, isHealthy: (peerId: string) => boolean): PromotionSummary {
    const summary: PromotionSummary = { downPeer, promoted: 0, backupsCleared: 0, atRisk: 0 }
    if (!this.isPrimary) return summary
    const affected = this.store.locationsWithPeer(downPeer)
    for (const loc of affected) {
      try {
        if (loc.active_peer === downPeer) {
          if (loc.backup_peer && loc.backup_peer !== downPeer && isHealthy(loc.backup_peer)) {
            this.appendOp({
              op: 'location.promote',
              chunkHash: loc.chunk_hash,
              newActive: loc.backup_peer,
              newBackup: null,
              state: 'degraded'
            })
            summary.promoted++
          } else {
            // No surviving healthy replica — the chunk is currently unavailable.
            summary.atRisk++
            log.error('promotion: chunk has no healthy replica', {
              hash: loc.chunk_hash.slice(0, 12),
              down: downPeer.slice(-8)
            })
          }
        } else if (loc.backup_peer === downPeer) {
          this.appendOp({ op: 'location.backup', chunkHash: loc.chunk_hash, backup: null, state: 'degraded' })
          summary.backupsCleared++
        }
      } catch (err) {
        log.warn('promotion op failed', { hash: loc.chunk_hash.slice(0, 12), error: String(err) })
      }
    }
    if (summary.promoted > 0 || summary.backupsCleared > 0) {
      log.info('promotion applied', { ...summary })
      if (!this.quorum) void this.broadcast()
    }
    return summary
  }

  /** Primary single-writer core: apply + log + advance lsn atomically. No push. */
  private appendOp(op: Op): number {
    const lsn = this.store.lsn + 1
    if (this.stagedOps) {
      this.store.apply(op)
      this.store.setLsn(lsn)
      this.stagedOps.push(op)
      return lsn
    }
    this.store.tx(() => {
      this.store.apply(op)
      this.metaLog!.append(lsn, op, Date.now())
      this.store.setLsn(lsn)
    })
    return lsn
  }

  // ---- primary: push (steady state) -----------------------------------------

  private async broadcast(): Promise<void> {
    if (this.quorum) return
    if (!this.isPrimary) return
    // Split-brain guard: if another node holds a better Primary claim (higher
    // epoch, or equal epoch + lower peerId), yield to it deterministically.
    const better = this.cluster.betterPrimaryThan(this._epoch, this.selfId)
    if (better) {
      log.warn('replication: yielding to better primary', { peer: better.peerId.slice(-8), epoch: better.epoch })
      this.stepDown(better.epoch)
      return
    }
    if (this.pushing) {
      this.pushPending = true
      return
    }
    this.pushing = true
    try {
      do {
        this.pushPending = false
        const fromLsn = this.lastBroadcastLsn
        if (this.store.lsn <= fromLsn) break
        const entries = this.metaLog!.read(fromLsn, this.opts.batchSize)
        if (entries.length === 0) {
          this.lastBroadcastLsn = this.store.lsn
          break
        }
        const toLsn = entries[entries.length - 1].lsn
        const sha = toLsn === this.store.lsn ? this.store.sha256() : undefined
        const msg: PushMsg = { fromLsn, toLsn, ops: entries.map((e) => e.op), sha256: sha }
        await this.pushToAll(msg)
        this.lastBroadcastLsn = toLsn
      } while (this.pushPending || this.lastBroadcastLsn < this.store.lsn)
    } finally {
      this.pushing = false
    }
  }

  private async pushToAll(msg: PushMsg): Promise<void> {
    const tieredActive = this.opts.tierEnabled || this.cluster.size >= (this.opts.tierScaleThreshold ?? 100)
    let targetPeers: PeerId[] = []

    if (tieredActive) {
      const relays = this.cluster.relayPeers(this.opts.relayRoster)
      targetPeers = relays.map((r) => r.peer).filter((p): p is PeerId => p !== undefined)
      if (targetPeers.length === 0) {
        targetPeers = this.node.getConnections().map((c) => c.remotePeer)
      }
    } else {
      targetPeers = this.node.getConnections().map((c) => c.remotePeer)
    }

    const seen = new Set<string>()
    for (const peer of targetPeers) {
      const id = peer.toString()
      if (seen.has(id)) continue
      seen.add(id)
      try {
        await rpc<PushMsg, PushResp>(this.node, peer, PROTOCOLS.metaPush, msg)
      } catch (err) {
        log.debug('push failed', { peer: id.slice(-8), error: String(err) })
      }
    }
  }

  private async relayPush(msg: PushMsg, sourcePeerId?: string): Promise<void> {
    const seen = new Set<string>()
    if (sourcePeerId) seen.add(sourcePeerId)
    const primaryId = this.cluster.primaryPeer()?.toString()
    if (primaryId) seen.add(primaryId)
    const relays = new Set(this.cluster.relayPeers(this.opts.relayRoster).map((r) => r.peerId))

    for (const conn of this.node.getConnections()) {
      const peer = conn.remotePeer
      const id = peer.toString()
      if (seen.has(id) || relays.has(id)) continue
      seen.add(id)
      try {
        await rpc<PushMsg, PushResp>(this.node, peer, PROTOCOLS.metaPush, msg)
      } catch (err) {
        log.debug('relay push failed', { peer: id.slice(-8), error: String(err) })
      }
    }
  }

  // ---- replica: pull (recovery / join) --------------------------------------

  private ensurePull(full = false): void {
    if (full) this.pullFull = true
    if (this.isPrimary || this.pulling) return
    void this.pull()
  }

  private retryPull(): void {
    if (this.pullTimer) return
    this.pullTimer = setTimeout(() => {
      this.pullTimer = undefined
      this.ensurePull()
    }, 1000)
  }

  private async pull(): Promise<void> {
    if (this.isPrimary || this.pulling) return
    this.pulling = true
    const full = this.pullFull
    this.pullFull = false
    this.store.setMode('pull') // honor "verify before serve": stop serving while catching up
    try {
      let sourcePeer: PeerId | undefined
      if (!this.isRelay) {
        const healthyRelay = this.cluster.relayPeers(this.opts.relayRoster).find((r) => r.peer)
        if (healthyRelay) sourcePeer = healthyRelay.peer
      }
      if (!sourcePeer) {
        sourcePeer = this.cluster.primaryPeer()
      }
      if (!sourcePeer) {
        this.retryPull()
        return
      }
      const snap = await rpc<Record<string, never>, SnapshotResp>(this.node, sourcePeer, PROTOCOLS.snapshot, {})
      // Normal catch-up loads the snapshot only if it is ahead of us; a full reset
      // (triggered by a hash mismatch / corruption) discards local state and rebases
      // on the snapshot unconditionally.
      if (snap.blob && (full || snap.lsn > this.store.lsn)) {
        this.store.load(Buffer.from(snap.blob, 'base64'))
        log.info('pull: loaded snapshot', { lsn: snap.lsn, full })
      }
      for (;;) {
        const resp = await rpc<PullReq, PullResp>(this.node, sourcePeer, PROTOCOLS.metaPull, { haveLsn: this.store.lsn })
        if (resp.error) throw new Error(resp.error)
        if (resp.ops.length > 0) {
          this.store.applyBatch(resp.ops, resp.toLsn, resp.latest ? resp.sha256 : undefined)
        }
        if (resp.latest) {
          if (resp.sha256 && this.store.sha256() !== resp.sha256) {
            throw new Error('post-pull sha mismatch')
          }
          break
        }
      }
      this.lastBroadcastLsn = this.store.lsn
      this.store.setMode('push')
      log.info('pull: complete, serving', { lsn: this.store.lsn, sha: this.store.sha256().slice(0, 8) })
    } catch (err) {
      log.warn('pull failed, will retry', { error: String(err) })
      this.pullFull = true // recover aggressively on the retry
      this.retryPull()
    } finally {
      this.pulling = false
    }
  }

  // ---- handlers -------------------------------------------------------------

  private registerHandlers(): void {
    if (this.quorum) {
      handle<QuorumRequest<MetadataCommand>, QuorumResponse>(this.node, PROTOCOLS.quorum, async (request, ctx) => {
        const peer = this.cluster.get(ctx.peer.toString())
        if (!peer || !this.opts.voters?.includes(peer.name)) throw new Error('Unknown metadata voter')
        return this.quorum!.handle(peer.name, request)
      })
      handle<WriteIntent, WriteResult>(this.node, PROTOCOLS.metaWrite, async (intent) => {
        if (!this.isPrimary) return { error: 'not-primary: a ready metadata majority is required', code: 'EUNAVAILABLE' }
        return this.commitIntent(intent)
      })
      return
    }
    // Replica receives pushes.
    handle<PushMsg, PushResp>(this.node, PROTOCOLS.metaPush, async (msg, ctx) => {
      if (this.isPrimary) return { status: 'ok', lsn: this.store.lsn }
      if (this.store.mode !== 'push' || this.store.lsn !== msg.fromLsn) {
        this.ensurePull(false)
        return { status: 'needPull', lsn: this.store.lsn }
      }
      try {
        this.store.applyBatch(msg.ops, msg.toLsn, msg.sha256)
        if (this.metaLog) {
          for (let i = 0; i < msg.ops.length; i++) {
            this.metaLog.append(msg.fromLsn + i + 1, msg.ops[i], Date.now())
          }
        }
        if (this.isRelay) {
          void this.relayPush(msg, ctx.peer.toString())
        }
        return { status: 'ok', lsn: this.store.lsn }
      } catch (err) {
        log.warn('push apply failed -> pull', { error: String(err) })
        this.ensurePull(true)
        return { status: 'needPull', lsn: this.store.lsn }
      }
    })

    // Primary or Relay serves pulls.
    handle<PullReq, PullResp>(this.node, PROTOCOLS.metaPull, async (req) => {
      if ((!this.isPrimary && !this.isRelay) || !this.metaLog) {
        return { ops: [], toLsn: req.haveLsn, latest: true, error: 'not-primary-or-relay' }
      }
      const entries = this.metaLog.read(req.haveLsn, this.opts.batchSize)
      if (entries.length === 0) {
        return { ops: [], toLsn: this.store.lsn, latest: true, sha256: this.store.sha256() }
      }
      const toLsn = entries[entries.length - 1].lsn
      const latest = toLsn === this.store.lsn
      return { ops: entries.map((e) => e.op), toLsn, latest, sha256: latest ? this.store.sha256() : undefined }
    })

    // Primary or Relay serves snapshots.
    handle<Record<string, never>, SnapshotResp>(this.node, PROTOCOLS.snapshot, async () => {
      if (!this.isPrimary && !this.isRelay) return { lsn: 0, blob: '', error: 'not-primary-or-relay' }
      const { lsn, blob } = this.store.snapshot()
      return { lsn, blob: blob.toString('base64') }
    })

    // Primary (or Relay forwarding to Primary) accepts forwarded write intents.
    handle<WriteIntent, WriteResult>(this.node, PROTOCOLS.metaWrite, async (intent) => {
      if (this.isPrimary) return this.performIntent(intent)
      const primary = this.cluster.primaryPeer()
      if (primary) {
        return rpc<WriteIntent, WriteResult>(this.node, primary, PROTOCOLS.metaWrite, intent, AbortSignal.timeout(12000))
      }
      return { error: 'not-primary' }
    })
  }
}
