/**
 * Fixed three-voter Raft, with no membership changes, snapshots, or read leases.
 *
 * Commands must be JSON-serializable and non-null (null is the internal no-op).
 * apply must be synchronous, deterministic, and confined to the supplied database.
 * It runs inside our transaction: do not BEGIN, COMMIT, reenter QuorumLog, or
 * perform external side effects. Callers must not have an open transaction.
 * The owner supplies synchronous=FULL, authenticates RPC peer names, gates legacy
 * metadata migration, and retains ownership of the database and transport.
 *
 * A proposal timeout/lost leadership is an uncertain outcome, not cancellation:
 * the command can still commit. Client retry deduplication belongs in commands.
 * A proposal timeout demotes the leader. A fresh election/no-op must resolve the
 * inherited log before more commands can be proposed from committed state.
 * Malformed remote responses count as unavailable voters, never acknowledgments.
 */
import type { DatabaseSync } from 'node:sqlite'
import type { Counters } from '../api/counters.js'
import { log } from '../logger.js'

export interface QuorumEntry<T> {
  index: number
  term: number
  command: T | null
}

export type QuorumRequest<T> =
  | { type: 'vote'; term: number; candidateId: string; lastLogIndex: number; lastLogTerm: number }
  | {
    type: 'append'
    term: number
    leaderId: string
    prevLogIndex: number
    prevLogTerm: number
    entries: QuorumEntry<T>[]
    leaderCommit: number
  }

export type QuorumResponse =
  | { type: 'vote'; term: number; voteGranted: boolean }
  | { type: 'append'; term: number; success: boolean; matchIndex: number; conflictIndex?: number }

export interface QuorumOptions<T> {
  id: string
  voters: string[]
  db: DatabaseSync
  send: (peer: string, request: QuorumRequest<T>) => Promise<QuorumResponse>
  apply: (command: T, index: number, term: number) => void
  electionMinMs?: number
  electionMaxMs?: number
  heartbeatMs?: number
  /** Bounds each RPC and the total wait for a proposal, independently. */
  requestTimeoutMs?: number
  /** Phase 7: optional monotonic counters surfaced by the /metrics endpoint. */
  counters?: Counters
}

interface StateRow {
  fingerprint: string
  term: number
  voted_for: string | null
  commit_index: number
  applied_index: number
}

interface EntryRow {
  idx: number
  term: number
  command: string
}

interface Pending {
  resolve: () => void
  reject: (error: Error) => void
  timer: NodeJS.Timeout
}

interface PeerProgress {
  next: number
  match: number
  busy: boolean
  again: boolean
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS quorum_state (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  fingerprint TEXT NOT NULL,
  term INTEGER NOT NULL,
  voted_for TEXT,
  commit_index INTEGER NOT NULL,
  applied_index INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS quorum_log (
  idx INTEGER PRIMARY KEY CHECK (idx > 0),
  term INTEGER NOT NULL CHECK (term > 0),
  command TEXT NOT NULL
);
`

function integer(value: unknown, min = 0): value is number {
  return typeof value === 'number' && Number.isSafeInteger(value) && value >= min
}

function errorOf(error: unknown): Error {
  return error instanceof Error ? error : new Error(String(error))
}

function encode(command: unknown): string {
  const json = JSON.stringify(command)
  if (json === undefined) throw new Error('Quorum command is not JSON-serializable')
  return json
}

function storedEntry(row: Record<string, unknown>): EntryRow {
  if (!integer(row.idx, 1) || !integer(row.term, 1) || typeof row.command !== 'string') {
    throw new Error('Invalid stored quorum entry')
  }
  return { idx: row.idx, term: row.term, command: row.command }
}

export class QuorumLog<T> {
  private readonly id: string
  private readonly voters: readonly string[]
  private readonly db: DatabaseSync
  private readonly send: QuorumOptions<T>['send']
  private readonly applyCommand: QuorumOptions<T>['apply']
  private readonly counters?: Counters
  private readonly electionMinMs: number
  private readonly electionMaxMs: number
  private readonly heartbeatMs: number
  private readonly requestTimeoutMs: number
  private readonly peers = new Map<string, PeerProgress>()
  private readonly pending = new Map<number, Pending>()
  private readonly cancellations = new Set<(error: Error) => void>()
  private readonly unhealthyPeers = new Set<string>()
  private _term = 0
  private votedFor: string | null = null
  private _commitIndex = 0
  private appliedIndex = 0
  private _lastIndex = 0
  private role: 'follower' | 'candidate' | 'leader' = 'follower'
  private _leaderId?: string
  private leaderNoop = 0
  private running = false
  private generation = 0
  private fatal?: Error
  private electionTimer?: NodeJS.Timeout
  private heartbeatTimer?: NodeJS.Timeout

  constructor(opts: QuorumOptions<T>) {
    if (opts.voters.length !== 3 || new Set(opts.voters).size !== 3 ||
        opts.voters.some(voter => typeof voter !== 'string' || voter.length === 0) ||
        !opts.voters.includes(opts.id)) {
      throw new Error('Quorum requires exactly three distinct nonempty voters including id')
    }
    this.id = opts.id
    this.voters = Object.freeze([...opts.voters].sort())
    this.db = opts.db
    this.send = opts.send
    this.applyCommand = opts.apply
    this.counters = opts.counters
    this.electionMinMs = opts.electionMinMs ?? 600
    this.electionMaxMs = opts.electionMaxMs ?? 1200
    this.heartbeatMs = opts.heartbeatMs ?? 100
    this.requestTimeoutMs = opts.requestTimeoutMs ?? 1500
    for (const ms of [this.electionMinMs, this.electionMaxMs, this.heartbeatMs, this.requestTimeoutMs]) {
      if (!integer(ms, 1) || ms > 2_147_483_647) throw new Error('Invalid quorum timer duration')
    }
    if (this.electionMaxMs < this.electionMinMs || this.heartbeatMs >= this.electionMinMs) {
      throw new Error('Quorum election range must be ordered and exceed the heartbeat interval')
    }
    for (const peer of this.voters) {
      if (peer !== this.id) this.peers.set(peer, { next: 1, match: 0, busy: false, again: false })
    }
    try {
      const fingerprint = JSON.stringify({ version: 1, id: this.id, voters: this.voters })
      this.transaction(() => {
        this.db.exec(SCHEMA)
        this.db.prepare(`
          INSERT OR IGNORE INTO quorum_state
            (singleton, fingerprint, term, voted_for, commit_index, applied_index)
          VALUES (1, ?, 0, NULL, 0, 0)
        `).run(fingerprint)
        const row = this.db.prepare('SELECT * FROM quorum_state WHERE singleton = 1').get() as StateRow | undefined
        if (!row) throw new Error('Missing persistent quorum state')
        if (row.fingerprint !== fingerprint) throw new Error('Quorum id or fixed membership changed for this database')
        this._term = row.term
        this.votedFor = row.voted_for
        this._commitIndex = row.commit_index
        this.appliedIndex = row.applied_index
        this._lastIndex = (this.db.prepare('SELECT COALESCE(MAX(idx), 0) AS idx FROM quorum_log').get() as { idx: number }).idx
        this.validateStoredState()
      })
      this.applyCommitted()
    } catch (error) {
      log.error('Quorum recovery failed', { id: this.id, error: errorOf(error).message })
      throw error
    }
  }

  get isLeader(): boolean { return this.running && this.role === 'leader' }
  get term(): number { return this._term }
  get leaderId(): string | undefined { return this._leaderId }
  get ready(): boolean {
    return this.isLeader && this.leaderNoop > 0 && this.appliedIndex >= this.leaderNoop
  }
  get commitIndex(): number { return this._commitIndex }
  get lastIndex(): number { return this._lastIndex }

  start(): void {
    if (this.fatal) throw this.fatal
    if (this.running) return
    this.running = true
    this.unhealthyPeers.clear()
    this.generation++
    this.resetElection()
  }

  stop(): void {
    this.running = false
    this.generation++
    this.role = 'follower'
    this._leaderId = undefined
    this.leaderNoop = 0
    this.clearTimers()
    const error = new Error('Quorum stopped')
    this.rejectPending(error)
    for (const cancel of [...this.cancellations]) cancel(error)
  }

  async propose(command: T): Promise<void> {
    this.requireRunning()
    if (!this.ready) throw new Error('Quorum leader is not ready or this node is not leader')
    let index: number
    try {
      const json = encode(command)
      if (json === 'null') throw new Error('Null is reserved for quorum no-op entries')
      index = this.appendLocal(json)
    } catch (error) {
      // Bad commands are caller errors, whereas storage failures must fail closed.
      log.error('Quorum proposal could not be persisted', { id: this.id, error: errorOf(error).message })
      throw error
    }
    return new Promise<void>((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(index)
        const error = new Error(`Quorum proposal ${index} timed out before majority commit and local apply (outcome uncertain)`)
        log.warn(error.message, { id: this.id, term: this._term })
        this.stepDown(this._term)
        reject(error)
      }, this.requestTimeoutMs)
      this.pending.set(index, { resolve, reject, timer })
      this.kickAll()
    })
  }

  /** Starts an election immediately. Completion does not imply leader readiness. */
  async campaign(): Promise<void> {
    this.requireRunning()
    if (this.isLeader) return
    this.counters?.inc('quorum_campaigns_total')
    try {
      this.persistTerm(this._term + 1, this.id)
      this.role = 'candidate'
      this._leaderId = undefined
      this.leaderNoop = 0
      const generation = ++this.generation
      const term = this._term
      this.resetElection()
      const request: QuorumRequest<T> = {
        type: 'vote', term, candidateId: this.id,
        lastLogIndex: this._lastIndex, lastLogTerm: this.entryTerm(this._lastIndex)
      }
      let votes = 1
      await Promise.all([...this.peers.keys()].map(async peer => {
        const response = await this.request(peer, request)
        if (!response || !this.running) return
        if (response.term > this._term) this.stepDown(response.term)
        if (generation !== this.generation || this.role !== 'candidate' || response.term !== term) return
        if (response.type === 'vote' && response.voteGranted && ++votes >= 2) this.becomeLeader()
      }))
    } catch (error) {
      this.fail(error, 'election')
      throw error
    }
  }

  async handle(from: string, request: QuorumRequest<T>): Promise<QuorumResponse> {
    this.requireRunning()
    try {
      this.validateRequest(from, request)
    } catch (error) {
      log.warn('Rejected quorum RPC', { id: this.id, from, error: errorOf(error).message })
      throw error
    }
    try {
      if (request.term > this._term) this.stepDown(request.term)
      if (request.type === 'vote') return this.handleVote(request)
      return this.handleAppend(request)
    } catch (error) {
      this.fail(error, 'incoming RPC')
      throw error
    }
  }

  private requireRunning(): void {
    if (this.fatal) throw this.fatal
    if (!this.running) throw new Error('Quorum is not started')
  }

  private transaction<R>(fn: () => R): R {
    // BEGIN is outside the catch: a caller's existing transaction is never rolled back.
    this.db.exec('BEGIN IMMEDIATE')
    try {
      const result = fn()
      this.db.exec('COMMIT')
      return result
    } catch (error) {
      try {
        this.db.exec('ROLLBACK')
      } catch (rollbackError) {
        throw new AggregateError([error, rollbackError], 'Quorum transaction and rollback failed')
      }
      throw error
    }
  }

  private validateStoredState(): void {
    if (!integer(this._term) || !integer(this._commitIndex) || !integer(this.appliedIndex) ||
        this.appliedIndex > this._commitIndex || this._commitIndex > this._lastIndex ||
        (this.votedFor !== null && !this.voters.includes(this.votedFor))) {
      throw new Error('Invalid persistent quorum state')
    }
    let index = 0
    let term = 0
    for (const row of this.db.prepare('SELECT idx, term, command FROM quorum_log ORDER BY idx').iterate()) {
      if (row.idx !== ++index || !integer(row.term, 1) || row.term < term || row.term > this._term ||
          typeof row.command !== 'string') throw new Error('Invalid persistent quorum log')
      JSON.parse(row.command)
      term = row.term
    }
  }

  private entryTerm(index: number): number {
    if (index === 0) return 0
    const row = this.db.prepare('SELECT term FROM quorum_log WHERE idx = ?').get(index)
    if (!row || typeof row.term !== 'number') throw new Error(`Missing quorum entry ${index}`)
    return row.term
  }

  private persistTerm(term: number, votedFor: string | null): void {
    if (!integer(term)) throw new Error('Quorum term exhausted')
    this.transaction(() => {
      this.db.prepare('UPDATE quorum_state SET term = ?, voted_for = ? WHERE singleton = 1').run(term, votedFor)
    })
    this._term = term
    this.votedFor = votedFor
  }

  private appendLocal(command: string): number {
    const index = this._lastIndex + 1
    if (!integer(index, 1)) throw new Error('Quorum index exhausted')
    try {
      this.transaction(() => {
        this.db.prepare('INSERT INTO quorum_log (idx, term, command) VALUES (?, ?, ?)').run(index, this._term, command)
      })
    } catch (error) {
      this.fail(error, 'persisting local entry')
      throw error
    }
    this._lastIndex = index
    return index
  }

  private applyCommitted(): void {
    const target = this._commitIndex
    if (this.appliedIndex >= target) return
    this.transaction(() => {
      const rows = this.db.prepare('SELECT idx, term, command FROM quorum_log WHERE idx > ? AND idx <= ? ORDER BY idx')
        .all(this.appliedIndex, target).map(storedEntry)
      if (rows.length !== target - this.appliedIndex) throw new Error('Committed quorum log contains a gap')
      for (const row of rows) {
        const command = JSON.parse(row.command) as T | null
        if (command !== null) {
          const result: unknown = this.applyCommand(command, row.idx, row.term)
          if (result !== null && (typeof result === 'object' || typeof result === 'function') &&
              'then' in result && typeof result.then === 'function') {
            void Promise.resolve(result).catch(error => {
              log.error('Quorum apply callback returned a rejected promise', { id: this.id, error: errorOf(error).message })
            })
            throw new Error('Quorum apply callback must be synchronous')
          }
        }
        this.db.prepare('UPDATE quorum_state SET applied_index = ? WHERE singleton = 1').run(row.idx)
      }
    })
    this.appliedIndex = target
    for (const [index, pending] of this.pending) {
      if (index <= target) {
        clearTimeout(pending.timer)
        this.pending.delete(index)
        pending.resolve()
      }
    }
  }

  private stepDown(term: number, leader?: string): void {
    if (term > this._term) this.persistTerm(term, null)
    this.generation++
    this.role = 'follower'
    this._leaderId = leader
    this.leaderNoop = 0
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    this.heartbeatTimer = undefined
    this.rejectPending(new Error('Quorum leadership lost (proposal outcome uncertain)'))
    // A leader has no election timer. Existing follower/candidate deadlines stay
    // unchanged until an authenticated current-term append or granted vote.
    if (!this.electionTimer) this.resetElection()
  }

  private handleVote(request: Extract<QuorumRequest<T>, { type: 'vote' }>): QuorumResponse {
    let granted = false
    if (request.term === this._term) {
      const lastTerm = this.entryTerm(this._lastIndex)
      const upToDate = request.lastLogTerm > lastTerm ||
        (request.lastLogTerm === lastTerm && request.lastLogIndex >= this._lastIndex)
      if (upToDate && (this.votedFor === null || this.votedFor === request.candidateId)) {
        this.persistTerm(this._term, request.candidateId)
        this._leaderId = undefined
        granted = true
        this.resetElection()
      }
    }
    return { type: 'vote', term: this._term, voteGranted: granted }
  }

  private handleAppend(request: Extract<QuorumRequest<T>, { type: 'append' }>): QuorumResponse {
    const failure = (conflictIndex: number): QuorumResponse => ({
      type: 'append', term: this._term, success: false, matchIndex: 0, conflictIndex
    })
    if (request.term < this._term) return failure(this._lastIndex + 1)
    if (this.role !== 'follower' || this._leaderId !== request.leaderId) {
      this.stepDown(request.term, request.leaderId)
    }
    this.resetElection()
    if (request.prevLogIndex > this._lastIndex) return failure(this._lastIndex + 1)
    const prevTerm = this.entryTerm(request.prevLogIndex)
    if (prevTerm !== request.prevLogTerm) {
      const row = this.db.prepare('SELECT MIN(idx) AS idx FROM quorum_log WHERE term = ?').get(prevTerm) as { idx: number }
      return failure(row.idx)
    }
    const encoded = request.entries.map(entry => ({ ...entry, json: encode(entry.command) }))
    let firstNew = encoded.length
    for (let i = 0; i < encoded.length; i++) {
      const entry = encoded[i]
      const existing = this.db.prepare('SELECT term, command FROM quorum_log WHERE idx = ?').get(entry.index)
      if (!existing) { firstNew = i; break }
      if (existing.term !== entry.term) {
        if (entry.index <= this._commitIndex) {
          log.error('Refusing to overwrite committed quorum entry', { id: this.id, index: entry.index })
          return failure(this._commitIndex + 1)
        }
        firstNew = i
        break
      }
      if (existing.command !== entry.json) {
        log.error('Refusing different commands at identical quorum index and term', { id: this.id, index: entry.index })
        return failure(entry.index)
      }
    }
    const matched = request.prevLogIndex + encoded.length
    const commit = Math.max(this._commitIndex, Math.min(request.leaderCommit, matched))
    let last = this._lastIndex
    this.transaction(() => {
      if (firstNew < encoded.length) {
        this.db.prepare('DELETE FROM quorum_log WHERE idx >= ?').run(encoded[firstNew].index)
        for (const entry of encoded.slice(firstNew)) {
          this.db.prepare('INSERT INTO quorum_log (idx, term, command) VALUES (?, ?, ?)').run(entry.index, entry.term, entry.json)
        }
        last = matched
      }
      this.db.prepare('UPDATE quorum_state SET commit_index = ? WHERE singleton = 1').run(commit)
    })
    this._lastIndex = last
    this._commitIndex = commit
    this.applyCommitted()
    return { type: 'append', term: this._term, success: true, matchIndex: matched }
  }

  private becomeLeader(): void {
    this.counters?.inc('quorum_elections_won_total')
    this.leaderNoop = this.appendLocal('null')
    this.role = 'leader'
    this._leaderId = this.id
    this.generation++
    if (this.electionTimer) clearTimeout(this.electionTimer)
    this.electionTimer = undefined
    for (const progress of this.peers.values()) {
      progress.next = this.leaderNoop
      progress.match = 0
    }
    this.heartbeatTimer = setInterval(() => this.kickAll(), this.heartbeatMs)
    this.heartbeatTimer.unref()
    this.kickAll()
  }

  private kickAll(): void {
    if (!this.isLeader) return
    for (const [peer, progress] of this.peers) {
      if (progress.busy) { progress.again = true; continue }
      this.kickPeer(peer, progress)
    }
  }

  private kickPeer(peer: string, progress: PeerProgress): void {
    // Slots survive elections and stop/start, so an old in-flight RPC must drain
    // before a new leadership generation can use that peer's progress.
    if (progress.busy || !this.isLeader) return
    progress.busy = true
    progress.again = false
    void this.replicate(peer, progress).catch(error => {
      this.fail(error, `replication to ${peer}`)
    }).finally(() => {
      progress.busy = false
      if (progress.again && this.isLeader) this.kickPeer(peer, progress)
    })
  }

  private async replicate(peer: string, progress: PeerProgress): Promise<void> {
    const generation = this.generation
    const term = this._term
    while (this.isLeader && generation === this.generation) {
      const prevLogIndex = progress.next - 1
      const rows = this.db.prepare('SELECT idx, term, command FROM quorum_log WHERE idx >= ? ORDER BY idx LIMIT 128')
        .all(progress.next).map(storedEntry)
      const entries = rows.map(row => ({ index: row.idx, term: row.term, command: JSON.parse(row.command) as T | null }))
      const matched = prevLogIndex + entries.length
      const response = await this.request(peer, {
        type: 'append', term, leaderId: this.id, prevLogIndex,
        prevLogTerm: this.entryTerm(prevLogIndex), entries, leaderCommit: this._commitIndex
      })
      if (!response || !this.running) return
      if (response.term > this._term) this.stepDown(response.term)
      if (!this.isLeader || generation !== this.generation || response.term !== term || response.type !== 'append') return
      if (response.success) {
        progress.match = Math.max(progress.match, matched)
        progress.next = Math.max(progress.next, matched + 1)
        this.advanceCommit()
        if (progress.next > this._lastIndex) return
      } else {
        const next = Math.max(progress.match + 1, Math.min(progress.next - 1, response.conflictIndex ?? progress.next - 1))
        if (next >= progress.next) {
          log.error('Quorum follower rejected a confirmed prefix', { id: this.id, peer, next })
          return
        }
        progress.next = next
      }
    }
  }

  private advanceCommit(): void {
    const matches = [this._lastIndex, ...[...this.peers.values()].map(progress => progress.match)]
    matches.sort((a, b) => b - a)
    const majorityIndex = matches[1]
    // Counting copies of an older-term entry is insufficient. A current-term
    // entry (including the election no-op) commits the entire preceding prefix.
    if (majorityIndex <= this._commitIndex || this.entryTerm(majorityIndex) !== this._term) return
    this.transaction(() => {
      this.db.prepare('UPDATE quorum_state SET commit_index = ? WHERE singleton = 1').run(majorityIndex)
    })
    this._commitIndex = majorityIndex
    this.applyCommitted()
    this.kickAll()
  }

  private async request(peer: string, request: QuorumRequest<T>): Promise<QuorumResponse | undefined> {
    let response: QuorumResponse
    try {
      response = await new Promise<QuorumResponse>((resolve, reject) => {
        const finish = (error?: Error, value?: QuorumResponse): void => {
          clearTimeout(timer)
          this.cancellations.delete(cancel)
          if (error) reject(error)
          else if (value) resolve(value)
          else reject(new Error('Empty quorum RPC response'))
        }
        const cancel = (error: Error): void => finish(error)
        const timer = setTimeout(() => finish(new Error(`Quorum RPC to ${peer} timed out`)), this.requestTimeoutMs)
        this.cancellations.add(cancel)
        // Promise adoption handles synchronous transport throws and late rejects.
        Promise.resolve().then(() => {
          this.requireRunning()
          return this.send(peer, request)
        }).then(
          value => finish(undefined, value),
          error => finish(errorOf(error))
        )
      })
    } catch (error) {
      if (this.running) this.rpcFailed(peer, errorOf(error).message)
      return undefined
    }
    if (!this.running) return undefined
    if (!response || response.type !== request.type || !integer(response.term) ||
        (response.type === 'vote' && typeof response.voteGranted !== 'boolean') ||
        (response.type === 'append' && (typeof response.success !== 'boolean' || !integer(response.matchIndex) ||
          (response.conflictIndex !== undefined && !integer(response.conflictIndex, 1))))) {
      this.rpcFailed(peer, 'Malformed quorum RPC response')
      return undefined
    }
    if (request.type === 'append' && response.type === 'append' && response.success &&
        response.matchIndex !== request.prevLogIndex + request.entries.length) {
      this.rpcFailed(peer, 'Quorum RPC response acknowledged an incorrect matchIndex')
      return undefined
    }
    this.unhealthyPeers.delete(peer)
    return response
  }

  private rpcFailed(peer: string, reason: string): void {
    const extra = { id: this.id, peer, error: reason }
    if (this.unhealthyPeers.has(peer)) {
      log.debug('Quorum RPC failed', extra)
    } else {
      this.unhealthyPeers.add(peer)
      log.warn('Quorum RPC failed', extra)
    }
  }

  private validateRequest(from: string, request: QuorumRequest<T>): void {
    if (!this.voters.includes(from)) throw new Error(`Nonmember quorum peer: ${from}`)
    if (!request || !integer(request.term, 1)) throw new Error('Invalid quorum request term')
    if (request.type === 'vote') {
      if (from !== request.candidateId) throw new Error('Quorum candidate identity mismatch')
      if (!integer(request.lastLogIndex) || !integer(request.lastLogTerm) ||
          request.lastLogTerm > request.term || (request.lastLogIndex === 0) !== (request.lastLogTerm === 0)) {
        throw new Error('Invalid quorum vote log position')
      }
    } else if (request.type === 'append') {
      if (from !== request.leaderId) throw new Error('Quorum leader identity mismatch')
      if (!integer(request.prevLogIndex) || !integer(request.prevLogTerm) || !integer(request.leaderCommit) ||
          request.prevLogTerm > request.term || (request.prevLogIndex === 0) !== (request.prevLogTerm === 0) ||
          !Array.isArray(request.entries)) throw new Error('Invalid quorum append prefix')
      let index = request.prevLogIndex
      let term = request.prevLogTerm
      for (const entry of request.entries) {
        if (!entry || !integer(entry.index, 1) || entry.index !== ++index ||
            !integer(entry.term, 1) || entry.term < term || entry.term > request.term) {
          throw new Error('Invalid quorum append entries')
        }
        encode(entry.command)
        term = entry.term
      }
    } else {
      throw new Error('Unknown quorum RPC type')
    }
  }

  private resetElection(): void {
    if (this.electionTimer) clearTimeout(this.electionTimer)
    this.electionTimer = undefined
    if (!this.running || this.role === 'leader') return
    const delay = this.electionMinMs + Math.floor(Math.random() * (this.electionMaxMs - this.electionMinMs + 1))
    this.electionTimer = setTimeout(() => {
      this.electionTimer = undefined
      void this.campaign().catch(error => {
        log.error('Quorum automatic election failed', { id: this.id, error: errorOf(error).message })
      })
    }, delay)
    this.electionTimer.unref()
  }

  private clearTimers(): void {
    if (this.electionTimer) clearTimeout(this.electionTimer)
    if (this.heartbeatTimer) clearInterval(this.heartbeatTimer)
    this.electionTimer = undefined
    this.heartbeatTimer = undefined
  }

  private rejectPending(error: Error): void {
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer)
      pending.reject(error)
    }
    this.pending.clear()
  }

  private fail(error: unknown, operation: string): void {
    const failure = errorOf(error)
    log.error('Quorum halted after local failure', { id: this.id, operation, error: failure.message })
    this.fatal = failure
    this.rejectPending(failure)
    this.stop()
  }
}
