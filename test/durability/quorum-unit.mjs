/**
 * Persistent fixed-three-voter Raft tests, without a network stack.
 * Run after the parent build: node --test test/durability/quorum-unit.mjs
 */
import test from 'node:test'
import assert from 'node:assert/strict'
import { DatabaseSync } from 'node:sqlite'
import { mkdtempSync, rmSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { setTimeout as sleep } from 'node:timers/promises'
import { QuorumLog } from '../../dist/replication/quorum.js'

const VOTERS = ['a', 'b', 'c']

async function until(predicate, message = 'condition', timeout = 2500) {
  const deadline = performance.now() + timeout
  while (!predicate()) {
    assert.ok(performance.now() < deadline, `Timed out waiting for ${message}`)
    await sleep(2)
  }
}

function deferred() {
  let resolve
  const promise = new Promise(done => { resolve = done })
  return { promise, resolve }
}

class Cluster {
  nodes = new Map()
  dbs = new Map()
  blocked = new Set()
  active = new Map()
  maxActive = new Map()
  intercept
  afterApply

  constructor(t, { files = false, ...timers } = {}) {
    this.directory = files ? mkdtempSync(join(tmpdir(), 'quorum-unit-')) : undefined
    this.timers = {
      electionMinMs: 60_000, electionMaxMs: 90_000,
      heartbeatMs: 30, requestTimeoutMs: 250, ...timers
    }
    t.after(() => {
      for (const node of this.nodes.values()) node.stop()
      for (const db of this.dbs.values()) db.close()
      if (this.directory) rmSync(this.directory, { recursive: true })
    })
    for (const id of VOTERS) this.create(id)
  }

  create(id, reopen = false) {
    this.nodes.get(id)?.stop()
    let db = this.dbs.get(id)
    if (reopen) {
      assert.ok(this.directory)
      db.close()
      db = undefined
    }
    if (!db) {
      db = new DatabaseSync(this.directory ? join(this.directory, `${id}.sqlite`) : ':memory:')
      db.exec(`
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = FULL;
        CREATE TABLE IF NOT EXISTS applied_commands (
          idx INTEGER PRIMARY KEY, term INTEGER NOT NULL, command TEXT NOT NULL
        );
      `)
      this.dbs.set(id, db)
    }
    const node = new QuorumLog({
      id, voters: [...VOTERS], db, ...this.timers,
      send: (peer, request) => this.send(id, peer, request),
      apply: (command, index, term) => {
        assert.equal(db.isTransaction, true, 'apply must share the applied-index transaction')
        db.prepare('INSERT INTO applied_commands (idx, term, command) VALUES (?, ?, ?)')
          .run(index, term, JSON.stringify(command))
        this.afterApply?.(id, command, index, term, db)
      }
    })
    this.nodes.set(id, node)
    node.start()
    return node
  }

  async send(from, to, request) {
    const key = `${from}:${to}`
    if (this.blocked.has(key)) throw new Error(`Partition ${key}`)
    const append = request.type === 'append'
    if (append) {
      const active = (this.active.get(key) ?? 0) + 1
      this.active.set(key, active)
      this.maxActive.set(key, Math.max(this.maxActive.get(key) ?? 0, active))
    }
    const context = {
      from, to, request: structuredClone(request),
      deliver: (wire = request) => {
        if (this.blocked.has(key)) throw new Error(`Partition ${key}`)
        return this.nodes.get(to).handle(from, structuredClone(wire))
      }
    }
    try {
      return await (this.intercept ? this.intercept(context) : context.deliver())
    } finally {
      if (append) this.active.set(key, this.active.get(key) - 1)
    }
  }

  partition(groups) {
    this.blocked.clear()
    for (const from of VOTERS) {
      for (const to of VOTERS) {
        if (!groups.some(group => group.includes(from) && group.includes(to))) this.blocked.add(`${from}:${to}`)
      }
    }
  }

  heal() { this.blocked.clear() }
  node(id) { return this.nodes.get(id) }
  db(id) { return this.dbs.get(id) }
  state(id) { return this.db(id).prepare('SELECT * FROM quorum_state').get() }
  log(id) { return this.db(id).prepare('SELECT idx, term, command FROM quorum_log ORDER BY idx').all() }
  commands(id) {
    return this.db(id).prepare('SELECT command FROM applied_commands ORDER BY idx').all()
      .map(row => JSON.parse(row.command))
  }

  async elect(id) {
    await this.node(id).campaign()
    await until(() => this.node(id).ready, `${id} current-term no-op commit`)
    return this.node(id)
  }
}

function append(leaderId, term, entries, {
  prevLogIndex = 0, prevLogTerm = 0, leaderCommit = 0
} = {}) {
  return { type: 'append', leaderId, term, entries, prevLogIndex, prevLogTerm, leaderCommit }
}

function vote(candidateId, term, lastLogIndex = 0, lastLogTerm = 0) {
  return { type: 'vote', candidateId, term, lastLogIndex, lastLogTerm }
}

test('requires exactly three fixed voters and persists the local identity/membership fingerprint', t => {
  const db = new DatabaseSync(':memory:')
  t.after(() => db.close())
  const opts = { id: 'a', voters: VOTERS, db, send: async () => { throw new Error('No network') }, apply: () => {} }
  for (const voters of [[], ['a'], ['a', 'b'], ['a', 'a', 'c'], ['a', 'b', 'c', 'd'], ['a', '', 'c']]) {
    assert.throws(() => new QuorumLog({ ...opts, voters }), /exactly three/)
  }
  assert.throws(() => new QuorumLog({ ...opts, id: 'outsider' }), /exactly three/)
  const first = new QuorumLog(opts)
  first.stop()
  const reordered = new QuorumLog({ ...opts, voters: ['c', 'a', 'b'] })
  reordered.stop()
  assert.throws(() => new QuorumLog({ ...opts, voters: ['a', 'b', 'd'] }), /membership changed/)
  assert.throws(() => new QuorumLog({ ...opts, id: 'b' }), /membership changed/)
  assert.throws(() => new QuorumLog({ ...opts, heartbeatMs: 600, electionMinMs: 600 }), /heartbeat/)
  assert.throws(() => new QuorumLog({ ...opts, requestTimeoutMs: 0 }), /timer/)
})

test('leader readiness waits for a majority-persisted no-op and writes need two of three, not all live nodes', async t => {
  const cluster = new Cluster(t)
  cluster.intercept = context => {
    if (context.request.type === 'append') throw new Error('Append blocked before readiness')
    return context.deliver()
  }
  const leader = cluster.node('a')
  await leader.campaign()
  assert.equal(leader.isLeader, true)
  assert.equal(leader.ready, false)
  assert.equal(leader.commitIndex, 0)
  assert.equal(leader.lastIndex, 1)
  await assert.rejects(leader.propose({ key: 'too-early' }), /not ready/)
  cluster.intercept = undefined
  await until(() => leader.ready, 'no-op committed')
  assert.deepEqual(cluster.commands('a'), [], 'no-op must not call apply')
  cluster.node('c').stop()
  const command = { key: 'majority' }
  await leader.propose(command)
  assert.deepEqual(cluster.commands('a'), [command], 'ack includes local apply')
  assert.ok(cluster.log('b').some(row => row.command === JSON.stringify(command)), 'ack has a second durable copy')
  await until(() => cluster.commands('b').length === 1, 'follower commit notification')
  assert.equal(leader.commitIndex, 2)
  assert.equal(cluster.state('a').applied_index, 2)
})

test('an isolated former leader cannot acknowledge and proposal waits are bounded', async t => {
  const cluster = new Cluster(t, { requestTimeoutMs: 100 })
  const leader = await cluster.elect('a')
  const committed = leader.commitIndex
  cluster.partition([['a'], ['b', 'c']])
  const started = performance.now()
  await assert.rejects(leader.propose({ key: 'minority' }), /timed out/)
  assert.ok(performance.now() - started < 1500)
  assert.equal(leader.commitIndex, committed)
  assert.equal(leader.lastIndex, committed + 1, 'uncertain proposals are not deleted locally')
  assert.deepEqual(cluster.commands('a'), [])
  assert.equal(cluster.state('a').commit_index, committed)
  assert.equal(leader.isLeader, false, 'proposal timeout must demote the leader')
  assert.equal(leader.ready, false)
  await assert.rejects(leader.propose({ key: 'unsafe-next-write' }), /not ready/)
})

test('proposal timeout rejects all pending writes and gates new commands until inherited entries apply with a fresh no-op', async t => {
  const cluster = new Cluster(t, { requestTimeoutMs: 100 })
  const leader = await cluster.elect('a')
  const oldTerm = leader.term
  cluster.partition([['a'], ['b', 'c']])
  const first = assert.rejects(leader.propose({ key: 'uncertain-first' }), /timed out/)
  const second = assert.rejects(leader.propose({ key: 'uncertain-second' }), /leadership lost/)
  await Promise.all([first, second])
  assert.equal(leader.isLeader, false)
  assert.equal(leader.ready, false)
  assert.equal(leader.leaderId, undefined)
  assert.equal(leader.term, oldTerm, 'demotion does not invent an election or erase the vote')
  assert.equal(cluster.state('a').voted_for, 'a')
  const uncommittedLast = leader.lastIndex
  await assert.rejects(leader.propose({ key: 'must-not-materialize-again' }), /not ready/)
  assert.equal(leader.lastIndex, uncommittedLast)
  assert.deepEqual(cluster.commands('a'), [])
  cluster.heal()
  await cluster.elect('a')
  assert.ok(leader.term > oldTerm)
  assert.deepEqual(cluster.commands('a'), [{ key: 'uncertain-first' }, { key: 'uncertain-second' }],
    'readiness must wait until uncertain commands have changed committed state')
  assert.equal(leader.commitIndex, uncommittedLast + 1)
  assert.equal(cluster.state('a').applied_index, leader.commitIndex)
  assert.equal(cluster.log('a').at(-1).command, 'null')
  await leader.propose({ key: 'safe-after-catch-up' })
  assert.equal(cluster.commands('a').at(-1).key, 'safe-after-catch-up')
})

test('majority elects a successor, preserves acknowledged writes, and replaces only the stale uncommitted suffix', async t => {
  const cluster = new Cluster(t, { requestTimeoutMs: 1000 })
  const old = await cluster.elect('a')
  await old.propose({ key: 'acknowledged' })
  await until(() => cluster.commands('b').length === 1, 'acknowledged entry applied on b')
  const acknowledgedIndex = old.commitIndex
  cluster.partition([['a'], ['b', 'c']])
  const rejected = assert.rejects(old.propose({ key: 'orphan' }), /leadership lost|timed out/)
  const next = await cluster.elect('b')
  assert.ok(next.term > old.term)
  assert.deepEqual(cluster.commands('b'), [{ key: 'acknowledged' }])
  await next.propose({ key: 'successor' })
  cluster.heal()
  await until(() => !old.isLeader && old.commitIndex === next.commitIndex &&
    cluster.commands('a').length === 2, 'old leader catches up')
  await rejected
  assert.equal(old.leaderId, 'b')
  assert.equal(old.ready, false)
  assert.deepEqual(cluster.commands('a'), [{ key: 'acknowledged' }, { key: 'successor' }])
  assert.deepEqual(cluster.log('a'), cluster.log('b'))
  assert.equal(cluster.log('a').find(row => row.idx === acknowledgedIndex).command, '{"key":"acknowledged"}')
})

test('one crashed leader cannot lose an acknowledged entry and stale logs cannot win', async t => {
  const cluster = new Cluster(t, { files: true })
  cluster.partition([['a', 'b'], ['c']])
  const leader = await cluster.elect('a')
  await leader.propose({ key: 'survives-one-crash' })
  assert.equal(cluster.node('c').lastIndex, 0)
  leader.stop()
  cluster.create('b', true)
  cluster.partition([['a'], ['b', 'c']])
  await cluster.node('c').campaign()
  assert.equal(cluster.node('c').isLeader, false, 'b must deny a candidate with a stale log')
  const successor = await cluster.elect('b')
  await until(() => cluster.commands('c').length === 1, 'surviving majority restores acknowledged entry')
  assert.deepEqual(cluster.commands('b'), [{ key: 'survives-one-crash' }])
  assert.deepEqual(cluster.commands('c'), cluster.commands('b'))
  assert.equal(successor.ready, true)
})

test('interrupted old-term majority entry commits only after the up-to-date successor commits its no-op', async t => {
  const cluster = new Cluster(t, { requestTimeoutMs: 1000 })
  cluster.partition([['a', 'b'], ['c']])
  const first = await cluster.elect('a')
  const inherited = { key: 'inherited-unacknowledged' }
  const saved = deferred()
  cluster.intercept = async context => {
    const response = await context.deliver()
    if (context.from === 'a' && context.to === 'b' && context.request.type === 'append' &&
        context.request.entries.some(entry => entry.command?.key === inherited.key)) {
      saved.resolve()
      return new Promise(() => {})
    }
    return response
  }
  const interrupted = assert.rejects(first.propose(inherited), /stopped/)
  await saved.promise
  const oldIndex = first.lastIndex
  assert.equal(cluster.node('b').lastIndex, oldIndex)
  assert.ok(cluster.node('b').commitIndex < oldIndex)
  assert.deepEqual(cluster.commands('b'), [])
  first.stop()
  await interrupted
  cluster.partition([['a'], ['b', 'c']])
  const allowNoop = deferred()
  cluster.intercept = async context => {
    if (context.from === 'b' && context.to === 'c' && context.request.type === 'append') await allowNoop.promise
    return context.deliver()
  }
  const successor = cluster.node('b')
  await successor.campaign()
  assert.equal(successor.isLeader, true)
  assert.equal(successor.ready, false)
  assert.ok(successor.commitIndex < oldIndex, 'old-term copies alone do not advance commit')
  assert.deepEqual(cluster.commands('b'), [])
  allowNoop.resolve()
  await until(() => successor.ready && cluster.commands('c').length === 1, 'new-term no-op commits inherited prefix')
  assert.deepEqual(cluster.commands('b'), [inherited])
  assert.deepEqual(cluster.commands('c'), [inherited])
  assert.ok(successor.commitIndex > oldIndex)
  assert.equal(cluster.log('b').at(-1).command, 'null')
  assert.equal(cluster.log('b').at(-1).term, successor.term)
})

test('file-backed restart retains term, vote, log, commit, applied state, and never double-applies', async t => {
  const cluster = new Cluster(t, { files: true })
  cluster.node('a').stop()
  cluster.node('c').stop()
  let node = cluster.node('b')
  assert.equal((await node.handle('c', vote('c', 7))).voteGranted, true)
  await node.handle('c', append('c', 7, [
    { index: 1, term: 7, command: null },
    { index: 2, term: 7, command: { key: 'disk' } }
  ], { leaderCommit: 2 }))
  const before = cluster.log('b')
  node = cluster.create('b', true)
  assert.equal(node.term, 7)
  assert.equal(node.commitIndex, 2)
  assert.equal(node.lastIndex, 2)
  assert.equal(node.isLeader, false)
  assert.equal(node.ready, false)
  assert.equal(node.leaderId, undefined)
  assert.equal(cluster.state('b').applied_index, 2)
  assert.equal(cluster.state('b').voted_for, 'c')
  assert.deepEqual(cluster.log('b'), before)
  assert.deepEqual(cluster.commands('b'), [{ key: 'disk' }])
  assert.equal((await node.handle('a', vote('a', 7, 2, 7))).voteGranted, false)
  assert.equal((await node.handle('a', vote('a', 8))).voteGranted, false, 'higher term does not excuse a stale log')
  node = cluster.create('b', true)
  assert.equal(node.term, 8)
  assert.equal(cluster.state('b').voted_for, null)
  assert.equal((await node.handle('a', vote('a', 8, 2, 7))).voteGranted, true)
  node = cluster.create('b', true)
  assert.equal((await node.handle('c', vote('c', 8, 2, 7))).voteGranted, false, 'vote survives reopening')
})

test('nonmembers, spoofed names, and malformed RPCs are rejected before changing durable term or vote', async t => {
  const cluster = new Cluster(t)
  const node = cluster.node('a')
  await assert.rejects(node.handle('outsider', vote('outsider', 100)), /Nonmember/)
  await assert.rejects(node.handle('b', vote('c', 100)), /identity mismatch/)
  await assert.rejects(node.handle('b', append('c', 100, [])), /identity mismatch/)
  await assert.rejects(node.handle('b', append('b', 100, [
    { index: 2, term: 100, command: null }
  ])), /append entries/)
  await assert.rejects(node.handle('b', append('b', 100, [
    { index: 1, term: 100, command: undefined }
  ])), /JSON-serializable/)
  await assert.rejects(node.handle('b', { type: 'unknown', term: 100 }), /Unknown/)
  assert.equal(node.term, 0)
  assert.equal(cluster.state('a').term, 0)
  assert.equal(cluster.state('a').voted_for, null)
  assert.equal((await node.handle('b', vote('b', 1))).voteGranted, true)
  assert.equal((await node.handle('c', vote('c', 1))).voteGranted, false)
  assert.equal(cluster.state('a').voted_for, 'b')
})

test('committed entries cannot be overwritten by conflicting terms or commands and stale terms cannot append', async t => {
  const cluster = new Cluster(t)
  const node = cluster.node('b')
  await node.handle('a', append('a', 1, [
    { index: 1, term: 1, command: null },
    { index: 2, term: 1, command: { key: 'committed' } }
  ], { leaderCommit: 2 }))
  const original = cluster.log('b')
  const conflicting = await node.handle('c', append('c', 2, [
    { index: 2, term: 2, command: { key: 'different-term' } }
  ], { prevLogIndex: 1, prevLogTerm: 1, leaderCommit: 2 }))
  assert.equal(conflicting.success, false)
  const sameTerm = await node.handle('c', append('c', 2, [
    { index: 2, term: 1, command: { key: 'different-command' } }
  ], { prevLogIndex: 1, prevLogTerm: 1, leaderCommit: 2 }))
  assert.equal(sameTerm.success, false)
  assert.equal((await node.handle('a', append('a', 1, []))).success, false)
  assert.deepEqual(cluster.log('b'), original)
  assert.deepEqual(cluster.commands('b'), [{ key: 'committed' }])
  assert.equal(node.term, 2)
})

test('short and delayed appends never truncate a matching suffix or commit an unverified suffix', async t => {
  const cluster = new Cluster(t)
  const node = cluster.node('b')
  await node.handle('a', append('a', 1, [
    { index: 1, term: 1, command: null },
    { index: 2, term: 1, command: { key: 'uncommitted-old-suffix' } }
  ], { leaderCommit: 1 }))
  const response = await node.handle('c', append('c', 2, [], {
    prevLogIndex: 1, prevLogTerm: 1, leaderCommit: 2
  }))
  assert.equal(response.success, true)
  assert.equal(response.matchIndex, 1)
  assert.equal(node.lastIndex, 2, 'empty append cannot trim a suffix')
  assert.equal(node.commitIndex, 1, 'only a verified prefix may commit')
  assert.deepEqual(cluster.commands('b'), [])
  await node.handle('c', append('c', 2, [
    { index: 2, term: 2, command: { key: 'replacement' } }
  ], { prevLogIndex: 1, prevLogTerm: 1, leaderCommit: 2 }))
  assert.deepEqual(cluster.commands('b'), [{ key: 'replacement' }])
  await node.handle('c', append('c', 2, [], { prevLogIndex: 1, prevLogTerm: 1, leaderCommit: 1 }))
  assert.equal(node.lastIndex, 2)
  assert.equal(node.commitIndex, 2, 'delayed commit watermark cannot rewind commit')
})

test('apply failure rolls back state-machine writes and applied index, rejects proposals, and recovers before start', async t => {
  const cluster = new Cluster(t)
  const leader = await cluster.elect('a')
  cluster.afterApply = (id, command) => {
    if (id === 'a' && command.key === 'recover') throw new Error('Injected apply failure')
  }
  await assert.rejects(leader.propose({ key: 'recover' }), /Injected apply failure/)
  assert.equal(leader.isLeader, false)
  assert.equal(leader.ready, false)
  assert.equal(cluster.state('a').commit_index, 2)
  assert.equal(cluster.state('a').applied_index, 1)
  assert.deepEqual(cluster.commands('a'), [], 'SQL writes performed before throw must roll back')
  assert.throws(() => leader.start(), /Injected apply failure/)
  await assert.rejects(leader.propose({ key: 'later' }), /Injected apply failure/)
  cluster.afterApply = undefined
  const db = cluster.db('a')
  const recovered = new QuorumLog({
    id: 'a', voters: VOTERS, db,
    send: (peer, request) => cluster.send('a', peer, request),
    apply: (command, index, term) => {
      assert.equal(db.isTransaction, true)
      db.prepare('INSERT INTO applied_commands (idx, term, command) VALUES (?, ?, ?)')
        .run(index, term, JSON.stringify(command))
    },
    ...cluster.timers
  })
  cluster.nodes.set('a', recovered)
  assert.deepEqual(cluster.commands('a'), [{ key: 'recover' }], 'constructor recovers before start/timers')
  assert.equal(cluster.state('a').applied_index, 2)
  assert.equal(recovered.isLeader, false)
  recovered.start()
  cluster.create('a')
  assert.deepEqual(cluster.commands('a'), [{ key: 'recover' }], 'applied marker prevents replay')
})

test('follower never positively replies when apply fails; constructor recovery errors also surface', async t => {
  const cluster = new Cluster(t)
  cluster.afterApply = id => {
    if (id === 'b') throw new Error('Follower apply failure')
  }
  await assert.rejects(cluster.node('b').handle('a', append('a', 1, [
    { index: 1, term: 1, command: { key: 'retry-on-restart' } }
  ], { leaderCommit: 1 })), /Follower apply failure/)
  assert.equal(cluster.state('b').commit_index, 1)
  assert.equal(cluster.state('b').applied_index, 0)
  assert.deepEqual(cluster.commands('b'), [])
  assert.throws(() => cluster.create('b'), /Follower apply failure/)
  assert.equal(cluster.state('b').applied_index, 0)
  cluster.afterApply = undefined
  cluster.create('b')
  assert.deepEqual(cluster.commands('b'), [{ key: 'retry-on-restart' }])
})

test('asynchronous apply callbacks and nested BEGIN fail explicitly without committing partial application', async t => {
  for (const asynchronous of [false, true]) {
    const db = new DatabaseSync(':memory:')
    db.exec('CREATE TABLE application (value TEXT)')
    const write = () => db.prepare('INSERT INTO application (value) VALUES (?)').run('partial')
    const node = new QuorumLog({
      id: 'a', voters: VOTERS, db,
      send: async () => { throw new Error('No network') },
      apply: asynchronous
        ? async () => { write(); throw new Error('Async callbacks are forbidden') }
        : () => { write(); db.exec('BEGIN') }
    })
    t.after(() => { node.stop(); db.close() })
    node.start()
    await assert.rejects(node.handle('b', append('b', 1, [
      { index: 1, term: 1, command: { key: 'bad-apply-contract' } }
    ], { leaderCommit: 1 })), asynchronous ? /must be synchronous/ : /within a transaction/)
    assert.equal(db.prepare('SELECT COUNT(*) AS count FROM application').get().count, 0)
    const state = db.prepare('SELECT commit_index, applied_index FROM quorum_state').get()
    assert.equal(state.commit_index, 1)
    assert.equal(state.applied_index, 0)
    assert.equal(node.isLeader, false)
  }
})

test('concurrent proposals use one replication worker per peer, catch up multi-batch logs, and snapshot commands', async t => {
  const cluster = new Cluster(t, { requestTimeoutMs: 1500 })
  const leader = await cluster.elect('a')
  cluster.intercept = async context => {
    if (context.request.type === 'append') await sleep(3)
    return context.deliver()
  }
  const command = { key: 'original' }
  const first = leader.propose(command)
  command.key = 'mutated-by-caller'
  const others = Array.from({ length: 270 }, (_, n) => leader.propose({ key: `entry-${n}` }))
  await Promise.all([first, ...others])
  await until(() => VOTERS.every(id => cluster.commands(id).length === 271), 'all batches applied')
  assert.equal(leader.commitIndex, 272)
  assert.deepEqual(cluster.commands('a')[0], { key: 'original' })
  assert.deepEqual(cluster.log('a'), cluster.log('b'))
  assert.deepEqual(cluster.log('a'), cluster.log('c'))
  for (const count of cluster.maxActive.values()) assert.equal(count, 1, 'append RPCs are serialized per peer')
})

test('per-peer workers survive leadership generations and discard delayed old-term replication replies', async t => {
  const cluster = new Cluster(t, { requestTimeoutMs: 1500 })
  const leader = await cluster.elect('a')
  const received = deferred()
  const release = deferred()
  let held = false
  cluster.intercept = async context => {
    const response = await context.deliver()
    if (!held && context.from === 'a' && context.to === 'b' && context.request.type === 'append' &&
        context.request.entries.some(entry => entry.command?.key === 'held')) {
      held = true
      received.resolve()
      await release.promise
    }
    return response
  }
  const pending = assert.rejects(leader.propose({ key: 'held' }), /leadership lost/)
  await received.promise
  const oldTerm = leader.term
  await leader.handle('c', vote('c', oldTerm + 1, leader.lastIndex, oldTerm))
  await pending
  await leader.campaign()
  await until(() => leader.ready, 're-elected leader uses the other peer')
  assert.ok(leader.term > oldTerm)
  assert.equal(cluster.active.get('a:b'), 1, 'old RPC retains ownership of its stable slot')
  release.resolve()
  await until(() => cluster.node('b').commitIndex === leader.commitIndex, 'delayed peer catches up')
  assert.deepEqual(cluster.log('a'), cluster.log('b'))
  assert.deepEqual(cluster.commands('b'), [{ key: 'held' }], 'a rejected uncertain proposal may later commit')
  assert.equal(cluster.maxActive.get('a:b'), 1, 'new leadership cannot overlap old peer replication')
})

test('SQLite persistence failures prevent positive vote and append replies and halt the instance', async t => {
  const cluster = new Cluster(t)
  const db = cluster.db('a')
  db.exec(`
    CREATE TRIGGER reject_vote BEFORE UPDATE OF voted_for ON quorum_state
    WHEN NEW.voted_for IS NOT NULL
    BEGIN SELECT RAISE(ABORT, 'Injected vote persistence failure'); END;
  `)
  await assert.rejects(cluster.node('a').handle('b', vote('b', 1)), /Injected vote persistence failure/)
  assert.equal(cluster.state('a').voted_for, null)
  assert.throws(() => cluster.node('a').start(), /Injected vote persistence failure/)
  db.exec('DROP TRIGGER reject_vote')
  let node = cluster.create('a')
  assert.equal((await node.handle('b', vote('b', 1))).voteGranted, true)
  assert.equal(cluster.state('a').voted_for, 'b')
  db.exec(`
    CREATE TRIGGER reject_log BEFORE INSERT ON quorum_log
    BEGIN SELECT RAISE(ABORT, 'Injected log persistence failure'); END;
  `)
  const request = append('b', 1, [{ index: 1, term: 1, command: { key: 'persist-first' } }], { leaderCommit: 1 })
  await assert.rejects(node.handle('b', request), /Injected log persistence failure/)
  assert.equal(node.lastIndex, 0)
  assert.equal(cluster.state('a').commit_index, 0)
  assert.deepEqual(cluster.log('a'), [])
  assert.deepEqual(cluster.commands('a'), [])
  db.exec('DROP TRIGGER reject_log')
  node = cluster.create('a')
  assert.equal((await node.handle('b', request)).success, true)
  assert.equal(cluster.log('a').length, 1)
  assert.deepEqual(cluster.commands('a'), [{ key: 'persist-first' }])
})

test('hung transport calls cannot hang elections or proposals; stop rejects pending work immediately', async t => {
  const cluster = new Cluster(t, { requestTimeoutMs: 80 })
  cluster.intercept = () => new Promise(() => {})
  const node = cluster.node('a')
  const before = performance.now()
  await node.campaign()
  assert.ok(performance.now() - before < 1500)
  assert.equal(node.isLeader, false)
  assert.equal(node.term, 1)
  cluster.intercept = undefined
  const leader = await cluster.elect('a')
  cluster.intercept = () => new Promise(() => {})
  await assert.rejects(leader.propose({ key: 'hung' }), /timed out/)
  assert.equal(leader.isLeader, false)
  cluster.intercept = undefined
  await cluster.elect('a')
  assert.deepEqual(cluster.commands('a'), [{ key: 'hung' }], 'timed-out command can commit after re-election')
  cluster.intercept = () => new Promise(() => {})
  const rejected = assert.rejects(leader.propose({ key: 'stopping' }), /stopped/)
  leader.stop()
  await rejected
  await assert.rejects(leader.propose({ key: 'after-stop' }), /not started/)
  assert.equal(leader.isLeader, false)
  assert.equal(leader.ready, false)
})

test('an authenticated higher-term reply immediately rejects pending writes and demotes a leader', async t => {
  const cluster = new Cluster(t, { requestTimeoutMs: 1000 })
  const leader = await cluster.elect('a')
  cluster.intercept = context => {
    if (context.from === 'a' && context.request.type === 'append') {
      return { type: 'append', term: leader.term + 10, success: false, matchIndex: 0, conflictIndex: 1 }
    }
    return context.deliver()
  }
  await assert.rejects(leader.propose({ key: 'lost-leadership' }), /leadership lost/)
  assert.equal(leader.isLeader, false)
  assert.equal(leader.ready, false)
  assert.equal(cluster.state('a').term, leader.term)
  assert.equal(cluster.state('a').voted_for, null)
})

test('malformed vote responses do not halt local consensus and repeated peer failures warn only once', async t => {
  const cluster = new Cluster(t)
  const output = t.mock.method(console, 'log', () => {})
  const node = cluster.node('a')
  for (const response of [
    { error: 'Unknown metadata voter before first announce' },
    { type: 'vote', term: 1, voteGranted: 'yes' },
    { type: 'vote', term: Number.NaN, voteGranted: true },
    { type: 'append', term: 4, success: true, matchIndex: 0 }
  ]) {
    cluster.intercept = () => response
    await node.campaign()
    assert.equal(node.isLeader, false)
    assert.doesNotThrow(() => node.start(), 'malformed remote input must not make the instance fatal')
  }
  const warnings = output.mock.calls.filter(call =>
    String(call.arguments[0]).includes('[WARN]') && String(call.arguments[0]).includes('Quorum RPC failed'))
  assert.equal(warnings.length, 2, 'only the first failure per peer warns until a valid response')
  cluster.intercept = undefined
  await cluster.elect('a')
  await node.propose({ key: 'valid-after-discovery' })
  assert.deepEqual(cluster.commands('a'), [{ key: 'valid-after-discovery' }])
})

test('generic discovery errors during append are unavailable peers, not local failures or fabricated acknowledgments', async t => {
  const cluster = new Cluster(t, { requestTimeoutMs: 100 })
  const leader = await cluster.elect('a')
  cluster.intercept = context => {
    if (context.from === 'a' && context.to === 'b' && context.request.type === 'append') {
      return { error: 'Unknown metadata voter before first announce' }
    }
    return context.deliver()
  }
  await leader.propose({ key: 'healthy-majority' })
  assert.equal(leader.ready, true, 'one malformed peer cannot halt the other majority')
  assert.ok(cluster.log('c').some(row => row.command === '{"key":"healthy-majority"}'))
  const committed = leader.commitIndex
  cluster.intercept = context => {
    if (context.from === 'a' && context.request.type === 'append') return { error: 'Discovery unavailable' }
    return context.deliver()
  }
  await assert.rejects(leader.propose({ key: 'not-an-ack' }), /timed out/)
  assert.equal(leader.commitIndex, committed)
  assert.deepEqual(cluster.commands('a'), [{ key: 'healthy-majority' }])
  assert.doesNotThrow(() => leader.start(), 'timeout demotes, but malformed replies cannot permanently halt')
  cluster.intercept = undefined
  await cluster.elect('a')
  assert.deepEqual(cluster.commands('a'), [{ key: 'healthy-majority' }, { key: 'not-an-ack' }])
  await leader.propose({ key: 'recovered' })
  assert.equal(cluster.commands('a').at(-1).key, 'recovered')
})

test('incorrect successful matchIndex replies cannot advance commit or permanently halt a node', async t => {
  const cluster = new Cluster(t, { requestTimeoutMs: 100 })
  const leader = await cluster.elect('a')
  const committed = leader.commitIndex
  cluster.partition([['a', 'b'], ['c']])
  cluster.intercept = async context => {
    const response = await context.deliver()
    if (context.from === 'a' && context.to === 'b' && context.request.type === 'append' && response.success) {
      return { ...response, matchIndex: response.matchIndex + 1 }
    }
    return response
  }
  await assert.rejects(leader.propose({ key: 'wrong-match-index' }), /timed out/)
  assert.equal(leader.commitIndex, committed)
  assert.deepEqual(cluster.commands('a'), [])
  assert.doesNotThrow(() => leader.start())
  cluster.intercept = undefined
  cluster.heal()
  await cluster.elect('a')
  assert.deepEqual(cluster.commands('a'), [{ key: 'wrong-match-index' }])
})

test('invalid commands fail explicitly without poisoning a healthy leader', async t => {
  const cluster = new Cluster(t)
  const leader = await cluster.elect('a')
  const before = leader.lastIndex
  const circular = {}
  circular.self = circular
  await assert.rejects(leader.propose(null), /Null is reserved/)
  await assert.rejects(leader.propose(undefined), /JSON-serializable/)
  await assert.rejects(leader.propose(circular), /circular/i)
  assert.equal(leader.lastIndex, before)
  assert.equal(leader.ready, true)
  await leader.propose({ key: 'still-healthy' })
  assert.deepEqual(cluster.commands('a'), [{ key: 'still-healthy' }])
})

test('automatic randomized elections and heartbeats elect a ready leader without manual campaign', async t => {
  const cluster = new Cluster(t, {
    electionMinMs: 60, electionMaxMs: 140, heartbeatMs: 10, requestTimeoutMs: 100
  })
  await until(() => [...cluster.nodes.values()].some(node => node.ready), 'automatic leader')
  const leaders = [...cluster.nodes.values()].filter(node => node.ready)
  assert.equal(leaders.length, 1)
  await leaders[0].propose({ key: 'automatic' })
  await until(() => VOTERS.every(id => cluster.commands(id).length === 1), 'automatic follower commit')
  assert.deepEqual(cluster.commands('a'), [{ key: 'automatic' }])
})
