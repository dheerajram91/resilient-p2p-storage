# Detailed Design & Engineering Plan

> Companion to [`IMPLEMENTATION_PLAN.md`](./IMPLEMENTATION_PLAN.md). That document is the
> architecture + decisions; **this** document is the low-level design you review before we build
> each phase: data schemas, wire protocols, algorithms, module interfaces, and phase-by-phase
> task lists with acceptance criteria.
>
> Confirmed context (from 2026-07-24): global namespace + per-folder ACLs; trusted PCs;
> WebDAV client; no public IPs (mesh-VPN or LAN); Node.js/TypeScript + js-libp2p; replication
> factor 2; 1–2 always-available anchor nodes.

---

## Implemented durability protocol (supersedes historical async acknowledgment/election below)

`METADATA_VOTERS` enables a fixed three-voter replicated log in `src/replication/quorum.ts`.
The original primary push/pull and gossip-ranked election remain legacy-only.
There is no online membership change or automatic legacy-state migration.

The consensus log durably stores terms, votes, and commands before responding. An elected leader
commits a current-term no-op before accepting mutations. Each namespace mutation is materialized
against committed metadata under a rollback savepoint, then proposed as one command containing its
ordered operations. Mutations are serialized; a complete file registration/overwrite is atomic.
Committed command application and applied-progress persistence share one SQLite transaction.
Obsolete leaders cannot acknowledge writes without a majority. Unknown voters cannot vote or append.

Upload ordering is: verify/flush local chunks, verify/flush chunks on a distinct backup, commit
locations + manifest + file through the metadata majority, wait for the receiving replica's apply,
then acknowledge. Failed backup placement does not replace a previously committed file. Failed or
timed-out commits may have uncertain outcomes; callers must read before retrying.

Uploads durably pin both copies during publication using synchronized sidecars under the content
root's `.upload-pins` directory. Pins are restored before the store becomes ready. Relocation uses
expected-location checks and local generation-conditional deletion to avoid deleting a newly
uploaded reference. `EUNAVAILABLE` and thrown/ambiguous commit outcomes retain both pins.
Successful remote release waits for the committed metadata LSN to apply locally. Abandoned pins
survive receiver restarts indefinitely; there is no TTL or automatic reclamation. Manual recovery
must quiesce uploads and resolve metadata outcomes before removing protection.

SQLite uses WAL and `synchronous=FULL`. Chunks use file synchronization before atomic rename and POSIX
directory synchronization; Windows process-crash recovery is supported but power-loss persistence
of directory entries is not certified. Data and identity directories must be persisted. Reads are
not linearizable quorum reads. No log compaction or coordinated chunk garbage collection is provided.
The acceptance target is acknowledged bytes surviving one subsequent node crash, with bounded
election/catch-up rather than uninterrupted availability. Hardware must honor flushes.

Run `npm run test:durability` for unit fault scenarios and a three-process bootstrap-only cluster
that crashes the Primary immediately after upload acknowledgment and exercises stale restart and
minority rejection. Physical power loss is not simulated.

## Table of Contents
1. [System model & invariants](#1-system-model--invariants)
2. [Identity, membership & networking](#2-identity-membership--networking)
3. [Metadata: schema & operations](#3-metadata-schema--operations)
4. [Content: chunking, storage & addressing](#4-content-chunking-storage--addressing)
5. [Location map & the RF=2 invariant](#5-location-map--the-rf2-invariant)
6. [Wire protocols (libp2p protocol IDs & messages)](#6-wire-protocols)
7. [Replication service (metadata sync state machine)](#7-replication-service)
8. [Availability service (failure detection, promotion, election)](#8-availability-service)
9. [Reconciler (restoring redundancy)](#9-reconciler)
10. [Split / rebalance service](#10-split--rebalance-service)
11. [Routing service](#11-routing-service)
12. [ACL model](#12-acl-model)
13. [Client surface: WebDAV mapping](#13-client-surface-webdav)
14. [MCP service](#14-mcp-service)
15. [Module / directory layout & interfaces](#15-module-layout--interfaces)
16. [Consistency & correctness guarantees](#16-consistency--correctness-guarantees)
17. [Phase-by-phase task breakdown & acceptance criteria](#17-phase-by-phase-task-breakdown)
18. [Open technical decisions to confirm](#18-open-technical-decisions)

---

## 1. System model & invariants

**Actors**
- **Node** — one process = one PC = one docker container. Runs all services. Has a stable
  `PeerID` (Ed25519). Contributes `quota` bytes (~20 GB) of content storage.
- **Anchor node** — a node flagged `role=anchor`; expected to be (nearly) always available.
  Preferred host for the **Primary** role and the **Snapshot Cache**.
- **Primary** — exactly one node at a time holds the authoritative metadata **log** and is the
  single writer for namespace changes. Elected; normally an anchor.
- **Client** — a user (via WebDAV) or an AI agent (via MCP).

**Core invariants** (enforced continuously; violations trigger recovery):
- **I1 — Metadata replica completeness:** every *healthy, serving* node holds a full metadata
  copy whose `(timestamp, sha256)` it can verify.
- **I2 — Single metadata writer:** only the Primary appends to the log.
- **I3 — Content redundancy:** every content chunk that is referenced by live metadata has
  **≥1 verified copy**, and the system drives toward **exactly `RF=2` copies on 2 distinct PCs**.
- **I4 — Non-co-location:** the active and backup copies of any chunk are never on the same node
  (and, when site info exists, preferably not the same site).
- **I5 — Verify-before-serve:** a node never serves metadata it cannot verify (I1) nor content
  whose hash doesn't match its ID.

**Failure budget (the user's requirement):** with `RF=2`, the loss of **any one node** loses no
data. After such a loss, affected chunks sit at 1 copy (degraded, accepted) until the Reconciler
restores 2 copies (once spare capacity / a new node exists).

**Time / ordering:** metadata uses a **monotonic logical timestamp** = the Primary's log
sequence number (`lsn`), not wall-clock. "Timestamp" everywhere below means this `lsn`.

---

## 2. Identity, membership & networking

### 2.1 Identity
- Ed25519 keypair generated on first boot, persisted to `/data/meta/peer.key`. `PeerID` derived
  from it — stable across restarts and IP changes.
- Node record: `{ peerId, name, role, quotaBytes, usedBytes, addrs[], lastSeenLsn, health }`.

### 2.2 Membership (trusted PCs)
- A **cluster PSK** (pre-shared key) is provided via `CLUSTER_PSK`. libp2p uses it as a private
  network pre-shared key (`/key/swarm/psk/1.0.0/`), so only nodes with the PSK can even complete
  a handshake. This is sufficient because PCs are trusted.
- Membership list is **soft state** learned via discovery + gossip; there is no static roster to
  maintain. Anchor multiaddrs (or the VPN) provide the bootstrap.

### 2.3 Networking modes (from IMPLEMENTATION_PLAN §0.1)
- **LAN:** mDNS discovery. Zero configuration.
- **Internet, no public IP:** run all nodes on a **mesh VPN** (Tailscale/WireGuard/Nebula); the
  agent then treats overlay IPs as directly dialable and discovers via mDNS-over-VPN or a static
  bootstrap list of anchor overlay addresses.
- Transport is always Noise-encrypted regardless of mode.

### 2.4 Health signal
- Two layers: **connection-level** (`peer:disconnect` from libp2p) and **application-level**
  (periodic `ping` RTT + a lightweight `health` gossip carrying `{peerId, usedBytes, lastLsn}`).
- A node is declared **DOWN** after `missedHeartbeats × interval` with no successful ping
  (default: 3 × 5s = 15s). Declared **SUSPECT** after 1 miss.

---

## 3. Metadata: schema & operations

Stored in an embedded DB per node. Recommended: **SQLite (better-sqlite3)** for relational
queries + WAL; alternative BadgerDB/LMDB KV. Schema (SQLite dialect shown):

```sql
-- Folder tree (root has parent_id = NULL, id = 0)
CREATE TABLE folders (
  id          INTEGER PRIMARY KEY,
  parent_id   INTEGER,                 -- FK folders.id
  name        TEXT NOT NULL,
  acl_id      INTEGER NOT NULL,
  ctime       INTEGER NOT NULL,        -- wall-clock ms (informational)
  mtime       INTEGER NOT NULL,
  UNIQUE(parent_id, name)
);

-- Files (metadata only; bytes live in content store)
CREATE TABLE files (
  id           INTEGER PRIMARY KEY,
  folder_id    INTEGER NOT NULL,       -- FK folders.id
  name         TEXT NOT NULL,
  ext          TEXT,
  size         INTEGER NOT NULL,
  content_type TEXT,
  manifest_id  INTEGER,                -- FK manifests.id (current version)
  acl_id       INTEGER,               -- NULL = inherit from folder
  ctime        INTEGER NOT NULL,
  mtime        INTEGER NOT NULL,
  UNIQUE(folder_id, name)
);

-- Ordered chunk list for one file version
CREATE TABLE manifests (
  id          INTEGER PRIMARY KEY,
  file_id     INTEGER NOT NULL,
  total_size  INTEGER NOT NULL,
  chunk_hashes TEXT NOT NULL           -- JSON array of chunk hash strings, in order
);

-- Where each chunk's copies live (see §5)
CREATE TABLE locations (
  chunk_hash  TEXT PRIMARY KEY,        -- content-addressed id
  size        INTEGER NOT NULL,
  active_peer TEXT NOT NULL,           -- PeerID string
  backup_peer TEXT,                    -- PeerID string; NULL = degraded (1 copy)
  state       TEXT NOT NULL,           -- 'ok' | 'degraded' | 'replicating'
  refcount    INTEGER NOT NULL DEFAULT 1
);

-- Access control lists
CREATE TABLE acls (
  id    INTEGER PRIMARY KEY,
  rules TEXT NOT NULL                  -- JSON: [{principal, rights}]  rights ⊆ {read,write,admin}
);

-- Cluster membership (soft state, refreshed by health gossip)
CREATE TABLE nodes (
  peer_id     TEXT PRIMARY KEY,
  name        TEXT,
  role        TEXT,                    -- 'anchor' | 'ordinary'
  quota_bytes INTEGER,
  used_bytes  INTEGER,
  site        TEXT,                    -- optional locality hint for non-co-location
  health      TEXT,                    -- 'up' | 'suspect' | 'down'
  last_lsn    INTEGER
);

-- The authoritative incremental log (AUTHORITATIVE ONLY ON PRIMARY)
CREATE TABLE meta_log (
  lsn      INTEGER PRIMARY KEY,        -- monotonically increasing
  op       TEXT NOT NULL,              -- see op list below
  payload  TEXT NOT NULL,              -- JSON args
  ts_ms    INTEGER NOT NULL
);

-- Local sync state (per node)
CREATE TABLE sync_state (
  k TEXT PRIMARY KEY,                  -- 'lsn' | 'snapshot_lsn' | 'meta_sha256' | 'mode'
  v TEXT NOT NULL
);
```

### 3.1 Namespace operations (the only ops that replicate)
Every op is appended to `meta_log` on the Primary and shipped to peers:

| op | payload | effect |
|----|---------|--------|
| `folder.create` | `{id, parentId, name, aclId}` | insert folder |
| `folder.rename` | `{id, name}` | rename |
| `folder.move` | `{id, newParentId}` | reparent |
| `folder.delete` | `{id}` | delete (must be empty) |
| `file.create` | `{id, folderId, name, ext, size, contentType, manifestId, aclId}` | insert file + manifest |
| `file.update` | `{id, size, manifestId}` | new version (new manifest) |
| `file.rename` | `{id, name}` | rename |
| `file.move` | `{id, newFolderId}` | reparent |
| `file.delete` | `{id}` | delete file + decref chunks |
| `acl.set` | `{aclId, rules}` | upsert ACL |
| `location.set` | `{chunkHash, size, active, backup, state}` | update content-location map |
| `location.promote` | `{chunkHash, newActive, newBackup, state}` | after failure/reconcile |
| `node.upsert` | `{peerId, ...}` | membership change |

> **Note:** `location.*` ops are metadata (they change where content lives) and therefore
> replicate. Writing chunk **bytes** does not — matching the proposal's "selective metadata
> updates."

### 3.2 Metadata hash
`meta_sha256 = SHA256( canonical_serialization(folders, files, manifests, locations, acls) )`,
recomputed after each applied batch. Canonical = deterministic ordering (by primary key) so all
nodes compute identical bytes. Used for the push-sync integrity check.

---

## 4. Content: chunking, storage & addressing

- **Chunking:** fixed 4 MiB chunks for v1 (simple; predictable). Optional upgrade later:
  content-defined chunking (FastCDC) for better dedup on edits.
- **Hash:** SHA-256 (v1; already needed for metadata). Chunk **id = hex(sha256(bytes))**.
  (BLAKE3 is a perf upgrade candidate.)
- **On-disk layout:** `/data/content/<hash[0:2]>/<hash[2:4]>/<hash>` — 2-level fan-out.
- **Manifest:** ordered list of chunk hashes; file bytes = concatenation. Dedup is automatic
  (identical chunks share one file + one `locations` row, `refcount` tracks references).
- **Integrity:** every read verifies `sha256(bytes) == id`; mismatch ⇒ discard + read-repair from
  the other copy.
- **Quota accounting:** `used_bytes = Σ size of locally stored chunks`. Writes are admitted only
  if the cluster can hold `RF` copies (admission control, §10).

---

## 5. Location map & the RF=2 invariant

Each `locations` row is a small state machine:

```
                 write completes to active
   (none) ─────────────────────────────────► [degraded]  (1 copy, backup_peer = NULL)
                                                   │
                        reconciler copies to backup│ (state='replicating' during copy)
                                                   ▼
                                                 [ok]      (2 copies, non-co-located)
                                                   │
             active peer goes DOWN                 │  backup peer goes DOWN
        ┌──────────────────────────────────────────┴───────────────────────────┐
        ▼                                                                        ▼
  promote backup→active                                              stays [degraded]
  backup=NULL, state='degraded' ──────────► reconciler restores ──► [ok]
```

**Placement rules** (choosing where the backup goes):
1. Candidate = healthy node, `used + size ≤ quota`, `peerId ≠ active`, **different `site`** if
   sites are known (I4).
2. Prefer the **least-utilized** candidate (`used/quota`).
3. Prefer a node already connected (lower dial cost); prefer an anchor if nothing else fits.
4. If no candidate exists → row stays `degraded`; Reconciler retries when capacity appears.

**Content write path (`WRITE_ACK=active`, the default — eager async backup):**
```
on PUT(fileBytes):                       # already ACL-checked & routed
    for each chunk in chunker(fileBytes):
        activeStore.put(chunk.hash, chunk.bytes)          # local, verified
        locations[chunk.hash] = {active:self, backup:NULL, state:'degraded'}
        emit location.set(...)                            # via Primary → replicates map
    ack client                                            # <-- 1-copy window opens
    for each chunk:                                       # eager, non-blocking
        dest = choosePlacement(chunk.size, exclude=[self])    # §5 rules
        spawn async:
            if dest != NULL and contentPut(dest, chunk.hash, chunk.bytes):
                emit location.set {backup:dest, state:'ok'}   # <-- window closes
            # else: leave 'degraded'; reconciler (§9) will retry  ← backstop

# WRITE_ACK=both: run the eager block BEFORE "ack client"; fail PUT if dest==NULL.
```

---

## 6. Wire protocols

All are libp2p stream protocols (length-prefixed JSON for v1; can switch to protobuf/CBOR later).

| Protocol ID | Purpose | Direction |
|-------------|---------|-----------|
| `/p2pstore/health/1` | gossip `{peerId, usedBytes, lastLsn, role}` | all ↔ all, periodic |
| `/p2pstore/meta-push/1` | Primary → peer: `{fromLsn, toLsn, ops[], sha256}` | Primary → peers |
| `/p2pstore/meta-pull/1` | peer → Primary: `{haveLsn}` → `{ops[], toLsn, sha256}` (batched) | peer → Primary |
| `/p2pstore/snapshot/1` | fetch latest snapshot blob `{lsn, bytes}` | peer → cache/anchor |
| `/p2pstore/content-put/1` | replicate a chunk: `{hash, size}` + raw bytes → `{ok}` | node → backup |
| `/p2pstore/content-get/1` | fetch a chunk: `{hash}` → raw bytes | any → holder |
| `/p2pstore/place/1` | ask a node to accept a backup: `{hash, size}` → `{accept|reject}` | node → candidate |
| `/p2pstore/election/1` | leader election messages (see §8.3) | all ↔ all |
| `/p2pstore/mcp/1` (later) | MCP JSON-RPC framing | client → node |

Message envelope: `{ v:1, type, id, body }` with a monotonic `id` for request/response matching.

---

## 7. Replication service

Implements the proposal's push/pull with SHA256 + lsn. A node is always in exactly one **mode**:
`PULL` (catching up) or `PUSH` (steady-state), stored in `sync_state.mode`.

### 7.1 Steady state (PUSH) — receiver logic
```
on meta-push {fromLsn, toLsn, ops, sha256}:
    if localLsn != fromLsn:            # gap
        goto FALLBACK
    applyInTxn(ops)                    # atomic
    if computeSha256() != sha256:      # corruption/divergence
        rollbackTxn(); goto FALLBACK
    commitTxn(); localLsn = toLsn
    # else: acknowledged implicitly by advancing
```
Primary pushes every `pushInterval` (or immediately after a write for low latency), to every
node registered for push.

### 7.2 Recovery (PULL)
```
PULL:
    stopServingMetadata()             # honor I5
    (snapLsn, blob) = fetchLatestSnapshot()
    loadState(blob); localLsn = snapLsn
    loop:
        resp = metaPull(haveLsn = localLsn)
        if resp.empty: break
        applyInTxn(resp.ops)
        if computeSha256() != resp.sha256: 
            # snapshot or log inconsistent — refetch snapshot, retry with backoff
            continue
        localLsn = resp.toLsn
    mode = PUSH; registerForPush(); resumeServingMetadata()
```

### 7.3 Fallback (PUSH → PULL)
Triggered by: lsn gap, sha256 mismatch, missed pushes (detected via health lastLsn drift),
network reconnect, or local corruption. Action: `mode=PULL`, discard suspect state, run §7.2.

### 7.4 Snapshot + log lifecycle (Primary)
```
every snapshotInterval:
    blob = serializeState()
    snapshotCache.put(lsn, blob)      # cache = anchor-hosted blob store
    meta_log.purge(where lsn < lsn - 2*snapshotInterval_in_lsn)   # keep ≥2 cycles
```
Snapshot cache = a simple content-addressed blob store on anchor node(s); a late node reads the
newest snapshot then pulls the delta.

### 7.5 Write path (Primary), e.g. `folder.create`
```
on client write (already ACL-checked & routed to Primary):
    txn:
        apply op to local tables
        lsn += 1; append (lsn, op, payload) to meta_log
        sha = computeSha256()
    commit
    push {fromLsn=lsn-1, toLsn=lsn, [op], sha} to all push-subscribers
    return ok to client
```

---

## 8. Availability service

### 8.1 Failure detection
- Maintains a health table from `ping` results + `/health` gossip.
- Transitions: `up → suspect` (1 miss) `→ down` (≥3 misses / 15s). `down → up` on successful
  reconnect + ping.

### 8.2 Promotion on node DOWN
```
on node D declared DOWN:
    routing.markUnhealthy(D)
    if D was Primary: trigger election (§8.3)
    for each locations row L where L.active_peer == D:
        if L.backup_peer != NULL and healthy(L.backup_peer):
            emit location.promote {chunk:L.hash, newActive:L.backup_peer, newBackup:NULL,
                                   state:'degraded'}   # goes through Primary → replicates
        else:
            mark L unrecoverable-for-now (should not happen under single-failure assumption)
    # rows where D was backup: just set backup=NULL, state='degraded'
    reconciler.wake()     # start restoring RF=2
```
Only the **Primary** actually appends `location.promote` (I2). A non-primary detecting the
failure sends a request to the Primary; if the Primary itself is the one down, election runs
first and the new Primary performs promotions.

### 8.3 Leader election (Primary permanently lost)
Small cluster ⇒ a simple, safe protocol:
```
trigger: Primary DOWN for > electionTimeout
candidates gossip: {peerId, lastLsn, meta_sha256, role}
winner = argmax over healthy nodes by:
         (1) highest lastLsn   (most up-to-date)
         (2) then role==anchor preferred
         (3) then lowest peerId (deterministic tiebreak)
winner must present a valid (lastLsn, meta_sha256) pair (self-verify before claiming).
on win:
    become Primary; reset snapshot subscriptions
    broadcast new-primary {peerId, lastLsn}
    all nodes: mode=PULL from new Primary (§7.2)
```
> **De-risk option (recommended, see §18):** back election + log ordering with **Raft**
> (`hashicorp/raft` has no JS peer, so in Node use `@libp2p` + a small Raft lib, or a lightweight
> lease/lock on the anchors). Given 1–2 always-on anchors, a simple **lease held by the
> lowest-lsn-valid anchor** may be enough for v1; full Raft can come later.

### 8.4 Proactive rotation (later, proposal §4.6)
Periodically promote a backup→active to keep it battle-tested; identical mechanism to §8.2 but
planned and graceful.

---

## 9. Reconciler

Background loop that drives I3/I4 back to `RF=2`.
```
every reconcileInterval (and on wake):
    degraded = SELECT * FROM locations WHERE state='degraded' ORDER BY size ASC
    for L in degraded (rate-limited, concurrency-capped):
        dest = choosePlacement(L.size, exclude=[L.active_peer], preferLeastUtilized) # §5 rules
        if dest == NULL: continue        # no capacity yet; try later
        set L.state='replicating' (local only, transient)
        ok = contentPut(dest, L.hash, bytesFrom(L.active_peer))
        if ok:
            emit location.set {chunk:L.hash, active:L.active_peer, backup:dest, state:'ok'}
        else:
            set L.state='degraded'       # retry next cycle
```
Guards: max N concurrent copies; prioritize smallest/most-referenced chunks first; back off on
repeated failure; stop when global free space can't hold another copy (admission control).

---

## 10. Split / rebalance service

Two-threshold hysteresis vs the storage budget, run on **every caught-up node** (`mode='push'`) against
its **own** usage:
```
T1 = REBALANCE_HIGH * quota   # start offloading (default 0.85)
T2 = REBALANCE_LOW  * quota   # stop  offloading (default 0.70)   T2 < T1 damps oscillation
every REBALANCE_INTERVAL_MS (and on demand via POST /admin/rebalance):
    if used <= T1: return
    for loc in heldChunksBySize(self):      # largest first → fewest moves
        if used <= T2: break
        fresh = getLocation(loc.hash)
        if fresh.state != 'ok': continue                    # RF work in flight → leave to reconciler
        other = amActive ? fresh.backup_peer : fresh.active_peer
        if not healthy(other): continue                     # moving ours could drop below RF
        dest = choosePlacement(size, exclude=[self, other]) # least-utilized healthy peer with room
        if dest == NULL: continue
        contentPut(dest, hash, verifiedLocalBytes)
        # update the map via the Primary BEFORE deleting locally — RF is never < 2:
        amActive ? relocateActive(hash, dest, fresh.backup_peer, 'ok')   # emits location.promote
                 : setBackup(hash, dest, 'ok')                           # emits location.backup
        deleteLocal(hash); used -= size
```
**RF is preserved at all times:** a chunk always has two holders, so we move only **one** copy and delete
our local copy only **after** the location map has committed. We never touch a chunk that isn't `ok`
(redundancy work already in flight) or whose other copy is on an unhealthy node.

**Admission control (cluster-wide):** a write is accepted only if this node has room for the whole active
copy **and** `Σ(quota - used) over healthy nodes ≥ RF * incomingSize` **and** ≥ RF distinct nodes have room.
Otherwise the write is rejected with `ENOSPC` (surfaced over HTTP as `507`, and to WebDAV as disk-full).
Opportunistic warm-caching on read is likewise capped at T2 so it never re-fills what rebalance just freed.

---

## 11. Routing service

A pure function over health + freshness, used by clients (via a thin SDK) and internally:
```
metadataRead():  candidates = healthy nodes with lastLsn == max(lastLsn)
                 return leastLoaded(candidates)
contentRead(h):  L = locations[h]
                 return healthy(L.active_peer) ? L.active_peer : L.backup_peer
metadataWrite(): return currentPrimary
contentWrite():  return active target per placement (§5)
```
For WebDAV (§13) the local node *is* the entry point; it applies these rules server-side and
proxies/redirects as needed.

---

## 12. ACL model ✅ (Phase 5 — done & verified)

- `acls.rules` = JSON array of `{ principal, rights }`, `rights ⊆ {read, write, admin}`.
- `principal` = user id / group id / `*` (everyone). Trusted-PC model: principals are cluster
  users, authenticated at the WebDAV/MCP layer (basic auth or token in v1).
- **Resolution:** a file's effective ACL = its own `acl_id` if set, else nearest ancestor
  folder's ACL (inheritance up the tree). Root has a default ACL.
- **Enforcement point:** every request is ACL-checked on the target (or parent, for creates)
  **before** routing — matching the proposal's flowcharts. Reads require `read`; namespace writes
  require `write`; ACL changes require `admin`.

> **Implemented (Phase 5, `src/acl/acl.ts` + `src/auth/basic.ts`):** WebDAV enforces `read`/`write`
> per request; `resolveEffectiveAcl(path)` returns the file's own ACL or walks up the folder chain to
> the root. ACL **changes** go through the admin API (`POST /admin/acl` → `folder.setacl`/`file.setacl`)
> rather than a per-principal `admin` right in v1. `acl_id 0` is the seeded world ACL (`*=rwx`) and
> doubles as the "inherit" sentinel, so `folder.create`/`file.create` default to inherit. Anonymous
> principals match only `*`; authenticated principals also match their username and groups.

---

## 13. Client surface: WebDAV ✅ (Phase 5 — done & verified)

WebDAV chosen because it yields a **mounted drive** (hierarchy + file bytes) with **no client
install** on Windows (`net use` / Map Network Drive), macOS (Finder → Connect to Server), and
Linux (`davfs2`/GVFS).

Mapping (served by each node on a **dedicated `WEBDAV_PORT`** — default `8090`, separate from the admin
API — via a small custom `node:http` handler in `src/api/webdav.ts`):

| WebDAV method | Maps to |
|---------------|---------|
| `PROPFIND` (depth 0/1) | list folder → `folders`/`files` metadata (local, fast) |
| `GET` | resolve manifest → fetch chunks from active content peer → stream |
| `PUT` | chunk bytes → `content-put` to **active** → ack client → **eager async** `content-put` to backup → `file.create/update` on Primary |
| `MKCOL` | `folder.create` on Primary |
| `DELETE` | `file.delete` / `folder.delete` on Primary |
| `MOVE` / `COPY` | `*.move` / manifest copy on Primary |
| `PROPPATCH` | timestamps/props (best-effort) |
| `LOCK`/`UNLOCK` | advisory locks (in-memory v1) |

Auth: HTTP Basic → principal → ACL checks (§12); anonymous callers match only `*`. Denials return
`401` (+ `WWW-Authenticate`) for anonymous and `403` for authenticated-but-forbidden;
`WEBDAV_ALLOW_ANON=false` forces auth even where an ACL grants `*`. A read-only **web UI** (HTML folder
listing) is already served on `GET` of a collection.

> **Implemented (Phase 5):** all verbs above, plus **recursive** `DELETE`, recursive `COPY`, `HEAD`,
> and `OPTIONS` (advertises DAV class 1+2). `PUT` returns `201` (new) / `204` (overwrite) from the
> Primary's authoritative result. Writes are **read-your-writes consistent** on the writing node
> (`submitIntent` waits for the local replica to apply the write's `lsn` before acking). Verified by
> `npm run test:phase5` → **PHASE 5: PASS**.

---

## 14. MCP service (✅ Phase 6)

- Each node exposes an HTTP MCP endpoint (`MCP_PORT`, JSON-RPC 2.0 over POST). Tools: `list_folder`,
  `stat_file`, `read_file`, `search`, and (ACL-permitting) `create_folder`, `write_file`.
- Metadata tools answer **locally** from the replica; content tools **resolve + fetch** from the
  content peer; writes route to Primary. Same ACL/routing path as WebDAV — AI is a first-class,
  non-special client (proposal §3.3/§4.3).

---

## 15. Module layout & interfaces

```
src/
  config.ts                 # ✅ done (Phase 1 + Phase 2 env vars: contentDir, chunkSizeBytes)
  logger.ts                 # ✅ done
  index.ts                  # ✅ wires host + metadata + replication + content + admin API
  p2p/
    host.ts                 # ✅ libp2p host
    protocols.ts            # ✅ protocol IDs + length-prefixed JSON RPC + content-put/get IDs
    announce.ts             # ✅ gossip + soft ClusterView (primary discovery, peer lsn/epoch, usedBytes/quotaBytes)
    health.ts               # ✅ Phase 3 — per-peer failure detector: up/suspect/down + bounded ping (§8.1)
  metadata/
    types.ts                # ✅ Op union (incl. location.backup/promote) + record types
    store.ts                # ✅ SQLite (node:sqlite); apply(op); sha256(); snapshot/load; manifest/location helpers
    log.ts                  # ✅ primary-only meta_log append/read/purge
  replication/
    service.ts              # ✅ push/pull state machine + snapshot serve/fetch (§7) + createFile/setBackup intents
                            #    ✅ Phase 3 — applyPromotion (§8.2) + becomePrimary/stepDown (§8.3) + split-brain guard
  api/
    admin.ts                # ✅ admin/debug HTTP API (status/tree/mkcol/rmdir/file/chunks/locations/corrupt/health/reconcile/capacity/rebalance/acl)
  content/                  # ✅ Phase 2 (done & verified)
    chunker.ts              # ✅ split/join, sha256 hashing
    store.ts                # ✅ on-disk chunk store (fan-out, verify, usedBytes)
    transfer.ts             # ✅ content-put / content-get binary protocols
    service.ts              # ✅ RF=2 write/read paths, placement, backup replication, read-repair
                            #    ✅ Phase 3 — health-aware placement + reconciler restoring RF=2 (§9)
                            #    ✅ Phase 4 — admission control (§10) + quota-aware warm-cache
  availability/             # ✅ Phase 3 (done & verified)
    service.ts              # ✅ wires detector → promotion (§8.2) + reconcile (§9)
    election.ts             # ✅ deterministic leader election (§8.3)
                            #    (detector → p2p/health.ts; promotion → replication/service.ts; reconciler → content/service.ts)
  rebalance/                # ✅ Phase 4 (done & verified)
    service.ts              # ✅ two-threshold hysteresis offload, RF-preserving relocate (§10)
  routing/
    router.ts               # selection functions (§11)
  acl/                      # ✅ Phase 5 (done & verified)
    acl.ts                  # ✅ effective-ACL resolution (inherit up the tree) + read/write checks (§12)
  auth/                     # ✅ Phase 5 (done & verified)
    basic.ts                # ✅ HTTP Basic: parse WEBDAV_USERS, decode header → principal
  api/
    webdav.ts               # ✅ Phase 5 — WebDAV server on its own WEBDAV_PORT (§13); auth + ACL enforced
    mcp.ts                  # ✅ Phase 6 — MCP JSON-RPC server on its own MCP_PORT (§14); auth + ACL enforced
```

**Key interfaces (sketch):**
```ts
interface MetadataStore {
  apply(op: Op): void                    // in a txn
  snapshot(): Buffer
  load(blob: Buffer): void
  sha256(): string
  lsn: number
  // typed reads
  listFolder(id: number): { folders: Folder[]; files: File[] }
  resolvePath(path: string): { kind: 'folder'|'file'|'none'; id?: number }
}
interface ContentStore {
  put(hash: string, bytes: Uint8Array): Promise<void>   // verifies hash
  get(hash: string): Promise<Uint8Array>                // verifies on read
  has(hash: string): boolean
  usedBytes(): number
}
interface HealthTable {
  get(peer: PeerId): 'up'|'suspect'|'down'
  onChange(cb: (peer: PeerId, state: Health) => void): void
  healthyNodes(): NodeRecord[]
}
interface ReplicationService { mode: 'push'|'pull'; start(): void; onWrite(op: Op): Promise<void> }
```

---

## 16. Consistency & correctness guarantees

- **Metadata:** linearizable via single-writer Primary + lsn-ordered log. Readers may lag by
  ≤ one push interval (bounded staleness); routing prefers max-lsn nodes to minimize it.
- **Content:** read-your-write on the writing node; eventually consistent replication of the
  backup copy (bounded by reconciler latency). Content is immutable (content-addressed), so no
  update conflicts — a "file update" is a new manifest, old chunks decref'd.
- **Durability:** an ack'd namespace write is in the Primary log and pushed; an ack'd content
  write is on the active node. **Backup replication is asynchronous but *eager*:** immediately
  after the client ack, the active node fires an async `content-put` to the chosen backup and
  flips the row to `state='ok'`. It does **not** wait for the periodic reconciler — the reconciler
  is only the **backstop** that catches rows left `degraded` because the eager copy failed (backup
  down, or active crashed between ack and copy). **1-copy window** = from client ack until the
  eager copy completes (normally ~one chunk-copy's worth of time); it only *persists* if the
  active node dies inside that window — the exact single-failure case the user accepts. **Config
  flag `WRITE_ACK`:** `active` (default — ack after 1 copy, eager async backup) vs `both`
  (synchronous — replicate to backup *before* acking, zero window, higher latency, write fails if
  no eligible backup is available).
- **Split-brain:** prevented by requiring the elected Primary to present a valid `(lsn, sha256)`
  and by lease/quorum among anchors; with 1–2 anchors, run 2 for a real quorum.

---

## 17. Phase-by-phase task breakdown

Legend: each task lists **files** and an **acceptance test**.

### Phase 0 — Walking skeleton ✅ (done & verified)
- libp2p host, mDNS discovery, auto-dial, ping heartbeat, disconnect detection, Docker artifacts.
- **Accepted:** two nodes discover→connect→heartbeat; killing one logs `peer disconnected`.

### Phase 1 — Metadata replication ✅ (done & verified)
- **T1.1** `metadata/store.ts` — SQLite schema (§3), typed CRUD, `sha256()`, snapshot/load. ✅
- **T1.2** `metadata/log.ts` — primary log append/read/purge. ✅
- **T1.3** `p2p/protocols.ts` — length-prefixed JSON RPC + protocol IDs. ✅
- **T1.4** `replication/service.ts` — push (§7.1), pull (§7.2), fallback (§7.3), write path (§7.5). ✅
- **T1.5** snapshot create/fetch — folded into `store.snapshot()/load()` + `service.ts` (no separate file). ✅
- **T1.6** Designate Primary — static `IS_PRIMARY` env for v1 (deterministic); election in Phase 3. ✅
- **Also delivered:** `p2p/announce.ts` (soft ClusterView so replicas find the Primary + track peer
  lsn) and `api/admin.ts` (status/tree/mkcol/rmdir/corrupt) — the debug surface + Phase 5 WebDAV seed.
- **Acceptance — verified** by `test/phase1/run.mjs` (local 4-process **and** docker-compose 3-node):
  - (a) `mkcol /alpha/beta` on Primary appears on all peers within one push interval (identical `sha256`). ✅
  - (b) a fresh late-joining node pull-syncs the whole tree from snapshot+log to the same `sha256`. ✅
  - (c) corrupt a peer's DB (`POST /admin/corrupt`) → next push detects the sha mismatch → the peer
    falls back to a **full** pull (snapshot reload) and re-converges. ✅
  - (d) all nodes report byte-identical `sha256` at the end. ✅
- **How to run:** `npm run test:phase1` (local processes) or `npm run docker:up` then
  `P2P_NO_SPAWN=1 P2P_ADMIN_PORTS=8081,8082,8083 node test/phase1/run.mjs` (containers).

### Phase 2 — Content storage with RF=2 ✅ (done & verified)
- **T2.1** `content/chunker.ts` (fixed size, `CHUNK_SIZE_BYTES` default 4 MiB, sha256) +
  `content/store.ts` (2-level fan-out dirs, integrity-verified on read+write, in-memory `usedBytes`). ✅
- **T2.2** `content/transfer.ts` — binary length-prefixed `content-put` / `content-get`. ✅
- **T2.3** Write path (`content/service.ts`): PUT → chunk → store active locally → Primary
  `createFile` intent (folders + `manifest.set` + per-chunk `location.set`, active-only `state='degraded'`,
  ack = **1-copy window**) → eager async `choosePlacement` (§5 least-utilized) → `content-put` to backup
  → `location.backup` (`state='ok'`, window closed). ✅
- **T2.4** Read path with read-repair: reassemble manifest → serve each chunk from verified local disk,
  else fetch from active/backup holder; a locally-corrupt chunk is deleted + re-fetched from the good
  replica. ✅
- **Also delivered:** `location.backup` op (§6), `announce.ts` `quotaBytes` gossip for placement, and
  admin endpoints `PUT/GET /admin/file`, `GET /admin/chunks`, `GET /admin/locations`,
  `POST /admin/corrupt-chunk`.
- **Acceptance — verified** by `test/phase2/run.mjs` (local 3-process **and** docker-compose 3-node):
  - (A) a written file is chunked and every chunk reaches **RF=2** — `locations.state='ok'`,
    `active_peer != backup_peer`, and each chunk physically present on **≥2 nodes** on disk. ✅
  - (B) **read-repair** — corrupt a chunk on its holder (`POST /admin/corrupt-chunk`) → `GET` still
    returns the exact original bytes and repairs the local copy. ✅
  - (C) **no data loss** — kill the **active** node (local: process kill; docker: `docker kill` the
    container) → a surviving node still returns the exact original bytes from the backup. ✅
- **How to run:** `npm run test:phase2` (local processes; runs A/B/C) or `npm run docker:up` then
  `P2P_NO_SPAWN=1 P2P_ADMIN_PORTS=8081,8082,8083 node test/phase2/run.mjs` (containers; A/B, C skipped).

### Phase 3 — Availability & self-healing ✅ (done & verified)
- **T3.1** `p2p/health.ts` detector — turns the gossip view into per-peer `up`/`suspect`/`down`
  (suspect ≈1.5×heartbeat, down ≈3×heartbeat), each transition confirmed by a **bounded ping**
  (`AbortSignal.timeout`); fires `onDown`/`onUp`, exposes `isHealthy(peer)` for placement. ✅
- **T3.2** Promotion on DOWN — `replication/service.ts` `applyPromotion(downPeer)` (Primary-only,
  single-writer): emits `location.promote` (dead active → surviving backup becomes active, `degraded`)
  and `location.backup`-clear (dead backup → cleared); idempotent, `atRisk` when no healthy replica (§8.2). ✅
- **T3.3** Reconciler — `content/service.ts` `reconcileOnce`/`reconcileOne` runs on **every** node,
  picking chunks where `active_peer==self AND state!='ok'`, copying the bytes it already holds to a fresh
  **healthy** peer (placement skips `down` nodes) and flipping the location back to `state='ok'` →
  **RF=2 restored** (§9). Guards: push-mode gate, fresh location re-read, `reconciling` overlap flag, batch cap. ✅
- **T3.4** `availability/service.ts` wires detector → promotion + reconcile; `availability/election.ts`
  runs a **deterministic** election when the **Primary** dies — winner = max committed `lsn` → anchor role →
  lowest `peerId`, computed independently by each replica (no voting); winner `becomePrimary(epoch=max+1)`
  (creates meta-log, switches to push), repairs locations off the dead Primary, and lagging replicas
  pull-snapshot. A monotonic `epoch` (term) in the gossip + deterministic `stepDown` resolves split-brain (§8.3). ✅
- **Also delivered:** `epoch` in `AnnounceMsg` + `bestPrimary()`/`betterPrimaryThan()` in `announce.ts`;
  store queries `degradedActiveLocations`/`locationsWithPeer`; health-derived config
  (`HEALTH_*`, `RECONCILE_*`, `ELECTION_*`); admin endpoints `GET /admin/health`, `POST /admin/reconcile`.
- **Acceptance — verified** by `test/phase3/run.mjs` (local 4-process cluster, 1.5s heartbeat):
  - (**A — self-heal**) kill the **active** holder of a file → all survivors detect it `down`, the Primary
    **promotes** the surviving backups to active, and each new active holder's **reconciler** re-copies its
    chunks to a fresh healthy peer → **RF=2 restored, zero data loss**, no location references the dead node. ✅
  - (**B — election**) then kill the **Primary** itself → every remaining replica independently computes the
    **same** new leader (deterministic, no split-brain); the new Primary accepts **new** writes, and the
    original file — now having survived **two** node failures — still reads back byte-for-byte. ✅
- **How to run:** `npm run test:phase3` (local processes; runs A/B). Against a running Docker cluster,
  `docker kill` the active node to watch promotion+reconcile, then `docker kill` the Primary to watch election.

### Phase 4 — Capacity management ✅ (done & verified)
- **T4.1** `rebalance/service.ts` — two-threshold hysteresis offload (§10). `sweepOnce()` runs on every
  caught-up node (`meta.mode==='push'`): above `REBALANCE_HIGH·quota` (T1) it offloads its **largest
  chunks first** (`store.heldChunksBySize`) to the least-utilized healthy peer with room, until under
  `REBALANCE_LOW·quota` (T2). RF is preserved — it moves only **one** of a chunk's two copies and deletes
  the local copy only **after** the map commits via the Primary: `replication.relocateActive`
  (`location.promote`, keeps the existing backup) when it holds the active copy, else `replication.setBackup`
  (`location.backup`). Skips chunks not `state='ok'` or whose other copy is on an unhealthy node.
- **T4.2** Admission control (§10) — `content.admitWrite(size)` rejects a write unless (a) this node has
  room for the whole active copy, (b) the cluster has `RF×size` free overall, and (c) ≥`RF` distinct
  healthy nodes have room. Rejections surface as **HTTP `507` + `code:'ENOSPC'`**. Warm-cache on read is
  now quota-aware (only caches while under T2) so it never fights the rebalancer.
- **T4.3** Config `REBALANCE_ENABLED/HIGH/LOW/INTERVAL_MS` + `STORAGE_QUOTA_BYTES` override; admin endpoints
  `GET /admin/capacity`, `POST /admin/rebalance`.
- **Acceptance:** `test/phase4/run.mjs` (3 nodes, 16-chunk quota). **Part A:** a write past T1 then
  `POST /admin/rebalance` drops the node under T2, all chunks stay RF=2, the file reads back byte-for-byte,
  and a second sweep moves 0 (no oscillation). **Part B:** an over-quota write → `507`/`ENOSPC`; filling the
  cluster makes admission return `507`; the Part-A file still reads correctly under a full cluster. ✅ Verified
  `npm run test:phase4` → **PHASE 4: PASS**.

### Phase 5 — Client surface (WebDAV) ✅ (done & verified)
- **T5.1** `api/webdav.ts` ✅ — full class-2 WebDAV on a dedicated `WEBDAV_PORT`: OPTIONS, PROPFIND
  (Depth 0/1), GET/HEAD (+ HTML listing), PUT (`201`/`204`), MKCOL, **recursive** DELETE, MOVE,
  recursive COPY, PROPPATCH, advisory LOCK/UNLOCK (§13). Read-your-writes on the writing node.
- **T5.2** `acl/acl.ts` ✅ + `auth/basic.ts` ✅ — effective-ACL resolution with inheritance + `read`/
  `write` checks (§12); HTTP Basic auth (`WEBDAV_USERS`); anonymous matches only `*`. ACLs are set via
  `POST /admin/acl` and replicate with the metadata (`folder.setacl`/`file.setacl`).
- **Acceptance:** `npm run test:phase5` → **PHASE 5: PASS** — (A) WebDAV protocol incl. recursive
  DELETE + overwrite semantics; (B) a PUT on node2 reaches RF=2 and node3 serves the same bytes and
  lists it; (C) per-folder ACL — anonymous→`401`, out-of-ACL→`403`, group read-only reads `200`/writes
  `403`, bad password→`401`, enforced identically on node3. Also mountable on Windows (`net use`) /
  macOS Finder / Linux davfs2.

### Phase 6 — MCP + AI access
- **T6.1** `api/mcp.ts` — MCP tools over the same routing/ACL path (§14).
- **Acceptance:** an MCP client lists folders, reads a file, and (with write ACL) creates one.

### Phase 7 — Hardening
- **T7.1** `content/store.ts` — AES-256-GCM authenticated encryption-at-rest per chunk (`STORAGE_KEY`, framing `[12B nonce][ciphertext][16B auth tag]`). Reads verify auth tag; tampering triggers transparent read-repair.
- **T7.2** `auth/basic.ts` & `auth/hash-password.ts` — scrypt password hashing (`$scrypt$N=16384,r=8,p=1$<salt>$<hash>`) with timing-safe comparison and backward-compatible plaintext fallback.
- **T7.3** `api/admin.ts` — HTTP Basic authentication via `ADMIN_USERS` gating all admin endpoints, plus enforcement of the `'admin'` ACL right on path-scoped mutations (`/admin/mkcol`, `/admin/rmdir`, `/admin/acl`).
- **T7.4** `api/metrics.ts` & `api/counters.ts` — Prometheus `/metrics` scrape endpoint (`METRICS_ENABLED`, `METRICS_PORT`), surfacing cluster gauges and monotonic counters (`uploads`, `reads`, `repairs`, `elections`).
- **T7.5** `availability/service.ts` — Proactive rotation of healthy passive/active replicas (`ROTATION_INTERVAL_MS`, `ROTATION_BATCH`).
- **Acceptance:** `test/phase7/run.mjs` verifies encryption framing, plaintext reads, read-repair on corrupted ciphertext, no data loss on active-node kill, admin auth gating, and metrics collection.

### Phase 8 — Tiered Scaling & Periodic Checkpointing
- **T8.1** `config.ts` & `config.default.json` — Comprehensive JSON configuration file loading (`CONFIG_FILE` or `config.json`), merging file defaults with environment overrides. Adds cluster capacity guard `MAX_CONTAINERS` (default 1,000) and Phase 8 knobs: `TIER_ENABLED`, `TIER_ROLE` (`relay` | `leaf` | `auto`), `TIER_SCALE_THRESHOLD`, `RELAY_ROSTER`, `CHECKPOINT_INTERVAL_HOURS`, `CHECKPOINT_RETENTION_DAYS`, `DELTA_LOG_RETENTION_OPS`.
- **T8.2** `p2p/announce.ts` & `replication/service.ts` — Hierarchical Tiered Relay Fanout:
  - Primary pushes metadata mutation batches exclusively to designated Tier-1 Relay nodes ($O(R)$ fanout, $R = 3 \sim 5$ anchors). Primary network egress complexity is $O(1)$ relative to total cluster size.
  - Tier-1 Relay nodes verify monotonic LSN contiguity, apply mutations locally, append to local delta log, and fan out to their connected Tier-2 Leaf nodes.
  - Leaf nodes apply batches and track replication watermarks. If a leaf node misses push batches, it pulls deltas from a healthy Relay node first, querying the Primary only as a final fallback.
  - Leaf node write intents forward through Relay nodes to the Primary.
- **T8.3** `metadata/checkpoint.ts` — Content-Addressed Periodic Checkpointing:
  - `CheckpointService` produces zero-downtime snapshots of the metadata hierarchy (`store.snapshot()`).
  - Snapshots are chunked, encrypted with AES-256-GCM, and stored directly in the distributed `ContentStore` under SHA-256 digests.
  - Checkpoint records are published in the `checkpoints` catalog table (`lsn`, `chunk_hash`, `size`, `sha256`, `created_at`).
  - Transaction log compaction: `metaLog.purge(oldestKeptLsn)` prunes historical operations prior to active checkpoints, bounding SQLite disk growth.
- **T8.4** `metadata/store.ts` & `replication/service.ts` — Cold-Node Bootstrap & Incremental Log Replay:
  - Newly joining containers and recovering nodes download the latest checkpoint chunk in parallel from peer content holders rather than requesting a heavy snapshot dump from the Primary.
  - Nodes restore the base SQLite database to the checkpoint LSN and pull only the short remaining delta log slice from their nearest Tier-1 Relay.
- **Acceptance:** `test/phase8/run.mjs` verifies multi-tier cluster fanout (Primary $\to$ Relay $\to$ Leaves), config file loading, cold-node bootstrap from checkpoint chunk, and live post-join updates. Unit tests `test/durability/config-file-unit.mjs` and `test/durability/checkpoint-unit.mjs` verify configuration parsing, snapshot chunking, and log purging.

---

## 18. Open technical decisions to confirm

**Resolved (2026-07-24 / 2026-11-16):**
1. **Metadata engine: SQLite** (built-in **`node:sqlite`** — zero native build; no `better-sqlite3`
   dependency) — ✅ confirmed & implemented.
2. **Consensus for Primary/log: simple** — single-writer + anchor lease + `(lsn,sha256)`-validated
   election for v1 (kept Raft-compatible for later) — ✅ confirmed.
3. **Write durability: `WRITE_ACK=active` default** — ack after the active copy, **eager async**
   backup replication, reconciler as backstop; `both` available as a config flag for zero-window
   synchronous RF=2 — ✅ confirmed (1-copy window accepted).
4. **Encryption-at-rest:** AES-256-GCM per chunk via `STORAGE_KEY` (Phase 7) — ✅ confirmed & implemented.
5. **Auth for WebDAV/MCP/Admin:** HTTP Basic + scrypt password hashing (Phase 7) — ✅ confirmed & implemented.

**Still open (safe defaults noted; will proceed unless you object):**
4. **Chunking:** fixed 4 MiB (v1) vs FastCDC (better dedup for edited files). Default: fixed.
5. **Auth for WebDAV/MCP:** HTTP Basic (v1) vs token/OIDC. Default: Basic + hashed creds.
6. **Snapshot cache host:** anchor-local blob store (v1) vs a dedicated cache service. Default:
   anchor-local.
7. **Encryption-at-rest:** off in v1 (trusted PCs) vs on. Default: off v1, add in Phase 7.
