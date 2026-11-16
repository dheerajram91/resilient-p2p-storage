# Implementation Plan — Distributed Peer-Container Storage System

> Implementation of the design proposal *"Distributed MCP-Enabled Storage System Using
> Resilient Peer Containers with Replicated Metadata for Scalable Deep Hierarchy Access"*
> ([`p2p-storage-proposal.md`](./p2p-storage-proposal.md)), scoped to the concrete deployment target:
> ordinary PCs, each running a `docker compose` stack, each donating some CPU and ~20 GB of
> disk, connected over LAN and/or the public internet, with nodes free to join and leave.

---

## 0. TL;DR

We build a single peer binary (the **node agent**), shipped as a Docker image, that every
participating PC runs via an identical `docker compose up`. Nodes discover each other over a
**libp2p** peer-to-peer network (works across LAN and internet with NAT traversal + encryption).
Each node contributes a **20 GB content store** and a **full replica of the hierarchy
metadata**. Every logical "drive"/folder's content has exactly **two copies on two different
physical PCs** (1 active + 1 backup, never co-located). The always-on 1–2 nodes act as the
**seed / primary / snapshot-cache** anchors. When a PC dies, its backups are promoted; the
system keeps serving with temporarily reduced redundancy until a replacement node joins and
re-replication restores the 2-copy invariant.

This document is a **plan**, not code. It ends with a phased milestone breakdown and an
explicit list of clarifying questions.

---

## 0.1 Confirmed Decisions (2026-07-24)

These answers from the owner override the assumptions in §2/§3:

1. **Drive = one global namespace with per-folder ACLs.** Any principal can read any document
   provided they hold read ACL on it. Single shared tree, ACL-gated per folder.
2. **PCs are trusted.** → We drop end-to-end / host-blind encryption and Byzantine tolerance from
   scope. Transport stays encrypted (libp2p Noise) and encryption-at-rest is optional/deferred.
3. **Client surface = WebDAV mount** (answer to the "which is quicker" question below), which gives
   a real mounted drive showing the folder hierarchy *and* file contents, with **zero client
   install** on Windows/macOS/Linux (all have built-in WebDAV clients). A small **web UI** can be
   layered on the same HTTP server later. FUSE was rejected as slower to ship and platform-fragile.
4. **No node has a stable/public IP.** See the networking decision below — for cross-internet
   deployments we run the peers over a lightweight mesh VPN so every peer is routable; on a single
   LAN, mDNS needs nothing extra.

**Implementation language changed: Go → Node.js/TypeScript with `js-libp2p`.** Rationale: the build
environment has Node 24 available (not Go), so we can actually build, run, and test the agent here;
`js-libp2p` is a first-class libp2p stack (DHT, mDNS, circuit-relay, Noise); and Node-in-Docker is
standard for the target PCs. All service responsibilities in §5 are unchanged — only the language.

**Networking decision for the all-NAT case (answer to §3 Q7):** true peer-to-peer connectivity
between machines that are *all* behind NAT with *no* reachable address is impossible without some
broker — this is a property of NAT, not a tool limitation. Two supported modes:
- **LAN mode (zero infra):** peers on the same network discover each other via **mDNS**. Nothing
  else needed.
- **Internet mode (trusted PCs, no public IP):** run every node on a **mesh VPN**
  (Tailscale / WireGuard / Nebula). The VPN handles NAT traversal via its own relays and gives each
  PC a stable overlay IP; our storage overlay then treats every peer as directly reachable and uses
  mDNS/static discovery over the VPN. This needs **no code** from us and no public IP on the PCs.
  (Alternative if a VPN is undesirable: designate the 1–2 "always-available" nodes as libp2p
  circuit-relays behind a single port-forward — but a mesh VPN is simpler and recommended.)

---

## 1. Mapping: Design → User Constraints

| Design concept | This deployment | Notes / simplification |
|---|---|---|
| Symmetric peer containers | One `docker compose` stack per PC | Identical image everywhere |
| Replicated hierarchy metadata (full copy per container) | Every node keeps the whole metadata tree | Cheap: metadata ≪ content |
| Decoupled content storage, primary + backup | Content chunks stored locally; **replication factor = 2** | This is the user's "backup always on another device" |
| Primary container + incremental log + snapshot cache | The always-available 1–2 nodes host primary + snapshot cache | User explicitly allows assuming 1–2 stable nodes |
| Routing Service | Client library / node-local router picks freshest, least-loaded node | |
| Replication Service (push/pull + SHA256 + timestamp) | Metadata sync protocol between nodes | Implemented as described |
| Availability Service (active-passive, never co-located, rotation, leader election) | Backup placement + promotion + primary election | **Core of the user's fault-tolerance requirement** |
| Split Service (two-threshold hysteresis) | Capacity rebalancing when a node nears 20 GB | Threshold tuned to 20 GB budget |
| MCP Service | MCP endpoint on each node for AI agents | Can be deferred to a later phase |

**User's tolerance model, restated precisely:** With replication factor 2, the system
survives **any single PC failure** with zero data loss. Immediately after a failure the
affected drives run at 1 copy (no backup) — this reduced-redundancy window is **expected and
accepted** — until a new node joins (or an existing node has spare space) and the system
re-replicates to restore 2 copies.

---

## 2. Assumptions (stated, since this is autopilot)

1. **Language/runtime:** Go, for a single static binary, first-class libp2p support, easy
   Docker packaging, and strong concurrency. (Alternative considered: Rust for perf, Node/Python
   for velocity — see §14.)
2. **P2P transport:** `go-libp2p` (Kademlia DHT for discovery, NAT traversal via
   AutoNAT/hole-punching/relay, Noise/TLS encryption, mDNS for LAN). This is what makes
   "internet + LAN + mixed" work without manual VPN setup.
3. **Content addressing:** files are split into content-addressed chunks (BLAKE3/SHA-256 hash =
   chunk ID). Deduplication and integrity verification come for free.
4. **Metadata store per node:** embedded, transactional — SQLite (via WAL) or BadgerDB — holding
   the folder tree, file metadata, ACLs, and the content-location map.
5. **Replication factor:** 2 (1 active + 1 backup) by default, configurable. This directly
   encodes "one PC can go down."
6. **Always-available nodes:** 1–2 stable nodes are configured as **seed/bootstrap peers** and are
   the preferred primary + snapshot-cache holders.
7. **Per-node budget:** ~20 GB content quota + small metadata/log/snapshot overhead, enforced by
   the node agent (soft cap triggers rebalancing/split before the hard 20 GB limit).
8. **Trust model (initial):** a shared cluster secret / pre-shared key gates membership. Full
   per-user ACL + multi-tenant auth is a later phase. (See clarifying questions.)
9. **Consistency target:** metadata = strongly consistent via a single writer (primary) + log,
   as the proposal specifies; content = eventually consistent replication with read-repair.
10. **Scale target for v1:** ~3–20 PCs. Not designed for thousands of nodes in phase 1.

---

## 3. Clarifying Questions (please answer to firm up the plan)

**Data & semantics**
1. What is a **"drive"** exactly — one folder tree per user? One global namespace shared by all
   nodes? Or one drive per contributing PC? This changes the metadata root design.
2. Expected **object sizes** — mostly small files (docs) or large (media/backups)? Affects
   chunk size and whether we need streaming.
3. Is **deduplication across users** acceptable (content-addressed storage means identical files
   share storage), or must each drive be storage-isolated/encrypted-at-rest per owner?

**Membership & trust**
4. Who may **join** the network — anyone with the image, or only invited PCs with a key? Is the
   set of PCs cooperative/trusted, or potentially adversarial (need Byzantine tolerance)?
5. Should data be **encrypted at rest** and **end-to-end** so a hosting PC cannot read the data it
   stores for others? (Recommended if PCs belong to different people.)

**Availability & topology**
6. Confirm **replication factor = 2** is the target, or do you want it configurable higher for
   important drives?
7. The always-available 1–2 nodes: are they on **stable public IPs** (can act as libp2p relays /
   bootstrap), or also behind NAT?
8. Is the **20 GB** a hard per-PC cap, or a default a user can raise/lower per PC?

**Access & clients**
9. How do end users interact — a **mounted virtual drive** (FUSE/WebDAV/SMB), a **web UI**, a
   **CLI/SDK**, or only the **MCP endpoint** for AI agents? This decides the client surface for v1.
10. Is the **MCP/AI-agent** access a v1 requirement or can it land in a later phase after core
    storage works?

**Operational**
11. Target OS mix for the PCs (Windows/macOS/Linux)? Docker Desktop vs native? Affects volume
    mounts and networking defaults.
12. Any **compliance/data-residency** constraints on where backups may physically live
    (e.g., "backup must stay within same LAN/region")?

> Default answers I will assume unless you say otherwise: (1) one shared global namespace with
> per-folder ACLs; (2) mixed file sizes, 4 MB chunks; (3) content-addressed dedup ON; (4) invite
> + shared key; (5) encryption-at-rest ON, E2E in a later phase; (6) RF=2 configurable; (7) the
> stable nodes have reachable addresses/relay; (8) 20 GB soft cap; (9) CLI/SDK + WebDAV mount for
> v1; (10) MCP in a later phase; (11) Linux containers via Docker on any host; (12) none.

---

## 4. High-Level Architecture

```
                ┌──────────────────────── One PC = one docker compose stack ───────────────────────┐
                │                                                                                   │
   Clients ───► │  node-agent (Go)                                                                  │
 (CLI/WebDAV/   │   ├─ P2P layer (libp2p: DHT discovery, NAT traversal, Noise encryption)           │
  MCP/AI)       │   ├─ Routing service      (pick freshest + least-loaded peer)                     │
                │   ├─ Metadata service     (full replica: tree, file meta, ACLs, location map)     │
                │   ├─ Replication service  (push/pull sync, SHA256 + timestamp validation)         │
                │   ├─ Content service      (chunk store on 20 GB volume, verify on read)           │
                │   ├─ Availability service (backup placement, promotion, leader election)          │
                │   ├─ Split/Rebalance svc  (two-threshold hysteresis vs 20 GB budget)              │
                │   └─ MCP service          (AI endpoint; later phase)                              │
                │                                                                                   │
                │  Volumes:  ./data/content (≤20GB)   ./data/meta   ./data/snapshots               │
                └───────────────────────────────────────────────────────────────────────────────────┘
                         ▲                         ▲                          ▲
                         └──────────── libp2p overlay (LAN mDNS + internet DHT/relay) ────────────┘

   Anchor nodes (the always-available 1–2 PCs): also run bootstrap + snapshot-cache + preferred primary.
```

Everything is the **same image**; a node's *role* (anchor vs ordinary, primary vs peer, active vs
passive for a given drive) is dynamic state, not a different build.

---

## 5. Components in Detail

### 5.1 P2P / Networking layer
- **Discovery:** libp2p Kademlia DHT for internet-wide discovery; **mDNS** for zero-config LAN
  discovery. Bootstrap list = the 1–2 anchor nodes' multiaddrs.
- **NAT traversal:** AutoNAT + DCUtR hole-punching; anchor nodes with public reachability act as
  **circuit-relay v2** fallbacks so NATed peers can always be reached.
- **Security:** Noise handshake (encrypted, authenticated). Membership gated by a **cluster PSK**
  (libp2p private network) or a signed join token.
- **Addressing:** each node has a stable libp2p **PeerID** (Ed25519 key persisted in the meta
  volume) so identity survives restarts/IP changes.

### 5.2 Metadata model (full replica per node)
Stored in embedded DB. Core tables/records:
- `folders(id, parent_id, name, acl_id, ctime, mtime)`
- `files(id, folder_id, name, ext, size, type, ctime, mtime, content_manifest_id)`
- `manifests(id, file_id, [chunk_hashes...])` — ordered chunk list per file version
- `locations(chunk_hash|drive_id, active_peer, backup_peer, state)` — the content-location map
- `acls(id, principal, rights)`
- `meta_log(ts, op, payload)` — incremental namespace-change log (**on primary**)
- `sync_state(local_ts, last_snapshot_ts, metadata_sha256)`

Only **namespace-changing** ops (folder/file create/delete/rename, ACL/permission, and
location-map updates) go through the log and replicate. Content byte writes do **not** touch
metadata replication — matching the proposal's "Selective Metadata Updates."

### 5.3 Replication service (metadata consistency)
Implements the proposal's protocol verbatim:
- **Push (steady state):** primary sends `{tsX→tsY, updates, SHA256(state)}`; receiver checks
  `local_ts == tsX`, applies, recomputes hash, advances to `tsY` only on match.
- **Pull (recovery/join):** load newest snapshot from cache → submit `local_ts` to primary →
  receive batched log after that ts → apply until caught up → switch to push.
- **Fallback push→pull:** on any gap / hash mismatch / outage, stop serving, discard, reload
  snapshot, pull-sync.
- **Snapshot + log lifecycle:** primary periodically serializes state → snapshot cache; purges
  log older than 2 snapshot cycles.

> **Engineering note / decision to confirm:** the proposal's single-writer-primary + SHA256 log is
> implementable directly. As a lower-risk alternative for the *leader-election + log* piece we can
> back it with **Raft (hashicorp/raft)** on the anchor nodes, which gives proven consensus and
> log replication while we keep the proposal's snapshot/SHA256 verification on top. Recommendation:
> implement the proposal's protocol for metadata *content*, but use Raft for *electing the primary and
> ordering the log* to de-risk split-brain. (Flagged as a clarifying decision.)

### 5.4 Content service (the 20 GB store)
- Files → **content-defined chunks** (~1–4 MB), each identified by hash. Stored under
  `./data/content/<hash-prefix>/<hash>`.
- **Integrity:** verify hash on every read; corrupt chunk → fetch from the replica peer
  (read-repair).
- **Quota enforcement:** node tracks bytes used; when usage crosses **Threshold_1** (e.g. 85% of
  20 GB) it asks the Split service to offload; **Threshold_2** (e.g. 70%) stops offloading
  (hysteresis, prevents oscillation).

### 5.5 Availability service — **the heart of the user requirement**
- **Placement invariant:** for every chunk/drive, `active_peer != backup_peer` and they must be on
  **different physical PCs** (and, if info available, different LAN/site). Enforced at write time
  and re-checked continuously.
- **Write path (RF=2):** client writes a chunk → node stores locally (active) → synchronously or
  quickly-async replicates to a chosen backup peer → location map updated on primary → metadata
  push-sync propagates the new mapping.
- **Backup peer selection:** least-utilized eligible peer that is not co-located with the active
  and has free quota; prefer an anchor node when no other backup fits.
- **Failure → promotion:** health monitor detects a dead peer (missed heartbeats). For every
  drive whose **active** was on the dead peer, promote its **backup** to active. Redundancy for
  those drives is now 1 (expected, temporary).
- **Re-replication:** a background reconciler continuously tries to restore RF=2 by copying
  single-copy chunks to a new eligible peer. When a fresh PC joins with free space, it becomes a
  re-replication target and redundancy is restored.
- **Proactive rotation (optional, later):** periodically promote passive→active to keep backups
  battle-tested (proposal §4.6).
- **Primary/leader election:** if the primary (anchor) is permanently lost, remaining nodes elect
  a new primary that must present a valid `SHA256(metadata)+timestamp`; then all pull-sync from it.
  With 1–2 anchors assumed always-up, this is a rare path but must exist.

### 5.6 Split / rebalance service
- Periodic utilization check per node against the 20 GB budget; two-threshold hysteresis as above.
- On over-threshold: pick under-utilized destination (or newly joined node), batch-move folder
  content, update location map on primary (→ metadata sync), then delete from source; re-check.

### 5.7 Routing service
- A thin selector (in the node and/or a small client SDK): for a metadata read pick the healthy
  peer with the latest `local_ts`, then least-loaded; for content resolve `locations` and hit the
  active peer (fall back to backup). Metadata writes always go to the **primary**.

### 5.8 MCP service (later phase)
- Each node exposes an MCP endpoint so AI agents can list/read metadata locally and be routed to
  content peers, using the same ACL/routing path as human clients.

---

## 6. Node Lifecycle (join / leave / fail)

**Join (new PC):**
1. `docker compose up` with the cluster PSK + anchor bootstrap multiaddrs.
2. Generate/load PeerID; connect to DHT/mDNS; authenticate with PSK.
3. Register capacity (≈20 GB) with the primary; **pull-sync** full metadata (snapshot + log).
4. Advertise as an eligible backup/rebalance target; reconciler starts placing replicas on it,
   restoring redundancy for any single-copy drives.

**Graceful leave:**
1. Node signals intent to leave. Reconciler ensures every drive it holds as *active* is
   re-replicated so RF=2 is preserved elsewhere before it exits.
2. Location map updated; node removed from eligible set.

**Ungraceful failure (the one-PC-down case):**
1. Heartbeat timeout → node marked unhealthy; routing stops sending traffic to it.
2. Availability service promotes backups→active for affected drives (now RF=1 there).
3. Reconciler works to restore RF=2 as capacity/new nodes allow. No data lost (the other copy
   survives).

---

## 7. Failure Handling Matrix (from proposal §6, concretized)

| Failure | Detection | Recovery |
|---|---|---|
| Missed metadata push | timestamp gap | fallback to pull-sync |
| Metadata corruption | SHA256 mismatch | discard, reload snapshot, pull-sync |
| Network partition | can't confirm contiguous ts / heartbeats | stop serving stale meta; pull-sync on reconnect |
| One PC down (content) | heartbeat timeout | promote backup→active; reconcile to RF=2 |
| Primary permanently lost | election timeout | leader election (SHA256+ts validated); all pull-sync |
| Chunk bit-rot | hash verify on read | read-repair from replica |

**Invariant:** no node serves metadata it can't verify; every chunk has ≥1 verified copy at all
times, and RF=2 is restored automatically when capacity permits.

---

## 8. Deployment Artifacts

Per-PC `docker-compose.yml` (single service is enough; the agent runs all internal services):
```yaml
services:
  node-agent:
    image: p2pstore/node-agent:latest
    restart: unless-stopped
    environment:
      CLUSTER_PSK: ${CLUSTER_PSK}          # shared membership secret
      BOOTSTRAP_PEERS: ${BOOTSTRAP_PEERS}  # anchor multiaddrs
      STORAGE_QUOTA_GB: 20
      REPLICATION_FACTOR: 2
      NODE_ROLE: ordinary                  # or "anchor" for the always-on PCs
    ports:
      - "4001:4001/tcp"                    # libp2p (public reachability for anchors)
      - "4001:4001/udp"                    # QUIC / hole-punching
      - "8080:8080"                        # local client API / WebDAV / MCP
    volumes:
      - ./data/content:/data/content       # the ~20 GB content store
      - ./data/meta:/data/meta             # metadata DB + PeerID key
      - ./data/snapshots:/data/snapshots   # snapshot cache (anchors)
```
Anchor PCs use `NODE_ROLE=anchor` and are expected to have reachable ports (public IP or port
forward) so they can serve as bootstrap + relay + snapshot cache.

---

## 9. Repository / Code Structure (proposed)

```
p2p-storage/
├─ cmd/node-agent/           # main() wiring config → services
├─ internal/
│  ├─ p2p/                   # libp2p host, discovery, transport, PSK
│  ├─ metadata/              # embedded DB, tree ops, ACLs, location map
│  ├─ replication/           # push/pull sync, snapshot, log, SHA256
│  ├─ content/               # chunker, chunk store, quota, read-repair
│  ├─ availability/          # placement, promotion, heartbeat, election
│  ├─ rebalance/             # split service, thresholds
│  ├─ routing/               # peer selection
│  ├─ mcp/                   # AI endpoint (later)
│  └─ api/                   # client API (CLI/SDK/WebDAV)
├─ pkg/client/               # client SDK + CLI
├─ deploy/
│  ├─ docker-compose.yml
│  ├─ docker-compose.anchor.yml
│  └─ Dockerfile
├─ test/
│  └─ e2e/                   # multi-node compose harness + chaos tests
└─ docs/                     # link back to p2p-storage-proposal.md
```

---

## 10. Phased Milestones

**Phase 0 — Skeleton (walking skeleton).** Node binary + Docker image; libp2p host with mDNS +
DHT; two nodes discover each other and exchange a heartbeat. *Exit:* `docker compose up` on two
PCs shows a formed cluster.

**Phase 1 — Metadata replication.** Embedded metadata store; single primary; log + snapshot;
push/pull sync with SHA256 + timestamp; a third node can pull-sync from scratch. *Exit:* create
folders on primary → appear on all peers; kill+rejoin a peer → it recovers via pull-sync.

**Phase 2 — Content storage with RF=2 (core value).** Chunker + chunk store + 20 GB quota;
write path replicates each chunk to a non-co-located backup; location map in metadata; read path
with read-repair. *Exit:* write a file, verify two copies on two PCs, kill one PC → file still
readable, no data loss.

**Phase 3 — Availability & self-healing.** Heartbeat/health; backup→active promotion; reconciler
restores RF=2 after a node dies and after a new node joins; leader election for primary loss.
*Exit:* chaos test — kill any one node repeatedly; invariant holds; redundancy self-restores.

**Phase 4 — Capacity management.** Split/rebalance with two-threshold hysteresis; graceful leave
drains data first. *Exit:* fill a node past threshold → data rebalances without oscillation.

**Phase 5 — Client surface.** CLI/SDK, and a WebDAV (or FUSE) mount so a "drive" is usable.
*Exit:* mount the drive on a PC and copy files in/out.

**Phase 6 — MCP + AI access.** MCP endpoint per node; ACL-gated metadata queries local, content
routed. *Exit:* an AI agent lists and reads files via MCP.

**Phase 7 — Hardening.** Encryption at rest (per-chunk AES-256-GCM via `STORAGE_KEY`), scrypt password hashing + admin auth/ACL enforcement, Prometheus `/metrics` endpoint with monotonic counters, proactive passive rotation (`AvailabilityService`), and comprehensive acceptance testing. *Exit:* `npm run test:phase7` asserts encryption framing, plaintext reads, tampered ciphertext read-repair, survivor failover, admin Basic auth, and Prometheus metrics. ✅ done.

**Phase 8 — Tiered Scaling & Periodic Checkpointing.** Configuration file support (`config.json`, `config.default.json`), cluster capacity guard `MAX_CONTAINERS` (default 1,000), Hierarchical Tiered Relay Fanout (Primary pushes to Tier-1 Relays, Relays fan out to Tier-2 Leaves), Content-Addressed Periodic Checkpointing (`CheckpointService` producing immutable SQLite snapshot chunks stored in `ContentStore`), cold-node bootstrap via checkpoint chunks, and circular transaction log pruning (`metaLog.purge`). *Exit:* `npm run test:phase8` asserts multi-tier fanout propagation, config file loading, cold-node bootstrap from checkpoint chunk, and live post-join updates. ✅ done.

---

## 11. Testing Strategy

- **Unit:** chunker, hash/verify, tree ops, sync state machine (push↔pull transitions).
- **Property tests:** metadata convergence — random op streams must yield identical SHA256 on all
  nodes.
- **Multi-node e2e:** a `docker compose` harness spins up N node containers on one host (simulating
  N PCs) on a shared bridge network; scripts drive joins/leaves/kills.
- **Chaos/fault injection:** randomly kill nodes, partition the network (tc/netem), corrupt a
  chunk; assert: no data loss, RF=2 restored, no split-brain.
- **Capacity tests:** fill nodes to force splits; assert hysteresis prevents thrashing.

---

## 12. Security Considerations

- Membership gated by cluster PSK / signed join token; libp2p Noise for transport encryption.
- **Encryption at rest** (per-chunk AEAD) so a hosting PC can't trivially read others' data;
  optional **end-to-end** (client-side) encryption so hosts *never* see plaintext (later phase).
- ACL enforcement on metadata + content access paths (the proposal's ACL checks precede all routing).
- Integrity via content-addressing (hash = ID) + SHA256 metadata validation.

---

## 13. Key Risks & Mitigations

| Risk | Mitigation |
|---|---|
| NAT traversal fails for some peers | anchor nodes as public relays; QUIC + hole-punching; fallback relay-through-anchor |
| Split-brain on primary election | back log/election with Raft on anchors; require SHA256+ts proof to win |
| Re-replication storms after a failure | rate-limit reconciler; prioritize single-copy drives; RF restore is best-effort/staged |
| Only 1 anchor up → it's an SPOF for writes | run 2 anchors; metadata writes queue/redirect on failover; reads stay available from replicas |
| 20 GB fills up cluster-wide | quota accounting + admission control; refuse writes when global free < needed RF copies |
| Malicious/faulty node | PSK membership now; Byzantine tolerance out of scope for v1 (clarify #4) |

---

## 14. Tech-Stack Alternatives (for the record)

- **Language:** Go (recommended) vs Rust (max perf/safety, slower dev) vs Node/Python (fast dev,
  weaker for long-running P2P daemons).
- **P2P:** libp2p (recommended, batteries-included NAT/discovery/crypto) vs raw QUIC + custom
  discovery (more control, much more work) vs an existing system (IPFS for content, but it doesn't
  give us the proposal's replicated-metadata + primary-log design out of the box).
- **Consensus:** the proposal's custom primary+SHA256 log (faithful to the proposal) vs Raft for
  election/ordering (de-risked) — recommendation: combine (see §5.3).
- **Metadata store:** SQLite (relational, easy queries) vs BadgerDB (fast KV) — either works.

---

## 15. Immediate Next Steps (once questions in §3 are answered)

1. Lock the answers to §3 (esp. drive semantics #1, trust model #4/#5, client surface #9).
2. Scaffold the repo (`p2p-storage/`) and Phase 0 walking skeleton.
3. Stand up a 3-container local compose harness to validate discovery + heartbeat.
4. Proceed through the phased milestones, demoing the "kill one PC, no data loss, self-heal"
   scenario at the end of Phase 3 as the headline proof of the proposal's resilience claim.

---

*This plan lives alongside the proposal in the same folder. It intentionally does not add code yet —
it defines the architecture, decisions, assumptions, and a build order so implementation can start
immediately after the clarifying questions in §3 are resolved.*
