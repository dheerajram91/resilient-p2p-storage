# Deployment Guide — running across real machines

This guide takes the node-agent from "3 containers on one laptop" to **real PCs on different
machines**. It covers three network scenarios and the exact steps for each.

> **Status legend**
> - ✅ **Works today** — no code changes needed.
> - The cross-network **bootstrap + persistent-identity + announce** enhancement is **implemented and
>   tested** (see [§6 How cross-network discovery works](#6-how-cross-network-discovery-works)). Path B
>   forms a cluster over a mesh VPN on its own.

---

## 0. Which path do I need?

| Your situation | Path | Status |
|----------------|------|--------|
| Several containers on **one host** (dev/demo) | Docker compose (see [README](README.md)) | ✅ Ready |
| Two+ PCs on the **same home/office LAN** | [Path A — native on a LAN](#path-a--same-lan-native-works-today) | ✅ Ready |
| PCs on **different networks / behind NAT / no public IP** | [Path B — mesh VPN (Tailscale/WireGuard)](#path-b--different-networks-via-a-mesh-vpn-tailscale) | ✅ Ready |
| Machines on the **public Internet / Cloud VMs / VPS** | [Path C — Public Internet & Cloud VMs](#path-c--public-internet--cloud-vps-deployment) | ✅ Ready |

**TL;DR:** 
- If machines are on the **same Wi-Fi/LAN**, start directly with **Path A** (automatic mDNS discovery).
- If machines are behind NAT without public IPs, **Path B** provides secure zero-config tunneling via a mesh VPN.
- If machines have public IPs or are hosted on Cloud providers (AWS, GCP, DigitalOcean, Hetzner), follow **Path C**.

---

## Durable mode: configure three voters first

For acknowledged-upload durability and safe automatic Primary failover, deploy **three machines**.
On all three set the same roster, using a distinct corresponding name on each:

```powershell
$env:METADATA_VOTERS = "pc1,pc2,pc3"
$env:NODE_NAME = "pc1" # pc2 / pc3 on the other machines
$env:REPLICATION_FACTOR = "2"
```

Use fresh `DATA_DIR` directories. An existing legacy database must be exported/imported into a new
cluster; no automatic conversion or deletion is performed. Keep each machine's metadata and identity
directories across restarts. The roster is fixed; do not rename a voter or change the list to force
recovery. Quorum leadership supersedes `IS_PRIMARY`; start at least two voters before writing.

The network instructions below still apply. For bootstrap-only discovery, make all voter pairs
reachable: configure each node with the other voters' stable multiaddresses, not just one Primary.
An initial seed alone is insufficient if that seed fails before the remaining voters know how to
connect. Each voter must directly reach the other voters; this is not a relayed gossip topology.
Content still has two copies, not three. Writes require both a metadata majority and two content
holders with room.

An isolated minority rejects writes. A successful upload survives a subsequent single process/node
failure after recovery; Windows sudden-power-loss durability is not certified. Failed/timed-out
requests may have committed; read before retrying. See README's durability section for limitations
and `npm run test:durability` for the qualification suite.

Retain the content root's `.upload-pins` directory too: those durable protections prevent
relocation from deleting chunks while an upload's metadata outcome is unresolved. Abandoned pins
survive restart and have no automatic expiration; they can require controlled manual recovery.

Without `METADATA_VOTERS`, the two-machine examples below select **legacy/demo mode**, which does not
provide majority-safe metadata failover or the acknowledged-upload durability guarantee.

## 1. Prerequisites (every machine)

- **Node.js 24+** and **npm** (`node -v`, `npm -v`). Docker is optional for real-PC deployment.
- The project folder copied to each machine, then:
  ```powershell
  npm install
  npm run build
  ```
- **Legacy mode only:** exactly one initial metadata Primary (`IS_PRIMARY=true`).
  Quorum mode elects its Primary; no flag can force leadership.
- Pick a **fixed** libp2p port so you can open the firewall and reference it: `LISTEN_PORT=4001`.
- **Firewall** (run PowerShell as Admin on each machine — you'll have admin on a personal PC):
  ```powershell
  # libp2p peer traffic (metadata sync + content transfer ride this one port)
  New-NetFirewallRule -DisplayName "p2p-storage libp2p 4001" -Direction Inbound -Protocol TCP -LocalPort 4001 -Action Allow
  # mDNS discovery (Path A / same-LAN only)
  New-NetFirewallRule -DisplayName "p2p-storage mDNS 5353" -Direction Inbound -Protocol UDP -LocalPort 5353 -Action Allow
  ```

---

## Path A — same LAN, native (✅ works today)

Two PCs on the same LAN discover each other automatically over **mDNS** — the same mechanism the Docker
demo uses, but now across real machines. No bootstrap config required.

### A.1 Machine 1 — the Primary
```powershell
cd <path-to>\p2p-storage
$env:NODE_NAME     = "pc1"
$env:NODE_ROLE     = "anchor"
$env:IS_PRIMARY    = "true"
$env:LISTEN_PORT   = "4001"
$env:ADMIN_PORT    = "8080"
$env:WEBDAV_ENABLED= "true"
$env:WEBDAV_PORT   = "8090"
$env:WEBDAV_USERS  = "alice:secret:staff,bob:hunter2"   # optional accounts
$env:DATA_DIR      = "C:\p2pdata"
$env:STORAGE_QUOTA_GB = "20"
node dist/index.js
```

### A.2 Machine 2 — a replica (same LAN)
Identical, but **not** primary and a different data dir:
```powershell
cd <path-to>\p2p-storage
$env:NODE_NAME     = "pc2"
$env:LISTEN_PORT   = "4001"
$env:ADMIN_PORT    = "8080"
$env:WEBDAV_ENABLED= "true"
$env:WEBDAV_PORT   = "8090"
$env:DATA_DIR      = "C:\p2pdata"
$env:STORAGE_QUOTA_GB = "20"
node dist/index.js
```

### A.3 Verify the cluster formed
On each machine (or from either, using the other's LAN IP):
```powershell
curl http://localhost:8080/admin/status     # note peerId, isPrimary, lsn, sha256
```
Both nodes should log `peer connected` and report the **same `sha256`** once converged. Then prove
replication end-to-end:
```powershell
# create a folder + file on pc1, read it from pc2
curl -X POST http://<pc1-ip>:8080/admin/mkcol -d '{\"path\":\"/shared\"}'
curl -X PUT  --data-binary "hi from pc1" "http://<pc1-ip>:8080/admin/file?path=/shared/a.txt"
curl "http://<pc2-ip>:8080/admin/file?path=/shared/a.txt"   # -> hi from pc1
```

> **Troubleshooting Path A:** no `peer connected`? (1) confirm both are on the same subnet
> (`ipconfig`); (2) firewall rules from §1 applied on **both**; (3) some corporate/"Public" network
> profiles block mDNS — set the adapter to **Private** or test on a home network; (4) guest-Wi-Fi
> "client isolation" blocks peer-to-peer traffic.

---

## Path B — different networks, via a mesh VPN (Tailscale) (✅ works today)

When the machines are **not** on the same LAN and have **no public IP**, put them on a **mesh VPN** so
each gets a stable, mutually-reachable overlay IP. mDNS does **not** cross a VPN (no multicast), so the
nodes are told where to find each other via `BOOTSTRAP_PEERS` — now wired end-to-end (§6).

### B.1 Install the VPN (all machines)
1. Install **Tailscale** (or WireGuard/Nebula/ZeroTier) and sign every PC into the **same** tailnet.
2. Get each machine's overlay IP:
   ```powershell
   tailscale ip -4        # e.g. 100.101.102.103
   ```
3. Sanity-check reachability: from PC2, `ping <pc1-tailscale-ip>` should succeed.
4. Firewall: ensure the TCP 4001 rule (§1) also applies on the Tailscale interface (it usually does;
   Tailscale traffic arrives as normal inbound TCP).

### B.2 Designate the Primary and get its identity
Start PC1 once (as in A.1) and note its **peerId** from the startup log / `GET /admin/status`. The
`dialAddrs` field of `/admin/status` gives you ready-made `/ip4/.../tcp/4001/p2p/<peerId>` strings to
paste straight into `BOOTSTRAP_PEERS`.

> ✅ **Persistent identity.** The node saves its Ed25519 key under `DATA_DIR/identity/node.key` and
> reloads it on every restart, so the Primary's **peerId is stable** — static `BOOTSTRAP_PEERS` keeps
> working across reboots.

### B.3 Configure bootstrap on every node
On **each** machine, point at the Primary over the tailnet:
```powershell
$env:LISTEN_PORT     = "4001"
$env:BOOTSTRAP_PEERS = "/ip4/<pc1-tailscale-ip>/tcp/4001/p2p/<pc1-peerId>"
# advertise this machine's overlay address so peers dial the tailnet IP:
$env:ANNOUNCE_MULTIADDRS = "/ip4/<this-machine-tailscale-ip>/tcp/4001"
# mDNS is useless across a VPN — turn it off so discovery is bootstrap-only:
$env:MDNS_ENABLED    = "false"
node dist/index.js
```
Start the **Primary first**, then the replicas. Each replica dials the Primary over Tailscale, the
gossip/announce protocol shares the rest of the cluster view, and they converge to the same `sha256`.

### B.4 How Path B forms the mesh
`BOOTSTRAP_PEERS` is dialed by the `@libp2p/bootstrap` discovery module; `ANNOUNCE_MULTIADDRS` sets the
addresses each node advertises so peers reach the overlay IP; `MDNS_ENABLED=false` disables multicast
(which a VPN can't carry). This exact configuration — mDNS off, bootstrap-only — is covered by the
`test:phase5.5` acceptance test. On the same LAN you can skip all of this because mDNS discovers peers
for you (Path A).

---

## Path C — Public Internet & Cloud VPS Deployment

When running nodes across the public Internet (cloud virtual machines such as AWS EC2, GCP Compute Engine, DigitalOcean, Hetzner, or physical servers with public IPs/port forwarding), nodes communicate directly over public IP addresses or domain names.

### C.1 Firewall & Network Prerequisites
1. **P2P Libp2p Port (Required)**: Open inbound TCP port `4001` on your cloud firewall / security group:
   - AWS / GCP / Azure: Add an inbound security group rule for `TCP 4001` from `0.0.0.0/0`.
   - Linux (UFW): `sudo ufw allow 4001/tcp`
   - Linux (iptables): `sudo iptables -A INPUT -p tcp --dport 4001 -j ACCEPT`
2. **WebDAV / Admin HTTP Ports (Optional)**:
   - For security on the public Internet, keep `ADMIN_PORT=8080`, `WEBDAV_PORT=8090`, and `MCP_PORT=8100` bound locally or protect them behind a reverse proxy (such as Caddy, Nginx, or Traefik) terminating **TLS/HTTPS**.
   - Set strong credentials via `ADMIN_USERS` and `WEBDAV_USERS` with scrypt hashed passwords.

### C.2 Setting Up the Primary Seed Node (Machine 1)
Start the primary or bootstrap seed node on the server with a known public IP or DNS record:

```bash
export NODE_NAME="seed-primary"
export NODE_ROLE="anchor"
export IS_PRIMARY="true"
export LISTEN_PORT="4001"
export ADMIN_PORT="8080"
export DATA_DIR="/var/p2pstorage/data"

# Inform the P2P swarm of the public address to dial:
export ANNOUNCE_MULTIADDRS="/ip4/<PUBLIC_IP_OR_DNS>/tcp/4001"

# Turn off local mDNS on the public internet:
export MDNS_ENABLED="false"

# Optional: Enable AES-256-GCM encryption-at-rest:
export STORAGE_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

node dist/index.js
```

Upon startup, inspect `GET http://localhost:8080/admin/status` or the console log to record the node's permanent `peerId`:
```
[info] peerId: 12D3KooW...
[info] dial address: /ip4/<PUBLIC_IP>/tcp/4001/p2p/12D3KooW...
```

> **Identity Persistence**: The node stores its Ed25519 identity key permanently in `DATA_DIR/identity/node.key`. The `peerId` remains completely stable across server reboots, container upgrades, and process restarts.

### C.3 Setting Up Additional Storage Nodes (Replicas / Leaves)
On any other machine on the Internet (another cloud provider, office server, or home lab):

```bash
export NODE_NAME="node-replica-2"
export LISTEN_PORT="4001"
export ADMIN_PORT="8080"
export DATA_DIR="/var/p2pstorage/data"

# Announce this node's own reachable public IP or DNS:
export ANNOUNCE_MULTIADDRS="/ip4/<REPLICA_PUBLIC_IP>/tcp/4001"

# Dial the Primary seed node over the Internet:
export BOOTSTRAP_PEERS="/ip4/<SEED_PUBLIC_IP>/tcp/4001/p2p/<SEED_PEER_ID>"

export MDNS_ENABLED="false"
export STORAGE_KEY="0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

node dist/index.js
```

### C.4 Security on the Public Internet
- **End-to-End Transport Security**: All peer-to-peer data plane and control plane traffic over port `4001` is automatically authenticated and encrypted using the **Noise Protocol Framework** (using Ed25519 identity keys and ChaCha20-Poly1305 / AES-GCM ciphers). No external VPN is required for wire confidentiality between nodes.
- **At-Rest Storage Encryption**: Set `STORAGE_KEY` (a 32-byte / 64-hex-character symmetric key) on all nodes. All chunk payloads written to disk are encrypted with AES-256-GCM.
- **Authentication**: Set `ADMIN_USERS` and `WEBDAV_USERS` to enforce scrypt password verification.

---

## 2. Docker across machines (notes)

The bundled `deploy/docker-compose.yml` is a **single-host** simulator: it does not publish the libp2p
port and containers announce their private `172.x` bridge IP, which another host can't reach. For real
machines, prefer **native** (Paths A/B). If you must use Docker cross-host:
- Linux: `network_mode: host` so the container uses the machine's real interfaces.
- Otherwise: publish `4001:4001` **and** set `ANNOUNCE_MULTIADDRS` to the host/overlay IP so peers dial
  a reachable address. Also mount a volume for `/data/identity` (see §6) or the container's peerId — and
  thus its bootstrap string — changes on every recreate. Windows/macOS Docker Desktop runs a Linux VM,
  so host networking + LAN mDNS is unreliable — native is the recommended path on those OSes.

---

## 3. Mount the drive on each machine (WebDAV)

Each node serves WebDAV at `http://localhost:8090/` when `WEBDAV_ENABLED=true`.

- **Anonymous** access (world-readable parts) works with no credentials.
- **Authenticated** (ACL-protected) mounts send HTTP Basic credentials. Windows blocks Basic-over-HTTP
  unless `BasicAuthLevel=2`. **On a personal PC you have admin**, so you can enable it once:
  ```powershell
  # Run as Administrator, then restart the WebClient service
  Set-ItemProperty "HKLM:\SYSTEM\CurrentControlSet\Services\WebClient\Parameters" BasicAuthLevel 2
  Restart-Service WebClient
  net use Z: http://localhost:8090/ /user:alice secret
  ```
  Alternatively serve WebDAV over **HTTPS** (Basic-over-SSL is allowed even at `BasicAuthLevel=1`) — the
  recommended posture for real deployment — or use a no-admin client (**WinSCP**/**rclone**).
- **macOS:** Finder → *Go → Connect to Server* → `http://<machine>:8090/`.
- **Linux:** `sudo mount -t davfs http://<machine>:8090/ /mnt/drive`.

---

## 4. Verifying a healthy multi-machine cluster

| Check | Command | Expect |
|-------|---------|--------|
| Convergence | `GET /admin/status` on each node | identical `sha256` |
| Peer liveness | `GET /admin/health` | every peer `up` |
| Content RF=2 | `GET /admin/locations` | each chunk `state=ok`, `active_peer != backup_peer` |
| Cross-node read | write on one, `GET /admin/file?path=…` on another | same bytes |

---

## 5. Configuration cheat-sheet (real-PC essentials)

| Var | Set to | Why |
|-----|--------|-----|
| `IS_PRIMARY` | `true` on exactly one machine | single writer (v1) |
| `NODE_ROLE` | `anchor` on the primary, else `ordinary` | election tie-breaks favour anchors |
| `LISTEN_PORT` | `4001` (fixed) | firewall + bootstrap need a known port |
| `ADMIN_PORT` | `8080` | debug/admin API |
| `WEBDAV_ENABLED` / `WEBDAV_PORT` | `true` / `8090` | mount surface (opt-in) |
| `DATA_DIR` | a real path, e.g. `C:\p2pdata` | metadata db + chunks live here |
| `STORAGE_QUOTA_GB` | your contribution (e.g. `20`) | admission + rebalance budget |
| `BOOTSTRAP_PEERS` | Primary's overlay multiaddr(s), comma-separated | **Path B** — dialed on startup |
| `ANNOUNCE_MULTIADDRS` | this machine's overlay multiaddr(s) | **Path B** — advertise the reachable IP |
| `MDNS_ENABLED` | `false` on a VPN, `true`/unset on a LAN | disable multicast discovery for Path B |

See the full table in [README.md](README.md#configuration-environment-variables).

---

## 6. How cross-network discovery works

Path A (same LAN) uses mDNS. **Path B (mesh VPN)** is powered by three implemented, tested features —
scoped in [`docs/DETAILED_DESIGN.md`](docs/DETAILED_DESIGN.md) §2.3 and proven by `npm run test:phase5.5`:

1. **`BOOTSTRAP_PEERS` is dialed.** `@libp2p/bootstrap` is registered as a `peerDiscovery` module in
   `src/p2p/host.ts`; each configured `/ip4/.../tcp/<port>/p2p/<peerId>` multiaddr is dialed on startup
   and the existing `peer:discovery` → dial handler forms the mesh.
2. **Identity is persistent.** The libp2p Ed25519 key is saved at `DATA_DIR/identity/node.key` and
   reloaded on start, so `peerId` is **stable across restarts** (static bootstrap + stable election).
3. **`ANNOUNCE_MULTIADDRS` override.** A node advertises its overlay IP explicitly so peers dial the
   Tailscale address instead of an unrelated NIC. Set `MDNS_ENABLED=false` to force bootstrap-only
   discovery (the VPN case).

The `test:phase5.5` acceptance test starts two nodes with **mDNS disabled** and only `BOOTSTRAP_PEERS`
set, and asserts persistent identity, a bootstrap-only connection, and metadata propagation — i.e. the
exact Path B scenario, verified without a real VPN.

> **Docker identity.** The bundled Compose file now persists `/data/identity` alongside metadata and
> content, and selects a fixed three-voter quorum. Retain all these volumes across restarts.
> Existing legacy metadata volumes need export/import to a fresh cluster before enabling quorum.
> `npm run docker:down` removes volumes and is destructive; do not use it to restart valuable storage.

---

## Related docs

- [README.md](README.md) — features, phases, admin API, local/Docker run, config reference.
- [docs/DETAILED_DESIGN.md](docs/DETAILED_DESIGN.md) — architecture, invariants, per-phase design (§2.3 networking).
- [docs/IMPLEMENTATION_PLAN.md](docs/IMPLEMENTATION_PLAN.md) — deployment target + rollout plan (§0.1 networking modes).
- [docs/p2p-storage-proposal.md](docs/p2p-storage-proposal.md) — the originating design proposal.
