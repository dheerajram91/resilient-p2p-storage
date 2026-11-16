/**
 * Prometheus-compatible /metrics endpoint (Phase 7 — Metrics & Observability).
 *
 * Serves a plain-text Prometheus scrape on GET /metrics using an HTTP server
 * bound to METRICS_PORT (default 9090). Opt-in via METRICS_ENABLED=true.
 * No external dependencies — metrics are assembled by string concatenation.
 */

import { createServer, type Server, type IncomingMessage, type ServerResponse } from 'node:http'
import type { ContentService } from '../content/service.js'
import type { MetadataStore } from '../metadata/store.js'
import type { ReplicationService } from '../replication/service.js'
import type { HealthDetector } from '../p2p/health.js'
import type { ClusterView } from '../p2p/announce.js'
import type { Counters } from './counters.js'
import { log } from '../logger.js'

export interface MetricsDeps {
  port: number
  content: ContentService
  store: MetadataStore
  replication: ReplicationService
  detector: HealthDetector
  cluster: ClusterView
  quotaBytes: number
  /** Phase 7: monotonic event counters (uploads, reads, repairs, moves, elections). */
  counters: Counters
}

export class MetricsApi {
  private server: Server

  constructor(private readonly deps: MetricsDeps) {
    this.server = createServer((req, res) => {
      void this.handle(req, res)
    })
  }

  start(): void {
    this.server.listen(this.deps.port, () => {
      log.info('metrics server listening', { port: this.deps.port })
    })
  }

  stop(): void {
    this.server.close()
  }

  private async handle(req: IncomingMessage, res: ServerResponse): Promise<void> {
    if (req.method !== 'GET' || req.url !== '/metrics') {
      res.writeHead(404, { 'Content-Type': 'text/plain' })
      res.end('Not Found\n')
      return
    }

    try {
      const body = await this.collect()
      res.writeHead(200, {
        'Content-Type': 'text/plain; version=0.0.4; charset=utf-8'
      })
      res.end(body)
    } catch (err) {
      log.warn('metrics collection error', { error: String(err) })
      res.writeHead(500, { 'Content-Type': 'text/plain' })
      res.end('Internal Server Error\n')
    }
  }

  private async collect(): Promise<string> {
    const { content, store, replication, detector, cluster, quotaBytes } = this.deps
    const lines: string[] = []

    const gauge = (name: string, help: string, value: number, labels?: Record<string, string>): void => {
      lines.push(`# HELP ${name} ${help}`)
      lines.push(`# TYPE ${name} gauge`)
      if (labels && Object.keys(labels).length > 0) {
        const labelStr = Object.entries(labels)
          .map(([k, v]) => `${k}="${v}"`)
          .join(',')
        lines.push(`${name}{${labelStr}} ${value}`)
      } else {
        lines.push(`${name} ${value}`)
      }
    }

    // p2p_chunks_stored_total
    const localChunks = await content.localChunks()
    gauge('p2p_chunks_stored_total', 'Number of chunks held locally', localChunks.length)

    // p2p_used_bytes
    gauge('p2p_used_bytes', 'Bytes used in content store', content.usedBytes())

    // p2p_quota_bytes
    gauge('p2p_quota_bytes', 'Content store quota in bytes', quotaBytes)

    // p2p_peers_total — count by state from health detector snapshot
    const peerSnap = detector.snapshot()
    const peerCounts: Record<string, number> = { up: 0, suspect: 0, down: 0 }
    for (const p of peerSnap) {
      peerCounts[p.state] = (peerCounts[p.state] ?? 0) + 1
    }
    lines.push('# HELP p2p_peers_total Connected peers by health state')
    lines.push('# TYPE p2p_peers_total gauge')
    lines.push(`p2p_peers_total{state="up"} ${peerCounts['up'] ?? 0}`)
    lines.push(`p2p_peers_total{state="suspect"} ${peerCounts['suspect'] ?? 0}`)
    lines.push(`p2p_peers_total{state="down"} ${peerCounts['down'] ?? 0}`)

    // p2p_metadata_lsn
    gauge('p2p_metadata_lsn', 'Current metadata log sequence number', store.lsn)

    // p2p_is_primary
    gauge('p2p_is_primary', 'Whether this node is the metadata primary (1=yes, 0=no)', replication.isPrimary ? 1 : 0)

    // p2p_replication_lag_lsn — primary lsn - store.lsn, clamped to 0 if we ARE primary
    let lag = 0
    if (!replication.isPrimary) {
      const primaryPeer = cluster.all().find((p) => p.isPrimary)
      if (primaryPeer) {
        lag = Math.max(0, primaryPeer.lsn - store.lsn)
      }
    }
    gauge('p2p_replication_lag_lsn', 'LSN gap between this node and the primary', lag)

    // p2p_locations_total — count by state
    const locations = store.allLocations()
    const locCounts: Record<string, number> = { ok: 0, degraded: 0, replicating: 0 }
    for (const loc of locations) {
      locCounts[loc.state] = (locCounts[loc.state] ?? 0) + 1
    }
    lines.push('# HELP p2p_locations_total Chunk locations by state')
    lines.push('# TYPE p2p_locations_total gauge')
    lines.push(`p2p_locations_total{state="ok"} ${locCounts['ok'] ?? 0}`)
    lines.push(`p2p_locations_total{state="degraded"} ${locCounts['degraded'] ?? 0}`)
    lines.push(`p2p_locations_total{state="replicating"} ${locCounts['replicating'] ?? 0}`)

    // p2p_quorum_term
    const qs = replication.quorumStatus() as Record<string, unknown>
    const term = typeof qs['term'] === 'number' ? qs['term'] : 0
    gauge('p2p_quorum_term', 'Current Raft term (quorum mode only; 0 in legacy mode)', term)

    // Monotonic counters (Phase 7). Names come from the services that inc() them.
    for (const [name, value] of Object.entries(this.deps.counters.snapshot()).sort(([a], [b]) => a.localeCompare(b))) {
      lines.push(`# TYPE p2p_${name} counter`)
      lines.push(`p2p_${name} ${value}`)
    }

    return lines.join('\n') + '\n'
  }
}
