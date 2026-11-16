/**
 * In-process monotonic counters for the Phase 7 /metrics endpoint.
 *
 * A deliberately tiny dependency-free registry: services call `inc()` at the
 * point of interest (uploads, repairs, rebalance moves, election wins) and the
 * metrics endpoint snapshots the whole map on scrape. Counters are per-process
 * and reset on restart — scrape deltas are the meaningful signal.
 */
export class Counters {
  private values = new Map<string, number>()

  /** Increment `name` by `by` (default 1). */
  inc(name: string, by = 1): void {
    this.values.set(name, (this.values.get(name) ?? 0) + by)
  }

  get(name: string): number {
    return this.values.get(name) ?? 0
  }

  snapshot(): Record<string, number> {
    return Object.fromEntries(this.values)
  }
}
