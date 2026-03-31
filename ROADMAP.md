# turbograph Roadmap

## Phase Baseline: Tigris Benchmark -- DONE
> Before: Phase Slingshot

4-level benchmark (cold/interior/index/warm) against Tigris with 100K persons.
Results in README.md. Key finding: Q5/Q6/Q7 cold penalty (~1.3s) is from 40
sequential seekable frame range GETs. Most queries sub-330ms cold.

---

## Phase Slingshot: Seekable Frame + Full Group Background Fetch
> After: Phase Baseline · Before: Phase Beacon

The highest-impact single change. When a seekable frame miss triggers a range GET
for one frame, also submit the entire page group to the prefetch pool as a background
job. The sync read returns the requested page immediately (~16KB range GET, ~25ms).
By the time the query reads the next page in that group, the background worker has
fetched the full group (~12MB, one GET) and all pages are cached.

This eliminates the serial 40-fetch penalty on Q5/Q6/Q7 without any Kuzu integration.
The seekable frame path gives low latency for the first page; the background full-group
fetch gives low latency for subsequent pages.

### a. Background group fetch after frame miss
- [ ] In `readOnePage`, after `fetchAndStoreFrame` returns, submit the page group ID
      to the prefetch pool (which fetches full groups via `fetchAndStoreGroup`)
- [ ] The group state stays NONE (not PRESENT) so individual page reads still check
      bitmap first, falling back to S3 only if the background fetch hasn't finished
- [ ] Prefetch workers use the full-group decode path (all frames), not per-frame

### b. Dedup: don't re-submit groups already in prefetch queue
- [ ] Track which groups have been submitted to avoid duplicate background fetches
- [ ] Use a `submittedGroups` set on TieredFileInfo, checked before submitPrefetch

### c. Measure impact
- [ ] Re-run 4-level benchmark
- [ ] Expected: Q5/Q6/Q7 cold drops from ~1.3s to ~300-500ms (one full-group GET
      instead of 40 frame GETs)
- [ ] Track: S3 fetch count should drop from 40 to ~3-5 per query

---

## Phase Beacon: Structural Page Pinning
> After: Phase Slingshot · Before: Phase Volley

Eager-fetch structural pages (page 0, catalog, metadata) during `openFile()` so
they're available before the first Cypher query runs. Currently these are fetched
on-demand during `Database()` construction, adding latency to cold open.

### a. Identify structural page ranges
- [ ] Read page 0 from S3 during `openFile()` to extract `catalogPageRange` and
      `metadataPageRange` from the DatabaseHeader
- [ ] Parse the Kuzu serialization format (magic bytes, storage version, then
      two PageRange structs: startPageIdx + numPages each as uint32_t)
- [ ] Store ranges on TieredFileInfo as pinned page sets

### b. Parallel eager fetch
- [ ] Compute which page groups contain structural pages
- [ ] Fetch those groups in parallel via prefetch pool during `openFile()`
- [ ] Block `openFile()` return until all structural groups are cached
- [ ] Mark structural pages in bitmap with a "never evict" flag

### c. Measure impact
- [ ] Expected: cold open latency for `Database()` construction drops by ~50-100ms
- [ ] Structural pages are ~1-2 page groups for a 25MB DB

---

## Phase Volley: Per-Table Prefetch Schedules
> After: Phase Beacon · Before: Phase Cypher

turbolite tracks miss counters per B-tree and uses different schedules for SEARCH
vs lookup. turbograph needs the same, adapted for graph:

- **Edge traversal schedule** (aggressive): for CSR scans where the query walks
  adjacency lists. Similar to turbolite's SEARCH schedule. On a miss in a
  relationship table's page range, ramp prefetch hard: `[0.3, 0.3, 0.4]`.
- **Node lookup schedule** (conservative): for hash index lookups on node primary
  keys. Similar to turbolite's lookup schedule. Point queries rarely benefit from
  prefetch: `[0.0, 0.0, 0.0]` (three free hops).
- **Property scan schedule** (moderate): for scanning node/rel properties in
  column chunks. Moderate prefetch: `[0.2, 0.3, 0.5]`.

### a. Page-to-table mapping
- [ ] During `openFile()` structural page parse (Phase Beacon), extract the
      per-table PageRange metadata from the metadataPageRange
- [ ] Build a `page -> table_id` lookup (sorted interval map, binary search)
- [ ] Store on TieredFileInfo alongside the manifest

### b. Per-table miss counters
- [ ] Replace global `consecutiveMisses` with `HashMap<table_id, uint8_t>`
- [ ] On cache miss, look up the page's table, increment that table's counter
- [ ] Reset counter for a table when that table has a cache hit
- [ ] Select schedule based on table type (relationship = edge, node = lookup)

### c. Schedule selection
- [ ] Relationship tables (CSR): edge traversal schedule
- [ ] Node tables with hash index access: node lookup schedule
- [ ] Property-only access (column scans): property scan schedule
- [ ] Configurable via env vars: `PREFETCH_EDGE`, `PREFETCH_LOOKUP`, `PREFETCH_SCAN`

### d. Measure impact
- [ ] Expected: multi-table join queries (Q5/Q6/Q7) no longer over-prefetch
      from node lookups escalating the global counter
- [ ] Expected: point queries (Q4) stay fast with conservative schedule

---

## Phase Cypher: Query Plan Frontrunning
> After: Phase Volley · Before: Phase Column

Parse Kuzu's query plan before execution to prefetch tables proactively. This is
turbolite's most impactful optimization: knowing which tables a query touches
before the first page read eliminates reactive prefetch latency entirely.

### a. Kuzu EXPLAIN integration
- [ ] Before executing a query, run `EXPLAIN` on the Cypher statement
- [ ] Parse the physical plan output for operator types:
      - `SCAN` (full table access) -> bulk prefetch all groups
      - `INDEX_LOOKUP` (hash index) -> prefetch index pages only
      - `HASH_JOIN` -> prefetch both sides
      - `RECURSIVE_JOIN` / `EXTEND` -> prefetch relationship table
- [ ] Map operator table references to page group ranges via manifest metadata

### b. Proactive dispatch
- [ ] For SCAN operators: submit ALL page groups for that table to prefetch pool
      before the query engine reads the first page
- [ ] For INDEX_LOOKUP: prefetch hash index page groups only (conservative)
- [ ] For multi-table joins: submit all tables' groups in parallel
- [ ] Implementation: hook into Kuzu's `Connection::query()` or provide a
      `prepareAndPrefetch(cypher)` API

### c. Fallback to reactive
- [ ] If EXPLAIN fails or returns unexpected output, fall back to per-table
      reactive schedules (Phase Volley)
- [ ] If a query accesses pages not predicted by the plan, reactive prefetch
      handles them normally

### d. Measure impact
- [ ] Expected: multi-table cold queries drop to near-warm latency because all
      table groups are prefetched in parallel before the first page read
- [ ] Expected: Q5/Q6/Q7 cold should approach ~100-200ms (parallel group
      fetches for HasInterest + LivesIn + Person)

---

## Phase Column: Structure-Aware Page Grouping
> After: Phase Cypher · Before: (future)

Only pursue if benchmarks show page group misses are the bottleneck (not query
execution time). Re-organize how pages are grouped in S3 to match graph access
patterns.

### a. Column-aware grouping
- [ ] Co-locate CSR offset/length pages with edge data pages in same group
- [ ] Group column chunks within the same node group together
- [ ] One edge traversal step = one S3 GET (CSR header + edge data)

### b. Neighborhood-aware prefetch
- [ ] When traversing edges, destination node IDs are known from CSR data
- [ ] Prefetch page groups containing destination nodes before query engine asks
- [ ] Unique to graphs: the data itself tells you where to go next

### c. Column-selective fetch
- [ ] Kuzu is columnar: query accessing only `name` and `age` shouldn't fetch
      all 20 columns
- [ ] Organize page groups per-column for selective range GETs
- [ ] Requires manifest metadata mapping columns to page groups

---

## Future

### Tiered eviction priorities
- Structural pages (catalog, PIP, CSR headers): never evicted
- Edge adjacency data: evicted only under pressure
- Property columns: evicted first

### Runtime schedule tuning
- `turbograph_config_set('prefetch_edge', '0.4,0.3,0.3')` via Cypher
- Per-connection schedule overrides without VFS restart
- `turbograph-tune` CLI tool: sweep schedule grid against real queries

### Larger benchmarks
- 1M persons (~250MB DB, multiple page groups)
- 10M persons (~2.5GB DB, S3 bandwidth becomes the bottleneck)
- Compare against Neo4j Aura, Amazon Neptune
