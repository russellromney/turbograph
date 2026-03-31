# turbograph Roadmap

## Phase Baseline: Tigris Benchmark
> Before: Phase Beacon

Benchmark the existing VFS as-is against Tigris. Establish cold/warm numbers for the 9 kuzudb-study Cypher queries with 100K persons. This is the baseline that all optimizations are measured against.

### a. Deploy benchmark to Fly
- [ ] Create Fly app `turbograph-bench` in IAD
- [ ] Build Dockerfile (turbograph + ladybug-fork)
- [ ] Deploy with perf-8x (8 CPU, 64GB RAM)
- [ ] Verify data load + checkpoint works

### b. Run baseline benchmark
- [ ] Cold queries: clear bitmap before each query, all reads from Tigris
- [ ] Warm queries: bitmap populated, local cache file has pages
- [ ] Record: per-query latency (p50), S3 fetch count, S3 bytes fetched
- [ ] Record: cold/warm ratio per query
- [ ] Compare against turbolite numbers on same hardware

### c. Identify bottlenecks
- [ ] Is cold penalty dominated by TTFB (many small fetches) or bandwidth (few large fetches)?
- [ ] Which queries show the worst cold/warm ratio?
- [ ] How many page groups does each query touch?
- [ ] Are there sequential S3 waits (one fetch blocks the next)?

---

## Phase Beacon: Catalog + CSR Header Pinning
> After: Phase Baseline · Before: Phase Cypher

Low-hanging fruit for cold start. Pin structural pages that every query needs so they're available before the first Cypher statement executes.

### a. Identify pinnable pages
- [ ] Catalog/schema pages (node/rel table definitions, type info)
- [ ] PIP (Page Index Pages) for all tables
- [ ] CSR offset + length columns for relationship tables
- [ ] Hash index root pages for node primary keys

### b. Eager fetch on VFS open
- [ ] During `openFile()`, identify page groups containing structural pages
- [ ] Fetch them in parallel before returning the file handle
- [ ] Mark as high-priority in bitmap (never evicted)

### c. Measure impact
- [ ] Re-run baseline benchmark with pinning enabled
- [ ] Expected: cold penalty drops significantly for queries that traverse edges
- [ ] Track: how many page groups are pinned, time spent in eager fetch

---

## Phase Cypher: Query Plan Frontrunning
> After: Phase Beacon · Before: Phase Column

Parse Kuzu's query plan to prefetch tables before the first page read. Same concept as turbolite's EQP frontrunning, adapted for Cypher.

### a. Kuzu EXPLAIN integration
- [ ] Execute `EXPLAIN` on Cypher query, parse output for scan/lookup operations
- [ ] Identify which node tables and relationship tables will be accessed
- [ ] Map table names to page group ranges via manifest metadata

### b. Prefetch dispatch
- [ ] For SCAN operations: submit all page groups for that table to prefetch pool
- [ ] For index lookups: prefetch hash index pages + CSR headers
- [ ] Fire all prefetches before the query engine reads the first page

### c. Per-table miss tracking
- [ ] Track consecutive misses per node/rel table (not globally)
- [ ] Multi-hop traversal (Person -> KNOWS -> Person -> LIVES_IN -> City) tracks each independently
- [ ] Prevent one table's misses from escalating prefetch on unrelated tables

### d. Measure impact
- [ ] Re-run benchmark with frontrunning enabled
- [ ] Expected: multi-table queries (Q2, Q5, Q6, Q7) see fewer sequential S3 waits
- [ ] Track: prefetch hit rate, wasted prefetch bytes

---

## Phase Column: Structure-Aware Page Grouping
> After: Phase Cypher · Before: (future)

Only pursue if benchmarks show page group misses are the bottleneck (not query execution time).

### a. Column-aware grouping
- [ ] Co-locate CSR offset/length pages with edge data pages in same group
- [ ] Group column chunks within the same node group together
- [ ] One edge traversal step = one S3 GET (CSR header + edge data)

### b. Neighborhood-aware prefetch
- [ ] When traversing edges, destination node IDs are known from CSR data
- [ ] Prefetch page groups containing destination nodes before query engine asks
- [ ] Unique to graphs: the data itself tells you where to go next

### c. Column-selective fetch
- [ ] Kuzu is columnar: query accessing only `name` and `age` shouldn't fetch all 20 columns
- [ ] Organize page groups per-column for selective range GETs
- [ ] Requires manifest metadata mapping columns to page groups

---

## Future

### Tiered eviction priorities
- Structural pages (catalog, PIP, CSR headers): never evicted
- Edge adjacency data: evicted only under pressure
- Property columns: evicted first

### Cross-table prediction
- Track which tables appear together in queries
- Build confidence-scored pattern table
- Prefetch correlated tables on first cache miss
