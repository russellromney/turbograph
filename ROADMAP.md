# turbograph Roadmap

Completed work is in CHANGELOG.md.

## Phase Extension: LadybugDB Extension Integration
> Before: Phase Volley (catalog)

Build turbograph as a proper LadybugDB extension. This unlocks UDFs, catalog
access, and query plan hooks that the VFS can't do standalone.

### a. Extension wrapper
- [ ] Create `extension/turbograph/` in ladybug-fork
- [ ] Register TieredFileSystem via LadybugDB's VirtualFileSystem
- [ ] Link turbograph static library + liblbug

### b. turbograph_config_set UDF
- [ ] Register scalar function: `CALL turbograph_config_set(key, value)`
- [ ] Keys: `prefetch` (set active schedule), `prefetch_scan`, `prefetch_lookup`
- [ ] UDF holds pointer to TieredFileSystem, calls setActiveSchedule/setSchedule
- [ ] Per-connection: each Connection gets its own schedule state

### c. turbograph_info UDF
- [ ] `CALL turbograph_info()` returns current config, cache stats, S3 fetch count
- [ ] Useful for debugging and benchmarking

---

## Phase Volley (catalog): Per-Table Prefetch Schedules
> After: Phase Extension · Before: Phase Cypher

With extension access to the catalog, parse table metadata to build a
page-to-table mapping. Select prefetch schedule automatically based on
which table a page belongs to.

### a. Catalog parse for page-to-table mapping
- [ ] During `openFile()`, use Kuzu's Deserializer to parse metadata pages
- [ ] Extract per-table column PageRanges (node groups, CSR columns, indexes)
- [ ] Build sorted interval map: page number -> (table_id, table_type)
- [ ] Store on TieredFileInfo alongside manifest

### b. Per-table miss counters
- [ ] Replace global `consecutiveMisses` with `HashMap<table_id, uint8_t>`
- [ ] On cache miss, look up page's table, increment that table's counter
- [ ] Reset counter on cache hit for that table
- [ ] Auto-select schedule: relationship tables -> scan, node tables -> lookup

### c. Measure impact
- [ ] Run with 1M persons (~250MB, multiple page groups per table)
- [ ] Expected: multi-table joins no longer over-prefetch from cross-table miss escalation

---

## Phase Cypher: Query Plan Frontrunning
> After: Phase Volley · Before: Phase Column

Parse Kuzu's query plan before execution to prefetch tables proactively.

### a. EXPLAIN integration
- [ ] Before executing a query, run `EXPLAIN` on the Cypher statement
- [ ] Parse physical plan for operator types: SCAN, INDEX_LOOKUP, HASH_JOIN, EXTEND
- [ ] Map operators to table page group ranges via catalog metadata

### b. Proactive dispatch
- [ ] SCAN: submit ALL page groups for that table before first page read
- [ ] INDEX_LOOKUP: prefetch index page groups only
- [ ] Multi-table joins: submit all tables' groups in parallel
- [ ] Hook into Connection::query() or provide prepareAndPrefetch() API

### c. Fallback
- [ ] If EXPLAIN fails, fall back to per-table reactive schedules (Phase Volley)

---

## Phase Column: Structure-Aware Page Grouping
> After: Phase Cypher · Before: (future)

Only pursue if benchmarks show page group misses are the bottleneck.

### a. Column-aware grouping
- [ ] Co-locate CSR offset/length pages with edge data in same group
- [ ] Group column chunks within same node group together
- [ ] One edge traversal step = one S3 GET

### b. Neighborhood-aware prefetch
- [ ] Parse destination node IDs from CSR data on fetch completion
- [ ] Prefetch page groups containing destination nodes before query asks

### c. Column-selective fetch
- [ ] Organize page groups per-column for selective range GETs

---

## Future

### Per-frame state tracking
- Replace group-level FETCHING/PRESENT/NONE with frame-level states
- Each frame gets its own atomic state
- Eliminates slingshot's "keep group FETCHING" workaround
- frameStates array: totalGroups * framesPerGroup entries

### Tiered eviction priorities
- Structural pages (catalog, PIP, CSR headers): never evicted
- Edge adjacency data: evicted only under pressure
- Property columns: evicted first

### turbograph-tune CLI
- Sweep schedule grid against real queries
- Output per-query comparison table with p50, GET count, bytes
- Recommend optimal schedule pair

### Larger benchmarks
- 1M persons (~250MB DB, multiple page groups per table)
- 10M persons (~2.5GB DB, S3 bandwidth becomes bottleneck)
- Compare against Neo4j Aura, Amazon Neptune
