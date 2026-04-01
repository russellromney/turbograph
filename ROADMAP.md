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
> After: Phase Volley · Before: Phase Vault

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

## Phase Vault: Page-Level Encryption and Compression on Disk
> After: Phase Cypher · Before: Phase Column

Encrypt pages at rest on local NVMe cache and in S3. Same architecture as
turbolite: CTR for cache (fast, no overhead), GCM for S3 (authenticated).
Compress then encrypt for S3. OpenSSL provides both ciphers (already linked).

### a. Crypto primitives
- [ ] Add `encryption_key: std::optional<std::array<uint8_t, 32>>` to `TieredConfig`
- [ ] Implement `aes256_ctr_encrypt(data, page_num, key)` / `aes256_ctr_decrypt(data, page_num, key)`
  - IV = page_num as 8-byte LE, zero-padded to 16 bytes
  - No size overhead (ciphertext = plaintext)
  - OpenSSL `EVP_aes_256_ctr`
- [ ] Implement `aes256_gcm_encrypt(data, key)` / `aes256_gcm_decrypt(data, key)`
  - Random 12-byte nonce, prepended to ciphertext
  - 16-byte auth tag appended
  - 28 bytes overhead per frame (nonce + tag)
  - OpenSSL `EVP_aes_256_gcm`
- [ ] Put these in `crypto.cpp` / `crypto.h`
- [ ] Tests: round-trip, wrong key fails, deterministic CTR nonce, GCM auth rejection

### b. Local cache encryption (CTR)
- [ ] In `readOnePage`: after `pread`, CTR decrypt if key is set
- [ ] In `writeOnePage` (dirty page flush to cache): CTR encrypt before `pwrite`
- [ ] In `fetchAndStoreGroup` (S3 -> cache): CTR encrypt each page before `pwrite`
- [ ] Cache hit path: `pread` -> CTR decrypt -> return plaintext
- [ ] Zero size overhead: page offsets and bitmap unchanged
- [ ] Tests: write encrypted, read decrypted, wrong key returns garbage

### c. S3 encryption (GCM)
- [ ] In `flushPendingPageGroups` (encode path): after zstd compress, GCM encrypt each frame
  - Seekable encoding: per-frame GCM (each frame gets own random nonce)
  - Legacy encoding: single GCM wrap around whole compressed blob
- [ ] In `fetchAndStoreGroup` (decode path): GCM decrypt before zstd decompress
- [ ] In `fetchAndStoreFrame` (range GET path): GCM decrypt the individual frame
- [ ] Update `FrameEntry.len` in manifest to include 28-byte GCM overhead
- [ ] Tests: round-trip via S3, tampered blob rejected, manifest frame offsets correct

### d. Key management
- [ ] Key set via `TieredConfig` (application provides the 32 bytes)
- [ ] Env var support: `TURBOGRAPH_ENCRYPTION_KEY` (hex-encoded 64 chars)
- [ ] Extension option: `turbograph_encryption_key` (confidential STRING, hex)
- [ ] Key is per-database, same for all pages and S3 objects
- [ ] Key never stored on disk. Provided at runtime via env var or extension option.
- [ ] Manifest records `"encrypted": true` flag (not the key). Opening an encrypted
  database without a key gives a clear error instead of a cryptic GCM auth failure.
- [ ] No key rotation in MVP (future: re-download, re-encrypt, re-upload all objects)

### e. Compression dictionary (future)
- [ ] Train zstd dictionary on first checkpoint's page data
- [ ] Store dictionary in manifest (or as separate S3 object)
- [ ] Pass dictionary to zstd compress/decompress in encode/decode paths
- [ ] 2-5x better compression on structured columnar data

---

## Phase Glacier: Automatic Cache Eviction
> After: Phase Vault · Before: Phase Column

Today the cache grows unbounded. For production (shared volumes, multi-tenant),
the cache needs a size limit and automatic eviction under pressure.

### a. Cache size limit
- [ ] Add `maxCacheBytes` to `TieredConfig` (0 = unlimited, default)
- [ ] Track current cache size via bitmap `presentCount() * pageSize`
- [ ] Eviction check after every `fetchAndStoreGroup`: if over limit, evict

### b. Eviction priority (tiered)
- [ ] Structural pages (catalog, metadata, page 0): never evicted
- [ ] Index pages (hash index, PIP): evicted only under heavy pressure
- [ ] Data pages (column chunks, CSR edges, overflow): evicted first
- [ ] Use existing `structuralPages` / `indexPages` bitmaps for classification

### c. Eviction strategy
- [ ] Track per-group last access time (or access count) in a parallel array
- [ ] On eviction: find coldest data groups, call `evictLocalGroup()` until under limit
- [ ] `evictLocalGroup` already handles bitmap clear, group state reset, hole punch (Linux)
- [ ] Batch eviction: evict enough groups to get to 80% of limit (avoid thrashing)

### d. Tests
- [ ] Unit: cache fills to limit, eviction triggers, structural pages survive
- [ ] Unit: access tracking updates on read, coldest groups evicted first
- [ ] Integration: continuous queries with small cache limit, verify no crashes or data loss

---

## Phase Orbit: graphstream Replication Integration
> After: Phase Glacier · Before: Phase Apex

Integrate graphstream (Kuzu journal replication) so turbograph's checkpoint
doubles as the hadb snapshot. Same pattern as turbolite + walrust.

### a. Checkpoint as snapshot
- [ ] On checkpoint (doSyncFile), after flushing page groups to S3, update manifest
- [ ] Manifest version is the replication cursor for hakuzu
- [ ] Replicas open with manifest, fetch page groups on demand
- [ ] No separate full-DB snapshot needed: page groups *are* the snapshot

### b. graphstream journal integration
- [ ] After each write transaction, capture journal segment via graphstream
- [ ] Upload journal segments to S3 (graphstream handles this)
- [ ] On follower: apply journal segments to local DB, then checkpoint flushes to page groups
- [ ] Journal segments are the WAL; page groups are the checkpoint

### c. Follower cold start
- [ ] Follower reads manifest from S3 to get latest checkpoint state
- [ ] Beacon fetches structural pages, metadata parser builds table map
- [ ] Follower serves read queries from S3-backed cache immediately
- [ ] Then pulls journal segments since last checkpoint for catch-up

### d. Leader election coordination
- [ ] hakuzu's LeaseStore determines who writes the manifest
- [ ] On promotion: follower catches up journal, takes lease, becomes writer
- [ ] On demotion: leader stops writing, releases lease
- [ ] turbograph is unaware of HA roles; it just reads/writes via the VFS

### e. Tests
- [ ] Integration: leader writes, checkpoint, follower reads via manifest
- [ ] Integration: leader crash, follower promotes, serves queries
- [ ] Unit: manifest version advances on checkpoint

---

## Phase Apex: Structure-Aware Page Grouping
> After: Phase Orbit · Before: (future)

Only pursue if benchmarks show page group misses are the bottleneck.

### a. Locality-aware grouping
- [ ] Co-locate CSR offset/length pages with edge data in same group
- [ ] Group pages within same node group together
- [ ] One edge traversal step = one S3 GET

### b. Neighborhood-aware prefetch
- [ ] Parse destination node IDs from CSR data on fetch completion
- [ ] Prefetch page groups containing destination nodes before query asks

### c. Selective fetch
- [ ] Organize page groups per-table for selective range GETs

---

## Future

### Per-frame state tracking
- Replace group-level FETCHING/PRESENT/NONE with frame-level states
- Each frame gets its own atomic state
- Eliminates slingshot's "keep group FETCHING" workaround
- frameStates array: totalGroups * framesPerGroup entries

### turbograph-tune CLI
- Sweep schedule grid against real queries
- Output per-query comparison table with p50, GET count, bytes
- Recommend optimal schedule pair

### Larger benchmarks
- 1M persons (~250MB DB, multiple page groups per table)
- 10M persons (~2.5GB DB, S3 bandwidth becomes bottleneck)
- Compare against Neo4j Aura, Amazon Neptune
