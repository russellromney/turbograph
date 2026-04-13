# turbograph Roadmap

Completed work is in CHANGELOG.md.

## Phase Glacier: Automatic Cache Eviction
> Before: Phase GraphDrift

Production requirement for multi-tenant. Without size limits, turbograph eats all NVMe.

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

## Phase GraphDrift: Subframe Overrides
> After: Phase Glacier . Before: Phase GraphZenith

Without overrides, every checkpoint uploads ~16MB per dirty group. Port of turbolite's Phase Drift. Prerequisite for S3Primary mode.

### a. Manifest + override tracking
- [ ] Add `subframeOverrides` to `Manifest` struct: `map<groupId, map<frameIndex, OverrideInfo>>` where `OverrideInfo = {key, frameEntry}`
- [ ] Same schema as turbolite's `SubframeOverride`
- [ ] Backward-compatible JSON (missing field = no overrides)

### b. Override write path
- [ ] In sync path: when a group has fewer dirty frames than threshold (default: pagesPerGroup/4), upload individual override objects instead of full group
- [ ] Override key format: `pg/{gid}_f{frameIdx}_v{version}` (same as turbolite)
- [ ] GC: old override keys collected alongside old base group keys

### c. Override read path
- [ ] When loading a group, check overrides per frame
- [ ] Fetch from override key instead of base group range GET when override exists
- [ ] Prefetch pool carries overrides per job, applies after base group fetch

### d. Auto-compaction
- [ ] When override count exceeds threshold (default: 8), merge into fresh base group
- [ ] `compactOverrideGroup()`: fetch base + overrides, merge, re-encode, upload
- [ ] Compaction fires at end of sync, or via explicit call

### e. Tests
- [ ] Write single page, verify only sub-frame uploaded
- [ ] Cold read with overrides
- [ ] Override then full rewrite, cold read correct
- [ ] Compaction fires at threshold
- [ ] Manifest round-trip with overrides (JSON backward compat)

---

## Phase GraphZenith: S3Primary Mode
> After: Phase GraphDrift . Before: Phase Apex

Every graph write durable in S3. RPO=0 for graph databases. Port of turbolite's Phase Zenith.

### a. S3Primary sync path
- [ ] On every `syncFile()`, upload dirty frames as overrides + publish manifest to S3
- [ ] Manifest publish IS the atomic commit point
- [ ] No local journal needed for durability (S3 has everything)

### b. UDFs for hakuzu integration
- [ ] `turbograph_sync()`: triggers checkpoint, returns new manifest version
- [ ] `turbograph_get_manifest_version()`: cheap version check (no full manifest fetch)
- [ ] `turbograph_set_manifest(json)`: follower applies remote manifest, invalidates stale cache entries

### c. Journal sequence coordination
- [ ] Manifest includes `journal_seq` field (graphstream position captured at checkpoint)
- [ ] hakuzu writes current graphstream sequence before calling `turbograph_sync()`
- [ ] On follower restore: load manifest, apply turbograph pages, set graphstream replay to `journal_seq`

### d. Cache validation on open
- [ ] On open: fetch S3 manifest, compare with local
- [ ] Version match: cache warm. Mismatch: diff manifests, invalidate changed groups
- [ ] Crash recovery (local ahead of S3): discard local, full cache invalidation

### e. Tests
- [ ] Write, checkpoint, kill, restore from S3 on fresh node
- [ ] Verify data integrity + journal position
- [ ] RPO=0 under SIGKILL
- [ ] Cache validation after external writer

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

### Encryption key rotation
- Download each page group, GCM decrypt with old key, re-encrypt with new key, re-upload
- Atomic manifest update with new version
- GC old S3 objects after rotation
- Same pattern as turbolite's `rotate_encryption_key()`

### Compression dictionary
- Train zstd dictionary on first checkpoint's page data
- Store dictionary in manifest (or as separate S3 object)
- Pass dictionary to zstd compress/decompress in encode/decode paths
- 2-5x better compression on structured columnar data

### turbograph-tune CLI
- Sweep schedule grid against real queries
- Output per-query comparison table with p50, GET count, bytes
- Recommend optimal schedule pair

### Larger benchmarks
- 1M persons (~250MB DB, multiple page groups per table)
- 10M persons (~2.5GB DB, S3 bandwidth becomes bottleneck)
- Compare against Neo4j Aura, Amazon Neptune
