# turbograph Roadmap

Completed work is in CHANGELOG.md.

## Phase Matryoshka: Graph Pages Inside Turbolite
> Spike branch: `codex/graph-on-turbolite-spike`

Deeply silly experiment: instead of teaching turbograph to own a separate
page-group backend forever, put Ladybug/Kuzu file pages into a SQLite database
and open that database through turbolite. If the latency is tolerable, graph
inherits turbolite's manifest, fencing, snapshot, fork, and object-store
machinery.

### a. Minimal SQLite page store
- [x] Add `SqlitePageStore`: `graph_files`, `graph_pages`, and
  `graph_page_hints`
- [x] Support full-page and partial read/write decomposition
- [x] Support file size, truncate, reopen persistence, and zero-filled holes
- [x] Accept an optional SQLite VFS name so the same file can later open via
  `vfs=turbolite`

### b. Prefetch hint escape hatch
- [x] Add per-page hints: `region_kind`, `region_id`, `locality_key`
- [x] Document the page-packing wrinkle: Ladybug pages are 4096-byte BLOBs,
  but SQLite row/B-tree overhead means "one graph page" is not guaranteed to
  occupy exactly one turbolite page
- [x] Split graph page size from SQLite container page size; default container
  pages are 64KiB so many 4KiB graph pages fit in one SQLite/turbolite page
  instead of paying row/B-tree overhead per graph page
- [ ] Teach a graph-facing VFS shim to populate hints from metadata/table-page
  knowledge
- [x] Decide whether prefetch lives above SQLite (graph-aware page cache) or
  below SQLite (turbolite hint ingestion)
  - Current decision: keep graph hints above SQLite first. Below SQLite,
    turbolite sees SQLite B-tree/container pages, not Ladybug/Kuzu table/CSR
    pages; pushing graph locality below SQLite would require rebuilding the
    graph-page-to-SQLite-page mapping as a side channel. Let turbolite continue
    doing generic SQLite page prefetch below the wrapper, and add graph-aware
    warming/lookup hints at the `SqliteGraphFileSystem`/page-store layer.

### c. Ladybug/Kuzu FileSystem shim
- [x] Wrap `SqlitePageStore` in `common::FileSystem`
- [x] Model `openFile`, `readFromFile`, `writeFile`, `truncate`, `syncFile`,
  `glob`, `fileOrPathExists`, temp files, and size tracking
- [x] Track file existence separately from file size so zero-length graph files
  behave like real files
- [x] Verify the shim through the same `FileInfo` surface Ladybug uses
- [x] Add an extension load mode that registers `SqliteGraphFileSystem` before
  `StorageManager::initDataFileHandle`; manual post-constructor registration is
  too late for a real database-path smoke
- [x] Add a real LadybugDB smoke test that registers the SQLite-backed shim
  before `StorageManager`, writes graph data, checkpoints, closes, reopens, and
  reads the graph back from the SQLite page store
- [x] Run the smoke in controlled mode: Ladybug auto-checkpoint disabled,
  checkpoint-on-close disabled, and SQLite WAL autocheckpoint set explicitly
- [ ] Run the same smoke through Ladybug's linked-extension auto-load path
- [x] Measure whether per-write SQLite transactions plus WAL autocheckpoint are
  sufficient; do not add an outer Ladybug-checkpoint transaction unless
  measurements prove it is needed
  - First smoke: 250 node writes + 249 rel writes with Kuzu/Ladybug
    `autoCheckpoint=true`, `checkpointThreshold=0`, SQLite
    `journal_mode=WAL`, `wal_autocheckpoint=100`, `cache_size=0`,
    `synchronous=NORMAL` passed and reopened from the SQLite-backed store.

### d. Turbolite backend trial
- [x] Prove the page-store opens through an explicit SQLite VFS name
- [x] Add a loadable-extension path in the constructor shape
- [x] Expose SQLite `wal_autocheckpoint`, `cache_size`, and `synchronous`
  controls for the SQLite-backed graph page store
- [x] Run a small graph write/read smoke test through SQLite-backed graph pages
- [x] Build or link against a SQLite that exposes `sqlite3_load_extension`, or
  register turbolite's VFS directly before constructing `SqlitePageStore`
- [x] Open the page-store SQLite DB with the `turbolite` VFS
- [x] Run the small graph write/read smoke test with `vfs=turbolite`
- [x] Run many-small-writes and graph-traversal perf through SQLite and
  `vfs=turbolite`
  - First local numbers: SQLite wrapper only: 250 node writes + 249 rel writes
    in 21.870s; 2 traversals in 11ms; graph bytes 4,489,216. Through actual
    `vfs=turbolite`: same writes in 42.707s; 2 traversals in 14ms; graph bytes
    4,489,216. This is debug C++ plus release turbolite and intentionally tiny,
    but it proves the stack is functionally real.
- [x] Add graph benchmark modes for `sqlite` and `turbolite`
  - 1k graph benchmark passed end-to-end for both: CSV generation, COPY load,
    checkpoint, reopen, cold/interior/index/warm query loops, and all 9
    kuzudb-study queries.
  - First debug 1k load/checkpoint numbers: SQLite page-store load 723ms,
    checkpoint 14ms; SQLite page-store through `vfs=turbolite` load 6460ms,
    checkpoint 21ms. Query timings were comparable at this scale, but this is
    too small to make a real performance call.
- [ ] Run graph benchmark against real Tigris for current `TieredFileSystem`
  baseline once `TIGRIS_STORAGE_ACCESS_KEY_ID`,
  `TIGRIS_STORAGE_SECRET_ACCESS_KEY`, and `TIGRIS_STORAGE_ENDPOINT` are present
- [x] Re-enable turbolite loadable-extension S3 mode (`turbolite-s3`) or expose
  an equivalent host-SQLite registration hook; current `turbolite-ffi`
  loadable S3 path is marked as being rewired, so the Matryoshka benchmark can
  use real `vfs=turbolite` locally but not real Tigris through turbolite yet
- [x] Re-enable enough turbolite loadable-extension S3 mode to register
  `turbolite-s3` from a host SQLite process
  - Implemented in the local turbolite checkout by wiring
    `TURBOLITE_BUCKET`, `TURBOLITE_PREFIX`, `TURBOLITE_CACHE_DIR`, and
    `TURBOLITE_ENDPOINT_URL` into `hadb_storage_s3::S3Storage::from_env(...)`
    plus `TurboliteVfs::with_backend(...)`.
- [x] Run graph benchmark through real `vfs=turbolite-s3` using `soup`
  `turbolite/development` secrets
  - 1k graph benchmark passed end-to-end against Tigris through turbolite:
    CSV generation, COPY load, checkpoint, reopen, cold/interior/index/warm
    query loops, and all 9 kuzudb-study queries.
  - First debug 1k Tigris/turbolite-S3 numbers: load 39.930s, checkpoint
    13ms, local disk size 7MB. The benchmark still reports `s3=0/0KB`
    because those counters are wired to turbograph's old `TieredFileSystem`,
    not turbolite's loadable-extension VFS.
- [x] Switch the turbolite-S3 benchmark path to local-then-flush durability
  during write-heavy loads
  - The local turbolite checkout now defaults `turbolite-s3` to
    `LOCAL_CHECKPOINT_ONLY`, starts a background flush loop controlled by
    `TURBOLITE_FLUSH_INTERVAL_MS` (default `15000`), and exposes
    `turbolite_flush_to_storage()` plus SQL-visible backend GET/PUT counters.
    The counters are collected by a `StorageBackend` wrapper so they follow the
    post-hadb storage path instead of a detached concrete-S3 clone.
  - Fresh 1k graph benchmark against real Tigris with SQLite
    `wal_autocheckpoint=1000`: COPY load 903ms, Ladybug checkpoint 12ms,
    explicit turbolite flush 671ms, object-store writes 4 PUTs / 94KB, reads
    0 GETs during the load/flush window. All 9 kuzudb-study queries passed.
  - Follow-up: first cold read logs
    `decode_and_cache_group_static` short final-group warning (`110/118`
    pages) while queries still pass; verify sparse final-group accounting
    before treating cold-read numbers as production-quality.
- [x] Run a larger 100k graph benchmark with cold reads fetched from real
  Tigris through `vfs=turbolite-s3`
  - First 100k attempt proved the load path but exposed a publish-boundary
    bug: after the explicit load flush, closing the graph DB forced one more
    SQLite/VFS sync, leaving 2 dirty groups pending. Cold cache eviction at
    that point could corrupt the local view and produce
    `database disk image is malformed`.
  - Added a post-close `turbolite_flush_to_storage()` before cold-cache
    eviction. The successful 100k run: data generation 530ms, COPY load
    4246ms, Ladybug checkpoint 14ms, first turbolite flush 8503ms for
    5 PUTs / 29MB, post-close flush 9551ms for 15 PUTs / 26MB plus
    9 GETs / 11MB. All 9 kuzudb-study queries passed.
  - Cold query averages fetched from Tigris through turbolite: Q1 2.0s,
    Q2 3.1s, Q3 870ms, Q4 694ms, Q5 1.4s, Q6 1.3s, Q7 1.4s, Q8 791ms,
    Q9 2.1s. Per-query cold reads were roughly 13-33 GETs and 5.5-9MB.
- [x] Sweep smaller seekable groups while preserving slingshot-style prefetch
  - A too-surgical run (`TURBOLITE_PAGES_PER_GROUP=16`,
    `TURBOLITE_SUB_PAGES_PER_FRAME=1`, zero prefetch schedules) cut cold bytes
    sharply but increased request count and latency: e.g. Q1 68 GETs / 3.6MB
    in 3.9-4.1s, Q2 78 GETs / 4.0MB in 4.4-5.0s, Q5 32 GETs / 1.6MB in 5.6s.
    This points at foreground range-GET/request latency, not bandwidth.
  - Hybrid run (`TURBOLITE_PAGES_PER_GROUP=64`,
    `TURBOLITE_SUB_PAGES_PER_FRAME=4`, search prefetch `0.3,0.3,0.4`, lookup
    `0,0,0`, plan-aware/prediction off) kept GET counts near the default run
    while roughly halving cold bytes. Flushes: 11 PUTs / 29MB plus final
    10 PUTs / 11MB. Cold averages: Q1 1.9s, Q2 2.4s, Q3 1.1s, Q4 827ms,
    Q5 1.5s, Q6 1.3s, Q7 1.1s, Q8 978ms, Q9 2.9s.
- [x] Add a repeatable proof runner for real `turbolite-s3`
  - `bench/prove_matryoshka_turbolite_s3.sh` runs fresh-prefix 100k cold
    reads, repeated write/checkpoint cycles, and a grouping matrix through
    `soup run --project turbolite --env development`.
  - The benchmark now accepts `BENCH_BASE_DIR`, `BENCH_TAG_SUFFIX`,
    `BENCH_COLD_ITERATIONS`, `BENCH_WARM_ITERATIONS`, `BENCH_WRITE_CYCLES`,
    and `BENCH_WRITE_CHECKPOINT_EVERY`, so the same binary can exercise fresh
    cache/path cases and bounded write loops without manual source edits.
- [x] Prove repeated write/checkpoint cycles after a published load
  - A first attempt intentionally exposed the publish boundary again:
    reopening for writes after the load-block flush but before the post-close
    flush could hit `database disk image is malformed`. Moving the post-close
    `turbolite_flush_to_storage()` before the write phase fixed it.
  - Real Tigris 100k run with `TURBOLITE_PAGES_PER_GROUP=64`,
    `TURBOLITE_SUB_PAGES_PER_FRAME=4`, 25 writes, checkpoint after every
    write: write phase passed in 942ms, write-cycle flush took 3486ms for
    9 PUTs / 8.3MB, and all 9 cold queries passed afterward.
- [x] Prove a separate-process fresh-cache reopen from Tigris
  - Reused the same remote prefix and SQLite path from a completed 100k run,
    deleted the local turbolite cache directory, skipped reload, and reopened
    in a new process. All 9 queries passed fetched from Tigris; local disk size
    before query execution was 0MB. Cold timings: Q1 982ms, Q2 1.0s,
    Q3 376ms, Q4 409ms, Q5 684ms, Q6 577ms, Q7 434ms, Q8 283ms, Q9 642ms.
- [x] Prove a stronger fresh clone with a different local SQLite/cache path
  - Added `BENCH_DB_PATH` to pin the logical graph DB identity separately from
    the local scratch directory, and `BENCH_FORCE_REUSE_DB=1` to open remote
    state without local artifacts.
  - A 100k publish from one local directory, followed by a reopen from a
    different local SQLite path and empty turbolite cache, passed all 9
    queries against the same Tigris prefix. The clone-side local disk size was
    0MB before query execution. Cold timings: Q1 848ms, Q2 1.1s, Q3 580ms,
    Q4 414ms, Q5 685ms, Q6 474ms, Q7 423ms, Q8 335ms, Q9 585ms.
  - Current identity contract: remote clone needs the same turbolite prefix,
    the same SQLite DB identity/name, the same logical graph DB path stored in
    `graph_files`, and a writable parent directory for Ladybug/Kuzu sidecars
    such as `*.wal`.
- [x] Run a wider 100k grouping/prefetch matrix on real Tigris
  - `ppg32/spf4`: more PUTs and slightly lower bytes than `64/4`, but GET
    count remained similar and latency did not improve materially. Cold:
    Q1 2.1s, Q2 2.5s, Q3 976ms, Q4 695ms, Q5 1.2s, Q6 1.7s, Q7 1.3s,
    Q8 711ms, Q9 1.9s.
  - `ppg64/spf8`: current best proof point. It reads more bytes than `64/4`
    but drops request count enough to win on latency. Cold: Q1 1.2s, Q2 1.4s,
    Q3 760ms, Q4 395ms, Q5 998ms, Q6 846ms, Q7 778ms, Q8 481ms, Q9 1.1s.
  - `ppg128/spf4`: similar request counts to `64/4` with larger reads; not a
    clear win. Cold: Q1 1.9s, Q2 2.5s, Q3 904ms, Q4 772ms, Q5 1.3s,
    Q6 1.2s, Q7 1.4s, Q8 781ms, Q9 1.9s.
- [x] Run an initial crash/interruption probe
  - A 10k `ppg64/spf8` run was killed by timeout during the cold-query loop
    after load, flush, final flush, and Q1/Q2. Reopening the same local path
    and remote prefix in a second process completed all 9 queries. This is not
    a full crash-safety matrix, but it proves a killed reader after publish did
    not poison the image.
- [ ] Complete the crash matrix
  - Added targeted harness knobs (`BENCH_SLEEP_AFTER_LOAD_MS`,
    `BENCH_SLEEP_AFTER_CHECKPOINT_MS`, `BENCH_SLEEP_AFTER_FLUSH_MS`,
    `BENCH_SLEEP_AFTER_FINAL_FLUSH_MS`, and `BENCH_WRITE_DELAY_MS`) so timeout
    kills can land at known phase boundaries.
  - Current result: killing during initial load leaves local Kuzu sidecar state
    (`*.wal.checkpoint`) that must be cleaned before reuse. Force-opening an
    unpublished or identity-mismatched remote prefix yields an empty catalog,
    as expected. Killing after explicit turbolite flush but before a fully
    graceful Kuzu/catalog lifecycle still did not produce a remotely readable
    clone in the small 10k harness; this needs a tighter publish barrier than
    "SQLite pages flushed".
  - The benchmark now exits non-zero on query failures so future crash probes
    cannot silently pass with `Table Person does not exist`.
- [ ] Compare against current turbograph on cold read, hot read, checkpoint
  write amplification, and object GET count
  - Attempted a native `TieredFileSystem` 100k baseline with the same soup
    Tigris credentials. The old path first rejected an `https://` endpoint;
    after stripping the scheme it aborted during manifest cleanup with
    `recursive_mutex lock failed: Invalid argument`. Baseline comparison is
    blocked on repairing the native S3 benchmark path.
- [ ] Run beyond 100k scale
  - 250k and 1M `turbolite-s3` runs both segfaulted during `COPY`/load before
    storage/query measurements. The 1M run generated 10M follow edges
    successfully, then crashed during load. This is the next scale blocker.

### e. Kill criteria
- [x] Measure whether double paging makes cold traversal obviously worse
  - Not obviously worse at 100k over real Tigris. The best current Matryoshka
    setting (`ppg64/spf8`) has cold Q8/Q9 at 481ms/1.1s, while warm/index
    tiers remain tens of milliseconds. Request count, not double paging itself,
    is the main limiter.
- [x] Confirm graph checkpoint boundaries can be represented as SQLite
  transactions without invasive Ladybug changes
  - The required contract is explicit: do the Ladybug/Kuzu checkpoint, close or
    otherwise drain graph/SQLite syncs, then call `turbolite_flush_to_storage()`
    before treating the epoch as remotely readable or before reopening for more
    writes from a clean image.
- [ ] Confirm prefetch hints do not require recreating all current turbograph
  logic below SQLite anyway

### f. Controlled settings for the next smoke
- [x] Clone and validate against a fresh Ladybug checkout at
  `/Users/russellromney/Documents/Github/ladybug-current`; the old
  `/Users/russellromney/Documents/Github/ladybug` checkout is stale and should
  only be treated as historical context
- [x] Run against current Kuzu/Hakuzu with a normal buffer pool; the buffer pool
  is a query/runtime cache, not the page-version boundary
- [ ] Set Kuzu/Ladybug `auto_checkpoint=false` and
  `force_checkpoint_on_close=false`; publish only when the caller chooses to
  materialize a physical graph-page epoch
- [x] Test the aggressive mode with Kuzu/Hakuzu `auto_checkpoint=0`; every
  committed write should force page materialization and touch the SQLite-backed
  "disk" while the buffer pool remains available for query execution
- [x] Set SQLite `journal_mode=WAL`, `wal_autocheckpoint=1000`,
  `cache_size=0`, and a chosen `synchronous` mode before running the
  turbolite-backed smoke

### g. Toward A Real Turbolite-Backed Turbograph
- [x] Write down the product/backend shape in
  `docs/matryoshka_turbograph_design.md`
  - Core decision: Turbograph should become a graph-aware adapter over a
    Turbolite-backed SQLite application file, not a second replication stack.
  - Product contract: explicit graph id, explicit publish epoch, single writer,
    many readers pinned to published epochs.
- [x] Start removing local-path identity from the graph page store
  - Added `SqliteGraphFileSystemConfig::dataFileId` plus
    `TURBOGRAPH_GRAPH_ID` / `turbograph_graph_id` / `BENCH_GRAPH_ID` plumbing.
  - The intercepted Kuzu data file can now be stored under a stable graph id
    rather than the absolute local DB path.
  - Added a FileSystem test proving data written through `/tmp/graph-a/data.kz`
    can be reopened through `/tmp/graph-b/data.kz` when both map to the same
    canonical graph id.
- [ ] Make the SQLite DB/container identity stable too
  - True clone still needs the same SQLite database identity/name because that
    is what Turbolite sees underneath SQLite. Next step: derive SQLite local
    path from graph id while keeping the remote Turbolite prefix stable, or add
    a Turbolite-side logical database identity.
- [ ] Add a real `turbograph_publish()` Matryoshka path
  - It should own: graph checkpoint, graph/catalog drain boundary, SQLite
    checkpoint, Turbolite flush, and returned manifest/version/counters.
- [ ] Debug the 250k/1M COPY segfault with `sqlite`/`turbolite`/`turbolite-s3`
  matrix under `lldb`

---

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
