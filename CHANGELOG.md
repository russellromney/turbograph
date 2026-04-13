# Changelog

## Phase Vault -- 2026-04-02

Page-level encryption at rest. CTR mode for local NVMe cache (zero overhead,
deterministic), GCM mode for S3 frames (authenticated, random nonce).

- `crypto.h`/`crypto.cpp`: AES-256-CTR and AES-256-GCM via OpenSSL EVP
- `TieredConfig::encryptionKey`: optional 32-byte key enables encryption
- Local cache: CTR encrypt on pwrite, CTR decrypt on pread (page_num as IV)
- S3 frames: GCM encrypt after zstd compression, GCM decrypt before decompression
- Per-frame GCM: each seekable frame gets its own random 12-byte nonce
- Manifest `encrypted` flag: opening encrypted DB without key gives clear error
- Key via env var `TURBOGRAPH_ENCRYPTION_KEY` (hex) or extension option
- `parse_hex_key()`: 64-char hex string to 32-byte key
- 14 crypto unit tests (CTR round-trip, deterministic IV, wrong key, GCM auth
  rejection, tampered data, empty payload, 1MB payload, hex parsing)
- 3 VFS integration tests (encrypted write-sync-read, wrong key reads garbage,
  manifest flag serialization)

## Phase Cypher -- 2026-04-01

Query plan frontrunning. Before query execution, walk the logical plan to
proactively prefetch all tables the query will touch.

- `extractTablesFromPlan()` walks logical plan tree for SCAN_NODE_TABLE + EXTEND operators
- Collects both node table IDs and relationship table IDs (including neighbor nodes)
- `prefetchTables()` converts table IDs to page group ranges, submits to prefetch pool
- Handles prepare failures gracefully (returns empty sets, falls back to reactive)
- Tests: node scan returns node IDs, edge traversal returns both, multi-table join
  returns all tables, invalid Cypher returns empty (no crash)

## Phase Volley (catalog) -- 2026-04-01

Per-table prefetch schedules. Parse metadata to build page-to-table mapping,
auto-select prefetch schedule based on table type.

- `TablePageMap`: sorted interval map with O(log n) binary search lookup
- `TableMissCounters`: lock-free per-table atomic miss counters
- `table_map_builder.cpp`: raw metadata parser using Kuzu's Deserializer, walks
  StorageManager binary format to extract PageRanges from all table types
- Per-table schedule selection in readOnePage: relationship tables get scan (aggressive),
  node tables get lookup (conservative)
- Cache hit resets per-table counter to prevent stale escalation
- `buildTablePageMap()` UDF path for manual rebuild after data changes
- 24 unit tests (data structures) + 3 extension integration tests

## Phase Extension -- 2026-04-01

LadybugDB extension integration. VFS, UDFs, metadata parser, and plan prefetch
work against upstream LadybugDB (no fork needed).

- TieredFileSystem registered via LadybugDB's VirtualFileSystem
- `turbograph_config_set(key, value)` UDF: switch prefetch schedules, manage table map
- `turbograph_config_get(key)` UDF: query prefetch state, S3 fetch counters
- Metadata parser callback wired into `openFile()` for per-table schedule selection
- Plan prefetch via `extractTablesFromPlan()` for proactive table fetching
- `make test-extension` target: builds and runs 8 extension tests against LadybugDB
- Tests: no-credentials load, UDF registration, unknown key error, config round-trip,
  bad float parsing, S3 counter reads, TFS state mutation, custom schedule override

## Phase Volley (VFS side) -- 2026-03-31

Named prefetch schedules with per-query switching.

- Three schedule slots: `scan` (aggressive, [0.3, 0.3, 0.4]), `lookup` (conservative, [0, 0, 0]), `default` ([0.33, 0.33])
- `setActiveSchedule(name)` / `getActiveSchedule()` API on TieredFileSystem
- `setSchedule(name, hops)` to override schedule values at runtime
- Benchmark sets schedule per query via `QueryDef.schedule` field
- Env vars: `PREFETCH_SCAN`, `PREFETCH_LOOKUP`, `PREFETCH_DEFAULT`
- Foundation for `turbograph_config_set` UDF when built as LadybugDB extension

## Phase Beacon -- 2026-03-31

Eager-fetch structural pages during `openFile()`.

- Parse page 0 (database header) to extract `catalogPageRange` and `metadataPageRange`
- Fixed binary layout: "LBUG" magic at offset 0, catalog PageRange at offset 12, metadata at offset 20
- Compute page groups containing structural pages, fetch in parallel via prefetch pool
- `openFile()` blocks until all structural groups are cached
- Skips gracefully for non-Kuzu data files (no LBUG magic = no eager fetch)
- `Database()` construction finds structural pages already in local cache

## Phase Slingshot -- 2026-03-31

Background full-group fetch after seekable frame miss.

- On seekable frame cache miss: return requested page via 16KB range GET immediately
- Submit full page group to prefetch pool as background job
- Group stays FETCHING; subsequent page reads wait for background worker
- Slingshot flag on PrefetchJob bypasses `tryClaimGroup` (group already FETCHING)
- Dedup via `slingshotSubmitted` set prevents duplicate background submissions
- Results: Q5/Q6/Q7 cold 1,300ms -> 44-154ms (up to 30x faster), S3 fetches 40 -> 7-8

## Seekable Compressed Frames -- 2026-03-31

Per-frame zstd compression with S3 range GETs.

- Each page group encoded as multiple independent zstd frames (default 4 pages/frame)
- `FrameEntry` stores offset, compressed length, and actual page count per frame
- Manifest `frameTables` + `subPagesPerFrame` fields (backward-compatible with legacy)
- `encodeSeekable()` / `decodeFrame()` / `extractPageFromFrame()` codec functions
- `decodeFrame` uses `ZSTD_getFrameContentSize` for reliable decompression sizing
- `fetchAndStoreFrame` fetches single frame via HTTP range GET
- `fetchAndStoreGroup` dispatches to seekable or legacy path based on manifest
- Sync path (`flushPendingPageGroups`) encodes seekable format, merges with old S3 data
- 15 codec + manifest tests including partial frames, non-divisible groups, wrong-estimate regression

## Cache Levels -- 2026-03-31

Selective cache eviction for 4-level benchmarking.

- Page classification via tracking bitmaps: structural (catalog, metadata) and index (PIP, hash)
- `beginTrackStructural()` / `beginTrackIndex()` / `endTrack()` for page classification
- `clearCacheAll()`: nuke everything (cold benchmark)
- `clearCacheKeepStructural()`: keep structural, evict index + data (interior benchmark)
- `clearCacheKeepIndex()`: keep structural + index, evict data (index benchmark)
- 4-phase benchmark: cold -> interior -> index -> warm

## Cold-Open Bug Fix -- 2026-03-31

Fixed seekable frame fetch marking group PRESENT prematurely.

- Root cause: after fetching one seekable frame, `readOnePage` marked the entire group
  PRESENT. Subsequent pages in the same group saw PRESENT + bitmap miss, returned zeros.
- Fix: after seekable frame fetch, reset group to NONE (later changed to FETCHING for slingshot)
- 10 cold-open S3 integration tests: basic restart, large groups, different cache dirs,
  page 0 first read, sequential byte reads, getFileSize, multiple restarts, fileOrPathExists,
  seekable restart, size-then-read

## Initial Release -- 2026-03-31

Extracted from ladybug-fork/extension/tiered/ into standalone project.

- S3-backed tiered storage for LadybugDB (Kuzu graph database fork)
- Three-tier page hierarchy: dirty (memory), local disk (pread/pwrite), S3/Tigris (zstd compressed)
- Page groups: 2048 pages per group (8MB at 4KB page size), one S3 object each
- Immutable versioned keys: `{prefix}/pg/{groupId}_v{version}`
- Manifest (JSON on S3) as atomic commit point
- Adaptive hop-based prefetch with background worker pool
- S3 client with SigV4 signing, connection pool, range requests
- Chunk codec with offset-header format for per-page compressed data
- Page bitmap for local cache tracking
- Local-file-as-cache architecture (pread/pwrite, compression only for S3)
- Minimal Kuzu interface stubs for standalone builds
- httplib fetched via CMake FetchContent (not vendored)
- 7 unit test suites, 5 S3 integration test suites
- Cypher benchmark with Fly.io deployment (Dockerfile + fly.toml)
