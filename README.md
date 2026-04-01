# turbograph

S3-backed tiered storage VFS + LadybugDB extension for [LadybugDB](https://github.com/LadybugDB/ladybug) (Kuzu graph database). Serve graph databases from object storage with transparent caching, compression, per-table adaptive prefetch, and query plan frontrunning.

## How it works

turbograph intercepts LadybugDB's file system calls and serves pages from a three-tier hierarchy:

1. **Dirty pages** (in-memory) -- raw, uncompressed writes
2. **Local disk file** (NVMe cache) -- uncompressed pages at natural offsets via pread/pwrite
3. **S3/Tigris** (durable) -- zstd-compressed page groups, immutable versioned keys

Cache misses trigger transparent S3 fetches. On cold open, turbograph parses Kuzu's metadata pages to build a page-to-table interval map, then auto-selects prefetch schedules per table type (aggressive scan for relationship tables, conservative lookup for node tables). Before query execution, the logical plan is walked to proactively prefetch all tables the query will touch.

Writes go to local disk first, then flush to S3 on checkpoint. The manifest (JSON on S3) is the atomic commit point.

## Performance

100K persons, 1M follow edges, 25MB graph database on [Tigris](https://tigris.dev) (IAD). Benchmark runs 9 Cypher queries from the [kuzudb-study](https://thedataquarry.com/blog/embedded-db-2/) at four cache levels.

| Query | Cold (S3) | Warm (local) | Neo4j 5.11 |
|---|---|---|---|
| Q1: Top 3 most-followed | 327ms | 300ms | 1,890ms |
| Q2: City of most-followed | 1,000ms | 719ms | 694ms |
| Q3: Youngest cities US | 56ms | 39ms | 44ms |
| Q4: Persons 30-40 by country | 14ms | 10ms | 47ms |
| Q5: Male fine diners London | 44ms | 38ms | 9ms |
| Q6: Female tennis by city | 105ms | 83ms | 23ms |
| Q7: US photographers 23-30 | 36ms | 25ms | 163ms |
| Q8: 2-hop path count | 21ms | 19ms | 3,453ms |
| Q9: Filtered 2-hop paths | 111ms | 78ms | 4,271ms |

**Cold** = every page fetched from S3 (no local cache). **Warm** = all pages on local disk.

Key results:
- **Q8 is 182x faster than Neo4j** (19ms vs 3.5s) for 2-hop graph traversals
- **7 of 9 queries faster than Neo4j** at warm cache level
- **Sub-330ms cold** for all queries except Q2 (multi-stage join)
- Seekable frames + Slingshot prefetch: first page via 16KB range GET, rest via background full-group fetch

Hardware: Fly.io shared-cpu-4x (4 vCPU, 4GB RAM), Tigris S3 same-region (IAD).

## Quick Start

```bash
# Build library + unit tests
make build

# Run unit tests (no S3 credentials needed)
make test

# Run S3 integration tests (needs Tigris credentials)
export TIGRIS_STORAGE_ACCESS_KEY_ID=...
export TIGRIS_STORAGE_SECRET_ACCESS_KEY=...
export TIGRIS_STORAGE_ENDPOINT=https://fly.storage.tigris.dev
make test-s3

# Build benchmark (needs LadybugDB source tree)
make bench LADYBUG_DIR=/path/to/ladybug

# Deploy benchmark to Fly.io
make deploy-bench
```

## Architecture

```
Read:   dirty map -> bitmap check -> pread local -> S3 fetch + decompress + cache
Write:  raw page -> dirty map -> (checkpoint) -> pwrite local + encode + S3 upload
Cold:   manifest GET -> Beacon (structural pages) -> metadata parse -> page group GETs
```

### Page groups

Pages are grouped into 2048-page chunks (8MB at 4KB page size). Each page group is one S3 object, zstd-compressed with seekable multi-frame encoding. Cache misses fetch a single 16KB frame (range GET), then background-fetch the full group.

### Per-table prefetch (Phase Volley)

On cold open, turbograph parses LadybugDB's serialized StorageManager metadata to build a sorted interval map (page number -> table ID + table type). Cache misses look up the table and select the schedule:

- **Relationship tables** (CSR edge data): aggressive scan schedule (0.3 / 0.3 / 0.4)
- **Node tables** (hash index access): conservative lookup schedule (3 free hops)
- **Structural pages** (catalog, metadata): global fallback schedule

Per-table miss counters (lock-free atomics) track consecutive misses independently per table.

### Query plan frontrunning (Phase Cypher)

Before query execution, `extractTablesFromPlan()` walks the logical plan tree to find `SCAN_NODE_TABLE` and `EXTEND` operators, collects all referenced table IDs, and calls `prefetchTables()` to submit their page groups to the background pool. The query starts with data already in-flight.

### Manifest

JSON manifest on S3 is the atomic commit point. Contains version, page count, frame tables (byte offsets for seekable range GETs), and immutable page group keys. Old versions are never overwritten; GC cleans up stale keys.

## Project Structure

```
include/        Public headers (S3 client, codec, manifest, bitmap, VFS, table page map)
src/            Implementation
stubs/          Minimal LadybugDB interface stubs for standalone builds
test/           Unit + S3 integration tests (80+ tests)
bench/          Cypher benchmark + Fly.io deployment (Dockerfile, fly.toml)
extension/      LadybugDB extension (UDFs, metadata parser, plan prefetch)
  include/      Extension headers
  src/          Extension implementation
  test/         Extension integration tests
```

### Extension

The `extension/` directory contains the LadybugDB extension that bridges turbograph with Kuzu internals. It provides:

- **Metadata parser** (`table_map_builder.cpp`): parses StorageManager binary format using Kuzu's `Deserializer` to build the page-to-table map during `openFile()`
- **Plan prefetch** (`plan_prefetch.cpp`): walks Kuzu's logical plan tree to extract table IDs for proactive prefetch
- **UDFs** (`turbograph_config.cpp`): `turbograph_config_set` / `turbograph_config_get` for runtime schedule switching, table map rebuild, S3 counters
- **Extension loader** (`turbograph_extension.cpp`): registers the VFS, metadata parser callback, and UDFs

The extension compiles against LadybugDB headers (requires `LADYBUG_DIR`). The core turbograph library has no LadybugDB dependency beyond the `FileSystem` interface stubs.

## Dependencies

- C++20 compiler (clang-19+ or GCC 13+)
- OpenSSL (S3 SigV4 signing + httplib TLS)
- zstd (compression)
- LadybugDB (for extension + benchmark)

## License

Apache-2.0
