# turbograph

S3-backed tiered storage for [LadybugDB](https://github.com/ladybug-db/ladybug) (Kuzu graph database fork). Serve graph databases from object storage with transparent caching, compression, and adaptive prefetch.

Sister project to [turbolite](https://github.com/nicholasgasior/turbolite) which does the same for SQLite.

## How it works

turbograph intercepts LadybugDB's file system calls and serves pages from a three-tier hierarchy:

1. **Dirty pages** (in-memory) - raw, uncompressed writes
2. **Local disk file** (cache) - uncompressed pages at natural offsets via pread/pwrite
3. **S3/Tigris** (durable) - zstd-compressed page groups, immutable versioned keys

Cache misses trigger transparent S3 fetches with adaptive prefetch. Writes go to local disk first, then flush to S3 in the background. The manifest (JSON on S3) is the atomic commit point.

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

**Cold** = every page fetched from S3 (no local cache), iter=1 p50 (excludes noisy first cold iteration).

**Warm** = all pages on local disk.

Key results:
- **Q8 is 182x faster than Neo4j** (19ms vs 3.5s) for 2-hop graph traversals
- **7 of 9 queries faster than Neo4j** at warm cache level
- **Sub-330ms cold** for all queries except Q2 (multi-stage join)
- Seekable frames + Slingshot prefetch: first page via 16KB range GET, rest via background full-group fetch. 7-8 S3 fetches per cold query.

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
make bench LADYBUG_DIR=../ladybug-fork

# Deploy benchmark to Fly.io
fly deploy --config bench/fly.toml --remote-only
```

See [bench/README.md](bench/README.md) for benchmark details and configuration.

## Architecture

```
Read:   dirty map -> bitmap check -> pread local file -> S3 fetch + decompress + cache
Write:  raw page -> dirty map -> (sync) -> pwrite local + encode + S3 upload
Cold:   manifest GET -> page group GETs (parallel) -> decompress -> local cache
```

### Page groups

Pages are grouped into 2048-page chunks (8MB at 4KB page size). Each page group is one S3 object, zstd-compressed per page. Cache misses fetch the entire group and populate the local file + bitmap.

### Prefetch

Hop-based adaptive prefetch: on consecutive cache misses, progressively fetch more neighboring page groups. Default schedule: 33% / 33% / remainder across 3 hops.

### Manifest

JSON manifest on S3 is the atomic commit point. Contains version, page count, and immutable page group keys (`{prefix}/pg/{groupId}_v{version}`). Old versions are never overwritten; GC cleans up stale keys.

## Project Structure

```
include/     - Public headers (S3 client, codec, manifest, bitmap, VFS)
src/         - Implementation
stubs/       - Minimal LadybugDB interface stubs for standalone builds
third_party/ - httplib (vendored)
test/        - Unit + S3 integration tests (82 tests)
bench/       - Cypher benchmark (requires full LadybugDB)
```

## Dependencies

- C++20 compiler (clang-19+ or GCC 13+)
- OpenSSL (for S3 SigV4 signing + httplib TLS)
- zstd (compression)
- LadybugDB (for benchmark only)

## License

MIT
