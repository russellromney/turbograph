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

**TODO**: Initial Tigris benchmarks pending. See ROADMAP.md.

The turbolite equivalent achieves 77-586ms cold query latency on a 1.5GB SQLite database from S3 Express. Graph databases have different access patterns (columnar + CSR vs B-tree), so performance profiles will differ.

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
```

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
