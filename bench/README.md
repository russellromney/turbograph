# turbograph benchmark

Cypher benchmark from the [kuzudb-study](https://thedataquarry.com/blog/embedded-db-2/): generates a social network (persons, cities, interests, follows), loads via CSV, then benchmarks 9 Cypher queries at four cache levels.

## Cache levels

| Level | What's cached | What's fetched from S3 |
|---|---|---|
| **Cold** | Nothing (fresh TFS + DB per query) | Everything |
| **Interior** | Structural pages (catalog, metadata, page 0) | Index + data pages |
| **Index** | Structural + index pages (PIP, hash index) | Data pages only |
| **Warm** | Everything on local NVMe | Nothing (fresh buffer pool per query) |

## Running locally

Requires LadybugDB source tree built as a static library.

```bash
# Build LadybugDB
cd ../ladybug-fork
cmake -B build/release -DCMAKE_BUILD_TYPE=Release \
  -DCMAKE_C_COMPILER=clang -DCMAKE_CXX_COMPILER=clang++ \
  -DBUILD_SHELL=FALSE -DBUILD_SINGLE_FILE_HEADER=FALSE .
cmake --build build/release --target lbug -- -j$(nproc)

# Build turbograph benchmark
cd ../turbograph
make bench LADYBUG_DIR=../ladybug-fork

# Run (requires Tigris credentials)
export TIGRIS_STORAGE_ACCESS_KEY_ID=...
export TIGRIS_STORAGE_SECRET_ACCESS_KEY=...
export TIGRIS_STORAGE_ENDPOINT=https://fly.storage.tigris.dev
./build/bench/cypher_bench 100000
```

## Deploying to Fly.io

The Dockerfile builds LadybugDB + turbograph from source. Deploy from the parent directory:

```bash
cd ..  # personal-website/
fly deploy --config turbograph/bench/fly.toml --remote-only
```

Requires:
- Fly app `turbograph-bench` with a volume named `bench_data`
- Tigris secrets set: `fly secrets set TIGRIS_STORAGE_ACCESS_KEY_ID=... TIGRIS_STORAGE_SECRET_ACCESS_KEY=... TIGRIS_STORAGE_ENDPOINT=...`

## Configuration

All via environment variables:

| Variable | Default | Description |
|---|---|---|
| `BENCH_PERSONS` | 100000 | Number of person nodes |
| `BUFFER_POOL_MB` | 256 | Kuzu buffer pool size |
| `PREFETCH_HOPS` | 0.33,0.33 | Hop schedule (comma-separated floats) |
| `PREFETCH_THREADS` | 8 | Background prefetch worker threads |
| `SUB_PAGES_PER_FRAME` | 4 | Pages per seekable frame (0 = legacy) |

## Queries

9 Cypher queries covering different graph access patterns:

| # | Query | Pattern |
|---|---|---|
| Q1 | Top 3 most-followed | Full edge scan + aggregation |
| Q2 | City of most-followed | Two-stage: aggregate + join |
| Q3 | Youngest cities (US) | Filter + join + aggregation |
| Q4 | Persons 30-40 by country | Filter + join + count |
| Q5 | Male fine diners in London | Multi-table join (Interest + City) |
| Q6 | Female tennis by city | Multi-table join + aggregation |
| Q7 | US photographers 23-30 | Filter + two joins |
| Q8 | 2-hop path count | Two-hop graph traversal |
| Q9 | Filtered 2-hop paths | Two-hop traversal with filters |
