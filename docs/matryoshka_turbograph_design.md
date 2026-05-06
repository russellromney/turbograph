# Matryoshka Turbograph Design

## Goal

Make Turbograph primarily a graph-aware adapter on top of Turbolite, not a
second object-store replication system. The storage stack should be:

```text
Ladybug/Kuzu graph files
        |
SqliteGraphFileSystem
        |
SQLite application file / page container
        |
SQLite VFS = turbolite
        |
Turbolite cache, manifest, object store, sync, replication
```

This is SQLite as a better `fopen()`: graph pages are opaque records in a
SQLite container, and Turbolite owns physical distribution of that container.

## What The Spike Proved

- 100k graph loads and all nine kuzudb-study queries work through
  `vfs=turbolite-s3`.
- Cold Tigris reads work from an empty local Turbolite cache.
- A stronger fresh clone works when the clone uses:
  - the same Turbolite prefix,
  - the same SQLite DB identity/name,
  - the same logical graph DB path stored in `graph_files`,
  - a writable local directory for Ladybug/Kuzu sidecars.
- The best measured 100k setting so far is `TURBOLITE_PAGES_PER_GROUP=64` and
  `TURBOLITE_SUB_PAGES_PER_FRAME=8`.
- Request latency dominates once groups get too small. Tiny one-frame fetches
  save bytes but lose badly on GET count.

## Product Shape

Expose this as a backend, not a benchmark mode:

```text
turbograph_backend = matryoshka
turbograph_graph_id = <stable logical graph id>
turbograph_sqlite_store = <local sqlite container path>
turbograph_turbolite_prefix = <remote prefix>
turbograph_publish_mode = explicit
```

The user-facing API should look like:

```text
open graph
write/query locally
checkpoint graph
publish graph epoch
open graph epoch from another cache
```

The implementation can still use SQLite/Turbolite environment wiring while the
Kuzu extension load path is constrained, but the semantics should be named in
Turbograph, not left as shell ceremony.

## Identity Contract

The current spike has too many implicit identities. Production should collapse
them into one explicit graph identity.

Current identities:

- Ladybug/Kuzu database path.
- SQLite page-store filename.
- `graph_files.file_id`.
- Turbolite object prefix.
- Local sidecar path for Kuzu WAL/checkpoint files.

Desired identity:

```text
graph_id = stable logical graph identity
sqlite_store_id = derived from graph_id
graph_file_id = derived from graph_id, not local absolute path
turbolite_prefix = user/project/graph_id[/epoch]
sidecar_dir = local scratch, not part of remote identity
```

This likely means `SqliteGraphFileSystem` should map the intercepted Kuzu data
file path to a canonical file id before storing rows. Absolute local paths
should never become remote identity.

## Publish Contract

The spike proved that flushing SQLite pages is necessary but not always
sufficient. A real publish boundary should be explicit:

1. Stop or quiesce graph writes.
2. Run Ladybug/Kuzu checkpoint.
3. Drain/close graph file handles enough that sidecar catalog state is sealed.
4. Run SQLite checkpoint or otherwise guarantee the SQLite container image is
   coherent for Turbolite.
5. Run `turbolite_flush_to_storage()`.
6. Record/pin the published Turbolite manifest/version as the readable epoch.

Open question: whether step 3 can be a lighter API than full database close.
The crash probes suggest we need a real Kuzu catalog/sidecar lifecycle barrier,
not just `CHECKPOINT; turbolite_flush_to_storage()`.

## Read Contract

Readers should open a named graph epoch:

```text
open graph_id at latest published epoch
open graph_id at manifest/version N
```

Readers should not depend on mutable writer-local sidecars. If Kuzu requires a
local sidecar path, Turbograph should create a fresh scratch directory and map
logical graph identity to the remote SQLite container internally.

## Write Contract

Start conservative:

- Single writer per graph prefix.
- Multiple readers pinned to published epochs.
- Writes are local-first.
- Publish is explicit.
- Writer may continue after publish, but new writes belong to the next epoch.

Only after this is solid should we consider multi-writer or live-reader
semantics.

## Prefetch Contract

Keep the first production pass below SQLite:

- Turbolite handles generic SQLite page prefetch.
- Default graph workload setting: `pages_per_group=64`,
  `sub_pages_per_frame=8`, search prefetch `0.3,0.3,0.4`, lookup prefetch
  `0,0,0`.

Then add graph-aware hints above SQLite:

- Map Kuzu table/CSR/page metadata to graph file page ranges.
- Use those hints to warm likely SQLite rows/pages before executing known graph
  traversal shapes.
- Avoid trying to teach Turbolite about Kuzu internals until measurements prove
  the above- SQLite hint layer is insufficient.

## Current Blockers

1. **Scale crash**: 250k and 1M runs segfault during `COPY`/load.
   First debugging target is local `sqlite` mode, then local `turbolite`, then
   `turbolite-s3`.

2. **Publish barrier**: crash probes show that a page flush is not enough to
   guarantee a remotely readable Kuzu catalog. Need a clean publish API.

3. **Identity cleanup**: absolute local paths leak into `graph_files.file_id`.
   True clone currently works only when the logical graph path is preserved.

4. **Native baseline**: old `TieredFileSystem` baseline currently fails before
   benchmark measurements on the current Tigris env.

5. **Extension integration**: Matryoshka backend is still activated by env vars
   and benchmark construction paths. It needs a first-class extension option
   surface.

## Next Implementation Slices

### Slice 1: Canonical Graph File IDs

Add an optional `fileIdOverride` or mapper to `SqliteGraphFileSystemConfig`.
When the intercepted Kuzu data file path matches `dataFilePath`, store it under
the canonical graph id rather than the local absolute path.

Proof:

- Load graph from local path A.
- Clone from local path B with empty local SQLite/cache.
- Same Turbolite prefix and graph id.
- All queries pass without preserving path A.

### Slice 2: Reproduce And Fix The Scale Crash

Run the same 250k load matrix:

- `sqlite`
- `turbolite`
- `turbolite-s3`

with debug symbols under `lldb`. If it crashes in `sqlite`, focus on
`SqlitePageStore`/`SqliteGraphFileSystem`; if only in Turbolite modes, focus on
SQLite VFS behavior under large WAL/COPY pressure.

### Slice 3: Publish API

Add a small `turbograph_publish()` path for Matryoshka:

- graph checkpoint,
- graph close/drain boundary if available,
- SQLite checkpoint,
- Turbolite flush,
- return manifest/version/counter summary.

The benchmark should call this instead of manually sequencing flushes.

### Slice 4: Product Backend Option

Introduce a named backend mode:

```text
TURBOGRAPH_BACKEND=matryoshka
TURBOGRAPH_GRAPH_ID=<id>
TURBOGRAPH_TURBOLITE_PREFIX=<prefix>
```

and derive the lower-level SQLite/Turbolite settings from that.

