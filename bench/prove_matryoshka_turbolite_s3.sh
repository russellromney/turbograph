#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BIN="${ROOT}/build-bench-turbolite/bench/cypher_bench"
EXT="${TURBOGRAPH_TURBOLITE_EXTENSION:-/Users/russellromney/Documents/Github/cinch-target/release/turbolite.dylib}"
OUT_DIR="${OUT_DIR:-${ROOT}/build-bench-turbolite/proofs}"
PERSONS="${PERSONS:-100000}"
BUFFER_POOL_MB="${BUFFER_POOL_MB:-1024}"

mkdir -p "${OUT_DIR}"

if [[ ! -x "${BIN}" ]]; then
  echo "missing benchmark binary: ${BIN}" >&2
  echo "build it with: cmake --build ${ROOT}/build-bench-turbolite --target cypher_bench" >&2
  exit 1
fi

if [[ ! -f "${EXT}" ]]; then
  echo "missing turbolite extension: ${EXT}" >&2
  exit 1
fi

run_case() {
  local name="$1"
  shift
  local stamp
  stamp="$(date +%Y%m%d-%H%M%S)"
  local log="${OUT_DIR}/${stamp}-${name}.log"
  local prefix="bench/matryoshka/${stamp}-${name}"
  local base="/tmp/matryoshka-${stamp}-${name}"

  echo "== ${name} =="
  echo "   log: ${log}"
  (
    export TURBOGRAPH_TURBOLITE_EXTENSION="${EXT}"
    export TURBOLITE_BUCKET="${TURBOLITE_BUCKET:-${TIERED_TEST_BUCKET:-}}"
    export TURBOLITE_ENDPOINT_URL="${TURBOLITE_ENDPOINT_URL:-${AWS_ENDPOINT_URL:-}}"
    export TURBOLITE_PREFIX="${prefix}"
    export TURBOLITE_LOCAL_THEN_FLUSH="${TURBOLITE_LOCAL_THEN_FLUSH:-1}"
    export TURBOLITE_FLUSH_INTERVAL_MS="${TURBOLITE_FLUSH_INTERVAL_MS:-15000}"
    export TURBOGRAPH_SQLITE_WAL_AUTOCHECKPOINT="${TURBOGRAPH_SQLITE_WAL_AUTOCHECKPOINT:-1000}"
    export TURBOGRAPH_SQLITE_CACHE_SIZE_PAGES="${TURBOGRAPH_SQLITE_CACHE_SIZE_PAGES:-0}"
    export TURBOGRAPH_SQLITE_SYNCHRONOUS="${TURBOGRAPH_SQLITE_SYNCHRONOUS:-NORMAL}"
    export BUFFER_POOL_MB="${BUFFER_POOL_MB}"
    export BENCH_BASE_DIR="${base}"
    export BENCH_TAG_SUFFIX="${name}"
    export BENCH_COLD_ITERATIONS="${BENCH_COLD_ITERATIONS:-1}"
    export BENCH_WARM_ITERATIONS="${BENCH_WARM_ITERATIONS:-1}"
    "$@"
  ) 2>&1 | tee "${log}"
}

require_s3_env() {
  if [[ -z "${TIERED_TEST_BUCKET:-${TURBOLITE_BUCKET:-}}" ]]; then
    echo "TIERED_TEST_BUCKET or TURBOLITE_BUCKET is required" >&2
    exit 1
  fi
  if [[ -z "${AWS_ENDPOINT_URL:-${TURBOLITE_ENDPOINT_URL:-}}" ]]; then
    echo "AWS_ENDPOINT_URL or TURBOLITE_ENDPOINT_URL is required" >&2
    exit 1
  fi
}

require_s3_env

run_case "fresh-clone-ppg64-spf4" \
  env \
    TURBOLITE_PAGES_PER_GROUP=64 \
    TURBOLITE_SUB_PAGES_PER_FRAME=4 \
    TURBOLITE_PREFETCH_SEARCH=0.3,0.3,0.4 \
    TURBOLITE_PREFETCH_LOOKUP=0,0,0 \
    TURBOLITE_PLAN_AWARE=0 \
    TURBOLITE_PREDICTION=0 \
    BENCH_WRITE_CYCLES=0 \
    "${BIN}" "${PERSONS}" turbolite-s3

run_case "write-cycles-ppg64-spf4" \
  env \
    TURBOLITE_PAGES_PER_GROUP=64 \
    TURBOLITE_SUB_PAGES_PER_FRAME=4 \
    TURBOLITE_PREFETCH_SEARCH=0.3,0.3,0.4 \
    TURBOLITE_PREFETCH_LOOKUP=0,0,0 \
    TURBOLITE_PLAN_AWARE=0 \
    TURBOLITE_PREDICTION=0 \
    BENCH_WRITE_CYCLES="${BENCH_WRITE_CYCLES:-100}" \
    BENCH_WRITE_CHECKPOINT_EVERY="${BENCH_WRITE_CHECKPOINT_EVERY:-1}" \
    "${BIN}" "${PERSONS}" turbolite-s3

run_case "matrix-ppg32-spf4" \
  env \
    TURBOLITE_PAGES_PER_GROUP=32 \
    TURBOLITE_SUB_PAGES_PER_FRAME=4 \
    TURBOLITE_PREFETCH_SEARCH=0.3,0.3,0.4 \
    TURBOLITE_PREFETCH_LOOKUP=0,0,0 \
    TURBOLITE_PLAN_AWARE=0 \
    TURBOLITE_PREDICTION=0 \
    "${BIN}" "${PERSONS}" turbolite-s3

run_case "matrix-ppg64-spf8" \
  env \
    TURBOLITE_PAGES_PER_GROUP=64 \
    TURBOLITE_SUB_PAGES_PER_FRAME=8 \
    TURBOLITE_PREFETCH_SEARCH=0.3,0.3,0.4 \
    TURBOLITE_PREFETCH_LOOKUP=0,0,0 \
    TURBOLITE_PLAN_AWARE=0 \
    TURBOLITE_PREDICTION=0 \
    "${BIN}" "${PERSONS}" turbolite-s3

run_case "matrix-ppg128-spf4" \
  env \
    TURBOLITE_PAGES_PER_GROUP=128 \
    TURBOLITE_SUB_PAGES_PER_FRAME=4 \
    TURBOLITE_PREFETCH_SEARCH=0.3,0.3,0.4 \
    TURBOLITE_PREFETCH_LOOKUP=0,0,0 \
    TURBOLITE_PLAN_AWARE=0 \
    TURBOLITE_PREDICTION=0 \
    "${BIN}" "${PERSONS}" turbolite-s3

echo "proof logs in ${OUT_DIR}"
