# Benchmark requires full LadybugDB (liblbug).
# Build with: cmake -DBUILD_BENCH=ON -DLADYBUG_DIR=/path/to/ladybug-fork ..

if(NOT DEFINED LADYBUG_DIR)
    message(WARNING "LADYBUG_DIR not set. Skipping cypher_bench.")
    return()
endif()

# Homebrew LLVM needs its own libc++ on macOS.
if(APPLE AND EXISTS "/opt/homebrew/opt/llvm/lib/c++")
    link_directories("/opt/homebrew/opt/llvm/lib/c++")
endif()

add_executable(cypher_bench cypher_bench.cpp)

# Find all static libraries from the LadybugDB build tree.
# liblbug.a depends on parquet, antlr4, utf8proc, etc. -- all built as .a files.
file(GLOB_RECURSE LBUG_LIBS
    "${LADYBUG_DIR}/build/release/src/*.a"
    "${LADYBUG_DIR}/build/release/third_party/*.a")

if(NOT LBUG_LIBS)
    # Try non-release build dir.
    file(GLOB_RECURSE LBUG_LIBS
        "${LADYBUG_DIR}/build/src/*.a"
        "${LADYBUG_DIR}/build/third_party/*.a")
endif()

if(NOT LBUG_LIBS)
    message(FATAL_ERROR "No .a files found in ${LADYBUG_DIR}/build/. Build LadybugDB first.")
endif()

# Link against the full LadybugDB library + all its dependencies.
# Use --start-group/--end-group to resolve circular deps between static archives
# (e.g., brotlidec -> brotlicommon, parquet -> thrift -> zstd).
target_link_libraries(cypher_bench PRIVATE
    turbograph
    -Wl,--start-group
    ${LBUG_LIBS}
    -Wl,--end-group
    ${OPENSSL_LIBRARIES}
    ${ZSTD_LIBRARIES}
    atomic
    dl
    pthread)
