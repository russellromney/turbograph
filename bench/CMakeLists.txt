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

# Find liblbug in the LadybugDB build tree.
find_library(LBUG_LIBRARY NAMES lbug
    HINTS ${LADYBUG_DIR}/build/release/src
          ${LADYBUG_DIR}/build/src
          ${LADYBUG_DIR}/build)

if(NOT LBUG_LIBRARY)
    message(FATAL_ERROR "liblbug not found in ${LADYBUG_DIR}/build/. Build LadybugDB first.")
endif()

# Link against the full LadybugDB library for Database/Connection APIs.
target_link_libraries(cypher_bench PRIVATE
    turbograph
    ${LBUG_LIBRARY}
    ${OPENSSL_LIBRARIES}
    ${ZSTD_LIBRARIES}
    dl
    pthread)
