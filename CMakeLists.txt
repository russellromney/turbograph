cmake_minimum_required(VERSION 3.20)
project(turbograph VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

option(BUILD_TESTS "Build unit and integration tests" ON)
option(BUILD_BENCH "Build cypher benchmark (requires LadybugDB)" OFF)

# --- Dependencies ---

find_package(OpenSSL REQUIRED)
find_package(PkgConfig QUIET)

# Find zstd via pkg-config or cmake, fall back to -lzstd.
# On macOS with Homebrew, add explicit search hints.
if(APPLE)
    list(APPEND CMAKE_PREFIX_PATH "/opt/homebrew/opt/zstd")
endif()

if(PkgConfig_FOUND)
    pkg_check_modules(ZSTD QUIET libzstd)
endif()
if(NOT ZSTD_FOUND)
    find_library(ZSTD_LIBRARIES NAMES zstd
        HINTS /opt/homebrew/opt/zstd/lib /usr/local/lib
        NO_DEFAULT_PATH)
    if(NOT ZSTD_LIBRARIES)
        find_library(ZSTD_LIBRARIES NAMES zstd)
    endif()
    find_path(ZSTD_INCLUDE_DIRS NAMES zstd.h
        HINTS /opt/homebrew/opt/zstd/include /usr/local/include)
endif()
if(NOT ZSTD_LIBRARIES)
    message(FATAL_ERROR "zstd not found. Install libzstd-dev or zstd (brew).")
endif()

# --- Include paths ---

# Turbograph headers.
include_directories(include)

# Kuzu interface stubs (for standalone builds without full LadybugDB).
# When BUILD_BENCH is ON and LADYBUG_DIR is set, real Kuzu headers take precedence.
if(BUILD_BENCH AND DEFINED LADYBUG_DIR)
    include_directories(${LADYBUG_DIR}/src/include)
    include_directories(${CMAKE_BINARY_DIR}/src/include)
else()
    include_directories(stubs)
endif()

# cpp-httplib (header-only, fetched at build time).
include(FetchContent)
FetchContent_Declare(httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG v0.18.7
    GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(httplib)
include_directories(${httplib_SOURCE_DIR})
include_directories(${OPENSSL_INCLUDE_DIR})
include_directories(${ZSTD_INCLUDE_DIRS})

add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)

# --- Library ---

add_library(turbograph STATIC
    src/s3_client.cpp
    src/connection_pool.cpp
    src/chunk_codec.cpp
    src/manifest.cpp
    src/page_bitmap.cpp
    src/tiered_file_system.cpp
    src/file_system_stubs.cpp)

# Add zstd library search path for the linker.
if(ZSTD_LIBRARY_DIRS)
    link_directories(${ZSTD_LIBRARY_DIRS})
endif()

target_link_libraries(turbograph
    PUBLIC
    ${OPENSSL_LIBRARIES}
    ${ZSTD_LIBRARIES}
    pthread)

# --- Tests ---

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(test)
endif()

# --- Benchmark (requires full LadybugDB) ---

if(BUILD_BENCH)
    add_subdirectory(bench)
endif()
