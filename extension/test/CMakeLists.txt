# Extension tests: build against LadybugDB to test UDF registration + config round-trip.
# Requires LADYBUG_DIR pointing to a built LadybugDB tree.

cmake_minimum_required(VERSION 3.20)
project(TurbographExtensionTest LANGUAGES C CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
enable_testing()

find_package(OpenSSL REQUIRED)
find_package(CURL REQUIRED)

if(NOT DEFINED httplib_SOURCE_DIR)
    include(FetchContent)
    FetchContent_Declare(httplib
        GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
        GIT_TAG v0.18.7
        GIT_SHALLOW TRUE)
    FetchContent_MakeAvailable(httplib)
endif()

# Homebrew LLVM needs its own libc++ on macOS.
if(APPLE AND EXISTS "/opt/homebrew/opt/llvm/lib/c++")
    link_directories("/opt/homebrew/opt/llvm/lib/c++")
endif()

# Turbograph root (this file is at turbograph/extension/test/CMakeLists.txt).
get_filename_component(TURBOGRAPH_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../.." ABSOLUTE)

# Find LadybugDB build artifacts.
if(EXISTS "${LADYBUG_DIR}/build/release")
    set(LBUG_BUILD_DIR "${LADYBUG_DIR}/build/release")
elseif(EXISTS "${LADYBUG_DIR}/build")
    set(LBUG_BUILD_DIR "${LADYBUG_DIR}/build")
else()
    message(FATAL_ERROR "Cannot find LadybugDB build dir. Build LadybugDB first: cd ${LADYBUG_DIR} && make release")
endif()

set(LBUG_LIB "${LBUG_BUILD_DIR}/src/liblbug.a")
if(NOT EXISTS "${LBUG_LIB}")
    find_library(LBUG_LIB NAMES lbug
        HINTS "${LBUG_BUILD_DIR}/src" "${LBUG_BUILD_DIR}/lib"
        NO_DEFAULT_PATH)
endif()

set(TURBOGRAPH_LBUG_ZSTD_HEADER "${LADYBUG_DIR}/third_party/zstd/include/zstd.h")
set(TURBOGRAPH_USE_LBUG_ZSTD_NAMESPACE FALSE)
if(EXISTS "${TURBOGRAPH_LBUG_ZSTD_HEADER}")
    file(READ "${TURBOGRAPH_LBUG_ZSTD_HEADER}" TURBOGRAPH_LBUG_ZSTD_HEADER_TEXT)
    if(TURBOGRAPH_LBUG_ZSTD_HEADER_TEXT MATCHES "namespace[ \t\r\n]+lbug_zstd")
        set(TURBOGRAPH_USE_LBUG_ZSTD_NAMESPACE TRUE)
    endif()
endif()
if(NOT LBUG_LIB)
    message(FATAL_ERROR "Cannot find liblbug. Build LadybugDB first: cd ${LADYBUG_DIR} && make release")
endif()

# Extension test compiles turbograph sources against real LadybugDB headers
# (not stubs). We compile the sources directly to control include order.
set(TURBOGRAPH_EXTENSION_TEST_SOURCES
    ${TURBOGRAPH_ROOT}/extension/src/main/turbograph_extension.cpp
    ${TURBOGRAPH_ROOT}/extension/src/main/turbograph_substrate.cpp
    ${TURBOGRAPH_ROOT}/extension/src/functions/turbograph_config.cpp
    ${TURBOGRAPH_ROOT}/extension/src/functions/turbograph_manifest.cpp
    ${TURBOGRAPH_ROOT}/extension/src/functions/table_map_builder.cpp
    ${TURBOGRAPH_ROOT}/extension/src/functions/plan_prefetch.cpp
    ${TURBOGRAPH_ROOT}/src/s3_client.cpp
    ${TURBOGRAPH_ROOT}/src/connection_pool.cpp
    ${TURBOGRAPH_ROOT}/src/chunk_codec.cpp
    ${TURBOGRAPH_ROOT}/src/manifest.cpp
    ${TURBOGRAPH_ROOT}/src/page_bitmap.cpp
    ${TURBOGRAPH_ROOT}/src/table_page_map.cpp
    ${TURBOGRAPH_ROOT}/src/crypto.cpp
    ${TURBOGRAPH_ROOT}/src/tiered_file_system.cpp)

add_executable(test_turbograph_extension
    test_turbograph_extension.cpp
    ${TURBOGRAPH_EXTENSION_TEST_SOURCES})

add_executable(test_turbograph_static_checkpoint
    test_turbograph_static_checkpoint.cpp
    ${TURBOGRAPH_EXTENSION_TEST_SOURCES})

foreach(target test_turbograph_extension test_turbograph_static_checkpoint)
    target_compile_definitions(${target} PRIVATE
        CPPHTTPLIB_OPENSSL_SUPPORT)
    if(TURBOGRAPH_USE_LBUG_ZSTD_NAMESPACE)
        target_compile_definitions(${target} PRIVATE LBUG_ZSTD_NAMESPACE)
    endif()

    # LadybugDB headers BEFORE turbograph includes so real headers win over stubs.
    # Do NOT include stubs/ here.
    # Include LadybugDB's vendored zstd BEFORE system zstd so lbug_zstd namespace resolves.
    target_include_directories(${target} BEFORE PRIVATE
        ${httplib_SOURCE_DIR}
        ${LADYBUG_DIR}/third_party/zstd/include
        ${LADYBUG_DIR}/src/include
        ${LBUG_BUILD_DIR}/src/include
        ${LADYBUG_DIR}/third_party/alp/include)

    target_include_directories(${target} PRIVATE
        ${TURBOGRAPH_ROOT}/extension/include
        ${TURBOGRAPH_ROOT}/include
        ${OPENSSL_INCLUDE_DIR})
endforeach()

# Link all LadybugDB third-party static libraries.
set(LBUG_THIRD_PARTY "${LBUG_BUILD_DIR}/third_party")
file(GLOB LBUG_THIRDPARTY_LIBS "${LBUG_THIRD_PARTY}/*/*.a")

foreach(target test_turbograph_extension test_turbograph_static_checkpoint)
    target_link_libraries(${target} PRIVATE
        ${LBUG_LIB}
        ${LBUG_THIRDPARTY_LIBS}
        ${OPENSSL_LIBRARIES}
        CURL::libcurl
        pthread)

    if(NOT APPLE)
        target_link_libraries(${target} PRIVATE dl atomic)
    endif()
endforeach()

add_test(NAME TurbographExtensionTest COMMAND test_turbograph_extension)
add_test(NAME TurbographStaticCheckpointTest COMMAND test_turbograph_static_checkpoint)
