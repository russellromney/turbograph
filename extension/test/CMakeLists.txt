# Extension tests: build against LadybugDB to test UDF registration + config round-trip.
# Requires LADYBUG_DIR pointing to a built LadybugDB tree.

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

# Find liblbug static library.
find_library(LBUG_LIB NAMES lbug
    HINTS "${LBUG_BUILD_DIR}/src" "${LBUG_BUILD_DIR}/lib"
    NO_DEFAULT_PATH)
if(NOT LBUG_LIB)
    message(FATAL_ERROR "Cannot find liblbug. Build LadybugDB first: cd ${LADYBUG_DIR} && make release")
endif()

# Extension test compiles turbograph sources against real LadybugDB headers
# (not stubs). We compile the sources directly to control include order.
add_executable(test_turbograph_extension
    test_turbograph_extension.cpp
    ${TURBOGRAPH_ROOT}/extension/src/main/turbograph_extension.cpp
    ${TURBOGRAPH_ROOT}/extension/src/functions/turbograph_config.cpp
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

target_compile_definitions(test_turbograph_extension PRIVATE
    CPPHTTPLIB_OPENSSL_SUPPORT LBUG_ZSTD_NAMESPACE)

# LadybugDB headers BEFORE turbograph includes so real headers win over stubs.
# Do NOT include stubs/ here.
# Include LadybugDB's vendored zstd BEFORE system zstd so lbug_zstd namespace resolves.
target_include_directories(test_turbograph_extension BEFORE PRIVATE
    ${LADYBUG_DIR}/third_party/zstd/include
    ${LADYBUG_DIR}/src/include
    ${LBUG_BUILD_DIR}/src/include
    ${LADYBUG_DIR}/third_party/alp/include)

target_include_directories(test_turbograph_extension PRIVATE
    ${TURBOGRAPH_ROOT}/extension/include
    ${TURBOGRAPH_ROOT}/include
    ${httplib_SOURCE_DIR})

# Link all LadybugDB third-party static libraries.
set(LBUG_THIRD_PARTY "${LBUG_BUILD_DIR}/third_party")
file(GLOB LBUG_THIRDPARTY_LIBS "${LBUG_THIRD_PARTY}/*/*.a")

target_link_libraries(test_turbograph_extension PRIVATE
    ${LBUG_LIB}
    ${LBUG_THIRDPARTY_LIBS}
    ${OPENSSL_LIBRARIES}
    pthread)

if(NOT APPLE)
    target_link_libraries(test_turbograph_extension PRIVATE dl atomic)
endif()

add_test(NAME TurbographExtensionTest COMMAND test_turbograph_extension)
