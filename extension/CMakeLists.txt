# Turbograph LadybugDB Extension
#
# Built in two modes:
#   1. As part of LadybugDB tree: add_subdirectory() from ladybug's extension/CMakeLists.txt
#      Uses build_extension_lib() macro, produces .lbug_extension shared library.
#   2. As part of cypher_bench: source files compiled directly into bench target.
#      No extension framework needed, just headers + liblbug static archives.
#
# Both modes require LADYBUG_DIR or being inside the ladybug build tree.

# When called from ladybug's extension system, these variables are already set:
#   PROJECT_SOURCE_DIR = ladybug root
#   BUILD_STATIC_EXTENSION = ON/OFF
# When called standalone, LADYBUG_DIR must be set.

SET(CMAKE_FIND_PACKAGE_SORT_ORDER NATURAL)
SET(CMAKE_FIND_PACKAGE_SORT_DIRECTION DEC)

if (${ENABLE_ADDRESS_SANITIZER})
    SET(OPENSSL_USE_STATIC_LIBS FALSE)
else ()
    SET(OPENSSL_USE_STATIC_LIBS TRUE)
endif ()

find_package(OpenSSL REQUIRED)
include_directories(${OPENSSL_INCLUDE_DIR})
add_compile_definitions(CPPHTTPLIB_OPENSSL_SUPPORT)

# Find zstd.
find_library(ZSTD_LIB NAMES zstd
    HINTS /opt/homebrew/opt/zstd/lib /usr/local/lib /usr/lib)
find_path(ZSTD_INCLUDE NAMES zstd.h
    HINTS /opt/homebrew/opt/zstd/include /usr/local/include /usr/include)

# Fetch httplib.
include(FetchContent)
FetchContent_Declare(httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG v0.18.7
    GIT_SHALLOW TRUE)
FetchContent_MakeAvailable(httplib)

# Determine ladybug source dir.
if(DEFINED LADYBUG_DIR)
    set(LBUG_SRC_DIR ${LADYBUG_DIR})
else()
    # Assume we're inside the ladybug tree (called via add_subdirectory).
    set(LBUG_SRC_DIR ${PROJECT_SOURCE_DIR})
endif()

# Determine turbograph root (this file is at turbograph/extension/CMakeLists.txt).
get_filename_component(TURBOGRAPH_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/.." ABSOLUTE)

include_directories(
        ${LBUG_SRC_DIR}/src/include
        ${httplib_SOURCE_DIR}
        ${ZSTD_INCLUDE}
        # Generated headers from ladybug build.
        ${CMAKE_BINARY_DIR}/src/include
        # turbograph VFS headers.
        ${TURBOGRAPH_ROOT}/include
        # Extension-specific headers.
        include)

# ALP third-party headers (needed by compression.h).
include_directories(${LBUG_SRC_DIR}/third_party/alp/include)

# Tell turbograph source files to use lbug_zstd:: namespace.
add_compile_definitions(LBUG_ZSTD_NAMESPACE)

add_library(turbograph_extension_source
        OBJECT
        # turbograph VFS core (from parent project).
        ${TURBOGRAPH_ROOT}/src/s3_client.cpp
        ${TURBOGRAPH_ROOT}/src/connection_pool.cpp
        ${TURBOGRAPH_ROOT}/src/chunk_codec.cpp
        ${TURBOGRAPH_ROOT}/src/manifest.cpp
        ${TURBOGRAPH_ROOT}/src/page_bitmap.cpp
        ${TURBOGRAPH_ROOT}/src/table_page_map.cpp
        ${TURBOGRAPH_ROOT}/src/tiered_file_system.cpp
        # Extension glue (registration + UDFs + parsers).
        src/main/turbograph_extension.cpp
        src/functions/turbograph_config.cpp
        src/functions/turbograph_zenith.cpp
        src/functions/table_map_builder.cpp
        src/functions/plan_prefetch.cpp)

set(TURBOGRAPH_EXTENSION_OBJECT_FILES
        ${TURBOGRAPH_EXTENSION_OBJECT_FILES} $<TARGET_OBJECTS:turbograph_extension_source>)

# Only call build_extension_lib if the macro exists (i.e., built from ladybug tree).
if(COMMAND build_extension_lib)
    build_extension_lib(${BUILD_STATIC_EXTENSION} "turbograph")
    target_link_libraries(lbug_${EXTENSION_LIB_NAME}_extension
            PRIVATE
            ${OPENSSL_LIBRARIES}
            ${ZSTD_LIB})
endif()

if (${BUILD_EXTENSION_TESTS})
    add_subdirectory(test)
endif ()
