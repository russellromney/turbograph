# Homebrew LLVM needs its own libc++ on macOS.
if(APPLE AND EXISTS "/opt/homebrew/opt/llvm/lib/c++")
    link_directories("/opt/homebrew/opt/llvm/lib/c++")
endif()

add_executable(test_turbograph_extension
    test_turbograph_extension.cpp
    ${PROJECT_SOURCE_DIR}/extension/turbograph/src/main/turbograph_extension.cpp
    ${PROJECT_SOURCE_DIR}/extension/turbograph/src/functions/turbograph_config.cpp
    ${PROJECT_SOURCE_DIR}/../turbograph/src/s3_client.cpp
    ${PROJECT_SOURCE_DIR}/../turbograph/src/connection_pool.cpp
    ${PROJECT_SOURCE_DIR}/../turbograph/src/chunk_codec.cpp
    ${PROJECT_SOURCE_DIR}/../turbograph/src/manifest.cpp
    ${PROJECT_SOURCE_DIR}/../turbograph/src/page_bitmap.cpp
    ${PROJECT_SOURCE_DIR}/../turbograph/src/tiered_file_system.cpp)

target_compile_definitions(test_turbograph_extension PRIVATE
    CPPHTTPLIB_OPENSSL_SUPPORT LBUG_ZSTD_NAMESPACE)

target_include_directories(test_turbograph_extension PRIVATE
    ${PROJECT_SOURCE_DIR}/extension/turbograph/include
    ${PROJECT_SOURCE_DIR}/../turbograph/include
    ${httplib_SOURCE_DIR})

target_link_libraries(test_turbograph_extension PRIVATE
    lbug
    ${OPENSSL_LIBRARIES}
    ${ZSTD_LIB}
    pthread)

if(NOT APPLE)
    target_link_libraries(test_turbograph_extension PRIVATE dl atomic)
endif()

add_test(NAME TurbographExtensionTest COMMAND test_turbograph_extension)
