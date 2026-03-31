# Homebrew LLVM needs its own libc++ on macOS.
if(APPLE AND EXISTS "/opt/homebrew/opt/llvm/lib/c++")
    link_directories("/opt/homebrew/opt/llvm/lib/c++")
endif()

set(TEST_LINK_LIBS turbograph ${OPENSSL_LIBRARIES} ${ZSTD_LIBRARIES} pthread)

# Unit test: chunk codec
add_executable(test_chunk_codec test_chunk_codec.cpp)
target_link_libraries(test_chunk_codec PRIVATE turbograph)
add_test(NAME ChunkCodecTest COMMAND test_chunk_codec)

# Unit test: manifest
add_executable(test_manifest test_manifest.cpp)
target_link_libraries(test_manifest PRIVATE turbograph)
add_test(NAME ManifestTest COMMAND test_manifest)

# Unit test: page bitmap
add_executable(test_page_bitmap test_page_bitmap.cpp)
target_link_libraries(test_page_bitmap PRIVATE turbograph)
add_test(NAME PageBitmapTest COMMAND test_page_bitmap)

# Unit test: page I/O decomposition
add_executable(test_page_io test_page_io.cpp)
add_test(NAME PageIOTest COMMAND test_page_io)

# Unit test: prefetch logic (pure math)
add_executable(test_prefetch test_prefetch.cpp)
target_link_libraries(test_prefetch PRIVATE turbograph)
add_test(NAME PrefetchTest COMMAND test_prefetch)

# Integration test: S3/Tigris (requires credentials)
add_executable(test_s3_integration test_s3_integration.cpp)
target_link_libraries(test_s3_integration PRIVATE turbograph)
add_test(NAME S3IntegrationTest COMMAND test_s3_integration)

# Integration test: S3 parallel (requires credentials)
add_executable(test_s3_parallel test_s3_parallel.cpp)
target_link_libraries(test_s3_parallel PRIVATE turbograph)
add_test(NAME S3ParallelTest COMMAND test_s3_parallel)

# Integration test: prefetch against Tigris (requires credentials)
add_executable(test_prefetch_s3 test_prefetch_s3.cpp)
target_link_libraries(test_prefetch_s3 PRIVATE turbograph)
add_test(NAME PrefetchS3Test COMMAND test_prefetch_s3)

# Integration test: S3 range requests + connection pool (requires credentials)
add_executable(test_range_requests test_range_requests.cpp)
target_link_libraries(test_range_requests PRIVATE turbograph)
add_test(NAME RangeRequestTest COMMAND test_range_requests)

# Unit test: seekable multi-frame codec + manifest
add_executable(test_seekable test_seekable.cpp)
target_link_libraries(test_seekable PRIVATE turbograph)
add_test(NAME SeekableTest COMMAND test_seekable)

# Unit test: VFS (no S3 required)
add_executable(test_vfs test_vfs.cpp)
target_link_libraries(test_vfs PRIVATE turbograph)
add_test(NAME VFSTest COMMAND test_vfs)

# Integration test: VFS end-to-end with S3
add_executable(test_vfs_s3 test_vfs_s3.cpp)
target_link_libraries(test_vfs_s3 PRIVATE turbograph)
add_test(NAME VFSS3Test COMMAND test_vfs_s3)
