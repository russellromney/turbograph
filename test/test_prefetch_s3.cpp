// Integration tests for hop-based adaptive prefetch against Tigris.
// Requires Tigris credentials in environment:
//   TIGRIS_STORAGE_ACCESS_KEY_ID, TIGRIS_STORAGE_SECRET_ACCESS_KEY, TIGRIS_STORAGE_ENDPOINT

#include "chunk_codec.h"
#include "page_bitmap.h"
#include "s3_client.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <fcntl.h>
#include <filesystem>
#include <string>
#include <unistd.h>

#include <zstd.h>

using namespace lbug::tiered;

static const uint32_t PAGE_SIZE = 4096;
static const uint32_t PAGES_PER_GROUP = 4;

static S3Config configFromEnv() {
    auto accessKey = std::getenv("TIGRIS_STORAGE_ACCESS_KEY_ID");
    auto secretKey = std::getenv("TIGRIS_STORAGE_SECRET_ACCESS_KEY");
    auto endpoint = std::getenv("TIGRIS_STORAGE_ENDPOINT");
    if (!accessKey || !secretKey || !endpoint) {
        std::printf("SKIP: Tigris credentials not set\n");
        std::exit(0);
    }
    return S3Config{endpoint, "cinch-data", "test/tiered-prefetch-integration",
        "auto", accessKey, secretKey};
}

static std::filesystem::path tmpDir() {
    auto dir = std::filesystem::temp_directory_path() / "tiered_prefetch_s3_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

// Compress a page for encoding into a page group (mirroring the VFS write path).
static std::vector<uint8_t> compressPageData(const uint8_t* data, uint64_t size) {
    auto maxSize = ZSTD_compressBound(size);
    std::vector<uint8_t> compressed(maxSize);
    auto actualSize = ZSTD_compress(compressed.data(), maxSize, data, size, 3);
    compressed.resize(actualSize);
    return compressed;
}

// Decompress a page extracted from a page group.
static std::vector<uint8_t> decompressPageData(const uint8_t* data, uint64_t compressedSize) {
    std::vector<uint8_t> decompressed(PAGE_SIZE);
    auto actualSize = ZSTD_decompress(decompressed.data(), PAGE_SIZE, data, compressedSize);
    decompressed.resize(actualSize);
    return decompressed;
}

static std::vector<uint8_t> makePageGroup(uint8_t fillByte) {
    std::vector<std::optional<std::vector<uint8_t>>> pages(PAGES_PER_GROUP);
    // Compress the page data, like the real VFS sync path does.
    std::vector<uint8_t> rawPage(PAGE_SIZE, fillByte);
    pages[0] = compressPageData(rawPage.data(), rawPage.size());
    return encodeChunk(pages, PAGES_PER_GROUP);
}

// Simulates readOnePage prefetch logic using bitmap + local file + S3 full-object GET.
struct PrefetchSimulator {
    S3Client& s3;
    PageBitmap& bitmap;
    int localFd;
    uint8_t consecutiveMisses = 0;
    uint64_t totalPageGroups = 0;

    bool readPageGroup(uint64_t pgId) {
        // Check if first page of this group is present in bitmap.
        auto firstPage = pgId * PAGES_PER_GROUP;
        if (bitmap.isPresent(firstPage)) {
            consecutiveMisses /= 2;
            return true;
        }

        if (consecutiveMisses < 255) {
            consecutiveMisses++;
        }

        // Prefetch: min 4 page groups on any miss (simple for this test).
        uint32_t miss = consecutiveMisses;
        uint64_t prefetchCount = static_cast<uint64_t>(miss) * miss * miss * miss;
        if (prefetchCount < 4) {
            prefetchCount = 4;
        }
        if (prefetchCount > 256) {
            prefetchCount = 256;
        }

        std::vector<uint64_t> fetchGroupIds;
        fetchGroupIds.push_back(pgId);
        uint64_t added = 0;
        for (uint64_t dist = 1; added < prefetchCount; dist++) {
            bool anyAdded = false;
            auto fwdPage = (pgId + dist) * PAGES_PER_GROUP;
            if (pgId + dist < totalPageGroups && !bitmap.isPresent(fwdPage)) {
                fetchGroupIds.push_back(pgId + dist);
                added++;
                anyAdded = true;
            }
            if (dist <= pgId) {
                auto bwdPage = (pgId - dist) * PAGES_PER_GROUP;
                if (!bitmap.isPresent(bwdPage)) {
                    fetchGroupIds.push_back(pgId - dist);
                    added++;
                    anyAdded = true;
                }
            }
            if (!anyAdded) {
                break;
            }
        }

        bool foundRequested = false;
        for (auto id : fetchGroupIds) {
            auto pgKey = s3.pageGroupKey(id, 1);
            auto blob = s3.getObject(pgKey);
            if (!blob.has_value()) continue;

            // Extract pages, decompress, write to local file, mark bitmap.
            for (uint32_t i = 0; i < PAGES_PER_GROUP; i++) {
                auto compressedPage = extractPage(*blob, i, PAGES_PER_GROUP);
                if (!compressedPage.has_value() || compressedPage->empty()) continue;
                auto rawPage = decompressPageData(compressedPage->data(), compressedPage->size());
                if (rawPage.size() != PAGE_SIZE) continue;

                auto absPageNum = id * PAGES_PER_GROUP + i;
                auto fileOffset = static_cast<off_t>(absPageNum * PAGE_SIZE);
                ::pwrite(localFd, rawPage.data(), PAGE_SIZE, fileOffset);
                bitmap.markPresent(absPageNum);
            }

            if (id == pgId) foundRequested = true;
        }

        return foundRequested;
    }
};

static PrefetchSimulator setupSimulator(S3Client& s3, PageBitmap& bitmap, int localFd,
    uint32_t numPageGroups) {
    PrefetchSimulator sim{s3, bitmap, localFd, 0, numPageGroups};

    for (uint32_t pg = 0; pg < numPageGroups; pg++) {
        auto blob = makePageGroup(static_cast<uint8_t>(pg));
        assert(s3.putObject(s3.pageGroupKey(pg, 1), blob.data(), blob.size()));
    }

    return sim;
}

static void cleanupPageGroups(S3Client& s3) {
    auto pgKeys = s3.listObjects(s3.prefix() + "/pg/");
    for (auto& key : pgKeys) {
        s3.deleteObject(key);
    }
}

// Test: first cache miss prefetches adjacent page groups (min prefetch = 4).
static void testPrefetchOnFirstMiss() {
    auto config = configFromEnv();
    S3Client client(config);
    auto dir = tmpDir();
    PageBitmap bitmap(dir / "page_bitmap");
    auto localFile = dir / "data.cache";
    int fd = ::open(localFile.c_str(), O_RDWR | O_CREAT, 0644);
    // Pre-extend for 8 page groups * 4 pages * 4096 bytes.
    ::ftruncate(fd, 8 * PAGES_PER_GROUP * PAGE_SIZE);

    auto sim = setupSimulator(client, bitmap, fd, 8);

    assert(sim.readPageGroup(0));
    // Page group 0's first page should be present.
    assert(bitmap.isPresent(0));
    // Prefetched groups should have their first pages present too.
    assert(bitmap.isPresent(1 * PAGES_PER_GROUP));
    assert(bitmap.isPresent(2 * PAGES_PER_GROUP));
    assert(bitmap.isPresent(3 * PAGES_PER_GROUP));
    assert(bitmap.isPresent(4 * PAGES_PER_GROUP));

    ::close(fd);
    cleanupPageGroups(client);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testPrefetchOnFirstMiss\n");
}

// Test: prefetch spans across many page groups.
static void testPrefetchAcrossPageGroups() {
    auto config = configFromEnv();
    S3Client client(config);
    auto dir = tmpDir();
    PageBitmap bitmap(dir / "page_bitmap");
    auto localFile = dir / "data.cache";
    int fd = ::open(localFile.c_str(), O_RDWR | O_CREAT, 0644);
    ::ftruncate(fd, 12 * PAGES_PER_GROUP * PAGE_SIZE);

    auto sim = setupSimulator(client, bitmap, fd, 12);
    sim.consecutiveMisses = 1;

    assert(sim.readPageGroup(3));
    // All nearby page groups should be fetched (prefetch escalation on miss 2).
    assert(bitmap.isPresent(3 * PAGES_PER_GROUP));
    assert(bitmap.isPresent(4 * PAGES_PER_GROUP));
    assert(bitmap.isPresent(5 * PAGES_PER_GROUP));
    assert(bitmap.isPresent(2 * PAGES_PER_GROUP));
    assert(bitmap.isPresent(1 * PAGES_PER_GROUP));
    assert(bitmap.isPresent(0));

    ::close(fd);
    cleanupPageGroups(client);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testPrefetchAcrossPageGroups\n");
}

// Test: data integrity through fetch path.
static void testDataIntegrity() {
    auto config = configFromEnv();
    S3Client client(config);
    auto dir = tmpDir();
    PageBitmap bitmap(dir / "page_bitmap");
    auto localFile = dir / "data.cache";
    int fd = ::open(localFile.c_str(), O_RDWR | O_CREAT, 0644);
    ::ftruncate(fd, 4 * PAGES_PER_GROUP * PAGE_SIZE);

    auto sim = setupSimulator(client, bitmap, fd, 4);

    assert(sim.readPageGroup(0));
    // Read page 0 from local file (first page of group 0, fill byte = 0).
    std::vector<uint8_t> buf(PAGE_SIZE);
    ::pread(fd, buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) {
        assert(b == 0);
    }

    assert(sim.readPageGroup(2));
    // Read first page of group 2 from local file (fill byte = 2).
    auto offset = static_cast<off_t>(2 * PAGES_PER_GROUP * PAGE_SIZE);
    ::pread(fd, buf.data(), PAGE_SIZE, offset);
    for (auto b : buf) {
        assert(b == 2);
    }

    ::close(fd);
    cleanupPageGroups(client);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testDataIntegrity\n");
}

// Test: cache hit decays consecutiveMisses by half.
static void testPrefetchDecayOnHit() {
    auto config = configFromEnv();
    S3Client client(config);
    auto dir = tmpDir();
    PageBitmap bitmap(dir / "page_bitmap");
    auto localFile = dir / "data.cache";
    int fd = ::open(localFile.c_str(), O_RDWR | O_CREAT, 0644);
    ::ftruncate(fd, 8 * PAGES_PER_GROUP * PAGE_SIZE);

    auto sim = setupSimulator(client, bitmap, fd, 8);

    sim.readPageGroup(0);
    assert(sim.consecutiveMisses == 1);

    sim.readPageGroup(1); // Should be cached from prefetch.
    assert(sim.consecutiveMisses == 0);

    sim.consecutiveMisses = 4;
    sim.readPageGroup(1); // Still cached.
    assert(sim.consecutiveMisses == 2);

    ::close(fd);
    cleanupPageGroups(client);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testPrefetchDecayOnHit\n");
}

int main() {
    std::printf("=== Prefetch S3 Integration Tests ===\n");
    testPrefetchOnFirstMiss();
    testPrefetchAcrossPageGroups();
    testDataIntegrity();
    testPrefetchDecayOnHit();
    std::printf("All prefetch S3 integration tests passed.\n");
    return 0;
}
