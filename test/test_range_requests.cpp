// Integration tests for byte-range requests and connection pool.
// Requires TIGRIS credentials:
//   TIGRIS_STORAGE_ACCESS_KEY_ID
//   TIGRIS_STORAGE_SECRET_ACCESS_KEY
//   TIGRIS_STORAGE_ENDPOINT (defaults to https://t3.storage.dev)
//   TIGRIS_BUCKET (defaults to cinch-data)

#include "httplib.h"  // Must come before connection_pool.h for complete type.
#include "s3_client.h"

#include <algorithm>
#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <random>
#include <thread>
#include <vector>

using namespace lbug::tiered;

static S3Config getConfig() {
    auto accessKey = std::getenv("TIGRIS_STORAGE_ACCESS_KEY_ID");
    auto secretKey = std::getenv("TIGRIS_STORAGE_SECRET_ACCESS_KEY");
    if (!accessKey || !secretKey) {
        std::printf("SKIP: TIGRIS credentials not set\n");
        std::exit(0);
    }
    auto endpoint = std::getenv("TIGRIS_STORAGE_ENDPOINT");
    auto bucket = std::getenv("TIGRIS_BUCKET");

    return S3Config{
        .endpoint = endpoint ? endpoint : "https://t3.storage.dev",
        .bucket = bucket ? bucket : "cinch-data",
        .prefix = "test-range-" + std::to_string(
            std::chrono::steady_clock::now().time_since_epoch().count()),
        .region = "auto",
        .accessKey = accessKey,
        .secretKey = secretKey,
        .poolSize = 8,
    };
}

static std::vector<uint8_t> makeData(uint64_t size, uint8_t seed = 0) {
    std::vector<uint8_t> data(size);
    for (uint64_t i = 0; i < size; i++) {
        data[i] = static_cast<uint8_t>((i + seed) & 0xFF);
    }
    return data;
}

// --- Range request tests ---

static void testSingleRangeRequest(S3Client& s3) {
    auto key = "range-test/obj1";
    auto data = makeData(1024 * 1024); // 1MB

    assert(s3.putObject(key, data.data(), data.size()));

    // Fetch bytes 1000-1999 (1000 bytes).
    auto result = s3.getObjectRange(key, 1000, 1000);
    assert(result.has_value());
    assert(result->size() == 1000);

    // Verify data matches.
    for (uint64_t i = 0; i < 1000; i++) {
        assert((*result)[i] == data[1000 + i]);
    }

    s3.deleteObject(key);
    std::printf("  PASS: testSingleRangeRequest\n");
}

static void testRangeAtStart(S3Client& s3) {
    auto key = "range-test/obj-start";
    auto data = makeData(10000);

    assert(s3.putObject(key, data.data(), data.size()));

    // Fetch first 100 bytes.
    auto result = s3.getObjectRange(key, 0, 100);
    assert(result.has_value());
    assert(result->size() == 100);

    for (uint64_t i = 0; i < 100; i++) {
        assert((*result)[i] == data[i]);
    }

    s3.deleteObject(key);
    std::printf("  PASS: testRangeAtStart\n");
}

static void testRangeAtEnd(S3Client& s3) {
    auto key = "range-test/obj-end";
    auto data = makeData(10000);

    assert(s3.putObject(key, data.data(), data.size()));

    // Fetch last 100 bytes.
    auto result = s3.getObjectRange(key, 9900, 100);
    assert(result.has_value());
    assert(result->size() == 100);

    for (uint64_t i = 0; i < 100; i++) {
        assert((*result)[i] == data[9900 + i]);
    }

    s3.deleteObject(key);
    std::printf("  PASS: testRangeAtEnd\n");
}

static void testRangeEntireObject(S3Client& s3) {
    auto key = "range-test/obj-full";
    auto data = makeData(5000);

    assert(s3.putObject(key, data.data(), data.size()));

    // Range covering entire object.
    auto result = s3.getObjectRange(key, 0, 5000);
    assert(result.has_value());
    assert(result->size() == 5000);
    assert(*result == data);

    s3.deleteObject(key);
    std::printf("  PASS: testRangeEntireObject\n");
}

static void testMultipleRangeRequests(S3Client& s3) {
    auto key = "range-test/obj-multi";
    auto data = makeData(2 * 1024 * 1024); // 2MB

    assert(s3.putObject(key, data.data(), data.size()));

    // Issue 4 parallel range requests for non-contiguous regions.
    std::vector<RangeRequest> requests = {
        {key, 0, 1000, 0},               // First 1KB
        {key, 500000, 2000, 1},           // Middle 2KB
        {key, 1000000, 500, 2},           // 1MB offset, 500 bytes
        {key, 2 * 1024 * 1024 - 100, 100, 3}, // Last 100 bytes
    };

    auto results = s3.getObjectRanges(requests);
    assert(results.size() == 4);

    // Build tag -> response map.
    std::unordered_map<uint64_t, std::vector<uint8_t>> byTag;
    for (auto& r : results) {
        byTag[r.tag] = std::move(r.data);
    }

    // Verify each range.
    assert(byTag[0].size() == 1000);
    for (uint64_t i = 0; i < 1000; i++) {
        assert(byTag[0][i] == data[i]);
    }

    assert(byTag[1].size() == 2000);
    for (uint64_t i = 0; i < 2000; i++) {
        assert(byTag[1][i] == data[500000 + i]);
    }

    assert(byTag[2].size() == 500);
    for (uint64_t i = 0; i < 500; i++) {
        assert(byTag[2][i] == data[1000000 + i]);
    }

    assert(byTag[3].size() == 100);
    for (uint64_t i = 0; i < 100; i++) {
        assert(byTag[3][i] == data[2 * 1024 * 1024 - 100 + i]);
    }

    s3.deleteObject(key);
    std::printf("  PASS: testMultipleRangeRequests\n");
}

static void testRangeRequestNonExistent(S3Client& s3) {
    // Range request on non-existent key.
    auto result = s3.getObjectRange("range-test/does-not-exist", 0, 100);
    assert(!result.has_value());

    std::printf("  PASS: testRangeRequestNonExistent\n");
}

// --- Page group key scheme ---

static void testPageGroupKeyScheme(S3Client& s3) {
    // Verify immutable key format: {prefix}/pg/{id}_v{version}.
    auto key0v1 = s3.pageGroupKey(0, 1);
    auto key42v7 = s3.pageGroupKey(42, 7);

    assert(key0v1.find("/pg/0_v1") != std::string::npos);
    assert(key42v7.find("/pg/42_v7") != std::string::npos);

    std::printf("  PASS: testPageGroupKeyScheme\n");
}

// --- Connection pool tests ---

static void testConnectionPoolBasic() {
    ConnectionPool pool("https://example.com", 4);
    assert(pool.poolSize() == 4);
    assert(pool.available() == 4);

    auto c1 = pool.acquire();
    assert(pool.available() == 3);

    auto c2 = pool.acquire();
    assert(pool.available() == 2);

    pool.release(std::move(c1));
    assert(pool.available() == 3);

    pool.release(std::move(c2));
    assert(pool.available() == 4);

    std::printf("  PASS: testConnectionPoolBasic\n");
}

static void testPooledClientRAII() {
    ConnectionPool pool("https://example.com", 2);

    {
        PooledClient c1(pool);
        assert(pool.available() == 1);
        {
            PooledClient c2(pool);
            assert(pool.available() == 0);
        }
        // c2 destroyed — returned to pool.
        assert(pool.available() == 1);
    }
    // c1 destroyed — returned to pool.
    assert(pool.available() == 2);

    std::printf("  PASS: testPooledClientRAII\n");
}

static void testConnectionPoolConcurrent() {
    ConnectionPool pool("https://example.com", 4);

    // Launch 16 threads, each acquires and releases a connection.
    std::atomic<int> completed{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 16; i++) {
        threads.emplace_back([&pool, &completed]() {
            auto client = pool.acquire();
            // Simulate some work.
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
            pool.release(std::move(client));
            completed++;
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    assert(completed.load() == 16);
    assert(pool.available() == 4);

    std::printf("  PASS: testConnectionPoolConcurrent\n");
}

static void testConnectionPoolBlocking() {
    ConnectionPool pool("https://example.com", 2);

    // Acquire both connections.
    auto c1 = pool.acquire();
    auto c2 = pool.acquire();
    assert(pool.available() == 0);

    // Start a thread that tries to acquire — it should block.
    std::atomic<bool> acquired{false};
    std::thread blocker([&pool, &acquired]() {
        auto c3 = pool.acquire();
        acquired.store(true);
        pool.release(std::move(c3));
    });

    // Give the blocker time to start blocking.
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    assert(!acquired.load()); // Should still be blocked.

    // Release one connection — blocker should unblock.
    pool.release(std::move(c1));
    blocker.join();
    assert(acquired.load());

    pool.release(std::move(c2));
    assert(pool.available() == 2);

    std::printf("  PASS: testConnectionPoolBlocking\n");
}

// --- Cleanup ---

static void cleanup(S3Client& s3) {
    // Best-effort cleanup of test objects.
    s3.deleteObject("range-test/obj1");
    s3.deleteObject("range-test/obj-start");
    s3.deleteObject("range-test/obj-end");
    s3.deleteObject("range-test/obj-full");
    s3.deleteObject("range-test/obj-multi");
    s3.deleteObject("range-test/does-not-exist");
    // Clean up any page group objects.
    auto pgKeys = s3.listObjects(s3.prefix() + "/pg/");
    for (auto& key : pgKeys) {
        s3.deleteObject(key);
    }
}

int main() {
    std::printf("=== Range Request & Connection Pool Tests ===\n");

    // Connection pool tests (no credentials needed).
    testConnectionPoolBasic();
    testPooledClientRAII();
    testConnectionPoolConcurrent();
    testConnectionPoolBlocking();

    // S3 integration tests (require credentials).
    auto config = getConfig();
    S3Client s3(config);

    testSingleRangeRequest(s3);
    testRangeAtStart(s3);
    testRangeAtEnd(s3);
    testRangeEntireObject(s3);
    testMultipleRangeRequests(s3);
    testRangeRequestNonExistent(s3);
    testPageGroupKeyScheme(s3);

    cleanup(s3);

    std::printf("All range request & connection pool tests passed.\n");
    return 0;
}
