// Integration tests for connection pool and parallel S3 operations.
// Requires Tigris credentials in environment:
//   TIGRIS_STORAGE_ACCESS_KEY_ID, TIGRIS_STORAGE_SECRET_ACCESS_KEY, TIGRIS_STORAGE_ENDPOINT

#include "s3_client.h"

#include <cassert>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <string>

using namespace lbug::tiered;

static S3Config configFromEnv() {
    auto accessKey = std::getenv("TIGRIS_STORAGE_ACCESS_KEY_ID");
    auto secretKey = std::getenv("TIGRIS_STORAGE_SECRET_ACCESS_KEY");
    auto endpoint = std::getenv("TIGRIS_STORAGE_ENDPOINT");
    if (!accessKey || !secretKey || !endpoint) {
        std::printf("SKIP: Tigris credentials not set\n");
        std::exit(0);
    }
    return S3Config{endpoint, "cinch-data", "test/tiered-parallel-integration",
        "auto", accessKey, secretKey};
}

// Test connection reuse: multiple sequential operations on same client.
static void testConnectionReuse() {
    auto config = configFromEnv();
    S3Client client(config);

    // Do 10 sequential put/get/delete cycles on the same client (persistent connection).
    for (int i = 0; i < 10; i++) {
        auto key = "test/tiered-parallel-integration/reuse_" + std::to_string(i);
        std::vector<uint8_t> data = {static_cast<uint8_t>(i)};
        assert(client.putObject(key, data.data(), data.size()));
        auto got = client.getObject(key);
        assert(got.has_value());
        assert((*got)[0] == static_cast<uint8_t>(i));
        assert(client.deleteObject(key));
    }

    std::printf("  PASS: testConnectionReuse\n");
}

// Test parallel range requests (v2 mechanism).
static void testParallelRangeRequests() {
    auto config = configFromEnv();
    S3Client client(config);

    // Upload a 4KB test object.
    std::string key = config.prefix + "/range_test_object";
    std::vector<uint8_t> data(4096);
    for (size_t i = 0; i < data.size(); i++) {
        data[i] = static_cast<uint8_t>(i & 0xFF);
    }
    assert(client.putObject(key, data.data(), data.size()));

    // Fetch 4 ranges in parallel.
    std::vector<RangeRequest> requests = {
        {key, 0, 1024, 0},
        {key, 1024, 1024, 1},
        {key, 2048, 1024, 2},
        {key, 3072, 1024, 3},
    };
    auto responses = client.getObjectRanges(requests);
    assert(responses.size() == 4);

    for (auto& resp : responses) {
        assert(resp.data.size() == 1024);
        auto expectedStart = static_cast<uint8_t>(resp.tag * 1024 & 0xFF);
        assert(resp.data[0] == expectedStart);
    }

    // Cleanup.
    assert(client.deleteObject(key));
    std::printf("  PASS: testParallelRangeRequests\n");
}

int main() {
    std::printf("=== S3 Parallel & Connection Pool Tests ===\n");
    testConnectionReuse();
    testParallelRangeRequests();
    std::printf("All S3 parallel tests passed.\n");
    return 0;
}
