// Integration test for S3Client against real Tigris.
// Requires environment variables:
//   TIGRIS_STORAGE_ACCESS_KEY_ID
//   TIGRIS_STORAGE_SECRET_ACCESS_KEY
//   TIGRIS_STORAGE_ENDPOINT (defaults to https://t3.storage.dev)
//   TIGRIS_BUCKET (defaults to cinch-data)
//
// Objects are written under prefix "test/tiered-vfs-integration/" and cleaned up after.

#include "manifest.h"
#include "s3_client.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>

using namespace lbug::tiered;

static S3Config getTestConfig() {
    auto accessKey = std::getenv("TIGRIS_STORAGE_ACCESS_KEY_ID");
    auto secretKey = std::getenv("TIGRIS_STORAGE_SECRET_ACCESS_KEY");
    if (!accessKey || !secretKey) {
        std::printf("SKIP: TIGRIS credentials not set\n");
        std::exit(0);
    }
    auto endpoint = std::getenv("TIGRIS_STORAGE_ENDPOINT");
    auto bucket = std::getenv("TIGRIS_BUCKET");

    S3Config config;
    config.accessKey = accessKey;
    config.secretKey = secretKey;
    config.endpoint = endpoint ? endpoint : "https://t3.storage.dev";
    config.bucket = bucket ? bucket : "cinch-data";
    config.prefix = "test/tiered-vfs-integration";
    config.region = "auto";
    return config;
}

static void testPutGetObject() {
    auto config = getTestConfig();
    S3Client client(config);

    std::string key = config.prefix + "/test-object";
    std::vector<uint8_t> data = {0x48, 0x65, 0x6C, 0x6C, 0x6F}; // "Hello"

    assert(client.putObject(key, data.data(), data.size()));

    auto got = client.getObject(key);
    assert(got.has_value());
    assert(*got == data);

    // Cleanup.
    assert(client.deleteObject(key));
    assert(!client.getObject(key).has_value());

    std::printf("  PASS: testPutGetObject\n");
}

static void testGetNonExistent() {
    auto config = getTestConfig();
    S3Client client(config);

    auto got = client.getObject(config.prefix + "/does-not-exist-" + std::to_string(time(nullptr)));
    assert(!got.has_value());

    std::printf("  PASS: testGetNonExistent\n");
}

static void testManifestRoundTrip() {
    auto config = getTestConfig();
    S3Client client(config);

    Manifest m;
    m.version = 7;
    m.pageCount = 1024;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {client.pageGroupKey(0, 7)};

    assert(client.putManifest(m));

    auto got = client.getManifest();
    assert(got.has_value());
    assert(got->version == 7);
    assert(got->pageCount == 1024);
    assert(got->pageSize == 4096);
    assert(got->pagesPerGroup == 2048);
    assert(got->pageGroupKeys.size() == 1);
    assert(got->pageGroupKeys[0] == client.pageGroupKey(0, 7));

    // Cleanup.
    client.deleteObject(config.prefix + "/manifest.json");

    std::printf("  PASS: testManifestRoundTrip\n");
}

static void testListObjects() {
    auto config = getTestConfig();
    S3Client client(config);

    auto prefix = config.prefix + "/list-test/";
    for (int i = 0; i < 3; i++) {
        auto key = prefix + "obj_" + std::to_string(i);
        std::vector<uint8_t> data = {static_cast<uint8_t>(i)};
        assert(client.putObject(key, data.data(), data.size()));
    }

    auto keys = client.listObjects(prefix);
    assert(keys.size() == 3);

    // Cleanup.
    for (auto& key : keys) {
        client.deleteObject(key);
    }
    // Verify cleanup.
    auto remaining = client.listObjects(prefix);
    assert(remaining.empty());

    std::printf("  PASS: testListObjects\n");
}

static void testEviction() {
    auto config = getTestConfig();
    S3Client client(config);

    auto pgPrefix = config.prefix + "/pg/";
    std::vector<uint8_t> dummy = {0x01};

    // Upload orphans (version < manifest) and valid keys.
    auto orphan1 = pgPrefix + "0_v1";
    auto orphan2 = pgPrefix + "1_v2";
    auto valid = pgPrefix + "0_v3";
    assert(client.putObject(orphan1, dummy.data(), dummy.size()));
    assert(client.putObject(orphan2, dummy.data(), dummy.size()));
    assert(client.putObject(valid, dummy.data(), dummy.size()));

    Manifest m;
    m.version = 4; // Higher than all orphans.
    m.pageGroupKeys = {valid};

    auto deleted = client.evictStalePageGroups(m);
    assert(deleted == 2);

    auto remaining = client.listObjects(pgPrefix);
    assert(remaining.size() == 1);
    assert(remaining[0] == valid);

    // Cleanup.
    client.deleteObject(valid);

    std::printf("  PASS: testEviction\n");
}

int main() {
    std::printf("=== S3 Integration Tests ===\n");
    testPutGetObject();
    testGetNonExistent();
    testManifestRoundTrip();
    testListObjects();
    testEviction();
    std::printf("All S3 integration tests passed.\n");
    return 0;
}
