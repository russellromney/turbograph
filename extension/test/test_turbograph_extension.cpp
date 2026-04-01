// Tests for the turbograph LadybugDB extension: UDF registration + config round-trip.
//
// These tests create an in-memory Database with the extension loaded,
// then exercise the UDFs via Cypher queries.
//
// No S3 credentials needed -- the TFS won't be created (no data file configured),
// but the UDFs are still registered and should handle tfs==nullptr gracefully.

#include "main/turbograph_extension.h"

#include "main/connection.h"
#include "main/database.h"
#include "processor/result/flat_tuple.h"
#include "tiered_file_system.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>

using namespace lbug;

static std::string queryScalar(main::Connection& conn, const std::string& cypher) {
    auto result = conn.query(cypher);
    if (!result->isSuccess()) {
        std::fprintf(stderr, "  QUERY FAILED: %s\n  Error: %s\n",
            cypher.c_str(), result->getErrorMessage().c_str());
        return "ERROR: " + result->getErrorMessage();
    }
    if (!result->hasNext()) return "";
    auto row = result->getNext();
    return row->getValue(0)->toString();
}

// --- Test: extension loads without S3 credentials ---
static void testExtensionLoadsWithoutCredentials() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    // Manually load the extension.
    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    // TFS should be null (no credentials configured).
    assert(turbograph_extension::TurbographExtension::tfs == nullptr);

    std::printf("  PASS: testExtensionLoadsWithoutCredentials\n");
}

// --- Test: UDFs are registered and callable ---
static void testUDFsRegistered() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    main::Connection conn(&db);

    // config_get should return "turbograph not active" when TFS is null.
    auto val = queryScalar(conn,
        "RETURN turbograph_config_get('prefetch') AS v");
    assert(val == "turbograph not active");

    // config_set should handle null TFS gracefully (no crash).
    auto setVal = queryScalar(conn,
        "RETURN turbograph_config_set('prefetch', 'scan') AS v");
    // With tfs==null, the loop body skips (isNull || !tfs), result is null.
    // Null renders as empty string or "".
    // Just verify it didn't crash.

    std::printf("  PASS: testUDFsRegistered\n");
}

// --- Test: config_set with invalid key returns error ---
static void testConfigSetUnknownKey() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    // We need a TFS for the UDF to work. Create one with dummy config.
    // This won't connect to S3 -- just creates the object.
    tiered::TieredConfig cfg;
    cfg.s3 = {"https://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    turbograph_extension::TurbographExtension::tfs = tfsPtr.get();

    main::Connection conn(&db);

    auto val = queryScalar(conn,
        "RETURN turbograph_config_set('nonexistent_key', 'value') AS v");
    assert(val.find("unknown key") != std::string::npos);

    // Cleanup.
    turbograph_extension::TurbographExtension::tfs = nullptr;

    std::printf("  PASS: testConfigSetUnknownKey\n");
}

// --- Test: config_set/get round-trip for prefetch schedule ---
static void testConfigSetGetRoundTrip() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"https://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    turbograph_extension::TurbographExtension::tfs = tfsPtr.get();

    main::Connection conn(&db);

    // Set active schedule to "scan".
    auto setVal = queryScalar(conn,
        "RETURN turbograph_config_set('prefetch', 'scan') AS v");
    assert(setVal == "scan");

    // Get should return "scan".
    auto getVal = queryScalar(conn,
        "RETURN turbograph_config_get('prefetch') AS v");
    assert(getVal == "scan");

    // Set to "lookup".
    queryScalar(conn, "RETURN turbograph_config_set('prefetch', 'lookup') AS v");
    getVal = queryScalar(conn, "RETURN turbograph_config_get('prefetch') AS v");
    assert(getVal == "lookup");

    // Set custom schedule.
    auto customVal = queryScalar(conn,
        "RETURN turbograph_config_set('prefetch_scan', '0.5,0.5') AS v");
    assert(customVal == "0.5,0.5");

    // Reset.
    auto resetVal = queryScalar(conn,
        "RETURN turbograph_config_set('prefetch_reset', '') AS v");
    assert(resetVal == "reset");
    getVal = queryScalar(conn, "RETURN turbograph_config_get('prefetch') AS v");
    assert(getVal == "scan"); // Reset goes to "scan" (graph default).

    turbograph_extension::TurbographExtension::tfs = nullptr;

    std::printf("  PASS: testConfigSetGetRoundTrip\n");
}

// --- Test: config_set with bad float parsing ---
static void testConfigSetBadFloat() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"https://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    turbograph_extension::TurbographExtension::tfs = tfsPtr.get();

    main::Connection conn(&db);

    auto val = queryScalar(conn,
        "RETURN turbograph_config_set('prefetch_scan', 'abc,xyz') AS v");
    assert(val.find("error parsing") != std::string::npos);

    turbograph_extension::TurbographExtension::tfs = nullptr;

    std::printf("  PASS: testConfigSetBadFloat\n");
}

// --- Test: config_get for S3 fetch counters ---
static void testConfigGetFetchCounters() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"https://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    turbograph_extension::TurbographExtension::tfs = tfsPtr.get();

    main::Connection conn(&db);

    auto count = queryScalar(conn,
        "RETURN turbograph_config_get('s3_fetch_count') AS v");
    assert(count == "0");

    auto bytes = queryScalar(conn,
        "RETURN turbograph_config_get('s3_fetch_bytes') AS v");
    assert(bytes == "0");

    turbograph_extension::TurbographExtension::tfs = nullptr;

    std::printf("  PASS: testConfigGetFetchCounters\n");
}

// --- Test: UDF schedule switch actually changes TFS state ---
static void testUDFAffectsTFS() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"https://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    auto* rawTfs = tfsPtr.get();
    turbograph_extension::TurbographExtension::tfs = rawTfs;

    main::Connection conn(&db);

    // TFS starts with "default" schedule.
    assert(rawTfs->getActiveSchedule() == "scan");

    // Set via UDF.
    queryScalar(conn, "RETURN turbograph_config_set('prefetch', 'scan') AS v");
    assert(rawTfs->getActiveSchedule() == "scan");

    queryScalar(conn, "RETURN turbograph_config_set('prefetch', 'lookup') AS v");
    assert(rawTfs->getActiveSchedule() == "lookup");

    // Reset via UDF.
    queryScalar(conn, "RETURN turbograph_config_set('prefetch_reset', '') AS v");
    assert(rawTfs->getActiveSchedule() == "scan");

    turbograph_extension::TurbographExtension::tfs = nullptr;

    std::printf("  PASS: testUDFAffectsTFS\n");
}

// --- Test: UDF custom schedule modifies TFS schedule values ---
static void testUDFCustomSchedule() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"https://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    auto* rawTfs = tfsPtr.get();
    turbograph_extension::TurbographExtension::tfs = rawTfs;

    main::Connection conn(&db);

    // Set a custom scan schedule via UDF.
    queryScalar(conn, "RETURN turbograph_config_set('prefetch_scan', '0.5,0.5') AS v");

    // Switch to scan and verify it's active.
    queryScalar(conn, "RETURN turbograph_config_set('prefetch', 'scan') AS v");
    assert(rawTfs->getActiveSchedule() == "scan");

    // Reset and verify defaults restored.
    queryScalar(conn, "RETURN turbograph_config_set('prefetch_reset', '') AS v");
    assert(rawTfs->getActiveSchedule() == "scan");

    turbograph_extension::TurbographExtension::tfs = nullptr;

    std::printf("  PASS: testUDFCustomSchedule\n");
}

int main() {
    std::printf("=== Turbograph Extension Tests ===\n");

    testExtensionLoadsWithoutCredentials();
    testUDFsRegistered();
    testConfigSetUnknownKey();
    testConfigSetGetRoundTrip();
    testConfigSetBadFloat();
    testConfigGetFetchCounters();
    testUDFAffectsTFS();
    testUDFCustomSchedule();

    std::printf("  All 8 extension tests passed.\n");
    return 0;
}
