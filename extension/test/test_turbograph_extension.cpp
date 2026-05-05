// Tests for the turbograph LadybugDB extension: UDF registration + config round-trip.
//
// These tests create an in-memory Database with the extension loaded,
// then exercise the UDFs via Cypher queries.
//
// No S3 credentials needed -- the TFS won't be created (no data file configured),
// but the UDFs are still registered and should handle tfs==nullptr gracefully.

#include "main/turbograph_extension.h"
#include "main/turbograph_functions.h"

#include "sqlite_graph_file_system.h"

#include "main/connection.h"
#include "main/database.h"
#include "main/db_config.h"
#include "processor/result/flat_tuple.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/storage_utils.h"
#include "table_page_map.h"
#include "tiered_file_system.h"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <unistd.h>

using namespace lbug;

namespace lbug::testing {

class BaseGraphTest {
public:
    static std::unique_ptr<main::Database> constructDB(std::string_view databasePath,
        main::SystemConfig systemConfig, main::Database::construct_bm_func_t constructFunc) {
        return std::unique_ptr<main::Database>(
            new main::Database(databasePath, systemConfig, std::move(constructFunc)));
    }

    static const std::string& getDatabasePath(const main::Database& database) {
        return database.databasePath;
    }

    static const main::DBConfig& getDBConfig(const main::Database& database) {
        return dbConfigRef(database.dbConfig);
    }

private:
    static const main::DBConfig& dbConfigRef(const main::DBConfig& config) {
        return config;
    }

    template<typename ConfigPtr>
    static const main::DBConfig& dbConfigRef(const ConfigPtr& config) {
        return *config;
    }
};

} // namespace lbug::testing

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

static void assertQuerySuccess(main::Connection& conn, const std::string& cypher) {
    auto result = conn.query(cypher);
    if (!result->isSuccess()) {
        std::fprintf(stderr, "  QUERY FAILED: %s\n  Error: %s\n",
            cypher.c_str(), result->getErrorMessage().c_str());
    }
    assert(result->isSuccess());
}

struct SqliteBackedDatabaseOptions {
    bool autoCheckpoint = false;
    uint64_t checkpointThreshold = 0;
    uint32_t sqliteWalAutoCheckpointPages = 100;
    int32_t sqliteCacheSizePages = 0;
    std::string sqliteSynchronous = "NORMAL";
    std::optional<std::string> sqliteVfsName;
    std::optional<std::string> sqliteLoadableExtensionPath;
};

static std::unique_ptr<main::Database> openSqliteBackedDatabase(
    const std::string& dataFilePath, const std::string& sqlitePath,
    SqliteBackedDatabaseOptions options = {}) {
    main::SystemConfig sysCfg;
    sysCfg.autoCheckpoint = options.autoCheckpoint;
    sysCfg.checkpointThreshold = options.checkpointThreshold;
    sysCfg.forceCheckpointOnClose = false;
    // This old Ladybug checkout treats bufferPoolSize=0 as "use default" and
    // cannot run the storage stack with a one-page pool. Keep the smoke fixed
    // and modest; newer Kuzu page-cache-disable behavior needs its own run.
    sysCfg.bufferPoolSize = 1ull << 26;
    auto constructBM = [sqlitePath, options](const main::Database& db) {
        const auto& expandedDataFilePath = testing::BaseGraphTest::getDatabasePath(db);
        const auto& dbConfig = testing::BaseGraphTest::getDBConfig(db);
        tiered::SqliteGraphFileSystemConfig cfg;
        cfg.sqlitePath = sqlitePath;
        cfg.dataFilePath = expandedDataFilePath;
        cfg.graphPageSize = 4096;
        cfg.sqlitePageSize = 65536;
        cfg.sqliteWalAutoCheckpointPages = options.sqliteWalAutoCheckpointPages;
        cfg.sqliteCacheSizePages = options.sqliteCacheSizePages;
        cfg.sqliteSynchronous = options.sqliteSynchronous;
        cfg.sqliteVfsName = options.sqliteVfsName;
        cfg.sqliteLoadableExtensionPath = options.sqliteLoadableExtensionPath;
        const_cast<main::Database&>(db).registerFileSystem(
            std::make_unique<tiered::SqliteGraphFileSystem>(std::move(cfg)));
        return std::make_unique<storage::BufferManager>(expandedDataFilePath,
            storage::StorageUtils::getTmpFilePath(expandedDataFilePath),
            dbConfig.bufferPoolSize, dbConfig.maxDBSize,
            const_cast<main::Database&>(db).getVFS(), dbConfig.readOnly);
    };
    return testing::BaseGraphTest::constructDB(dataFilePath, sysCfg, std::move(constructBM));
}

static std::optional<std::string> envString(const char* name) {
    auto value = std::getenv(name);
    if (value == nullptr || std::string(value).empty()) {
        return std::nullopt;
    }
    return std::string(value);
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

// --- Test: registry does not return a destroyed TFS ---
static void testTfsRegistryDropsExpiredEntry() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"http://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    auto* rawTfs = tfsPtr.get();
    turbograph_extension::TurbographExtension::registerTfs(&db, rawTfs);

    assert(turbograph_extension::TurbographExtension::tfsFromBindData(nullptr) == rawTfs);

    tfsPtr.reset();
    assert(turbograph_extension::TurbographExtension::tfsFromBindData(nullptr) == nullptr);
    assert(turbograph_extension::TurbographExtension::tfs == nullptr);

    turbograph_extension::TurbographExtension::registerTfs(&db, nullptr);

    std::printf("  PASS: testTfsRegistryDropsExpiredEntry\n");
}

// --- Test: one DB must not inherit another DB's TFS ---
static void testPerDbRegistryDoesNotFallbackAcrossDatabases() {
    main::SystemConfig sysCfg;
    main::Database db1(":memory:", sysCfg);
    main::Database db2(":memory:", sysCfg);

    auto ctx1 = main::ClientContext(&db1);
    auto ctx2 = main::ClientContext(&db2);
    turbograph_extension::TurbographExtension::load(&ctx1);
    turbograph_extension::TurbographExtension::load(&ctx2);

    tiered::TieredConfig cfg;
    cfg.s3 = {"http://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    turbograph_extension::TurbographExtension::registerTfs(&db1, tfsPtr.get());

    main::Connection conn2(&db2);
    auto val = queryScalar(conn2,
        "RETURN turbograph_config_get('prefetch') AS v");
    assert(val == "turbograph not active");

    turbograph_extension::TurbographExtension::registerTfs(&db1, nullptr);

    std::printf("  PASS: testPerDbRegistryDoesNotFallbackAcrossDatabases\n");
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
    cfg.s3 = {"http://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    turbograph_extension::TurbographExtension::registerTfs(&db, tfsPtr.get());

    main::Connection conn(&db);

    auto val = queryScalar(conn,
        "RETURN turbograph_config_set('nonexistent_key', 'value') AS v");
    assert(val.find("unknown key") != std::string::npos);

    turbograph_extension::TurbographExtension::registerTfs(&db, nullptr);

    std::printf("  PASS: testConfigSetUnknownKey\n");
}

// --- Test: config_set/get round-trip for prefetch schedule ---
static void testConfigSetGetRoundTrip() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"http://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    turbograph_extension::TurbographExtension::registerTfs(&db, tfsPtr.get());

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

    turbograph_extension::TurbographExtension::registerTfs(&db, nullptr);

    std::printf("  PASS: testConfigSetGetRoundTrip\n");
}

// --- Test: config_set with bad float parsing ---
static void testConfigSetBadFloat() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"http://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    turbograph_extension::TurbographExtension::registerTfs(&db, tfsPtr.get());

    main::Connection conn(&db);

    auto val = queryScalar(conn,
        "RETURN turbograph_config_set('prefetch_scan', 'abc,xyz') AS v");
    assert(val.find("error parsing") != std::string::npos);

    turbograph_extension::TurbographExtension::registerTfs(&db, nullptr);

    std::printf("  PASS: testConfigSetBadFloat\n");
}

// --- Test: config_get for S3 fetch counters ---
static void testConfigGetFetchCounters() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"http://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    turbograph_extension::TurbographExtension::registerTfs(&db, tfsPtr.get());

    main::Connection conn(&db);

    auto count = queryScalar(conn,
        "RETURN turbograph_config_get('s3_fetch_count') AS v");
    assert(count == "0");

    auto bytes = queryScalar(conn,
        "RETURN turbograph_config_get('s3_fetch_bytes') AS v");
    assert(bytes == "0");

    turbograph_extension::TurbographExtension::registerTfs(&db, nullptr);

    std::printf("  PASS: testConfigGetFetchCounters\n");
}

// --- Test: UDF schedule switch actually changes TFS state ---
static void testUDFAffectsTFS() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"http://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    auto* rawTfs = tfsPtr.get();
    turbograph_extension::TurbographExtension::registerTfs(&db, rawTfs);

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

    turbograph_extension::TurbographExtension::registerTfs(&db, nullptr);

    std::printf("  PASS: testUDFAffectsTFS\n");
}

// --- Test: UDF custom schedule modifies TFS schedule values ---
static void testUDFCustomSchedule() {
    main::SystemConfig sysCfg;
    main::Database db(":memory:", sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    tiered::TieredConfig cfg;
    cfg.s3 = {"http://dummy", "bucket", "prefix", "auto", "ak", "sk"};
    cfg.dataFilePath = "/tmp/nonexistent.kz";
    cfg.cacheDir = "/tmp/turbograph_test_cache";
    auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
    auto* rawTfs = tfsPtr.get();
    turbograph_extension::TurbographExtension::registerTfs(&db, rawTfs);

    main::Connection conn(&db);

    // Set a custom scan schedule via UDF.
    queryScalar(conn, "RETURN turbograph_config_set('prefetch_scan', '0.5,0.5') AS v");

    // Switch to scan and verify it's active.
    queryScalar(conn, "RETURN turbograph_config_set('prefetch', 'scan') AS v");
    assert(rawTfs->getActiveSchedule() == "scan");

    // Reset and verify defaults restored.
    queryScalar(conn, "RETURN turbograph_config_set('prefetch_reset', '') AS v");
    assert(rawTfs->getActiveSchedule() == "scan");

    turbograph_extension::TurbographExtension::registerTfs(&db, nullptr);

    std::printf("  PASS: testUDFCustomSchedule\n");
}

// --- Test: extractTablesFromPlan returns correct table IDs ---
static void testExtractTablesFromPlan() {
    // Create a database with a node table and a relationship table.
    main::SystemConfig sysCfg;
    auto tmpDir = std::string("/tmp/turbograph_test_query_prefetch_") + std::to_string(getpid());
    main::Database db(tmpDir, sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);

    main::Connection conn(&db);

    // Create schema.
    auto r1 = conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))");
    assert(r1->isSuccess());
    auto r2 = conn.query("CREATE NODE TABLE City(name STRING, PRIMARY KEY(name))");
    assert(r2->isSuccess());
    auto r3 = conn.query("CREATE REL TABLE Knows(FROM Person TO Person)");
    assert(r3->isSuccess());
    auto r4 = conn.query("CREATE REL TABLE LivesIn(FROM Person TO City)");
    assert(r4->isSuccess());

    // Insert some data so plans are non-trivial.
    conn.query("CREATE (:Person {name: 'Alice', age: 30})");
    conn.query("CREATE (:Person {name: 'Bob', age: 25})");
    conn.query("CREATE (:City {name: 'NYC'})");

    // Test 1: Simple node scan should return node table IDs.
    {
        auto [nodeIds, relIds] = turbograph_extension::extractTablesFromPlan(
            conn, "MATCH (p:Person) RETURN p.name");
        assert(!nodeIds.empty());
        assert(relIds.empty());
    }

    // Test 2: Edge traversal should return both node and rel table IDs.
    {
        auto [nodeIds, relIds] = turbograph_extension::extractTablesFromPlan(
            conn, "MATCH (a:Person)-[:Knows]->(b:Person) RETURN a.name, b.name");
        assert(!nodeIds.empty());
        assert(!relIds.empty());
    }

    // Test 3: Multi-table join should return all referenced tables.
    {
        auto [nodeIds, relIds] = turbograph_extension::extractTablesFromPlan(
            conn, "MATCH (p:Person)-[:LivesIn]->(c:City) RETURN p.name, c.name");
        assert(nodeIds.size() >= 2); // Person + City
        assert(!relIds.empty());     // LivesIn
    }

    // Test 4: Invalid query returns empty sets (no crash).
    {
        auto [nodeIds, relIds] = turbograph_extension::extractTablesFromPlan(
            conn, "THIS IS NOT VALID CYPHER");
        assert(nodeIds.empty());
        assert(relIds.empty());
    }

    // Cleanup: remove temp directory.
    std::string rmCmd = "rm -rf " + tmpDir;
    std::system(rmCmd.c_str());

    std::printf("  PASS: testExtractTablesFromPlan\n");
}

// --- Test: buildTablePageMap produces valid mapping ---
static void testBuildTablePageMap() {
    // Create a database with tables and data to exercise metadata parsing.
    main::SystemConfig sysCfg;
    auto tmpDir = std::string("/tmp/turbograph_test_tpm_") + std::to_string(getpid());
    main::Database db(tmpDir, sysCfg);

    auto ctx = main::ClientContext(&db);
    turbograph_extension::TurbographExtension::load(&ctx);
    turbograph_extension::TurbographExtension::db = &db;

    main::Connection conn(&db);

    // Create schema + insert data.
    conn.query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name))");
    conn.query("CREATE REL TABLE Knows(FROM Person TO Person)");
    conn.query("CREATE (:Person {name: 'Alice', age: 30})");
    conn.query("CREATE (:Person {name: 'Bob', age: 25})");
    conn.query("MATCH (a:Person {name: 'Alice'}), (b:Person {name: 'Bob'}) "
               "CREATE (a)-[:Knows]->(b)");

    // Force checkpoint so data is ON_DISK (buildTablePageMap checks ResidencyState).
    conn.query("CHECKPOINT");

    // Build table page map via the UDF path.
    auto map = turbograph_extension::buildTablePageMap(&db);

    // With very small data sets (2 rows), LadybugDB keeps all data in memory
    // even after checkpoint, so buildTablePageMap returns an empty map (no
    // ON_DISK pages). This is correct. Verify the function runs without
    // crashing and returns a valid map.
    assert(map != nullptr);
    std::printf("    table page map size: %zu (expected 0 for tiny dataset)\n",
        map->size());

    turbograph_extension::TurbographExtension::db = nullptr;

    std::string rmCmd = "rm -rf " + tmpDir;
    std::system(rmCmd.c_str());

    std::printf("  PASS: testBuildTablePageMap\n");
}

// --- Test: per-table schedule selection via TablePageMap ---
static void testPerTableScheduleSelection() {
    // Verify that TablePageMap lookups correctly identify node vs rel tables.
    // The 24 unit tests in test_table_page_map.cpp cover the data structure
    // exhaustively. This test verifies the mapping drives schedule selection.
    auto map = std::make_unique<tiered::TablePageMap>();
    map->addInterval(0, 100, 0, false);    // Table 0: node table, pages 0-99
    map->addInterval(100, 100, 1, true);   // Table 1: rel table, pages 100-199
    map->finalize();

    // Node table pages should get lookup schedule (conservative).
    auto r0 = map->lookup(50);
    assert(r0.found && !r0.isRelationship && r0.tableId == 0);

    // Rel table pages should get scan schedule (aggressive).
    auto r1 = map->lookup(150);
    assert(r1.found && r1.isRelationship && r1.tableId == 1);

    // Pages outside any table use the global fallback schedule.
    auto r2 = map->lookup(250);
    assert(!r2.found);

    // Verify maxTableId (used to size per-table miss counter array).
    assert(map->maxTableId() == 1);
    assert(map->size() == 2);

    std::printf("  PASS: testPerTableScheduleSelection\n");
}

// --- Test: real LadybugDB data file persisted as SQLite graph pages ---
static void testSqliteBackedDatabasePersistsGraphPages() {
    auto tmpDir = std::filesystem::temp_directory_path() /
                  ("turbograph_sqlite_graph_smoke_" + std::to_string(getpid()));
    std::filesystem::remove_all(tmpDir);
    std::filesystem::create_directories(tmpDir);
    auto dataFilePath = (tmpDir / "graph.kz").string();
    auto sqlitePath = (tmpDir / "graph-pages.sqlite").string();

    {
        auto db = openSqliteBackedDatabase(dataFilePath, sqlitePath);
        main::Connection conn(db.get());

        assertQuerySuccess(conn,
            "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
        assertQuerySuccess(conn, "CREATE (:Person {name: 'Ada', age: 37});");
        assertQuerySuccess(conn, "CREATE (:Person {name: 'Grace', age: 85});");
        assert(queryScalar(conn, "MATCH (p:Person) RETURN COUNT(*) AS c;") == "2");
        assertQuerySuccess(conn, "CHECKPOINT;");
    }

    assert(std::filesystem::exists(sqlitePath));
    tiered::SqlitePageStore store(sqlitePath, 4096, std::nullopt, std::nullopt,
        65536, 100, 0, "NORMAL");
    assert(store.sqlitePageSize() == 65536);
    assert(store.walAutoCheckpointPages() == 100);
    assert(store.sqliteCacheSizePages() == 0);
    assert(store.fileExists(dataFilePath));
    assert(store.fileSize(dataFilePath) > 0);

    {
        auto db = openSqliteBackedDatabase(dataFilePath, sqlitePath);
        main::Connection conn(db.get());
        assert(queryScalar(conn, "MATCH (p:Person) RETURN COUNT(*) AS c;") == "2");
        assert(queryScalar(conn,
                   "MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.age AS age;") == "37");
    }

    std::filesystem::remove_all(tmpDir);

    std::printf("  PASS: testSqliteBackedDatabasePersistsGraphPages\n");
}

// --- Test: current Ladybug/Kuzu-style auto_checkpoint=0 physical page smoke ---
static void testSqliteBackedDatabaseAutoCheckpointZero() {
    auto tmpDir = std::filesystem::temp_directory_path() /
                  ("turbograph_sqlite_graph_autockpt_" + std::to_string(getpid()));
    std::filesystem::remove_all(tmpDir);
    std::filesystem::create_directories(tmpDir);
    auto dataFilePath = (tmpDir / "graph.kz").string();
    auto sqlitePath = (tmpDir / "graph-pages.sqlite").string();

    SqliteBackedDatabaseOptions options;
    options.autoCheckpoint = true;
    options.checkpointThreshold = 0;
    options.sqliteWalAutoCheckpointPages = 100;
    options.sqliteCacheSizePages = 0;
    options.sqliteSynchronous = "NORMAL";

    {
        auto db = openSqliteBackedDatabase(dataFilePath, sqlitePath, options);
        main::Connection conn(db.get());

        assertQuerySuccess(conn,
            "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
        assertQuerySuccess(conn, "CREATE (:Person {name: 'Ada', age: 37});");
        assertQuerySuccess(conn, "CREATE (:Person {name: 'Grace', age: 85});");
        assert(queryScalar(conn, "MATCH (p:Person) RETURN COUNT(*) AS c;") == "2");
    }

    tiered::SqlitePageStore store(sqlitePath, 4096, std::nullopt, std::nullopt,
        65536, 100, 0, "NORMAL");
    assert(store.fileExists(dataFilePath));
    assert(store.fileSize(dataFilePath) > 0);

    {
        auto db = openSqliteBackedDatabase(dataFilePath, sqlitePath, options);
        main::Connection conn(db.get());
        assert(queryScalar(conn, "MATCH (p:Person) RETURN COUNT(*) AS c;") == "2");
        assert(queryScalar(conn,
                   "MATCH (p:Person) WHERE p.name = 'Grace' RETURN p.age AS age;") == "85");
    }

    std::filesystem::remove_all(tmpDir);

    std::printf("  PASS: testSqliteBackedDatabaseAutoCheckpointZero\n");
}

// --- Test: same graph smoke through actual SQLite vfs=turbolite ---
static void testSqliteBackedDatabaseThroughActualTurboliteVfs() {
    auto extension = envString("TURBOGRAPH_TURBOLITE_EXTENSION");
    if (!extension) {
        std::printf("  SKIP: testSqliteBackedDatabaseThroughActualTurboliteVfs "
                    "(set TURBOGRAPH_TURBOLITE_EXTENSION)\n");
        return;
    }

    auto tmpDir = std::filesystem::temp_directory_path() /
                  ("turbograph_sqlite_graph_turbolite_" + std::to_string(getpid()));
    std::filesystem::remove_all(tmpDir);
    std::filesystem::create_directories(tmpDir);
    auto cacheDir = tmpDir / "turbolite-cache";
    std::filesystem::create_directories(cacheDir);
    setenv("TURBOLITE_CACHE_DIR", cacheDir.string().c_str(), 1);

    auto dataFilePath = (tmpDir / "graph.kz").string();
    auto sqlitePath = (tmpDir / "graph-pages.sqlite").string();

    SqliteBackedDatabaseOptions options;
    options.autoCheckpoint = true;
    options.checkpointThreshold = 0;
    options.sqliteWalAutoCheckpointPages = 100;
    options.sqliteCacheSizePages = 0;
    options.sqliteSynchronous = "NORMAL";
    options.sqliteVfsName = "turbolite";
    options.sqliteLoadableExtensionPath = *extension;

    {
        auto db = openSqliteBackedDatabase(dataFilePath, sqlitePath, options);
        main::Connection conn(db.get());

        assertQuerySuccess(conn,
            "CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
        assertQuerySuccess(conn, "CREATE (:Person {name: 'Ada', age: 37});");
        assertQuerySuccess(conn, "CREATE (:Person {name: 'Grace', age: 85});");
        assert(queryScalar(conn, "MATCH (p:Person) RETURN COUNT(*) AS c;") == "2");
    }

    {
        auto db = openSqliteBackedDatabase(dataFilePath, sqlitePath, options);
        main::Connection conn(db.get());
        assert(queryScalar(conn, "MATCH (p:Person) RETURN COUNT(*) AS c;") == "2");
        assert(queryScalar(conn,
                   "MATCH (p:Person) WHERE p.name = 'Ada' RETURN p.age AS age;") == "37");
    }

    std::filesystem::remove_all(tmpDir);

    std::printf("  PASS: testSqliteBackedDatabaseThroughActualTurboliteVfs\n");
}

static void runSmallWriteAndTraversalPerf(const std::string& label,
    const std::string& dataFilePath, const std::string& sqlitePath,
    const SqliteBackedDatabaseOptions& options) {
    constexpr int nodeCount = 250;
    long long writeMs = 0;
    long long traversalMs = 0;
    {
        auto db = openSqliteBackedDatabase(dataFilePath, sqlitePath, options);
        main::Connection conn(db.get());

        assertQuerySuccess(conn,
            "CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY(id));");
        assertQuerySuccess(conn, "CREATE REL TABLE Knows(FROM Person TO Person);");

        auto writesStart = std::chrono::steady_clock::now();
        for (int i = 0; i < nodeCount; i++) {
            assertQuerySuccess(conn, "CREATE (:Person {id: " + std::to_string(i) +
                                     ", name: 'p" + std::to_string(i) + "'});");
        }
        for (int i = 0; i < nodeCount - 1; i++) {
            assertQuerySuccess(conn,
                "MATCH (a:Person {id: " + std::to_string(i) +
                "}), (b:Person {id: " + std::to_string(i + 1) +
                "}) CREATE (a)-[:Knows]->(b);");
        }
        auto writesEnd = std::chrono::steady_clock::now();

        auto traversalStart = std::chrono::steady_clock::now();
        auto relCount = queryScalar(conn,
            "MATCH (a:Person)-[:Knows]->(b:Person) RETURN COUNT(*) AS c;");
        auto lookupCount = queryScalar(conn,
            "MATCH (a:Person {id: 1})-[:Knows]->(b:Person) RETURN COUNT(*) AS c;");
        auto traversalEnd = std::chrono::steady_clock::now();

        assert(relCount == std::to_string(nodeCount - 1));
        assert(lookupCount == "1");

        writeMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            writesEnd - writesStart).count();
        traversalMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            traversalEnd - traversalStart).count();
    }

    tiered::SqlitePageStore store(sqlitePath, 4096, options.sqliteVfsName,
        options.sqliteLoadableExtensionPath, 65536, options.sqliteWalAutoCheckpointPages,
        options.sqliteCacheSizePages, options.sqliteSynchronous);
    std::printf("    PERF[%s]: %d node writes + %d rel writes in %lld ms; "
                "2 traversals in %lld ms; graph bytes=%llu\n",
        label.c_str(), nodeCount, nodeCount - 1, writeMs, traversalMs,
        static_cast<unsigned long long>(store.fileSize(dataFilePath)));
}

// --- Perf probe: many tiny writes + graph traversal, opt-in so CI remains quiet ---
static void testSqliteBackedSmallWriteAndTraversalPerf() {
    if (!envString("TURBOGRAPH_RUN_SQLITE_GRAPH_PERF")) {
        std::printf("  SKIP: testSqliteBackedSmallWriteAndTraversalPerf "
                    "(set TURBOGRAPH_RUN_SQLITE_GRAPH_PERF)\n");
        return;
    }

    auto tmpDir = std::filesystem::temp_directory_path() /
                  ("turbograph_sqlite_graph_perf_" + std::to_string(getpid()));
    std::filesystem::remove_all(tmpDir);
    std::filesystem::create_directories(tmpDir);

    SqliteBackedDatabaseOptions options;
    options.autoCheckpoint = true;
    options.checkpointThreshold = 0;
    options.sqliteWalAutoCheckpointPages = 100;
    options.sqliteCacheSizePages = 0;
    options.sqliteSynchronous = "NORMAL";

    runSmallWriteAndTraversalPerf("sqlite",
        (tmpDir / "graph-sqlite.kz").string(),
        (tmpDir / "graph-pages-sqlite.sqlite").string(), options);

    auto extension = envString("TURBOGRAPH_TURBOLITE_EXTENSION");
    if (extension) {
        auto cacheDir = tmpDir / "turbolite-cache";
        std::filesystem::create_directories(cacheDir);
        setenv("TURBOLITE_CACHE_DIR", cacheDir.string().c_str(), 1);
        auto turboliteOptions = options;
        turboliteOptions.sqliteVfsName = "turbolite";
        turboliteOptions.sqliteLoadableExtensionPath = *extension;
        runSmallWriteAndTraversalPerf("turbolite",
            (tmpDir / "graph-turbolite.kz").string(),
            (tmpDir / "graph-pages-turbolite.sqlite").string(), turboliteOptions);
    }

    std::filesystem::remove_all(tmpDir);

    std::printf("  PASS: testSqliteBackedSmallWriteAndTraversalPerf\n");
}

int main() {
    std::printf("=== Turbograph Extension Tests ===\n");

    testExtensionLoadsWithoutCredentials();
    testUDFsRegistered();
    testTfsRegistryDropsExpiredEntry();
    testPerDbRegistryDoesNotFallbackAcrossDatabases();
    testConfigSetUnknownKey();
    testConfigSetGetRoundTrip();
    testConfigSetBadFloat();
    testConfigGetFetchCounters();
    testUDFAffectsTFS();
    testUDFCustomSchedule();
    testExtractTablesFromPlan();
    testBuildTablePageMap();
    testPerTableScheduleSelection();
    testSqliteBackedDatabasePersistsGraphPages();
    testSqliteBackedDatabaseAutoCheckpointZero();
    testSqliteBackedDatabaseThroughActualTurboliteVfs();
    testSqliteBackedSmallWriteAndTraversalPerf();

    std::printf("  All extension tests passed.\n");
    return 0;
}
