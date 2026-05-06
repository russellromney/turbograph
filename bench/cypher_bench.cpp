// Cypher Benchmark — kuzudb-study queries against Tiered Storage (Tigris-backed).
//
// Generates a social network dataset (100K persons, ~1M follow edges, cities, interests),
// loads via COPY FROM CSV, then benchmarks 9 Cypher queries in warm and cold modes.
//
// Build: part of the Ladybug CMake build with -DBUILD_EXTENSIONS="tiered" -DBUILD_EXTENSION_TESTS=TRUE
// Run:   TIGRIS_STORAGE_ACCESS_KEY_ID=... TIGRIS_STORAGE_SECRET_ACCESS_KEY=...
//        TIGRIS_STORAGE_ENDPOINT=... ./cypher_bench

#include "tiered_file_system.h"
#include "sqlite_graph_file_system.h"
#include "table_page_map.h"
#include "main/turbograph_functions.h"

#include "main/connection.h"
#include "main/database.h"
#include "main/db_config.h"
#include "storage/buffer_manager/buffer_manager.h"
#include "storage/storage_utils.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <memory>
#include <optional>
#include <random>
#include <string>
#include <thread>
#include <vector>

#include <sqlite3.h>

using Clock = std::chrono::steady_clock;

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
        return *database.dbConfig;
    }
};

} // namespace lbug::testing

// --- Configuration ---

struct BenchConfig {
    uint32_t numPersons = 100000;
    uint32_t numCities = 200;
    uint32_t numInterests = 42;
    uint32_t followEdges = 0;         // 0 = auto (10x person count).
    int warmIterations = 3;
    int coldIterations = 2;
};

enum class StorageMode {
    TieredTigris,
    LocalFile,
    Sqlite,
    Turbolite,
    TurboliteS3,
};

static StorageMode parseStorageMode(int argc, char** argv) {
    if (argc <= 2) return StorageMode::TieredTigris;
    if (std::strcmp(argv[2], "local") == 0) return StorageMode::LocalFile;
    if (std::strcmp(argv[2], "sqlite") == 0) return StorageMode::Sqlite;
    if (std::strcmp(argv[2], "turbolite") == 0) return StorageMode::Turbolite;
    if (std::strcmp(argv[2], "turbolite-s3") == 0) return StorageMode::TurboliteS3;
    return StorageMode::TieredTigris;
}

static const char* modeName(StorageMode mode) {
    switch (mode) {
    case StorageMode::TieredTigris: return "TIERED_TIGRIS";
    case StorageMode::LocalFile: return "LOCAL";
    case StorageMode::Sqlite: return "SQLITE_PAGE_STORE";
    case StorageMode::Turbolite: return "SQLITE_PAGE_STORE_TURBOLITE_VFS";
    case StorageMode::TurboliteS3: return "SQLITE_PAGE_STORE_TURBOLITE_S3_VFS";
    }
    return "UNKNOWN";
}

static bool usesTigris(StorageMode mode) {
    return mode == StorageMode::TieredTigris;
}

static bool usesSqlitePageStore(StorageMode mode) {
    return mode == StorageMode::Sqlite || mode == StorageMode::Turbolite ||
           mode == StorageMode::TurboliteS3;
}

static std::optional<std::string> envString(const char* name) {
    auto value = std::getenv(name);
    if (value == nullptr || std::string(value).empty()) {
        return std::nullopt;
    }
    return std::string(value);
}

static int envInt(const char* name, int defaultValue) {
    auto value = std::getenv(name);
    if (value == nullptr || std::string(value).empty()) {
        return defaultValue;
    }
    return std::atoi(value);
}

static bool envBool(const char* name, bool defaultValue) {
    auto value = std::getenv(name);
    if (value == nullptr || std::string(value).empty()) {
        return defaultValue;
    }
    auto s = std::string(value);
    std::transform(s.begin(), s.end(), s.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    return !(s == "0" || s == "false" || s == "no" || s == "off");
}

static void sleepFromEnvMs(const char* name) {
    auto ms = envInt(name, 0);
    if (ms <= 0) {
        return;
    }
    std::printf("  Sleeping for %dms (%s)...\n", ms, name);
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

// --- Data Generation ---

static const char* FIRST_NAMES[] = {
    "James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
    "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica",
    "Thomas", "Sarah", "Charles", "Karen", "Daniel", "Lisa", "Matthew", "Nancy",
    "Anthony", "Betty", "Mark", "Margaret", "Donald", "Sandra", "Steven", "Ashley",
    "Paul", "Dorothy", "Andrew", "Kimberly", "Joshua", "Emily", "Kenneth", "Donna",
    "Kevin", "Michelle", "Brian", "Carol", "George", "Amanda", "Timothy", "Melissa",
    "Ronald", "Deborah"
};
static constexpr int NUM_FIRST = 50;

static const char* LAST_NAMES[] = {
    "Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
    "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson",
    "Thomas", "Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson",
    "White", "Harris", "Sanchez", "Clark", "Ramirez", "Lewis", "Robinson", "Walker",
    "Young", "Allen", "King", "Wright", "Scott", "Torres", "Nguyen", "Hill", "Flores"
};
static constexpr int NUM_LAST = 40;

static const char* CITIES[] = {
    "New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
    "San Antonio", "San Diego", "Dallas", "San Jose", "Austin", "Jacksonville",
    "Fort Worth", "Columbus", "Charlotte", "Indianapolis", "San Francisco",
    "Seattle", "Denver", "Washington", "Nashville", "Oklahoma City", "El Paso",
    "Boston", "Portland", "Las Vegas", "Memphis", "Louisville", "Baltimore",
    "Milwaukee", "Albuquerque", "Tucson", "Fresno", "Sacramento", "Mesa",
    "Kansas City", "Atlanta", "Omaha", "Colorado Springs", "Raleigh",
    "Long Beach", "Virginia Beach", "Miami", "Oakland", "Minneapolis",
    "Tampa", "Tulsa", "Arlington", "New Orleans", "Wichita",
    // UK cities
    "London", "Birmingham", "Manchester", "Leeds", "Glasgow", "Liverpool",
    "Bristol", "Sheffield", "Edinburgh", "Leicester", "Cardiff", "Belfast",
    "Nottingham", "Newcastle", "Brighton", "Oxford", "Cambridge", "York",
    "Bath", "Aberdeen",
    // Canadian cities
    "Toronto", "Montreal", "Vancouver", "Calgary", "Edmonton", "Ottawa",
    "Winnipeg", "Quebec City", "Hamilton", "Kitchener", "Halifax", "Victoria",
    "London ON", "Oshawa", "Windsor", "Saskatoon", "Regina", "St Johns",
    "Kelowna", "Barrie"
};

static const char* CITY_COUNTRIES[] = {
    // US (50)
    "United States", "United States", "United States", "United States", "United States",
    "United States", "United States", "United States", "United States", "United States",
    "United States", "United States", "United States", "United States", "United States",
    "United States", "United States", "United States", "United States", "United States",
    "United States", "United States", "United States", "United States", "United States",
    "United States", "United States", "United States", "United States", "United States",
    "United States", "United States", "United States", "United States", "United States",
    "United States", "United States", "United States", "United States", "United States",
    "United States", "United States", "United States", "United States", "United States",
    "United States", "United States", "United States", "United States", "United States",
    // UK (20)
    "United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom",
    "United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom",
    "United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom",
    "United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom", "United Kingdom",
    // Canada (20)
    "Canada", "Canada", "Canada", "Canada", "Canada", "Canada", "Canada", "Canada",
    "Canada", "Canada", "Canada", "Canada", "Canada", "Canada", "Canada", "Canada",
    "Canada", "Canada", "Canada", "Canada"
};

static const char* STATES[] = {
    // US
    "New York", "California", "Illinois", "Texas", "Arizona", "Pennsylvania",
    "Texas", "California", "Texas", "California", "Texas", "Florida",
    "Texas", "Ohio", "North Carolina", "Indiana", "California",
    "Washington", "Colorado", "District of Columbia", "Tennessee", "Oklahoma", "Texas",
    "Massachusetts", "Oregon", "Nevada", "Tennessee", "Kentucky", "Maryland",
    "Wisconsin", "New Mexico", "Arizona", "California", "California", "Arizona",
    "Missouri", "Georgia", "Nebraska", "Colorado", "North Carolina",
    "California", "Virginia", "Florida", "California", "Minnesota",
    "Florida", "Oklahoma", "Texas", "Louisiana", "Kansas",
    // UK
    "England", "England", "England", "England", "Scotland", "England",
    "England", "England", "Scotland", "England", "Wales", "Northern Ireland",
    "England", "England", "England", "England", "England", "England",
    "England", "Scotland",
    // Canada
    "Ontario", "Quebec", "British Columbia", "Alberta", "Alberta", "Ontario",
    "Manitoba", "Quebec", "Ontario", "Ontario", "Nova Scotia", "British Columbia",
    "Ontario", "Ontario", "Ontario", "Saskatchewan", "Saskatchewan", "Newfoundland",
    "British Columbia", "Ontario"
};

static const char* INTERESTS[] = {
    "Photography", "Golf", "Tennis", "Cooking", "Anime", "Fine Dining",
    "Hiking", "Swimming", "Reading", "Gaming", "Painting", "Yoga",
    "Cycling", "Running", "Gardening", "Fishing", "Dancing", "Music",
    "Travel", "Meditation", "Rock Climbing", "Surfing", "Skiing", "Chess",
    "Pottery", "Woodworking", "Knitting", "Baking", "Bird Watching", "Astronomy",
    "Film Making", "Writing", "Martial Arts", "Archery", "Fencing", "Sailing",
    "Scuba Diving", "Paragliding", "Mountain Biking", "Kayaking", "Board Games",
    "Volunteering"
};

static void generateCSVData(const std::string& dataDir, const BenchConfig& cfg) {
    std::mt19937 rng(42);
    std::filesystem::create_directories(dataDir);

    // Persons CSV.
    {
        std::ofstream f(dataDir + "/persons.csv");
        f << "id,name,gender,age,isMarried\n";
        for (uint32_t i = 0; i < cfg.numPersons; i++) {
            auto firstName = FIRST_NAMES[rng() % NUM_FIRST];
            auto lastName = LAST_NAMES[rng() % NUM_LAST];
            auto gender = (rng() % 2 == 0) ? "male" : "female";
            auto age = 18 + (rng() % 62);  // 18-79.
            auto married = (rng() % 2 == 0) ? "true" : "false";
            f << i << "," << firstName << " " << lastName << "," << gender << ","
              << age << "," << married << "\n";
        }
    }

    // Cities CSV.
    {
        std::ofstream f(dataDir + "/cities.csv");
        f << "id,city,state,country,population\n";
        uint32_t numCities = std::min(cfg.numCities, (uint32_t)90);
        for (uint32_t i = 0; i < numCities; i++) {
            auto pop = 100000 + (rng() % 9000000);
            f << i << "," << CITIES[i] << "," << STATES[i] << "," << CITY_COUNTRIES[i]
              << "," << pop << "\n";
        }
    }

    // Interests CSV.
    {
        std::ofstream f(dataDir + "/interests.csv");
        f << "id,interest\n";
        uint32_t numInterests = std::min(cfg.numInterests, (uint32_t)42);
        for (uint32_t i = 0; i < numInterests; i++) {
            f << i << "," << INTERESTS[i] << "\n";
        }
    }

    // Follows edges (person -> person).
    {
        std::ofstream f(dataDir + "/follows.csv");
        f << "from,to\n";
        std::uniform_int_distribution<uint32_t> dist(0, cfg.numPersons - 1);
        for (uint32_t i = 0; i < cfg.followEdges; i++) {
            auto from = dist(rng);
            auto to = dist(rng);
            if (from != to) {
                f << from << "," << to << "\n";
            }
        }
    }

    // LivesIn edges (person -> city).
    {
        std::ofstream f(dataDir + "/lives_in.csv");
        f << "from,to\n";
        uint32_t numCities = std::min(cfg.numCities, (uint32_t)90);
        std::uniform_int_distribution<uint32_t> cityDist(0, numCities - 1);
        for (uint32_t i = 0; i < cfg.numPersons; i++) {
            f << i << "," << cityDist(rng) << "\n";
        }
    }

    // HasInterest edges (person -> interest, 1-4 each).
    {
        std::ofstream f(dataDir + "/has_interest.csv");
        f << "from,to\n";
        uint32_t numInterests = std::min(cfg.numInterests, (uint32_t)42);
        std::uniform_int_distribution<uint32_t> intDist(0, numInterests - 1);
        for (uint32_t i = 0; i < cfg.numPersons; i++) {
            auto count = 1 + (rng() % 4);
            for (uint32_t j = 0; j < count; j++) {
                f << i << "," << intDist(rng) << "\n";
            }
        }
    }

    std::printf("  Generated CSV data in %s\n", dataDir.c_str());
}

// --- Benchmark Helpers ---

struct QueryDef {
    const char* name;
    const char* cypher;
    const char* schedule; // "scan", "lookup", or "default"
};

static const QueryDef QUERIES[] = {
    {"Q1: Top 3 most-followed",
     "MATCH (follower:Person)-[:Follows]->(person:Person) "
     "RETURN person.name AS name, count(follower.id) AS numFollowers "
     "ORDER BY numFollowers DESC LIMIT 3",
     "scan"},

    {"Q2: City of most-followed",
     "MATCH (follower:Person)-[:Follows]->(person:Person) "
     "WITH person, count(follower.id) as numFollowers "
     "ORDER BY numFollowers DESC LIMIT 1 "
     "MATCH (person)-[:LivesIn]->(city:City) "
     "RETURN person.name AS name, numFollowers, city.city AS city, city.country AS country",
     "scan"},

    {"Q3: Youngest cities US",
     "MATCH (p:Person)-[:LivesIn]->(c:City) "
     "WHERE c.country = 'United States' "
     "RETURN c.city AS city, avg(p.age) AS averageAge "
     "ORDER BY averageAge LIMIT 5",
     "scan"},

    {"Q4: Persons 30-40 by country",
     "MATCH (p:Person)-[:LivesIn]->(c:City) "
     "WHERE p.age >= 30 AND p.age <= 40 "
     "RETURN c.country AS country, count(p) AS personCount "
     "ORDER BY personCount DESC LIMIT 3",
     "scan"},

    {"Q5: Male fine diners London",
     "MATCH (p:Person)-[:HasInterest]->(i:Interest) "
     "WHERE lower(i.interest) = 'fine dining' AND lower(p.gender) = 'male' "
     "WITH p "
     "MATCH (p)-[:LivesIn]->(c:City) "
     "WHERE c.city = 'London' AND c.country = 'United Kingdom' "
     "RETURN count(p) AS numPersons",
     "scan"},

    {"Q6: Female tennis by city",
     "MATCH (p:Person)-[:HasInterest]->(i:Interest) "
     "WHERE lower(i.interest) = 'tennis' AND lower(p.gender) = 'female' "
     "WITH p "
     "MATCH (p)-[:LivesIn]->(c:City) "
     "RETURN count(p.id) AS numPersons, c.city AS city, c.country AS country "
     "ORDER BY numPersons DESC LIMIT 5",
     "scan"},

    {"Q7: US photographers 23-30",
     "MATCH (p:Person)-[:LivesIn]->(c:City) "
     "WHERE p.age >= 23 AND p.age <= 30 AND c.country = 'United States' "
     "WITH p "
     "MATCH (p)-[:HasInterest]->(i:Interest) "
     "WHERE lower(i.interest) = 'photography' "
     "RETURN count(p.id) AS numPersons",
     "scan"},

    {"Q8: 2-hop path count",
     "MATCH (a:Person)-[:Follows]->(b:Person)-[:Follows]->(c:Person) "
     "RETURN count(*) AS numPaths",
     "scan"},

    {"Q9: Filtered 2-hop paths",
     "MATCH (a:Person)-[:Follows]->(b:Person)-[:Follows]->(c:Person) "
     "WHERE b.age < 50 AND c.age > 25 "
     "RETURN count(*) AS numPaths",
     "scan"},
};
static constexpr int NUM_QUERIES = 9;

struct LatencyStats {
    std::vector<int64_t> samples;
    int64_t avg() const {
        if (samples.empty()) return 0;
        int64_t sum = 0;
        for (auto s : samples) sum += s;
        return sum / static_cast<int64_t>(samples.size());
    }
    int64_t p50() const {
        if (samples.empty()) return 0;
        auto sorted = samples;
        std::sort(sorted.begin(), sorted.end());
        return sorted[sorted.size() / 2];
    }
};

// Prepare query to get the plan, extract table IDs, prefetch their page groups.
// Returns number of groups submitted (0 if no TFS or no table map).
static uint64_t frontrunPrefetch(lbug::main::Connection& conn,
    lbug::tiered::TieredFileSystem* tfs, const char* cypher) {
    if (!tfs) return 0;
    auto [nodeIds, relIds] =
        lbug::turbograph_extension::extractTablesFromPlan(conn, cypher);
    std::vector<uint32_t> allIds;
    for (auto id : nodeIds) allIds.push_back(static_cast<uint32_t>(id));
    for (auto id : relIds) allIds.push_back(static_cast<uint32_t>(id));
    if (allIds.empty()) return 0;
    return tfs->prefetchTables(allIds);
}

static std::string formatUs(int64_t us) {
    char buf[32];
    if (us >= 1000000) {
        std::snprintf(buf, sizeof(buf), "%4.1fM", us / 1e6);
    } else if (us >= 1000) {
        std::snprintf(buf, sizeof(buf), "%4.1fK", us / 1e3);
    } else {
        std::snprintf(buf, sizeof(buf), "%4lld", (long long)us);
    }
    return std::string(buf);
}

static uint64_t pathSize(const std::string& path) {
    auto status = std::filesystem::status(path);
    if (std::filesystem::is_regular_file(status)) {
        return std::filesystem::file_size(path);
    }
    uint64_t total = 0;
    if (std::filesystem::is_directory(status)) {
        for (auto& entry : std::filesystem::recursive_directory_iterator(path)) {
            if (entry.is_regular_file()) {
                total += entry.file_size();
            }
        }
    }
    return total;
}

struct TurboliteS3Counters {
    uint64_t gets = 0;
    uint64_t getBytes = 0;
    uint64_t puts = 0;
    uint64_t putBytes = 0;
};

class TurboliteSqlMetrics {
public:
    explicit TurboliteSqlMetrics(const std::string& extensionPath) {
        if (sqlite3_open(":memory:", &db_) != SQLITE_OK) {
            fail("open metrics sqlite connection");
        }
        sqlite3_enable_load_extension(db_, 1);
        auto path = normalizeExtensionPath(extensionPath);
        char* err = nullptr;
        auto rc = sqlite3_load_extension(db_, path.c_str(), "sqlite3_turbolite_init", &err);
        if (rc != SQLITE_OK) {
            std::string message = err ? err : "unknown error";
            sqlite3_free(err);
            fail(("load turbolite metrics extension: " + message).c_str());
        }
    }

    ~TurboliteSqlMetrics() {
        if (db_) sqlite3_close(db_);
    }

    TurboliteSqlMetrics(const TurboliteSqlMetrics&) = delete;
    TurboliteSqlMetrics& operator=(const TurboliteSqlMetrics&) = delete;

    void reset() {
        (void)scalar("SELECT turbolite_reset_s3_counters()");
    }

    void clearCache(const char* mode) {
        auto sql = std::string("SELECT turbolite_clear_cache('") + mode + "')";
        auto rc = scalar(sql.c_str());
        if (rc != 0) {
            fail("turbolite_clear_cache failed");
        }
    }

    int64_t flushToStorage() {
        auto start = Clock::now();
        auto rc = scalar("SELECT turbolite_flush_to_storage()");
        if (rc != 0) {
            fail("turbolite_flush_to_storage failed");
        }
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now() - start).count();
    }

    TurboliteS3Counters counters() {
        TurboliteS3Counters c;
        c.gets = scalar("SELECT turbolite_s3_gets()");
        c.getBytes = scalar("SELECT turbolite_s3_bytes()");
        c.puts = scalar("SELECT turbolite_s3_puts()");
        c.putBytes = scalar("SELECT turbolite_s3_put_bytes()");
        return c;
    }

private:
    static std::string normalizeExtensionPath(std::string path) {
#if defined(__APPLE__)
        const std::string suffix = ".dylib";
#else
        const std::string suffix = ".so";
#endif
        if (path.size() > suffix.size() &&
            path.compare(path.size() - suffix.size(), suffix.size(), suffix) == 0) {
            path.resize(path.size() - suffix.size());
        }
        return path;
    }

    uint64_t scalar(const char* sql) {
        sqlite3_stmt* stmt = nullptr;
        auto rc = sqlite3_prepare_v2(db_, sql, -1, &stmt, nullptr);
        if (rc != SQLITE_OK) {
            fail(sqlite3_errmsg(db_));
        }
        rc = sqlite3_step(stmt);
        if (rc != SQLITE_ROW) {
            auto msg = sqlite3_errmsg(db_);
            sqlite3_finalize(stmt);
            fail(msg);
        }
        auto value = sqlite3_column_int64(stmt, 0);
        sqlite3_finalize(stmt);
        return static_cast<uint64_t>(value);
    }

    [[noreturn]] void fail(const char* message) const {
        std::fprintf(stderr, "Turbolite metrics error: %s\n", message);
        std::exit(1);
    }

    sqlite3* db_ = nullptr;
};

struct StoragePaths {
    std::string dbPath;
    std::string dataDir;
    std::string cacheDir;
    std::string sqlitePath;
    std::string turboliteCacheDir;
};

static std::unique_ptr<lbug::common::FileSystem> makeSqliteGraphFileSystem(
    const StoragePaths& paths, StorageMode mode) {
    lbug::tiered::SqliteGraphFileSystemConfig cfg;
    cfg.sqlitePath = paths.sqlitePath;
    cfg.dataFilePath = paths.dbPath;
    cfg.dataFileId = envString("BENCH_GRAPH_ID").value_or(paths.dbPath);
    cfg.graphPageSize = 4096;
    cfg.sqlitePageSize = 65536;
    cfg.sqliteWalAutoCheckpointPages =
        std::getenv("TURBOGRAPH_SQLITE_WAL_AUTOCHECKPOINT") ?
        std::atoi(std::getenv("TURBOGRAPH_SQLITE_WAL_AUTOCHECKPOINT")) : 1000;
    cfg.sqliteCacheSizePages =
        std::getenv("TURBOGRAPH_SQLITE_CACHE_SIZE_PAGES") ?
        std::atoi(std::getenv("TURBOGRAPH_SQLITE_CACHE_SIZE_PAGES")) : 0;
    cfg.sqliteSynchronous = envString("TURBOGRAPH_SQLITE_SYNCHRONOUS").value_or("NORMAL");

    if (mode == StorageMode::Turbolite || mode == StorageMode::TurboliteS3) {
        auto extension = envString("TURBOGRAPH_TURBOLITE_EXTENSION");
        if (!extension) {
            std::fprintf(stderr,
                "Set TURBOGRAPH_TURBOLITE_EXTENSION for turbolite benchmark modes\n");
            std::exit(1);
        }
        std::filesystem::create_directories(paths.turboliteCacheDir);
        setenv("TURBOLITE_CACHE_DIR", paths.turboliteCacheDir.c_str(), 1);
        cfg.sqliteVfsName = mode == StorageMode::TurboliteS3 ? "turbolite-s3" : "turbolite";
        cfg.sqliteLoadableExtensionPath = *extension;
    }

    return std::make_unique<lbug::tiered::SqliteGraphFileSystem>(std::move(cfg));
}

static void addGraphFileSystem(std::vector<std::unique_ptr<lbug::common::FileSystem>>& fsList,
    const StoragePaths& paths, StorageMode mode, const lbug::tiered::TieredConfig& tieredCfg) {
    if (mode == StorageMode::TieredTigris) {
        fsList.push_back(std::make_unique<lbug::tiered::TieredFileSystem>(tieredCfg));
    } else if (usesSqlitePageStore(mode)) {
        fsList.push_back(makeSqliteGraphFileSystem(paths, mode));
    }
}

struct OpenBenchDatabaseResult {
    std::unique_ptr<lbug::main::Database> db;
    lbug::tiered::TieredFileSystem* tfs = nullptr;
    lbug::tiered::S3Client* s3 = nullptr;
};

static OpenBenchDatabaseResult openBenchDatabase(const StoragePaths& paths, StorageMode mode,
    const lbug::tiered::TieredConfig& tieredCfg, lbug::main::SystemConfig sysCfg,
    std::optional<std::string> tieredCacheDir = std::nullopt) {
    struct Handles {
        lbug::tiered::TieredFileSystem* tfs = nullptr;
        lbug::tiered::S3Client* s3 = nullptr;
    };
    auto handles = std::make_shared<Handles>();

    auto constructBM = [paths, mode, tieredCfg, tieredCacheDir, handles](
                           const lbug::main::Database& db) mutable {
        const auto& expandedDataFilePath = lbug::testing::BaseGraphTest::getDatabasePath(db);
        const auto& dbConfig = lbug::testing::BaseGraphTest::getDBConfig(db);

        if (mode == StorageMode::TieredTigris) {
            auto cfg = tieredCfg;
            cfg.dataFilePath = expandedDataFilePath;
            if (tieredCacheDir) {
                cfg.cacheDir = *tieredCacheDir;
            }
            auto tfs = std::make_unique<lbug::tiered::TieredFileSystem>(cfg);
            tfs->setMetadataParser(lbug::turbograph_extension::parseMetadataPages);
            handles->tfs = tfs.get();
            handles->s3 = &tfs->s3();
            const_cast<lbug::main::Database&>(db).registerFileSystem(std::move(tfs));
        } else if (usesSqlitePageStore(mode)) {
            auto fs = makeSqliteGraphFileSystem(paths, mode);
            const_cast<lbug::main::Database&>(db).registerFileSystem(std::move(fs));
        }

        return std::make_unique<lbug::storage::BufferManager>(expandedDataFilePath,
            lbug::storage::StorageUtils::getTmpFilePath(expandedDataFilePath),
            dbConfig.bufferPoolSize, dbConfig.maxDBSize,
            const_cast<lbug::main::Database&>(db).getVFS(), dbConfig.readOnly);
    };

    OpenBenchDatabaseResult result;
    result.db = lbug::testing::BaseGraphTest::constructDB(paths.dbPath, sysCfg,
        std::move(constructBM));
    result.tfs = handles->tfs;
    result.s3 = handles->s3;
    return result;
}

static void exec(lbug::main::Connection& conn, const char* query) {
    auto r = conn.query(query);
    if (!r->isSuccess()) {
        std::fprintf(stderr, "FAILED: %s\n  %s\n", query, r->getErrorMessage().c_str());
        std::exit(1);
    }
}

static void runWriteCheckpointCycles(lbug::main::Connection& conn, uint32_t startId,
    int cycles, int checkpointEvery) {
    if (cycles <= 0) {
        return;
    }
    if (checkpointEvery <= 0) {
        checkpointEvery = 1;
    }

    std::printf("  Repeated write proof: %d writes, checkpoint every %d...\n",
        cycles, checkpointEvery);
    auto start = Clock::now();
    auto writeDelayMs = envInt("BENCH_WRITE_DELAY_MS", 0);
    for (int i = 0; i < cycles; i++) {
        auto id = static_cast<uint64_t>(startId) + static_cast<uint64_t>(i);
        auto q = std::string("CREATE (:Person {id: ") + std::to_string(id) +
                 ", name: 'Write Probe " + std::to_string(i) +
                 "', gender: 'probe', age: " + std::to_string(20 + (i % 50)) +
                 ", isMarried: false})";
        exec(conn, q.c_str());
        if (writeDelayMs > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(writeDelayMs));
        }
        if ((i + 1) % checkpointEvery == 0) {
            exec(conn, "CHECKPOINT");
        }
    }
    if (cycles % checkpointEvery != 0) {
        exec(conn, "CHECKPOINT");
    }

    auto verify = std::string("MATCH (p:Person) WHERE p.id >= ") +
                  std::to_string(startId) + " RETURN count(p)";
    exec(conn, verify.c_str());
    auto elapsedMs = std::chrono::duration_cast<std::chrono::milliseconds>(
        Clock::now() - start).count();
    std::printf("  Repeated write proof: passed in %lldms\n", (long long)elapsedMs);
}

// Published Neo4j 5.11 numbers from kuzudb-study (100K persons, M2 Mac).
// Source: thedataquarry.com/blog/embedded-db-2/
static const int64_t NEO4J_US[] = {
    1889900,  // Q1: 1.8899s
     693600,  // Q2: 0.6936s
      44200,  // Q3: 0.0442s
      47300,  // Q4: 0.0473s
       8600,  // Q5: 0.0086s
      22600,  // Q6: 0.0226s
     162500,  // Q7: 0.1625s
    3452900,  // Q8: 3.4529s
    4270700,  // Q9: 4.2707s
};

// --- Main ---

int main(int argc, char** argv) {
    std::setvbuf(stdout, nullptr, _IONBF, 0);  // Unbuffered stdout for container logs.

    BenchConfig cfg;
    if (argc > 1) cfg.numPersons = std::atoi(argv[1]);
    auto personsEnv = std::getenv("BENCH_PERSONS");
    if (personsEnv) cfg.numPersons = std::atoi(personsEnv);
    cfg.coldIterations = envInt("BENCH_COLD_ITERATIONS", cfg.coldIterations);
    cfg.warmIterations = envInt("BENCH_WARM_ITERATIONS", cfg.warmIterations);
    auto storageMode = parseStorageMode(argc, argv);
    bool localMode = storageMode == StorageMode::LocalFile;
    if (cfg.followEdges == 0) cfg.followEdges = cfg.numPersons * 10;

    auto ak = std::getenv("TIGRIS_STORAGE_ACCESS_KEY_ID");
    auto sk = std::getenv("TIGRIS_STORAGE_SECRET_ACCESS_KEY");
    auto ep = std::getenv("TIGRIS_STORAGE_ENDPOINT");
    if (usesTigris(storageMode) && (!ak || !sk || !ep)) {
        std::fprintf(stderr,
            "Set TIGRIS_STORAGE_* env vars (or pass local/sqlite/turbolite/turbolite-s3 as 3rd arg)\n");
        return 1;
    }

    // Deterministic prefix so data persists across runs.
    std::string tag = std::to_string(cfg.numPersons / 1000) + "k";
    if (auto tagSuffix = envString("BENCH_TAG_SUFFIX")) {
        tag += "_" + *tagSuffix;
    }
    auto modeSuffix = std::string("_") + modeName(storageMode);
    std::replace(modeSuffix.begin(), modeSuffix.end(), '/', '_');

    // Use /data (Fly volume) if it exists, else /tmp.
    std::string base = envString("BENCH_BASE_DIR").value_or(
        std::filesystem::exists("/data") ? "/data" : "/tmp");
    std::filesystem::create_directories(base);
    StoragePaths paths;
    paths.dbPath = envString("BENCH_DB_PATH").value_or(
        base + "/cypher_bench_" + tag + modeSuffix + ".kz");
    paths.dataDir = base + "/cypher_bench_data_" + tag + modeSuffix;
    paths.cacheDir = base + "/cypher_bench_cache_" + tag + modeSuffix;
    paths.sqlitePath = base + "/cypher_bench_pages_" + tag + modeSuffix + ".sqlite";
    paths.turboliteCacheDir = base + "/cypher_bench_turbolite_cache_" + tag + modeSuffix;

    bool dbExists = std::filesystem::exists(paths.dbPath);
    if (usesSqlitePageStore(storageMode)) {
        dbExists = std::filesystem::exists(paths.sqlitePath);
    }
    if (envBool("BENCH_FORCE_REUSE_DB", false)) {
        dbExists = true;
    }
    std::printf("  Mode: %s\n", modeName(storageMode));
    std::printf("  DB path: %s (%s)\n", paths.dbPath.c_str(), dbExists ? "exists" : "new");
    if (usesSqlitePageStore(storageMode)) {
        std::printf("  SQLite page store: %s\n", paths.sqlitePath.c_str());
    }

    lbug::tiered::TieredConfig tieredCfg;
    if (usesTigris(storageMode)) {
        auto bucket = envString("TIGRIS_STORAGE_BUCKET").value_or("cinch-data");
        tieredCfg.s3 = {ep, bucket, "bench/cypher/v2/" + tag, "auto", ak, sk};
        tieredCfg.dataFilePath = paths.dbPath;
        tieredCfg.cacheDir = paths.cacheDir;
        tieredCfg.pageSize = 4096;
        // Prefetch schedules: PREFETCH_SCAN, PREFETCH_LOOKUP env vars
        // (comma-separated floats). Defaults: scan=0.3,0.3,0.4  lookup=0,0,0
        auto parseHops = [](const char* env, std::vector<float>& out) {
            if (!env) return;
            out.clear();
            std::string s(env);
            size_t pos = 0;
            while (pos < s.size()) {
                auto comma = s.find(',', pos);
                auto token = (comma == std::string::npos) ? s.substr(pos) : s.substr(pos, comma - pos);
                if (!token.empty()) out.push_back(std::stof(token));
                if (comma == std::string::npos) break;
                pos = comma + 1;
            }
        };
        parseHops(std::getenv("PREFETCH_SCAN"), tieredCfg.schedules.scan);
        parseHops(std::getenv("PREFETCH_LOOKUP"), tieredCfg.schedules.lookup);
        parseHops(std::getenv("PREFETCH_DEFAULT"), tieredCfg.schedules.defaultSchedule);
        auto threadsEnv = std::getenv("PREFETCH_THREADS");
        if (threadsEnv) tieredCfg.prefetchThreads = std::atoi(threadsEnv);
        auto subPagesEnv = std::getenv("SUB_PAGES_PER_FRAME");
        if (subPagesEnv) tieredCfg.subPagesPerFrame = std::atoi(subPagesEnv);
        std::printf("  Schedules: scan=[");
        for (size_t i = 0; i < tieredCfg.schedules.scan.size(); i++)
            std::printf("%s%.2f", i ? "," : "", tieredCfg.schedules.scan[i]);
        std::printf("] lookup=[");
        for (size_t i = 0; i < tieredCfg.schedules.lookup.size(); i++)
            std::printf("%s%.2f", i ? "," : "", tieredCfg.schedules.lookup[i]);
        std::printf("]\n");
        auto resolvedThreads = tieredCfg.prefetchThreads > 0
            ? tieredCfg.prefetchThreads
            : std::max(1u, std::thread::hardware_concurrency() - 1);
        std::printf(" (+ implicit remainder)  threads=%u  sub_pages_per_frame=%u\n",
            resolvedThreads, tieredCfg.subPagesPerFrame);
    }

    // Buffer pool size: BUFFER_POOL_MB env var (default: 256MB).
    // For 100K (25MB DB), 256MB is plenty. For 3M (16GB), set to ~51000 on Fly.
    uint64_t bufferPoolMB = 256;
    auto bpEnv = std::getenv("BUFFER_POOL_MB");
    if (bpEnv) bufferPoolMB = std::atoi(bpEnv);
    uint64_t bufferPoolBytes = bufferPoolMB * 1024ULL * 1024ULL;

    int64_t loadMs = 0;
    int64_t ckptMs = 0;
    uint64_t dbSizeBytes = 0;

    // Load data (skip if DB already exists locally or in Tigris).
    if (!dbExists) {
        // Clean up ALL stale local artifacts from previous crashed runs
        // (DB dir, cache dir, CSV data dir, shadow files, WAL files, etc.).
        std::filesystem::remove_all(paths.dbPath);
        std::filesystem::remove_all(paths.cacheDir);
        std::filesystem::remove_all(paths.dataDir);
        std::filesystem::remove(paths.sqlitePath);
        std::filesystem::remove(paths.sqlitePath + "-wal");
        std::filesystem::remove(paths.sqlitePath + "-shm");
        std::filesystem::remove_all(paths.turboliteCacheDir);
        for (auto& entry : std::filesystem::directory_iterator(base)) {
            auto name = entry.path().filename().string();
            if (name.find("cypher_bench") != std::string::npos && name.find(tag) != std::string::npos) {
                std::filesystem::remove_all(entry.path());
            }
        }

        // In tiered mode, unconditionally clean up ALL Tigris objects at this prefix.
        // The Kuzu catalog (node/rel table defs) lives on local disk and doesn't
        // survive container restarts, so we must load fresh every time.
        if (usesTigris(storageMode)) {
            lbug::tiered::S3Client s3(tieredCfg.s3);

            // Delete manifest explicitly by key (listObjects has SigV4 issues).
            auto manifestKey = tieredCfg.s3.prefix + "/manifest.json";
            std::printf("  Deleting manifest: %s\n", manifestKey.c_str());
            s3.deleteObject(manifestKey);

            // Also try to list and delete page groups.
            auto pgKeys = s3.listObjects(tieredCfg.s3.prefix + "/pg/");
            std::printf("  Found %zu page group objects.\n", pgKeys.size());
            for (auto& key : pgKeys) {
                s3.deleteObject(key);
            }

            // Verify manifest is gone.
            auto manifest = s3.getManifest();
            std::printf("  Manifest after cleanup: %s\n",
                manifest.has_value() ? "EXISTS (BUG!)" : "absent (good)");
        }
    }

    if (!dbExists) {
        std::printf("  Generating data (%u persons, %u follow edges)...\n",
            cfg.numPersons, cfg.followEdges);
        auto genStart = Clock::now();
        generateCSVData(paths.dataDir, cfg);
        auto genMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now() - genStart).count();
        std::printf("  Data generation: %lldms\n", (long long)genMs);

        std::printf("  Creating database (%s mode)...\n", modeName(storageMode));

        lbug::main::SystemConfig sysCfg;
        sysCfg.bufferPoolSize = bufferPoolBytes;
        if (usesSqlitePageStore(storageMode)) {
            sysCfg.autoCheckpoint = true;
            sysCfg.checkpointThreshold = 0;
            sysCfg.forceCheckpointOnClose = false;
        }
        auto openDb = openBenchDatabase(paths, storageMode, tieredCfg, sysCfg);
        std::unique_ptr<TurboliteSqlMetrics> turboliteMetrics;
        if (storageMode == StorageMode::TurboliteS3) {
            turboliteMetrics = std::make_unique<TurboliteSqlMetrics>(
                envString("TURBOGRAPH_TURBOLITE_EXTENSION").value());
            turboliteMetrics->reset();
        }
        lbug::main::Connection conn(openDb.db.get());

        std::printf("  Creating schema...\n");
        exec(conn, "CREATE NODE TABLE Person(id INT64, name STRING, gender STRING, "
                   "age INT64, isMarried BOOLEAN, PRIMARY KEY (id))");
        exec(conn, "CREATE NODE TABLE City(id INT64, city STRING, state STRING, "
                   "country STRING, population INT32, PRIMARY KEY (id))");
        exec(conn, "CREATE NODE TABLE Interest(id INT64, interest STRING, PRIMARY KEY (id))");
        exec(conn, "CREATE REL TABLE Follows(FROM Person TO Person)");
        exec(conn, "CREATE REL TABLE LivesIn(FROM Person TO City)");
        exec(conn, "CREATE REL TABLE HasInterest(FROM Person TO Interest)");

        std::printf("  Loading data...\n");
        auto loadStart = Clock::now();

        auto copyCmd = [&](const char* table, const char* file) {
            auto q = std::string("COPY ") + table + " FROM '" + paths.dataDir + "/" + file + "'";
            exec(conn, q.c_str());
        };
        copyCmd("Person", "persons.csv");
        copyCmd("City", "cities.csv");
        copyCmd("Interest", "interests.csv");
        copyCmd("Follows", "follows.csv");
        copyCmd("LivesIn", "lives_in.csv");
        copyCmd("HasInterest", "has_interest.csv");

        loadMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now() - loadStart).count();
        std::printf("  Data loaded in %lldms\n", (long long)loadMs);
        sleepFromEnvMs("BENCH_SLEEP_AFTER_LOAD_MS");

        std::printf("  Checkpointing%s...\n",
            usesTigris(storageMode) ? " (flushing to Tigris)" : "");
        auto ckptStart = Clock::now();
        exec(conn, "CHECKPOINT");
        ckptMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now() - ckptStart).count();
        std::printf("  Checkpoint: %lldms\n", (long long)ckptMs);
        sleepFromEnvMs("BENCH_SLEEP_AFTER_CHECKPOINT_MS");

        if (turboliteMetrics) {
            std::printf("  Flushing turbolite staging to object storage...\n");
            auto flushMs = turboliteMetrics->flushToStorage();
            auto c = turboliteMetrics->counters();
            std::printf("  Turbolite flush: %lldms  s3-get=%llu/%lluKB  s3-put=%llu/%lluKB\n",
                (long long)flushMs,
                (unsigned long long)c.gets, (unsigned long long)(c.getBytes / 1024),
                (unsigned long long)c.puts, (unsigned long long)(c.putBytes / 1024));
            sleepFromEnvMs("BENCH_SLEEP_AFTER_FLUSH_MS");
        }

        // Measure total data size: dbPath (metadata) + cacheDir (tiered pages).
        if (std::filesystem::exists(paths.dbPath))
            dbSizeBytes += pathSize(paths.dbPath);
        if (std::filesystem::exists(paths.cacheDir))
            dbSizeBytes += pathSize(paths.cacheDir);
        if (std::filesystem::exists(paths.sqlitePath))
            dbSizeBytes += pathSize(paths.sqlitePath);
        if (std::filesystem::exists(paths.turboliteCacheDir))
            dbSizeBytes += pathSize(paths.turboliteCacheDir);

        // Clean up CSV data.
        std::filesystem::remove_all(paths.dataDir);
    } else {
        std::printf("  Reusing existing DB (skipping data generation + load).\n");
        if (std::filesystem::exists(paths.dbPath))
            dbSizeBytes += pathSize(paths.dbPath);
        if (std::filesystem::exists(paths.cacheDir))
            dbSizeBytes += pathSize(paths.cacheDir);
        if (std::filesystem::exists(paths.sqlitePath))
            dbSizeBytes += pathSize(paths.sqlitePath);
        if (std::filesystem::exists(paths.turboliteCacheDir))
            dbSizeBytes += pathSize(paths.turboliteCacheDir);
    }

    // Closing the graph DB can force one last SQLite/VFS sync after the load
    // block's explicit flush. Drain that before any cold-cache eviction.
    if (storageMode == StorageMode::TurboliteS3) {
        TurboliteSqlMetrics finalMetrics(envString("TURBOGRAPH_TURBOLITE_EXTENSION").value());
        finalMetrics.reset();
        auto flushMs = finalMetrics.flushToStorage();
        auto c = finalMetrics.counters();
        if (flushMs > 0 || c.puts > 0 || c.gets > 0) {
            std::printf("  Turbolite final flush: %lldms  s3-get=%llu/%lluKB  s3-put=%llu/%lluKB\n",
                (long long)flushMs,
                (unsigned long long)c.gets, (unsigned long long)(c.getBytes / 1024),
                (unsigned long long)c.puts, (unsigned long long)(c.putBytes / 1024));
        }
        sleepFromEnvMs("BENCH_SLEEP_AFTER_FINAL_FLUSH_MS");
    }

    auto writeCycles = envInt("BENCH_WRITE_CYCLES", 0);
    if (writeCycles > 0) {
        lbug::main::SystemConfig writeSysCfg;
        writeSysCfg.bufferPoolSize = bufferPoolBytes;
        writeSysCfg.autoCheckpoint = true;
        writeSysCfg.checkpointThreshold = 0;
        writeSysCfg.forceCheckpointOnClose = false;
        auto db = openBenchDatabase(paths, storageMode, tieredCfg, writeSysCfg);
        lbug::main::Connection conn(db.db.get());
        runWriteCheckpointCycles(conn, cfg.numPersons,
            writeCycles, envInt("BENCH_WRITE_CHECKPOINT_EVERY", 1));
    }

    if (storageMode == StorageMode::TurboliteS3 && writeCycles > 0) {
        TurboliteSqlMetrics writeMetrics(envString("TURBOGRAPH_TURBOLITE_EXTENSION").value());
        writeMetrics.reset();
        auto flushMs = writeMetrics.flushToStorage();
        auto c = writeMetrics.counters();
        std::printf("  Turbolite write-cycle flush: %lldms  s3-get=%llu/%lluKB  s3-put=%llu/%lluKB\n",
            (long long)flushMs,
            (unsigned long long)c.gets, (unsigned long long)(c.getBytes / 1024),
            (unsigned long long)c.puts, (unsigned long long)(c.putBytes / 1024));
    }

    // In tiered mode, report actual data size from manifest page count.
    // Local files are just cache — real data size is page_count * page_size.
    std::printf("  Local disk size: %lluMB  Buffer pool: %lluMB\n",
        (unsigned long long)(dbSizeBytes / (1024 * 1024)),
        (unsigned long long)bufferPoolMB);

    // Four-level benchmark.
    //
    // Sequence (one shared TFS + DB for all tiers):
    //   1. COLD:     clearCacheAll() between iterations. Nothing cached.
    //   2. INTERIOR: clearCacheKeepStructural() between iterations. Catalog+metadata cached.
    //   3. INDEX:    clearCacheKeepIndex() between iterations. Structural+index cached.
    //   4. WARM:     no cache clearing. Close/reopen Connection to nuke buffer pool.
    //
    // After cold, transition to interior by doing clearCacheKeepStructural().
    // After interior, transition to index by doing clearCacheKeepIndex().
    // After index, all pages are cached for warm.

    LatencyStats coldStats[NUM_QUERIES];
    LatencyStats interiorStats[NUM_QUERIES];
    LatencyStats indexStats[NUM_QUERIES];
    LatencyStats warmStats[NUM_QUERIES];
    std::unique_ptr<TurboliteSqlMetrics> queryTurboliteMetrics;
    if (storageMode == StorageMode::TurboliteS3) {
        queryTurboliteMetrics = std::make_unique<TurboliteSqlMetrics>(
            envString("TURBOGRAPH_TURBOLITE_EXTENSION").value());
    }

    auto runQuery = [&](lbug::main::Database& db, lbug::tiered::S3Client* s3Ptr,
        lbug::tiered::TieredFileSystem* tfs,
        int q, int iter, const char* label, LatencyStats* stats) {
        try {
            if (s3Ptr) s3Ptr->resetCounters();
            if (queryTurboliteMetrics) queryTurboliteMetrics->reset();
            if (tfs) tfs->setActiveSchedule(QUERIES[q].schedule);
            lbug::main::Connection conn(&db);

            // Frontrun prefetch (skip for warm).
            if (std::strcmp(label, "warm") != 0) {
                frontrunPrefetch(conn, tfs, QUERIES[q].cypher);
            }

            auto start = Clock::now();
            auto result = conn.query(QUERIES[q].cypher);
            auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                Clock::now() - start).count();
            if (!result->isSuccess()) {
                std::fprintf(stderr, "  FAIL [%s] %s: %s\n",
                    label, QUERIES[q].name, result->getErrorMessage().c_str());
                std::exit(1);
            }
            uint64_t s3Reqs = s3Ptr ? s3Ptr->fetchCount.load() : 0;
            uint64_t s3Bytes = s3Ptr ? s3Ptr->fetchBytes.load() : 0;
            if (queryTurboliteMetrics) {
                auto c = queryTurboliteMetrics->counters();
                s3Reqs = c.gets;
                s3Bytes = c.getBytes;
            }
            stats[q].samples.push_back(us);
            std::printf("    [%-8s] iter=%d %s: %lldms  s3=%llu/%lluKB\n",
                label, iter, QUERIES[q].name, (long long)(us / 1000),
                (unsigned long long)s3Reqs, (unsigned long long)(s3Bytes / 1024));
        } catch (const std::exception& e) {
            std::fprintf(stderr, "  CRASH [%s] %s: %s\n", label, QUERIES[q].name, e.what());
        }
    };

    std::printf("  Running benchmark (%d/%d iterations)...\n",
        cfg.coldIterations, cfg.warmIterations);

    // --- COLD (fresh TFS + DB per query, nothing cached) ---
    // Runs BEFORE the shared DB is created so there's no file lock conflict.
    // Each cold query gets its own TFS with a temp cache dir and a fresh Database.
    for (int iter = 0; iter < cfg.coldIterations; iter++) {
        for (int q = 0; q < NUM_QUERIES; q++) {
            try {
                auto coldCacheDir = paths.cacheDir + "_cold_" + std::to_string(iter) + "_" +
                                    std::to_string(q);
                std::filesystem::remove_all(coldCacheDir);

                lbug::tiered::S3Client* coldS3 = nullptr;
                lbug::tiered::TieredFileSystem* coldTfsPtr = nullptr;
                lbug::main::SystemConfig coldSysCfg;
                coldSysCfg.bufferPoolSize = bufferPoolBytes;
                coldSysCfg.forceCheckpointOnClose = false;
                coldSysCfg.autoCheckpoint = false;
                auto coldDb = openBenchDatabase(paths, storageMode, tieredCfg, coldSysCfg,
                    coldCacheDir);
                coldTfsPtr = coldDb.tfs;
                coldS3 = coldDb.s3;
                if (coldS3) coldS3->resetCounters();
                if (queryTurboliteMetrics) {
                    queryTurboliteMetrics->clearCache("all");
                    queryTurboliteMetrics->reset();
                }
                lbug::main::Connection conn(coldDb.db.get());

                // Frontrun prefetch before query execution.
                auto submitted = frontrunPrefetch(conn, coldTfsPtr, QUERIES[q].cypher);
                if (submitted > 0) {
                    std::printf("    [frontrun] %s: %llu groups\n",
                        QUERIES[q].name, (unsigned long long)submitted);
                }

                auto start = Clock::now();
                auto result = conn.query(QUERIES[q].cypher);
                auto us = std::chrono::duration_cast<std::chrono::microseconds>(
                    Clock::now() - start).count();
                if (!result->isSuccess()) {
                    std::fprintf(stderr, "  FAIL [cold] %s: %s\n",
                        QUERIES[q].name, result->getErrorMessage().c_str());
                    std::exit(1);
                }
                uint64_t s3Reqs = coldS3 ? coldS3->fetchCount.load() : 0;
                uint64_t s3Bytes = coldS3 ? coldS3->fetchBytes.load() : 0;
                if (queryTurboliteMetrics) {
                    auto c = queryTurboliteMetrics->counters();
                    s3Reqs = c.gets;
                    s3Bytes = c.getBytes;
                }
                coldStats[q].samples.push_back(us);
                std::printf("    [%-8s] iter=%d %s: %lldms  s3=%llu/%lluKB\n",
                    "cold", iter, QUERIES[q].name, (long long)(us / 1000),
                    (unsigned long long)s3Reqs, (unsigned long long)(s3Bytes / 1024));

                std::filesystem::remove_all(coldCacheDir);
            } catch (const std::exception& e) {
                std::fprintf(stderr, "  CRASH [cold] %s: %s\n", QUERIES[q].name, e.what());
            }
        }
    }

    // --- Shared TFS + DB for interior/index/warm ---
    {
        lbug::main::SystemConfig sysCfg;
        sysCfg.bufferPoolSize = bufferPoolBytes;
        sysCfg.forceCheckpointOnClose = false;
        sysCfg.autoCheckpoint = false;

        auto db = openBenchDatabase(paths, storageMode, tieredCfg, sysCfg);
        auto tfsPtr = db.tfs;
        auto s3Ptr = db.s3;

        std::printf("  Per-table prefetch: %s\n",
            (tfsPtr && tfsPtr->hasTablePageMap()) ? "active" : "inactive");

        // Track index pages during a warmup query.
        if (tfsPtr) tfsPtr->beginTrackIndex();
        std::printf("  Warmup (tracking index pages)...\n");
        {
            lbug::main::Connection conn(db.db.get());
            for (int q = 0; q < NUM_QUERIES; q++) {
                auto r = conn.query(QUERIES[q].cypher);
                if (!r->isSuccess()) {
                    std::fprintf(stderr, "  FAIL [warmup] %s: %s\n",
                        QUERIES[q].name, r->getErrorMessage().c_str());
                    std::exit(1);
                }
            }
        }
        if (tfsPtr) tfsPtr->endTrack();

        // --- INTERIOR (structural cached, index+data evicted) ---
        for (int iter = 0; iter < cfg.coldIterations; iter++) {
            if (tfsPtr) tfsPtr->clearCacheKeepStructural();
            for (int q = 0; q < NUM_QUERIES; q++) {
                runQuery(*db.db, s3Ptr, tfsPtr, q, iter, "interior", interiorStats);
            }
        }

        // Transition: keep structural+index for index tier.
        if (tfsPtr) tfsPtr->clearCacheKeepIndex();

        // --- INDEX (structural+index cached, data evicted) ---
        for (int iter = 0; iter < cfg.coldIterations; iter++) {
            if (tfsPtr) tfsPtr->clearCacheKeepIndex();
            for (int q = 0; q < NUM_QUERIES; q++) {
                runQuery(*db.db, s3Ptr, tfsPtr, q, iter, "index", indexStats);
            }
        }

        // --- WARM (everything cached, nuke buffer pool via Connection close) ---
        for (int iter = 0; iter < cfg.warmIterations; iter++) {
            for (int q = 0; q < NUM_QUERIES; q++) {
                runQuery(*db.db, s3Ptr, tfsPtr, q, iter, "warm", warmStats);
            }
        }
    }

    // Results table.
    std::printf("\n=== Cypher Benchmark — %s (kuzudb-study) ===\n",
        modeName(storageMode));
    std::printf("  Persons: %u  Follow edges: %u\n", cfg.numPersons, cfg.followEdges);
    std::printf("  Local disk size: %lluMB  Buffer pool: %lluMB\n",
        (unsigned long long)(dbSizeBytes / (1024 * 1024)),
        (unsigned long long)bufferPoolMB);
    if (loadMs > 0) {
        std::printf("  Data load: %lldms  Checkpoint: %lldms\n",
            (long long)loadMs, (long long)ckptMs);
    }
    std::printf("\n");

    std::printf("  %-28s %10s %10s %10s %10s %10s\n",
        "", "Cold avg", "Interior", "Index", "Warm avg", "Neo4j");
    std::printf("  %-28s %10s %10s %10s %10s %10s\n",
        "", "--------", "--------", "-----", "--------", "-----");

    for (int q = 0; q < NUM_QUERIES; q++) {
        std::printf("  %-28s %10s %10s %10s %10s %10s\n",
            QUERIES[q].name,
            formatUs(coldStats[q].avg()).c_str(),
            formatUs(interiorStats[q].avg()).c_str(),
            formatUs(indexStats[q].avg()).c_str(),
            formatUs(warmStats[q].avg()).c_str(),
            formatUs(NEO4J_US[q]).c_str());
    }
    std::printf("\n");

    return 0;
}
