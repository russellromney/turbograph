// Cypher Benchmark — kuzudb-study queries against Tiered Storage (Tigris-backed).
//
// Generates a social network dataset (100K persons, ~1M follow edges, cities, interests),
// loads via COPY FROM CSV, then benchmarks 9 Cypher queries in warm and cold modes.
//
// Build: part of the Ladybug CMake build with -DBUILD_EXTENSIONS="tiered" -DBUILD_EXTENSION_TESTS=TRUE
// Run:   TIGRIS_STORAGE_ACCESS_KEY_ID=... TIGRIS_STORAGE_SECRET_ACCESS_KEY=...
//        TIGRIS_STORAGE_ENDPOINT=... ./cypher_bench

#include "tiered_file_system.h"
#include "table_page_map.h"
#include "main/turbograph_functions.h"

#include "main/connection.h"
#include "main/database.h"

#include <algorithm>
#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <numeric>
#include <random>
#include <string>
#include <vector>

using Clock = std::chrono::steady_clock;

// --- Configuration ---

struct BenchConfig {
    uint32_t numPersons = 100000;
    uint32_t numCities = 200;
    uint32_t numInterests = 42;
    uint32_t followEdges = 0;         // 0 = auto (10x person count).
    int warmIterations = 3;
    int coldIterations = 2;
};

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

// Phase Cypher: prepare query to get the plan, extract table IDs, prefetch their page groups.
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

static void exec(lbug::main::Connection& conn, const char* query) {
    auto r = conn.query(query);
    if (!r->isSuccess()) {
        std::fprintf(stderr, "FAILED: %s\n  %s\n", query, r->getErrorMessage().c_str());
        std::exit(1);
    }
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
    bool localMode = (argc > 2 && std::strcmp(argv[2], "local") == 0);
    if (cfg.followEdges == 0) cfg.followEdges = cfg.numPersons * 10;

    auto ak = std::getenv("TIGRIS_STORAGE_ACCESS_KEY_ID");
    auto sk = std::getenv("TIGRIS_STORAGE_SECRET_ACCESS_KEY");
    auto ep = std::getenv("TIGRIS_STORAGE_ENDPOINT");
    if (!localMode && (!ak || !sk || !ep)) {
        std::fprintf(stderr, "Set TIGRIS_STORAGE_* env vars (or pass 'local' as 3rd arg)\n");
        return 1;
    }

    // Deterministic prefix so data persists across runs.
    std::string tag = std::to_string(cfg.numPersons / 1000) + "k";

    // Use /data (Fly volume) if it exists, else /tmp.
    std::string base = std::filesystem::exists("/data") ? "/data" : "/tmp";
    std::string dbPath = base + "/cypher_bench_" + tag + ".kz";
    std::string dataDir = base + "/cypher_bench_data_" + tag;
    std::string cacheDir = base + "/cypher_bench_cache_" + tag;

    bool dbExists = std::filesystem::exists(dbPath);
    std::printf("  DB path: %s (%s)\n", dbPath.c_str(), dbExists ? "exists" : "new");

    lbug::tiered::TieredConfig tieredCfg;
    if (!localMode) {
        tieredCfg.s3 = {ep, "cinch-data", "bench/cypher/v2/" + tag, "auto", ak, sk};
        tieredCfg.dataFilePath = dbPath;
        tieredCfg.cacheDir = cacheDir;
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

    // Phase 1: Load data (skip if DB already exists locally or in Tigris).
    if (!dbExists) {
        // Clean up ALL stale local artifacts from previous crashed runs
        // (DB dir, cache dir, CSV data dir, shadow files, WAL files, etc.).
        std::filesystem::remove_all(dbPath);
        std::filesystem::remove_all(cacheDir);
        std::filesystem::remove_all(dataDir);
        for (auto& entry : std::filesystem::directory_iterator(base)) {
            auto name = entry.path().filename().string();
            if (name.find("cypher_bench") != std::string::npos && name.find(tag) != std::string::npos) {
                std::filesystem::remove_all(entry.path());
            }
        }

        // In tiered mode, unconditionally clean up ALL Tigris objects at this prefix.
        // The Kuzu catalog (node/rel table defs) lives on local disk and doesn't
        // survive container restarts, so we must load fresh every time.
        if (!localMode) {
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
        generateCSVData(dataDir, cfg);
        auto genMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now() - genStart).count();
        std::printf("  Data generation: %lldms\n", (long long)genMs);

        std::printf("  Creating database (%s mode)...\n", localMode ? "LOCAL" : "TIERED");

        std::vector<std::unique_ptr<lbug::common::FileSystem>> fsList;
        if (!localMode) {
            fsList.push_back(std::make_unique<lbug::tiered::TieredFileSystem>(tieredCfg));
        }

        lbug::main::SystemConfig sysCfg;
        sysCfg.bufferPoolSize = bufferPoolBytes;
        lbug::main::Database db(dbPath, sysCfg, std::move(fsList));
        lbug::main::Connection conn(&db);

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
            auto q = std::string("COPY ") + table + " FROM '" + dataDir + "/" + file + "'";
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

        std::printf("  Checkpointing%s...\n", localMode ? "" : " (flushing to Tigris)");
        auto ckptStart = Clock::now();
        exec(conn, "CHECKPOINT");
        ckptMs = std::chrono::duration_cast<std::chrono::milliseconds>(
            Clock::now() - ckptStart).count();
        std::printf("  Checkpoint: %lldms\n", (long long)ckptMs);

        // Measure total data size: dbPath (metadata) + cacheDir (tiered pages).
        if (std::filesystem::exists(dbPath))
            dbSizeBytes += pathSize(dbPath);
        if (std::filesystem::exists(cacheDir))
            dbSizeBytes += pathSize(cacheDir);

        // Clean up CSV data.
        std::filesystem::remove_all(dataDir);
    } else {
        std::printf("  Reusing existing DB (skipping data generation + load).\n");
        if (std::filesystem::exists(dbPath))
            dbSizeBytes += pathSize(dbPath);
        if (std::filesystem::exists(cacheDir))
            dbSizeBytes += pathSize(cacheDir);
    }

    // In tiered mode, report actual data size from manifest page count.
    // Local files are just cache — real data size is page_count * page_size.
    std::printf("  Local disk size: %lluMB  Buffer pool: %lluMB\n",
        (unsigned long long)(dbSizeBytes / (1024 * 1024)),
        (unsigned long long)bufferPoolMB);

    // Phase 2: Four-level benchmark.
    //
    // Sequence (one shared TFS + DB for all phases):
    //   1. COLD:     clearCacheAll() between iterations. Nothing cached.
    //   2. INTERIOR: clearCacheKeepStructural() between iterations. Catalog+metadata cached.
    //   3. INDEX:    clearCacheKeepIndex() between iterations. Structural+index cached.
    //   4. WARM:     no cache clearing. Close/reopen Connection to nuke buffer pool.
    //
    // After cold phase, transition to interior by doing clearCacheKeepStructural().
    // After interior phase, transition to index by doing clearCacheKeepIndex().
    // After index phase, all pages are cached for warm.

    LatencyStats coldStats[NUM_QUERIES];
    LatencyStats interiorStats[NUM_QUERIES];
    LatencyStats indexStats[NUM_QUERIES];
    LatencyStats warmStats[NUM_QUERIES];

    auto runQuery = [&](lbug::main::Database& db, lbug::tiered::S3Client* s3Ptr,
        lbug::tiered::TieredFileSystem* tfs,
        int q, int iter, const char* label, LatencyStats* stats) {
        try {
            if (s3Ptr) s3Ptr->resetCounters();
            if (tfs) tfs->setActiveSchedule(QUERIES[q].schedule);
            lbug::main::Connection conn(&db);

            // Phase Cypher: frontrun prefetch (skip for warm).
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
            }
            uint64_t s3Reqs = s3Ptr ? s3Ptr->fetchCount.load() : 0;
            uint64_t s3Bytes = s3Ptr ? s3Ptr->fetchBytes.load() : 0;
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

    // --- Phase 1: COLD (fresh TFS + DB per query, nothing cached) ---
    // Runs BEFORE the shared DB is created so there's no file lock conflict.
    // Each cold query gets its own TFS with a temp cache dir and a fresh Database.
    for (int iter = 0; iter < cfg.coldIterations; iter++) {
        for (int q = 0; q < NUM_QUERIES; q++) {
            try {
                auto coldCacheDir = cacheDir + "_cold_" + std::to_string(iter) + "_" + std::to_string(q);
                std::filesystem::remove_all(coldCacheDir);

                lbug::tiered::S3Client* coldS3 = nullptr;
                lbug::tiered::TieredFileSystem* coldTfsPtr = nullptr;
                std::vector<std::unique_ptr<lbug::common::FileSystem>> coldFsList;
                if (!localMode) {
                    auto coldCfg = tieredCfg;
                    coldCfg.cacheDir = coldCacheDir;
                    auto coldTfs = std::make_unique<lbug::tiered::TieredFileSystem>(coldCfg);
                    coldTfs->setMetadataParser(lbug::turbograph_extension::parseMetadataPages);
                    coldTfsPtr = coldTfs.get();
                    coldS3 = &coldTfs->s3();
                    coldS3->resetCounters();
                    coldFsList.push_back(std::move(coldTfs));
                }
                lbug::main::SystemConfig coldSysCfg;
                coldSysCfg.bufferPoolSize = bufferPoolBytes;
                coldSysCfg.forceCheckpointOnClose = false;
                coldSysCfg.autoCheckpoint = false;
                lbug::main::Database coldDb(dbPath, coldSysCfg, std::move(coldFsList));
                lbug::main::Connection conn(&coldDb);

                // Phase Cypher: frontrun prefetch before query execution.
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
                }
                uint64_t s3Reqs = coldS3 ? coldS3->fetchCount.load() : 0;
                uint64_t s3Bytes = coldS3 ? coldS3->fetchBytes.load() : 0;
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

    // --- Phases 2-4: shared TFS + DB for interior/index/warm ---
    {
        std::vector<std::unique_ptr<lbug::common::FileSystem>> fsList;
        lbug::tiered::TieredFileSystem* tfsPtr = nullptr;
        lbug::tiered::S3Client* s3Ptr = nullptr;
        if (!localMode) {
            auto tfs = std::make_unique<lbug::tiered::TieredFileSystem>(tieredCfg);
            tfs->setMetadataParser(lbug::turbograph_extension::parseMetadataPages);
            tfsPtr = tfs.get();
            s3Ptr = &tfs->s3();
            fsList.push_back(std::move(tfs));
        }
        lbug::main::SystemConfig sysCfg;
        sysCfg.bufferPoolSize = bufferPoolBytes;
        sysCfg.forceCheckpointOnClose = false;
        sysCfg.autoCheckpoint = false;

        // Track structural pages during Database construction.
        if (tfsPtr) tfsPtr->beginTrackStructural();
        lbug::main::Database db(dbPath, sysCfg, std::move(fsList));
        if (tfsPtr) tfsPtr->endTrack();

        std::printf("  Per-table prefetch: %s\n",
            (tfsPtr && tfsPtr->hasTablePageMap()) ? "active" : "inactive");

        // Track index pages during a warmup query.
        if (tfsPtr) tfsPtr->beginTrackIndex();
        std::printf("  Warmup (tracking index pages)...\n");
        {
            lbug::main::Connection conn(&db);
            for (int q = 0; q < NUM_QUERIES; q++) {
                auto r = conn.query(QUERIES[q].cypher);
                if (!r->isSuccess()) {
                    std::fprintf(stderr, "  FAIL [warmup] %s: %s\n",
                        QUERIES[q].name, r->getErrorMessage().c_str());
                }
            }
        }
        if (tfsPtr) tfsPtr->endTrack();

        // --- Phase 2: INTERIOR (structural cached, index+data evicted) ---
        for (int iter = 0; iter < cfg.coldIterations; iter++) {
            if (tfsPtr) tfsPtr->clearCacheKeepStructural();
            for (int q = 0; q < NUM_QUERIES; q++) {
                runQuery(db, s3Ptr, tfsPtr, q, iter, "interior", interiorStats);
            }
        }

        // Transition: keep structural+index for index phase.
        if (tfsPtr) tfsPtr->clearCacheKeepIndex();

        // --- Phase 3: INDEX (structural+index cached, data evicted) ---
        for (int iter = 0; iter < cfg.coldIterations; iter++) {
            if (tfsPtr) tfsPtr->clearCacheKeepIndex();
            for (int q = 0; q < NUM_QUERIES; q++) {
                runQuery(db, s3Ptr, tfsPtr, q, iter, "index", indexStats);
            }
        }

        // --- Phase 4: WARM (everything cached, nuke buffer pool via Connection close) ---
        for (int iter = 0; iter < cfg.warmIterations; iter++) {
            for (int q = 0; q < NUM_QUERIES; q++) {
                runQuery(db, s3Ptr, tfsPtr, q, iter, "warm", warmStats);
            }
        }
    }

    // Results table.
    std::printf("\n=== Cypher Benchmark — %s (kuzudb-study) ===\n",
        localMode ? "LOCAL" : "TIERED");
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
