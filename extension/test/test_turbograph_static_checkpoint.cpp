#include "extension/loaded_extension.h"
#include "main/connection.h"
#include "main/database.h"
#include "main/turbograph_extension.h"
#include "main/turbograph_substrate.h"
#include "processor/result/flat_tuple.h"
#include "tiered_file_system.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <string>
#include <unistd.h>
#include <vector>

using namespace lbug;

namespace lbug {
namespace extension {

void loadLinkedExtensions(main::ClientContext* context,
    std::vector<LoadedExtension>& loadedExtensions) {
    turbograph_extension::TurbographExtension::load(context);
    loadedExtensions.emplace_back(
        turbograph_extension::TurbographExtension::EXTENSION_NAME,
        "static", ExtensionSource::STATIC_LINKED);
}

} // namespace extension
} // namespace lbug

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

static bool hasRequiredEnv() {
    return std::getenv("TURBOGRAPH_S3_ACCESS_KEY") &&
        std::getenv("TURBOGRAPH_S3_SECRET_KEY") &&
        std::getenv("TURBOGRAPH_S3_ENDPOINT") &&
        std::getenv("TURBOGRAPH_S3_BUCKET");
}

static tiered::S3Config s3Config(const std::string& prefix) {
    auto region = std::getenv("TURBOGRAPH_S3_REGION");
    return tiered::S3Config{
        std::getenv("TURBOGRAPH_S3_ENDPOINT"),
        std::getenv("TURBOGRAPH_S3_BUCKET"),
        prefix,
        region ? region : "auto",
        std::getenv("TURBOGRAPH_S3_ACCESS_KEY"),
        std::getenv("TURBOGRAPH_S3_SECRET_KEY"),
    };
}

static void cleanupS3(const std::string& prefix) {
    tiered::S3Client s3(s3Config(prefix));
    s3.deleteObject(prefix + "/manifest.json");
    for (auto& key : s3.listObjects(prefix + "/pg/")) {
        s3.deleteObject(key);
    }
}

int main() {
    if (!hasRequiredEnv()) {
        std::printf("SKIP: Turbograph static checkpoint S3 env not set\n");
        return 0;
    }

    auto root = std::filesystem::temp_directory_path() /
        ("turbograph_static_checkpoint_" + std::to_string(getpid()));
    std::filesystem::remove_all(root);
    std::filesystem::create_directories(root);

    auto dataFile = root / "graph.kz";
    auto cache1 = root / "cache1";
    auto cache2 = root / "cache2";
    auto prefix = std::string("test/turbograph-static-checkpoint/") +
        std::to_string(getpid());

    setenv("TURBOGRAPH_DATA_FILE", dataFile.c_str(), 1);
    setenv("TURBOGRAPH_CACHE_DIR", cache1.c_str(), 1);
    setenv("TURBOGRAPH_S3_PREFIX", prefix.c_str(), 1);
    cleanupS3(prefix);

    {
        main::Database db(dataFile.string());
        auto substrate = turbograph_extension::TurbographSubstrate::fromDatabase(&db);
        auto initialVersion = substrate.manifestVersion();
        assert(initialVersion > 0);

        main::Connection conn(&db);
        auto created = conn.query(
            "CREATE NODE TABLE DirectProof(id INT64, name STRING, PRIMARY KEY(id))");
        assert(created->isSuccess());
        auto inserted = conn.query("CREATE (:DirectProof {id: 1, name: 'checkpoint'})");
        assert(inserted->isSuccess());

        auto version = substrate.syncCheckpointedBase();
        assert(version > initialVersion);
    }

    std::filesystem::remove_all(cache1);
    setenv("TURBOGRAPH_CACHE_DIR", cache2.c_str(), 1);

    {
        main::Database db(dataFile.string());
        main::Connection conn(&db);
        auto count = queryScalar(conn, "MATCH (n:DirectProof) RETURN count(n)");
        assert(count == "1");
    }

    cleanupS3(prefix);
    std::filesystem::remove_all(root);
    std::printf("PASS: turbograph static checkpoint cold reopen proof\n");
    return 0;
}
