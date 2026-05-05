#include "main/turbograph_extension.h"

#include "common/types/types.h"
#include "common/types/value/value.h"
#include "crypto.h"
#include "main/client_context.h"
#include "main/database.h"
#include "main/turbograph_functions.h"
#include "tiered_file_system.h"
#include "function/function.h"
#include "function/scalar_function.h"

#include <cstdio>
#include <cstdlib>
#include <mutex>
#include <unordered_map>
#include <utility>

namespace lbug {
namespace turbograph_extension {

tiered::TieredFileSystem* TurbographExtension::tfs = nullptr;
main::Database* TurbographExtension::db = nullptr;

static std::mutex& registryMutex() {
    static std::mutex mutex;
    return mutex;
}

struct RegisteredTfs {
    tiered::TieredFileSystem* tfs = nullptr;
    std::weak_ptr<void> lifetime;
};

static RegisteredTfs makeRegistration(tiered::TieredFileSystem* tfs) {
    RegisteredTfs registration;
    registration.tfs = tfs;
    if (tfs) {
        registration.lifetime = tfs->lifetimeToken();
    }
    return registration;
}

static tiered::TieredFileSystem* liveTfs(RegisteredTfs& registration) {
    if (!registration.tfs) return nullptr;
    if (registration.lifetime.expired()) {
        registration = {};
        return nullptr;
    }
    return registration.tfs;
}

static std::unordered_map<main::Database*, RegisteredTfs>& tfsRegistry() {
    static std::unordered_map<main::Database*, RegisteredTfs> registry;
    return registry;
}

static RegisteredTfs& fallbackTfsRegistration() {
    static RegisteredTfs registration;
    return registration;
}

struct TurbographBindData final : function::FunctionBindData {
    tiered::TieredFileSystem* tfs = nullptr;
    std::weak_ptr<void> lifetime;
    main::Database* db = nullptr;

    TurbographBindData(const function::ScalarBindFuncInput& input,
        tiered::TieredFileSystem* boundTfs)
        : FunctionBindData(common::LogicalType(
              input.definition->ptrCast<function::ScalarOrAggregateFunction>()->returnTypeID)),
          tfs(boundTfs),
          db(input.context ? input.context->getDatabase() : nullptr) {
        if (tfs) lifetime = tfs->lifetimeToken();
    }

    TurbographBindData(std::vector<common::LogicalType> paramTypes,
        common::LogicalType resultType, tiered::TieredFileSystem* boundTfs,
        main::Database* boundDb)
        : FunctionBindData(std::move(paramTypes), std::move(resultType)),
          tfs(boundTfs),
          db(boundDb) {
        if (tfs) lifetime = tfs->lifetimeToken();
    }

    std::unique_ptr<function::FunctionBindData> copy() const override {
        auto copied = std::make_unique<TurbographBindData>(
            common::LogicalType::copy(paramTypes), resultType.copy(), tfs, db);
        copied->lifetime = lifetime;
        return copied;
    }
};

static tiered::TieredFileSystem* liveBoundTfs(TurbographBindData& bound) {
    if (!bound.tfs) return nullptr;
    if (bound.lifetime.expired()) {
        bound.tfs = nullptr;
        return nullptr;
    }
    return bound.tfs;
}

static tiered::TieredFileSystem* registeredTfs(main::Database* database) {
    if (!database) return nullptr;
    std::lock_guard<std::mutex> guard(registryMutex());
    auto it = tfsRegistry().find(database);
    if (it == tfsRegistry().end()) return nullptr;
    auto* tfs = liveTfs(it->second);
    if (!tfs) {
        tfsRegistry().erase(it);
    }
    return tfs;
}

static main::ClientContext* contextFromBindData(void* dataPtr) {
    auto* bindData = reinterpret_cast<function::FunctionBindData*>(dataPtr);
    return bindData ? bindData->clientContext : nullptr;
}

tiered::TieredFileSystem* TurbographExtension::tfsFromBindData(void* dataPtr) {
    if (auto* bound = dynamic_cast<TurbographBindData*>(
            reinterpret_cast<function::FunctionBindData*>(dataPtr))) {
        if (bound->tfs) return liveBoundTfs(*bound);
    }

    auto* context = contextFromBindData(dataPtr);
    auto* database = context ? context->getDatabase() : nullptr;
    if (!database) {
        std::lock_guard<std::mutex> guard(registryMutex());
        auto* liveFallback = liveTfs(fallbackTfsRegistration());
        tfs = liveFallback;
        return liveFallback ? liveFallback : tfs;
    }

    std::lock_guard<std::mutex> guard(registryMutex());
    auto it = tfsRegistry().find(database);
    if (it != tfsRegistry().end()) {
        auto* registered = liveTfs(it->second);
        if (registered) return registered;
        tfsRegistry().erase(it);
    }
    return nullptr;
}

main::Database* TurbographExtension::dbFromBindData(void* dataPtr) {
    if (auto* bound = dynamic_cast<TurbographBindData*>(
            reinterpret_cast<function::FunctionBindData*>(dataPtr))) {
        if (bound->db) return bound->db;
    }

    auto* context = contextFromBindData(dataPtr);
    if (context) return context->getDatabase();
    return db;
}

std::unique_ptr<function::FunctionBindData> TurbographExtension::bindFunction(
    const function::ScalarBindFuncInput& input) {
    auto* database = input.context ? input.context->getDatabase() : nullptr;
    return std::make_unique<TurbographBindData>(input, registeredTfs(database));
}

void TurbographExtension::registerTfs(main::Database* database, tiered::TieredFileSystem* activeTfs) {
    std::lock_guard<std::mutex> guard(registryMutex());
    if (database) {
        if (activeTfs) {
            tfsRegistry()[database] = makeRegistration(activeTfs);
        } else {
            tfsRegistry().erase(database);
        }
    }
    fallbackTfsRegistration() = makeRegistration(activeTfs);
    tfs = activeTfs;
}

tiered::TieredFileSystem* TurbographExtension::tfsForDatabase(main::Database* database) {
    return registeredTfs(database);
}

static tiered::TieredConfig configFromExtensionOptions(main::ClientContext* context) {
    tiered::TieredConfig cfg;

    auto getOpt = [&](const char* name) -> std::string {
        try {
            return context->getCurrentSetting(name).getValue<std::string>();
        } catch (...) {
            return "";
        }
    };

    cfg.s3.accessKey = getOpt("turbograph_s3_access_key");
    cfg.s3.secretKey = getOpt("turbograph_s3_secret_key");
    cfg.s3.endpoint = getOpt("turbograph_s3_endpoint");
    cfg.s3.bucket = getOpt("turbograph_s3_bucket");
    cfg.s3.prefix = getOpt("turbograph_s3_prefix");
    cfg.s3.region = getOpt("turbograph_s3_region");
    if (cfg.s3.region.empty()) cfg.s3.region = "auto";

    cfg.dataFilePath = getOpt("turbograph_data_file");
    cfg.cacheDir = getOpt("turbograph_cache_dir");

    return cfg;
}

static std::string envFirst(std::initializer_list<const char*> names) {
    for (auto* name : names) {
        if (auto* value = std::getenv(name); value && value[0] != '\0') {
            return value;
        }
    }
    return "";
}

static bool isAwsS3Endpoint(const std::string& endpoint) {
    return endpoint.find("amazonaws.com") != std::string::npos;
}

static void applyEnvFallbacks(tiered::TieredConfig& cfg) {
    if (cfg.s3.accessKey.empty()) {
        cfg.s3.accessKey = envFirst({
            "TURBOGRAPH_S3_ACCESS_KEY",
            "TIGRIS_STORAGE_ACCESS_KEY_ID",
            "AWS_ACCESS_KEY_ID",
        });
    }
    if (cfg.s3.secretKey.empty()) {
        cfg.s3.secretKey = envFirst({
            "TURBOGRAPH_S3_SECRET_KEY",
            "TIGRIS_STORAGE_SECRET_ACCESS_KEY",
            "AWS_SECRET_ACCESS_KEY",
        });
    }
    if (cfg.s3.endpoint.empty()) {
        cfg.s3.endpoint = envFirst({
            "TURBOGRAPH_S3_ENDPOINT",
            "TIGRIS_STORAGE_ENDPOINT",
            "AWS_ENDPOINT_URL_S3",
            "AWS_ENDPOINT_URL",
        });
    }
    if (cfg.s3.bucket.empty()) {
        cfg.s3.bucket = envFirst({
            "TURBOGRAPH_S3_BUCKET",
            "S3_TEST_BUCKET",
            "AWS_BUCKET",
        });
    }
    if (cfg.s3.prefix.empty()) {
        cfg.s3.prefix = envFirst({
            "TURBOGRAPH_S3_PREFIX",
            "AWS_PREFIX",
        });
    }
    auto turbographRegion = envFirst({"TURBOGRAPH_S3_REGION"});
    if (!turbographRegion.empty()) {
        cfg.s3.region = turbographRegion;
    } else if (cfg.s3.region.empty()) {
        auto awsRegion = envFirst({"AWS_REGION", "AWS_DEFAULT_REGION"});
        cfg.s3.region = !awsRegion.empty() && isAwsS3Endpoint(cfg.s3.endpoint)
            ? awsRegion : "auto";
    } else if (cfg.s3.region == "auto" && isAwsS3Endpoint(cfg.s3.endpoint)) {
        auto awsRegion = envFirst({"AWS_REGION", "AWS_DEFAULT_REGION"});
        if (!awsRegion.empty()) cfg.s3.region = awsRegion;
    }
    if (cfg.dataFilePath.empty()) {
        cfg.dataFilePath = envFirst({"TURBOGRAPH_DATA_FILE"});
    }
    if (cfg.cacheDir.empty()) {
        cfg.cacheDir = envFirst({"TURBOGRAPH_CACHE_DIR"});
    }
}

static void registerExtensionOptions(main::Database* db) {
    // S3 credentials (confidential).
    db->addExtensionOption("turbograph_s3_access_key", common::LogicalTypeID::STRING,
        common::Value{std::string("")}, true);
    db->addExtensionOption("turbograph_s3_secret_key", common::LogicalTypeID::STRING,
        common::Value{std::string("")}, true);
    db->addExtensionOption("turbograph_s3_endpoint", common::LogicalTypeID::STRING,
        common::Value{std::string("")});
    db->addExtensionOption("turbograph_s3_bucket", common::LogicalTypeID::STRING,
        common::Value{std::string("")});
    db->addExtensionOption("turbograph_s3_prefix", common::LogicalTypeID::STRING,
        common::Value{std::string("")});
    db->addExtensionOption("turbograph_s3_region", common::LogicalTypeID::STRING,
        common::Value{std::string("auto")});

    // VFS config.
    db->addExtensionOption("turbograph_data_file", common::LogicalTypeID::STRING,
        common::Value{std::string("")});
    db->addExtensionOption("turbograph_cache_dir", common::LogicalTypeID::STRING,
        common::Value{std::string("")});

    // Encryption key (hex-encoded 64-char string, confidential).
    db->addExtensionOption("turbograph_encryption_key", common::LogicalTypeID::STRING,
        common::Value{std::string("")}, true);
}

void TurbographExtension::load(main::ClientContext* context) {
    auto db = context->getDatabase();

    // Register extension options first (so SET commands work).
    registerExtensionOptions(db);

    // Build config from options or env vars.
    auto cfg = configFromExtensionOptions(context);

    // Static extensions load during Database construction, before callers can
    // issue SET commands. Env fallbacks let embedded clients activate the VFS
    // before lbug opens the database file we need to intercept.
    applyEnvFallbacks(cfg);

    if (std::getenv("TURBOGRAPH_DEBUG_CONFIG")) {
        std::fprintf(stderr,
            "turbograph config: access=%s secret=%s endpoint=%s bucket=%s prefix=%s data_file=%s cache_dir=%s\n",
            cfg.s3.accessKey.empty() ? "missing" : "set",
            cfg.s3.secretKey.empty() ? "missing" : "set",
            cfg.s3.endpoint.empty() ? "missing" : cfg.s3.endpoint.c_str(),
            cfg.s3.bucket.empty() ? "missing" : cfg.s3.bucket.c_str(),
            cfg.s3.prefix.empty() ? "missing" : cfg.s3.prefix.c_str(),
            cfg.dataFilePath.empty() ? "missing" : cfg.dataFilePath.c_str(),
            cfg.cacheDir.empty() ? "missing" : cfg.cacheDir.c_str());
    }

    // Parse encryption key from env var or extension option.
    std::string encKeyStr;
    try {
        encKeyStr = context->getCurrentSetting("turbograph_encryption_key")
            .getValue<std::string>();
    } catch (...) {}
    if (encKeyStr.empty()) {
        auto envKey = std::getenv("TURBOGRAPH_ENCRYPTION_KEY");
        if (envKey) encKeyStr = envKey;
    }
    if (!encKeyStr.empty()) {
        auto key = tiered::parse_hex_key(encKeyStr);
        if (key) {
            cfg.encryptionKey = *key;
        }
    }

    // Only register the TieredFileSystem once per Database. Static linked
    // extensions may be refreshed after checkpoint catalog load to restore UDF
    // registrations; the UDFs must continue to point at the filesystem that
    // actually intercepted the database file.
    tiered::TieredFileSystem* activeTfs = registeredTfs(db);
    if (!activeTfs && !cfg.s3.accessKey.empty() && !cfg.dataFilePath.empty()) {
        auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
        activeTfs = tfsPtr.get();
        db->registerFileSystem(std::move(tfsPtr));
    }
    registerTfs(db, activeTfs);

    // Store Database pointer for table map builder (UDF manual rebuild path).
    TurbographExtension::db = db;

    // Register the raw metadata parser callback on the TFS.
    // This runs during openFile() after structural pages are fetched,
    // building a page-to-table map for per-table prefetch scheduling.
    if (activeTfs) {
        activeTfs->setMetadataParser([](const uint8_t* data, size_t len) {
            return parseMetadataPages(data, len);
        });
    }

    // Register UDFs.
    extension::ExtensionUtils::addScalarFunc<TurbographConfigSetFunction>(*db);
    extension::ExtensionUtils::addScalarFunc<TurbographConfigGetFunction>(*db);

    // Hakuzu integration UDFs.
    extension::ExtensionUtils::addScalarFunc<TurbographSyncFunction>(*db);
    extension::ExtensionUtils::addScalarFunc<TurbographGetManifestVersionFunction>(*db);
    extension::ExtensionUtils::addScalarFunc<TurbographSetManifestFunction>(*db);
    extension::ExtensionUtils::addScalarFunc<TurbographGetManifestFunction>(*db);

    // Opaque-payload wire UDFs.
    extension::ExtensionUtils::addScalarFunc<TurbographManifestBytesFunction>(*db);
    extension::ExtensionUtils::addScalarFunc<TurbographManifestBytesWithGraphstreamDeltaFunction>(*db);
    extension::ExtensionUtils::addScalarFunc<TurbographSetManifestBytesFunction>(*db);
}

} // namespace turbograph_extension
} // namespace lbug

// ---------------------------------------------------------------------------
// Dynamic extension entry points.
//
// lbug's ExtensionLibLoader uses dlsym() to find these C symbols after
// dlopen(). Without extern "C", the symbols are C++-mangled and invisible.
// Every lbug extension (httpfs, fts, etc.) exports these.
// ---------------------------------------------------------------------------

extern "C" {

#if defined(_WIN32)
#define TURBOGRAPH_EXPORT __declspec(dllexport)
#else
#define TURBOGRAPH_EXPORT __attribute__((visibility("default")))
#endif

TURBOGRAPH_EXPORT void init(lbug::main::ClientContext* context) {
    lbug::turbograph_extension::TurbographExtension::load(context);
}

TURBOGRAPH_EXPORT const char* name() {
    return lbug::turbograph_extension::TurbographExtension::EXTENSION_NAME;
}

} // extern "C"
