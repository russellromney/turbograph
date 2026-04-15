#include "main/turbograph_extension.h"

#include "common/types/types.h"
#include "common/types/value/value.h"
#include "crypto.h"
#include "main/client_context.h"
#include "main/database.h"
#include "main/turbograph_functions.h"
#include "tiered_file_system.h"

namespace lbug {
namespace turbograph_extension {

tiered::TieredFileSystem* TurbographExtension::tfs = nullptr;
main::Database* TurbographExtension::db = nullptr;

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

    // Fall back to env vars if options are empty.
    if (cfg.s3.accessKey.empty()) {
        auto ak = std::getenv("TIGRIS_STORAGE_ACCESS_KEY_ID");
        auto sk = std::getenv("TIGRIS_STORAGE_SECRET_ACCESS_KEY");
        auto ep = std::getenv("TIGRIS_STORAGE_ENDPOINT");
        if (ak) cfg.s3.accessKey = ak;
        if (sk) cfg.s3.secretKey = sk;
        if (ep) cfg.s3.endpoint = ep;
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

    // Only register the TieredFileSystem if we have S3 credentials + data file path.
    if (!cfg.s3.accessKey.empty() && !cfg.dataFilePath.empty()) {
        auto tfsPtr = std::make_unique<tiered::TieredFileSystem>(cfg);
        tfs = tfsPtr.get();
        db->registerFileSystem(std::move(tfsPtr));
    }

    // Store Database pointer for table map builder (UDF manual rebuild path).
    TurbographExtension::db = db;

    // Register the raw metadata parser callback on the TFS.
    // This runs during openFile() after Beacon fetches structural pages,
    // building a page-to-table map for per-table prefetch scheduling.
    if (tfs) {
        tfs->setMetadataParser([](const uint8_t* data, size_t len) {
            return parseMetadataPages(data, len);
        });
    }

    // Register UDFs.
    extension::ExtensionUtils::addScalarFunc<TurbographConfigSetFunction>(*db);
    extension::ExtensionUtils::addScalarFunc<TurbographConfigGetFunction>(*db);

    // Phase GraphZenith: hakuzu integration UDFs.
    extension::ExtensionUtils::addScalarFunc<TurbographSyncFunction>(*db);
    extension::ExtensionUtils::addScalarFunc<TurbographGetManifestVersionFunction>(*db);
    extension::ExtensionUtils::addScalarFunc<TurbographSetManifestFunction>(*db);
    extension::ExtensionUtils::addScalarFunc<TurbographGetManifestFunction>(*db);
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
