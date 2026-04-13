#pragma once

#include "common/types/types.h"
#include "function/function.h"

#include <memory>
#include <string>
#include <unordered_set>
#include <utility>

namespace lbug {

namespace main {
class ClientContext;
class Connection;
class Database;
}

namespace tiered {
class TablePageMap;
}

namespace turbograph_extension {

// turbograph_config_set(key STRING, value STRING) -> STRING
// Sets VFS configuration. Returns the new value.
// Keys: "prefetch" (active schedule), "prefetch_scan", "prefetch_lookup",
//        "prefetch_default", "table_map" (value "build" to populate)
struct TurbographConfigSetFunction {
    static constexpr const char* name = "turbograph_config_set";
    static function::function_set getFunctionSet();
};

// turbograph_config_get(key STRING) -> STRING
// Gets VFS configuration.
struct TurbographConfigGetFunction {
    static constexpr const char* name = "turbograph_config_get";
    static function::function_set getFunctionSet();
};

// Phase GraphZenith: turbograph_sync() -> INT64
// Triggers doSyncFile and returns the new manifest version.
struct TurbographSyncFunction {
    static constexpr const char* name = "turbograph_sync";
    static function::function_set getFunctionSet();
};

// Phase GraphZenith: turbograph_get_manifest_version() -> INT64
// Returns the current manifest version without syncing.
struct TurbographGetManifestVersionFunction {
    static constexpr const char* name = "turbograph_get_manifest_version";
    static function::function_set getFunctionSet();
};

// Phase GraphZenith: turbograph_set_manifest(json STRING) -> INT64
// Follower applies a remote manifest. Returns the new version.
struct TurbographSetManifestFunction {
    static constexpr const char* name = "turbograph_set_manifest";
    static function::function_set getFunctionSet();
};

// Phase GraphBridge: turbograph_get_manifest() -> STRING
// Returns the current manifest as a JSON string. Does not sync.
struct TurbographGetManifestFunction {
    static constexpr const char* name = "turbograph_get_manifest";
    static function::function_set getFunctionSet();
};

// Phase Cypher: extract table IDs from a Cypher query's logical plan.
// Returns (nodeTableIds, relTableIds). Does not execute the query.
std::pair<std::unordered_set<common::table_id_t>, std::unordered_set<common::table_id_t>>
extractTablesFromPlan(main::Connection& conn, const std::string& cypher);

// Parse raw metadata pages into a page-to-table mapping.
// Called by TieredFileSystem during openFile() via the MetadataParserFn callback.
// Uses Kuzu's Deserializer to walk the StorageManager binary format.
std::unique_ptr<tiered::TablePageMap> parseMetadataPages(
    const uint8_t* data, size_t len);

// Build a page-to-table mapping by walking the StorageManager's deserialized tables.
// Used by UDF for manual rebuild. Returns null if StorageManager/Catalog unavailable.
std::unique_ptr<tiered::TablePageMap> buildTablePageMap(main::Database* db);

} // namespace turbograph_extension
} // namespace lbug
