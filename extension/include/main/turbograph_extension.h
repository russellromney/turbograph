#pragma once

#include "extension/extension.h"

namespace lbug {
namespace tiered {
class TieredFileSystem;
}

namespace turbograph_extension {

class TurbographExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "TURBOGRAPH";

    static void load(main::ClientContext* context);

    // Global pointer to the active TieredFileSystem, set once during load().
    // Used by UDFs to call setActiveSchedule(), s3() etc.
    //
    // Thread safety: set exactly once during load() (before any UDF can execute),
    // read-only thereafter. The TFS itself has internal mutexes for all mutable
    // state. Safe for concurrent UDF execution from multiple connections.
    //
    // Limitation: assumes one TieredFileSystem per process. If multiple Database
    // instances load the extension, the last one's TFS wins. This is acceptable
    // because turbograph intercepts a single data file path.
    static tiered::TieredFileSystem* tfs;

    // Global pointer to the Database, set once during load().
    // Used by table_map builder to access StorageManager and Catalog.
    static main::Database* db;
};

} // namespace turbograph_extension
} // namespace lbug
