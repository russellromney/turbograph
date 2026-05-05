#pragma once

#include "extension/extension.h"
#include "function/function.h"

#include <memory>

namespace lbug {
namespace main {
class ClientContext;
class Database;
}

namespace tiered {
class TieredFileSystem;
}

namespace turbograph_extension {

class TurbographExtension final : public extension::Extension {
public:
    static constexpr char EXTENSION_NAME[] = "TURBOGRAPH";

    static void load(main::ClientContext* context);
    static tiered::TieredFileSystem* tfsFromBindData(void* dataPtr);
    static main::Database* dbFromBindData(void* dataPtr);
    static std::unique_ptr<function::FunctionBindData> bindFunction(
        const function::ScalarBindFuncInput& input);
    static void registerTfs(main::Database* db, tiered::TieredFileSystem* tfs);

    // Fallback pointer maintained by registerTfs() for legacy single-DB callers.
    // UDFs should prefer tfsFromBindData() so multiple embedded databases in
    // one process do not stomp each other's active TieredFileSystem.
    static tiered::TieredFileSystem* tfs;

    // Fallback database pointer for legacy single-DB callers.
    static main::Database* db;
};

} // namespace turbograph_extension
} // namespace lbug
