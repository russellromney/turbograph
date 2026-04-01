#include "main/turbograph_functions.h"

#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"
#include "function/scalar_function.h"
#include "main/database.h"
#include "main/turbograph_extension.h"
#include "table_page_map.h"
#include "tiered_file_system.h"

#include <sstream>

namespace lbug {
namespace turbograph_extension {

using namespace function;
using namespace common;

// --- turbograph_config_set(key, value) -> STRING ---

static void configSetExec(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    const std::vector<SelectionVector*>& parameterSelVectors, ValueVector& result,
    SelectionVector* resultSelVector, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    auto* tfs = TurbographExtension::tfs;

    for (auto i = 0u; i < resultSelVector->getSelSize(); i++) {
        auto keyPos = (*parameterSelVectors[0])[i];
        auto valPos = (*parameterSelVectors[1])[i];
        auto resultPos = (*resultSelVector)[i];

        bool isNull = parameters[0]->isNull(keyPos) || parameters[1]->isNull(valPos);
        result.setNull(resultPos, isNull);
        if (isNull || !tfs) continue;

        auto key = parameters[0]->getValue<ku_string_t>(keyPos).getAsString();
        auto val = parameters[1]->getValue<ku_string_t>(valPos).getAsString();

        if (key == "prefetch") {
            // Set active schedule by name: "scan", "lookup", "default".
            tfs->setActiveSchedule(val);
            StringVector::addString(&result, resultPos, val);
        } else if (key == "prefetch_scan" || key == "prefetch_lookup" || key == "prefetch_default") {
            // Parse comma-separated floats and set the named schedule.
            try {
                std::vector<float> hops;
                std::istringstream ss(val);
                std::string token;
                while (std::getline(ss, token, ',')) {
                    if (!token.empty()) hops.push_back(std::stof(token));
                }
                auto scheduleName = key.substr(9); // Strip "prefetch_" prefix.
                tfs->setSchedule(scheduleName, hops);
                StringVector::addString(&result, resultPos, val);
            } catch (const std::exception& e) {
                StringVector::addString(&result, resultPos,
                    std::string("error parsing schedule: ") + e.what());
            }
        } else if (key == "prefetch_reset") {
            // Reset all schedules to defaults.
            tfs->setSchedule("scan", {0.3f, 0.3f, 0.4f});
            tfs->setSchedule("lookup", {0.0f, 0.0f, 0.0f});
            tfs->setSchedule("default", {0.33f, 0.33f});
            tfs->setActiveSchedule("scan");
            StringVector::addString(&result, resultPos, std::string("reset"));
        } else if (key == "table_map") {
            // Build or clear the page-to-table mapping for per-table prefetch.
            if (val == "build") {
                auto* db = TurbographExtension::db;
                if (!db) {
                    StringVector::addString(&result, resultPos,
                        std::string("error: database not available"));
                    continue;
                }
                auto map = buildTablePageMap(db);
                if (map && map->size() > 0) {
                    auto count = map->size();
                    tfs->setTablePageMap(std::move(map));
                    StringVector::addString(&result, resultPos,
                        std::string("built ") + std::to_string(count) + " intervals");
                } else {
                    StringVector::addString(&result, resultPos,
                        std::string("no tables found"));
                }
            } else if (val == "clear") {
                tfs->setTablePageMap(nullptr);
                StringVector::addString(&result, resultPos, std::string("cleared"));
            } else if (val == "status") {
                StringVector::addString(&result, resultPos,
                    tfs->hasTablePageMap() ? std::string("active") : std::string("inactive"));
            } else {
                StringVector::addString(&result, resultPos,
                    std::string("table_map values: build, clear, status"));
            }
        } else {
            StringVector::addString(&result, resultPos, std::string("unknown key: ") + key);
        }
    }
}

function_set TurbographConfigSetFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING, LogicalTypeID::STRING},
        LogicalTypeID::STRING, configSetExec));
    return result;
}

// --- turbograph_config_get(key) -> STRING ---

static void configGetExec(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    const std::vector<SelectionVector*>& parameterSelVectors, ValueVector& result,
    SelectionVector* resultSelVector, void* /*dataPtr*/) {
    result.resetAuxiliaryBuffer();
    auto* tfs = TurbographExtension::tfs;

    for (auto i = 0u; i < resultSelVector->getSelSize(); i++) {
        auto keyPos = (*parameterSelVectors[0])[i];
        auto resultPos = (*resultSelVector)[i];

        bool isNull = parameters[0]->isNull(keyPos);
        result.setNull(resultPos, isNull);
        if (isNull) continue;

        if (!tfs) {
            StringVector::addString(&result, resultPos, std::string("turbograph not active"));
            continue;
        }

        auto key = parameters[0]->getValue<ku_string_t>(keyPos).getAsString();

        if (key == "prefetch") {
            StringVector::addString(&result, resultPos, tfs->getActiveSchedule());
        } else if (key == "table_map") {
            StringVector::addString(&result, resultPos,
                tfs->hasTablePageMap() ? std::string("active") : std::string("inactive"));
        } else if (key == "s3_fetch_count") {
            StringVector::addString(&result, resultPos,
                std::to_string(tfs->s3().fetchCount.load()));
        } else if (key == "s3_fetch_bytes") {
            StringVector::addString(&result, resultPos,
                std::to_string(tfs->s3().fetchBytes.load()));
        } else {
            StringVector::addString(&result, resultPos, std::string("unknown key: ") + key);
        }
    }
}

function_set TurbographConfigGetFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING},
        LogicalTypeID::STRING, configGetExec));
    return result;
}

} // namespace turbograph_extension
} // namespace lbug
