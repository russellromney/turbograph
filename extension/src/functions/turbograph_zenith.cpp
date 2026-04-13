// Phase GraphZenith: UDFs for hakuzu integration.
// turbograph_sync(), turbograph_get_manifest_version(), turbograph_set_manifest(json).

#include "main/turbograph_functions.h"

#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"
#include "function/scalar_function.h"
#include "main/turbograph_extension.h"
#include "tiered_file_system.h"

namespace lbug {
namespace turbograph_extension {

using namespace function;
using namespace common;

// --- turbograph_sync() -> INT64 ---

static void syncExec(const std::vector<std::shared_ptr<ValueVector>>& /*parameters*/,
    const std::vector<SelectionVector*>& /*parameterSelVectors*/, ValueVector& result,
    SelectionVector* resultSelVector, void* /*dataPtr*/) {
    auto* tfs = TurbographExtension::tfs;

    for (auto i = 0u; i < resultSelVector->getSelSize(); i++) {
        auto resultPos = (*resultSelVector)[i];

        if (!tfs) {
            result.setNull(resultPos, true);
            continue;
        }

        try {
            auto version = tfs->syncAndGetVersion();
            result.setNull(resultPos, false);
            result.setValue(resultPos, static_cast<int64_t>(version));
        } catch (const std::exception& e) {
            result.setNull(resultPos, true);
        }
    }
}

function_set TurbographSyncFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{}, LogicalTypeID::INT64, syncExec));
    return result;
}

// --- turbograph_get_manifest_version() -> INT64 ---

static void getVersionExec(const std::vector<std::shared_ptr<ValueVector>>& /*parameters*/,
    const std::vector<SelectionVector*>& /*parameterSelVectors*/, ValueVector& result,
    SelectionVector* resultSelVector, void* /*dataPtr*/) {
    auto* tfs = TurbographExtension::tfs;

    for (auto i = 0u; i < resultSelVector->getSelSize(); i++) {
        auto resultPos = (*resultSelVector)[i];

        if (!tfs) {
            result.setNull(resultPos, true);
            continue;
        }

        try {
            auto version = tfs->getManifestVersion();
            result.setNull(resultPos, false);
            result.setValue(resultPos, static_cast<int64_t>(version));
        } catch (const std::exception& e) {
            result.setNull(resultPos, true);
        }
    }
}

function_set TurbographGetManifestVersionFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{}, LogicalTypeID::INT64, getVersionExec));
    return result;
}

// --- turbograph_get_manifest() -> STRING (Phase GraphBridge) ---

static void getManifestExec(const std::vector<std::shared_ptr<ValueVector>>& /*parameters*/,
    const std::vector<SelectionVector*>& /*parameterSelVectors*/, ValueVector& result,
    SelectionVector* resultSelVector, void* /*dataPtr*/) {
    auto* tfs = TurbographExtension::tfs;

    for (auto i = 0u; i < resultSelVector->getSelSize(); i++) {
        auto resultPos = (*resultSelVector)[i];

        if (!tfs) {
            result.setNull(resultPos, true);
            continue;
        }

        try {
            auto json = tfs->getManifestJSON();
            result.setNull(resultPos, false);
            StringVector::addString(&result, resultPos, json);
        } catch (const std::exception& e) {
            result.setNull(resultPos, true);
        }
    }
}

function_set TurbographGetManifestFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{}, LogicalTypeID::STRING, getManifestExec));
    return result;
}

// --- turbograph_set_manifest(json STRING) -> INT64 ---

static void setManifestExec(const std::vector<std::shared_ptr<ValueVector>>& parameters,
    const std::vector<SelectionVector*>& parameterSelVectors, ValueVector& result,
    SelectionVector* resultSelVector, void* /*dataPtr*/) {
    auto* tfs = TurbographExtension::tfs;

    for (auto i = 0u; i < resultSelVector->getSelSize(); i++) {
        auto jsonPos = (*parameterSelVectors[0])[i];
        auto resultPos = (*resultSelVector)[i];

        bool isNull = parameters[0]->isNull(jsonPos);
        if (isNull || !tfs) {
            result.setNull(resultPos, true);
            continue;
        }

        auto jsonStr = parameters[0]->getValue<ku_string_t>(jsonPos).getAsString();
        try {
            auto version = tfs->applyRemoteManifest(jsonStr);
            result.setNull(resultPos, false);
            result.setValue(resultPos, static_cast<int64_t>(version));
        } catch (const std::exception& e) {
            result.setNull(resultPos, true);
        }
    }
}

function_set TurbographSetManifestFunction::getFunctionSet() {
    function_set result;
    result.push_back(std::make_unique<ScalarFunction>(name,
        std::vector<LogicalTypeID>{LogicalTypeID::STRING},
        LogicalTypeID::INT64, setManifestExec));
    return result;
}

} // namespace turbograph_extension
} // namespace lbug
