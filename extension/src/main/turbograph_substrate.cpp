#include "main/turbograph_substrate.h"

#include "main/connection.h"
#include "main/query_result.h"
#include "main/turbograph_extension.h"

#include <stdexcept>

namespace lbug {
namespace turbograph_extension {

TurbographSubstrate::TurbographSubstrate(main::Database* db, tiered::TieredFileSystem* tfs)
    : db_(db), tfs_(tfs) {}

TurbographSubstrate TurbographSubstrate::fromDatabase(main::Database* db) {
    return TurbographSubstrate(db, TurbographExtension::tfsForDatabase(db));
}

tiered::TieredFileSystem& TurbographSubstrate::requireTfs() const {
    if (!tfs_) {
        throw std::runtime_error("TurbographSubstrate: no active TieredFileSystem");
    }
    return *tfs_;
}

uint64_t TurbographSubstrate::syncCheckpointedBase() {
    if (!db_) {
        throw std::runtime_error("TurbographSubstrate: checkpoint requires a database");
    }

    main::Connection conn(db_);
    auto result = conn.query("CHECKPOINT");
    if (!result->isSuccess()) {
        throw std::runtime_error(
            "TurbographSubstrate: CHECKPOINT failed: " + result->getErrorMessage());
    }
    while (result->hasNext()) {
        (void)result->getNext();
    }

    return requireTfs().syncAndGetVersion();
}

uint64_t TurbographSubstrate::syncBaseForTestWithoutCheckpoint() {
    return requireTfs().syncAndGetVersion();
}

uint64_t TurbographSubstrate::manifestVersion() const {
    return requireTfs().getManifestVersion();
}

std::optional<std::vector<uint8_t>> TurbographSubstrate::manifestBytes() const {
    return requireTfs().manifestBytes();
}

std::vector<uint8_t> TurbographSubstrate::manifestBytesWithGraphstreamDelta(
    uint64_t journalSeq, const std::string& segmentPrefix) const {
    return requireTfs().manifestBytesWithGraphstreamDelta(journalSeq, segmentPrefix);
}

std::pair<uint64_t, std::string> TurbographSubstrate::preflightManifestBytes(
    const std::vector<uint8_t>& bytes) const {
    return requireTfs().preflightManifestBytes(bytes);
}

std::pair<uint64_t, std::string> TurbographSubstrate::setManifestBytes(
    const std::vector<uint8_t>& bytes) {
    return requireTfs().setManifestBytes(bytes);
}

tiered::DecodedManifestBytes TurbographSubstrate::decodeManifestBytes(
    const std::vector<uint8_t>& bytes) const {
    return requireTfs().decodeManifestBytes(bytes);
}

} // namespace turbograph_extension
} // namespace lbug
