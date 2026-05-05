#pragma once

#include "tiered_file_system.h"

#include <cstdint>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace lbug {
namespace main {
class Database;
}

namespace turbograph_extension {

class TurbographSubstrate {
public:
    TurbographSubstrate(main::Database* db, tiered::TieredFileSystem* tfs);

    static TurbographSubstrate fromDatabase(main::Database* db);

    uint64_t syncCheckpointedBase();
    uint64_t syncBaseForTestWithoutCheckpoint();
    uint64_t manifestVersion() const;
    std::optional<std::vector<uint8_t>> manifestBytes() const;
    std::vector<uint8_t> manifestBytesWithGraphstreamDelta(
        uint64_t journalSeq, const std::string& segmentPrefix) const;
    std::pair<uint64_t, std::string> preflightManifestBytes(
        const std::vector<uint8_t>& bytes) const;
    std::pair<uint64_t, std::string> setManifestBytes(const std::vector<uint8_t>& bytes);
    tiered::DecodedManifestBytes decodeManifestBytes(const std::vector<uint8_t>& bytes) const;

private:
    tiered::TieredFileSystem& requireTfs() const;

    main::Database* db_;
    tiered::TieredFileSystem* tfs_;
};

} // namespace turbograph_extension
} // namespace lbug
