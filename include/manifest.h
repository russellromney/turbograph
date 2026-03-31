#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace lbug {
namespace tiered {

// Byte range within an S3 object for one seekable frame.
struct FrameEntry {
    uint64_t offset = 0;  // Byte offset from start of S3 object.
    uint32_t len = 0;     // Compressed frame length in bytes.
};

struct Manifest {
    uint64_t version = 0;
    uint64_t pageCount = 0;
    uint32_t pageSize = 0;
    uint32_t pagesPerGroup = 0;          // Pages per page group (2048 = 8MB).

    // Maps logical page group index to immutable S3 key.
    // e.g. pageGroupKeys[0] = "{prefix}/pg/0_v1"
    std::vector<std::string> pageGroupKeys;

    // Seekable frame tables. frameTables[gid] = vector of FrameEntry.
    // Empty means legacy single-frame format for that group.
    std::vector<std::vector<FrameEntry>> frameTables;

    // Pages per seekable sub-frame (e.g., 4 pages = ~16KB uncompressed).
    // 0 = legacy format (no seekable frames).
    uint32_t subPagesPerFrame = 0;

    bool isSeekable() const { return subPagesPerFrame > 0; }

    std::string toJSON() const;
    static std::optional<Manifest> fromJSON(const std::string& json);
};

} // namespace tiered
} // namespace lbug
