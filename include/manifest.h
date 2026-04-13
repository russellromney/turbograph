#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <unordered_map>
#include <vector>

namespace lbug {
namespace tiered {

// Byte range within an S3 object for one seekable frame.
struct FrameEntry {
    uint64_t offset = 0;      // Byte offset from start of S3 object.
    uint32_t len = 0;         // Compressed frame length in bytes.
    uint32_t pageCount = 0;   // Actual number of pages encoded in this frame.
};

// Phase GraphDrift: a subframe override entry.
// When only a few frames in a group are dirty, we upload those frames as
// independent S3 objects instead of rewriting the full group.
struct SubframeOverride {
    std::string key;      // S3 key: "{prefix}/pg/{gid}_f{frameIdx}_v{version}"
    FrameEntry entry;     // offset=0, len=full object size, pageCount from frame
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

    // Phase GraphDrift: per-group subframe overrides.
    // subframeOverrides[gid][frameIndex] = SubframeOverride.
    // When present, the override S3 object replaces the corresponding frame
    // in the base group for reads.
    std::vector<std::unordered_map<size_t, SubframeOverride>> subframeOverrides;

    // True if page data is encrypted (CTR for cache, GCM for S3 frames).
    // The key itself is never stored in the manifest.
    bool encrypted = false;

    // Phase GraphZenith: graphstream journal position captured at checkpoint.
    // Followers replay journal entries after this sequence.
    // Default 0 for backward compat with older manifests.
    uint64_t journalSeq = 0;

    bool isSeekable() const { return subPagesPerFrame > 0; }

    // Ensure subframeOverrides vector has the same length as pageGroupKeys.
    void normalizeOverrides();

    std::string toJSON() const;
    static std::optional<Manifest> fromJSON(const std::string& json);
};

} // namespace tiered
} // namespace lbug
