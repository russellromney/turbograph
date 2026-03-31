#pragma once

#include "manifest.h"

#include <cstdint>
#include <optional>
#include <vector>

namespace lbug {
namespace tiered {

// --- Legacy chunk format (per-page compressed) ---
//
// Layout:
//   [offset_0: u32] [offset_1: u32] ... [offset_N: u32] [sentinel: u32]
//   [page_0 bytes] [page_1 bytes] ... [page_N-1 bytes]
//
// offset[i] = byte position where page i's data starts (from chunk start)
// sentinel = total chunk size
// Empty page: offset[i] == offset[i+1]

uint64_t chunkHeaderSize(uint32_t chunkSize);

std::vector<uint8_t> encodeChunk(const std::vector<std::optional<std::vector<uint8_t>>>& pages,
    uint32_t chunkSize);

std::optional<std::vector<uint8_t>> extractPage(const std::vector<uint8_t>& chunkData,
    uint32_t localIdx, uint32_t chunkSize);

// --- Seekable multi-frame format ---
//
// Each page group is encoded as multiple independently-decompressible zstd frames.
// Each frame contains subPagesPerFrame consecutive pages (raw, then zstd compressed).
// The frame table records byte offsets within the S3 object so individual frames
// can be fetched via HTTP range GETs.
//
// Layout of S3 object:
//   [frame_0: zstd(page_0..page_N)] [frame_1: zstd(page_N..page_2N)] ...
//
// Frame table (stored in manifest, not in the S3 object):
//   frameTables[gid][frameIdx] = { offset, len }

struct SeekableEncodeResult {
    std::vector<uint8_t> blob;            // Concatenated compressed frames.
    std::vector<FrameEntry> frameTable;   // Per-frame byte offsets.
};

// Encode a page group as seekable multi-frame format.
// pages: raw (uncompressed) page data, indexed by local page index within the group.
// Missing/empty pages are zero-filled within their frame.
SeekableEncodeResult encodeSeekable(
    const std::vector<std::optional<std::vector<uint8_t>>>& pages,
    uint32_t pageSize, uint32_t subPagesPerFrame, int compressionLevel);

// Decode a single seekable frame into raw pages.
// frameData: the compressed bytes for one frame (from S3 range GET).
// Returns decompressed raw page data (subPagesPerFrame * pageSize bytes).
std::vector<uint8_t> decodeFrame(const std::vector<uint8_t>& frameData,
    uint32_t pagesInFrame, uint32_t pageSize);

// Extract a single page from a decoded frame buffer.
// frameBuffer: decompressed frame (from decodeFrame).
// localIdxInFrame: page index within the frame (0..subPagesPerFrame-1).
std::optional<std::vector<uint8_t>> extractPageFromFrame(
    const std::vector<uint8_t>& frameBuffer,
    uint32_t localIdxInFrame, uint32_t pageSize);

} // namespace tiered
} // namespace lbug
