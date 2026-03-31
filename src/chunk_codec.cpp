#include "chunk_codec.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>
#include <zstd.h>

namespace lbug {
namespace tiered {

static uint32_t readU32LE(const uint8_t* data) {
    uint32_t val;
    std::memcpy(&val, data, sizeof(val));
    return val; // Assumes little-endian host (x86/ARM).
}

static void writeU32LE(uint8_t* dst, uint32_t val) {
    std::memcpy(dst, &val, sizeof(val));
}

uint64_t chunkHeaderSize(uint32_t chunkSize) {
    return static_cast<uint64_t>(chunkSize + 1) * sizeof(uint32_t);
}

std::vector<uint8_t> encodeChunk(const std::vector<std::optional<std::vector<uint8_t>>>& pages,
    uint32_t chunkSize) {
    auto headerBytes = chunkHeaderSize(chunkSize);
    auto numEntries = static_cast<uint32_t>(chunkSize + 1);

    // Calculate offsets.
    std::vector<uint32_t> offsets(numEntries);
    auto pos = static_cast<uint32_t>(headerBytes);
    for (uint32_t i = 0; i < chunkSize; i++) {
        offsets[i] = pos;
        if (i < pages.size() && pages[i].has_value()) {
            pos += static_cast<uint32_t>(pages[i]->size());
        }
    }
    offsets[chunkSize] = pos; // Sentinel.

    // Write header + page data.
    std::vector<uint8_t> chunk(pos);
    for (uint32_t i = 0; i < numEntries; i++) {
        writeU32LE(chunk.data() + i * sizeof(uint32_t), offsets[i]);
    }
    for (uint32_t i = 0; i < chunkSize; i++) {
        if (i < pages.size() && pages[i].has_value()) {
            std::memcpy(chunk.data() + offsets[i], pages[i]->data(), pages[i]->size());
        }
    }
    return chunk;
}

std::optional<std::vector<uint8_t>> extractPage(const std::vector<uint8_t>& chunkData,
    uint32_t localIdx, uint32_t chunkSize) {
    auto headerBytes = chunkHeaderSize(chunkSize);
    if (chunkData.size() < headerBytes || localIdx >= chunkSize) {
        return std::nullopt;
    }
    auto offStart =
        readU32LE(chunkData.data() + static_cast<uint64_t>(localIdx) * sizeof(uint32_t));
    auto offEnd =
        readU32LE(chunkData.data() + static_cast<uint64_t>(localIdx + 1) * sizeof(uint32_t));
    if (offStart == offEnd) {
        return std::nullopt; // Empty slot.
    }
    if (offEnd > chunkData.size() || offStart > offEnd) {
        return std::nullopt; // Corrupt.
    }
    return std::vector<uint8_t>(chunkData.begin() + offStart, chunkData.begin() + offEnd);
}

// --- Seekable multi-frame format ---

SeekableEncodeResult encodeSeekable(
    const std::vector<std::optional<std::vector<uint8_t>>>& pages,
    uint32_t pageSize, uint32_t subPagesPerFrame, int compressionLevel) {
    SeekableEncodeResult result;

    // Find last non-empty page to avoid encoding trailing empty frames.
    uint32_t lastPage = 0;
    for (uint32_t i = 0; i < pages.size(); i++) {
        if (pages[i].has_value() && !pages[i]->empty()) {
            lastPage = i + 1;
        }
    }
    if (lastPage == 0) {
        return result; // All empty.
    }

    auto numFrames = (lastPage + subPagesPerFrame - 1) / subPagesPerFrame;

    for (uint32_t f = 0; f < numFrames; f++) {
        auto startPage = f * subPagesPerFrame;
        auto endPage = std::min(startPage + subPagesPerFrame, lastPage);
        auto pagesInFrame = endPage - startPage;

        // Build raw frame: concatenated page data, zero-filled for missing pages.
        std::vector<uint8_t> raw(pagesInFrame * pageSize, 0);
        for (uint32_t i = 0; i < pagesInFrame; i++) {
            auto pageIdx = startPage + i;
            if (pageIdx < pages.size() && pages[pageIdx].has_value()) {
                auto& data = *pages[pageIdx];
                auto copyLen = std::min(static_cast<size_t>(pageSize), data.size());
                std::memcpy(raw.data() + i * pageSize, data.data(), copyLen);
            }
        }

        // Compress the frame.
        auto maxSize = ZSTD_compressBound(raw.size());
        std::vector<uint8_t> compressed(maxSize);
        auto actualSize = ZSTD_compress(compressed.data(), maxSize,
            raw.data(), raw.size(), compressionLevel);
        if (ZSTD_isError(actualSize)) {
            throw std::runtime_error("zstd compress failed in encodeSeekable");
        }
        compressed.resize(actualSize);

        FrameEntry entry;
        entry.offset = result.blob.size();
        entry.len = static_cast<uint32_t>(compressed.size());
        entry.pageCount = pagesInFrame;
        result.frameTable.push_back(entry);

        result.blob.insert(result.blob.end(), compressed.begin(), compressed.end());
    }

    return result;
}

std::vector<uint8_t> decodeFrame(const std::vector<uint8_t>& frameData,
    uint32_t pagesInFrame, uint32_t pageSize) {
    // Use zstd's content size when available (more reliable than caller's estimate).
    auto contentSize = ZSTD_getFrameContentSize(frameData.data(), frameData.size());
    size_t decompressedSize;
    if (contentSize != ZSTD_CONTENTSIZE_UNKNOWN && contentSize != ZSTD_CONTENTSIZE_ERROR) {
        decompressedSize = contentSize;
    } else {
        decompressedSize = static_cast<size_t>(pagesInFrame) * pageSize;
    }

    std::vector<uint8_t> raw(decompressedSize);
    auto actualSize = ZSTD_decompress(raw.data(), decompressedSize,
        frameData.data(), frameData.size());
    if (ZSTD_isError(actualSize)) {
        throw std::runtime_error("zstd decompress failed in decodeFrame");
    }
    raw.resize(actualSize);
    return raw;
}

std::optional<std::vector<uint8_t>> extractPageFromFrame(
    const std::vector<uint8_t>& frameBuffer,
    uint32_t localIdxInFrame, uint32_t pageSize) {
    auto offset = static_cast<size_t>(localIdxInFrame) * pageSize;
    if (offset + pageSize > frameBuffer.size()) {
        return std::nullopt;
    }
    return std::vector<uint8_t>(
        frameBuffer.begin() + offset,
        frameBuffer.begin() + offset + pageSize);
}

} // namespace tiered
} // namespace lbug
