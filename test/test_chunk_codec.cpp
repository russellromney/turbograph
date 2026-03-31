#include "chunk_codec.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>

using namespace lbug::tiered;

static void testHeaderSize() {
    assert(chunkHeaderSize(128) == 129 * 4);
    assert(chunkHeaderSize(512) == 513 * 4);
    assert(chunkHeaderSize(1) == 2 * 4);
    std::printf("  PASS: testHeaderSize\n");
}

static void testEncodeDecodeRoundTrip() {
    uint32_t cs = 4;
    std::vector<std::optional<std::vector<uint8_t>>> pages(cs);
    pages[0] = std::vector<uint8_t>{0x01, 0x02, 0x03};
    pages[1] = std::nullopt; // Empty slot.
    pages[2] = std::vector<uint8_t>{0xAA, 0xBB};
    pages[3] = std::vector<uint8_t>{0xFF};

    auto chunk = encodeChunk(pages, cs);

    // Extract each page.
    auto p0 = extractPage(chunk, 0, cs);
    assert(p0.has_value());
    assert(p0->size() == 3);
    assert((*p0)[0] == 0x01 && (*p0)[1] == 0x02 && (*p0)[2] == 0x03);

    auto p1 = extractPage(chunk, 1, cs);
    assert(!p1.has_value()); // Empty slot.

    auto p2 = extractPage(chunk, 2, cs);
    assert(p2.has_value());
    assert(p2->size() == 2);
    assert((*p2)[0] == 0xAA && (*p2)[1] == 0xBB);

    auto p3 = extractPage(chunk, 3, cs);
    assert(p3.has_value());
    assert(p3->size() == 1);
    assert((*p3)[0] == 0xFF);

    std::printf("  PASS: testEncodeDecodeRoundTrip\n");
}

static void testExtractOutOfBounds() {
    uint32_t cs = 4;
    std::vector<std::optional<std::vector<uint8_t>>> pages(cs);
    pages[0] = std::vector<uint8_t>{0x01};
    auto chunk = encodeChunk(pages, cs);

    // Beyond chunk size.
    assert(!extractPage(chunk, 4, cs).has_value());
    assert(!extractPage(chunk, 100, cs).has_value());

    // Truncated chunk data.
    auto truncated = std::vector<uint8_t>(chunk.begin(), chunk.begin() + 2);
    assert(!extractPage(truncated, 0, cs).has_value());

    std::printf("  PASS: testExtractOutOfBounds\n");
}

static void testEmptyChunk() {
    uint32_t cs = 128;
    std::vector<std::optional<std::vector<uint8_t>>> pages(cs);
    auto chunk = encodeChunk(pages, cs);

    // Header only, no page data.
    assert(chunk.size() == chunkHeaderSize(cs));

    for (uint32_t i = 0; i < cs; i++) {
        assert(!extractPage(chunk, i, cs).has_value());
    }
    std::printf("  PASS: testEmptyChunk\n");
}

static void testLargeChunkSize() {
    uint32_t cs = 512;
    std::vector<std::optional<std::vector<uint8_t>>> pages(cs);
    // Only populate a few pages at scattered indices.
    pages[0] = std::vector<uint8_t>(4096, 0xAA);
    pages[255] = std::vector<uint8_t>(4096, 0xBB);
    pages[511] = std::vector<uint8_t>(4096, 0xCC);

    auto chunk = encodeChunk(pages, cs);

    auto p0 = extractPage(chunk, 0, cs);
    assert(p0.has_value() && p0->size() == 4096 && (*p0)[0] == 0xAA);

    auto p255 = extractPage(chunk, 255, cs);
    assert(p255.has_value() && p255->size() == 4096 && (*p255)[0] == 0xBB);

    auto p511 = extractPage(chunk, 511, cs);
    assert(p511.has_value() && p511->size() == 4096 && (*p511)[0] == 0xCC);

    // Empty slot in between.
    assert(!extractPage(chunk, 1, cs).has_value());
    assert(!extractPage(chunk, 100, cs).has_value());

    std::printf("  PASS: testLargeChunkSize\n");
}

int main() {
    std::printf("=== Chunk Codec Tests ===\n");
    testHeaderSize();
    testEncodeDecodeRoundTrip();
    testExtractOutOfBounds();
    testEmptyChunk();
    testLargeChunkSize();
    std::printf("All chunk codec tests passed.\n");
    return 0;
}
