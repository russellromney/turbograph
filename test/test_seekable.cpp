// Tests for seekable multi-frame encoding/decoding and manifest serialization.

#include "chunk_codec.h"
#include "manifest.h"

#include <cassert>
#include <cstdio>
#include <cstring>

using namespace lbug::tiered;

static constexpr uint32_t PAGE_SIZE = 4096;
static constexpr uint32_t SUB_PAGES = 4;

// --- Seekable Codec Tests ---

static void testEncodeDecodeRoundtrip() {
    // 10 pages, 4 pages/frame -> 3 frames (4, 4, 2).
    std::vector<std::optional<std::vector<uint8_t>>> pages;
    for (uint32_t i = 0; i < 10; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, 0);
        page[0] = static_cast<uint8_t>(i + 1); // Marker byte.
        pages.push_back(std::move(page));
    }

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.size() == 3);
    assert(!result.blob.empty());

    // Decode frame 0: pages 0-3.
    auto& e0 = result.frameTable[0];
    std::vector<uint8_t> f0(result.blob.begin() + e0.offset,
        result.blob.begin() + e0.offset + e0.len);
    auto raw0 = decodeFrame(f0, 4, PAGE_SIZE);
    assert(raw0.size() == 4 * PAGE_SIZE);
    assert(raw0[0] == 1);                       // Page 0 marker.
    assert(raw0[PAGE_SIZE] == 2);               // Page 1 marker.
    assert(raw0[2 * PAGE_SIZE] == 3);           // Page 2 marker.
    assert(raw0[3 * PAGE_SIZE] == 4);           // Page 3 marker.

    // Decode frame 1: pages 4-7.
    auto& e1 = result.frameTable[1];
    std::vector<uint8_t> f1(result.blob.begin() + e1.offset,
        result.blob.begin() + e1.offset + e1.len);
    auto raw1 = decodeFrame(f1, 4, PAGE_SIZE);
    assert(raw1.size() == 4 * PAGE_SIZE);
    assert(raw1[0] == 5); // Page 4 marker.

    // Decode frame 2: pages 8-9 (partial, 2 pages).
    auto& e2 = result.frameTable[2];
    std::vector<uint8_t> f2(result.blob.begin() + e2.offset,
        result.blob.begin() + e2.offset + e2.len);
    auto raw2 = decodeFrame(f2, 2, PAGE_SIZE);
    assert(raw2.size() == 2 * PAGE_SIZE);
    assert(raw2[0] == 9);          // Page 8 marker.
    assert(raw2[PAGE_SIZE] == 10); // Page 9 marker.

    std::printf("  PASS: testEncodeDecodeRoundtrip\n");
}

static void testExtractPageFromFrame() {
    std::vector<std::optional<std::vector<uint8_t>>> pages;
    for (uint32_t i = 0; i < 4; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, 0);
        page[0] = static_cast<uint8_t>(i + 10);
        pages.push_back(std::move(page));
    }

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.size() == 1);

    auto& e = result.frameTable[0];
    std::vector<uint8_t> frameData(result.blob.begin() + e.offset,
        result.blob.begin() + e.offset + e.len);
    auto rawFrame = decodeFrame(frameData, 4, PAGE_SIZE);

    auto p0 = extractPageFromFrame(rawFrame, 0, PAGE_SIZE);
    assert(p0.has_value());
    assert(p0->size() == PAGE_SIZE);
    assert((*p0)[0] == 10);

    auto p3 = extractPageFromFrame(rawFrame, 3, PAGE_SIZE);
    assert(p3.has_value());
    assert((*p3)[0] == 13);

    // Out of bounds.
    auto p4 = extractPageFromFrame(rawFrame, 4, PAGE_SIZE);
    assert(!p4.has_value());

    std::printf("  PASS: testExtractPageFromFrame\n");
}

static void testSparsePages() {
    // Pages 0, 2, 4 present; 1, 3 missing (should be zero-filled).
    std::vector<std::optional<std::vector<uint8_t>>> pages(5);
    for (uint32_t i : {0, 2, 4}) {
        std::vector<uint8_t> page(PAGE_SIZE, 0);
        page[0] = static_cast<uint8_t>(i + 1);
        pages[i] = std::move(page);
    }

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    // 5 pages, 4/frame -> 2 frames.
    assert(result.frameTable.size() == 2);

    // Frame 0: pages 0-3. Pages 1,3 should be zeros.
    auto& e0 = result.frameTable[0];
    std::vector<uint8_t> f0(result.blob.begin() + e0.offset,
        result.blob.begin() + e0.offset + e0.len);
    auto raw0 = decodeFrame(f0, 4, PAGE_SIZE);
    assert(raw0[0] == 1);           // Page 0 present.
    assert(raw0[PAGE_SIZE] == 0);   // Page 1 zero-filled.
    assert(raw0[2 * PAGE_SIZE] == 3); // Page 2 present.
    assert(raw0[3 * PAGE_SIZE] == 0); // Page 3 zero-filled.

    std::printf("  PASS: testSparsePages\n");
}

static void testEmptyPages() {
    // All empty pages -> empty result.
    std::vector<std::optional<std::vector<uint8_t>>> pages(10);
    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.empty());
    assert(result.blob.empty());

    std::printf("  PASS: testEmptyPages\n");
}

static void testSinglePage() {
    std::vector<std::optional<std::vector<uint8_t>>> pages(1);
    std::vector<uint8_t> page(PAGE_SIZE, 0xAB);
    pages[0] = std::move(page);

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.size() == 1);

    auto& e = result.frameTable[0];
    std::vector<uint8_t> frameData(result.blob.begin() + e.offset,
        result.blob.begin() + e.offset + e.len);
    auto raw = decodeFrame(frameData, 1, PAGE_SIZE);
    assert(raw.size() == PAGE_SIZE);
    assert(raw[0] == 0xAB);
    assert(raw[PAGE_SIZE - 1] == 0xAB);

    std::printf("  PASS: testSinglePage\n");
}

static void testFrameOffsets() {
    // Verify frame offsets are contiguous (no gaps).
    std::vector<std::optional<std::vector<uint8_t>>> pages;
    for (uint32_t i = 0; i < 12; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i));
        pages.push_back(std::move(page));
    }

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.size() == 3);

    // First frame starts at 0.
    assert(result.frameTable[0].offset == 0);

    // Each frame starts where the previous ended.
    for (size_t i = 1; i < result.frameTable.size(); i++) {
        assert(result.frameTable[i].offset ==
            result.frameTable[i-1].offset + result.frameTable[i-1].len);
    }

    // Total blob size matches last frame end.
    auto last = result.frameTable.back();
    assert(result.blob.size() == last.offset + last.len);

    std::printf("  PASS: testFrameOffsets\n");
}

// --- Manifest Seekable Serialization Tests ---

static void testManifestSeekableRoundtrip() {
    Manifest m;
    m.version = 5;
    m.pageCount = 100;
    m.pageSize = PAGE_SIZE;
    m.pagesPerGroup = 2048;
    m.subPagesPerFrame = 4;
    m.pageGroupKeys = {"prefix/pg/0_v5", "prefix/pg/1_v5"};
    m.frameTables = {
        {{0, 1000}, {1000, 900}, {1900, 950}},
        {{0, 1100}, {1100, 1050}}
    };

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->version == 5);
    assert(parsed->pageCount == 100);
    assert(parsed->subPagesPerFrame == 4);
    assert(parsed->isSeekable());
    assert(parsed->frameTables.size() == 2);
    assert(parsed->frameTables[0].size() == 3);
    assert(parsed->frameTables[0][0].offset == 0);
    assert(parsed->frameTables[0][0].len == 1000);
    assert(parsed->frameTables[0][2].offset == 1900);
    assert(parsed->frameTables[0][2].len == 950);
    assert(parsed->frameTables[1].size() == 2);
    assert(parsed->frameTables[1][1].offset == 1100);
    assert(parsed->frameTables[1][1].len == 1050);

    std::printf("  PASS: testManifestSeekableRoundtrip\n");
}

static void testManifestLegacyBackwardCompat() {
    // A manifest without seekable fields should parse fine.
    Manifest m;
    m.version = 1;
    m.pageCount = 50;
    m.pageSize = PAGE_SIZE;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"prefix/pg/0_v1"};

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->subPagesPerFrame == 0);
    assert(!parsed->isSeekable());
    assert(parsed->frameTables.empty());

    std::printf("  PASS: testManifestLegacyBackwardCompat\n");
}

static void testManifestSeekableOmittedWhenZero() {
    // When subPagesPerFrame is 0, seekable fields are omitted from JSON.
    Manifest m;
    m.version = 1;
    m.pageCount = 10;
    m.pageSize = PAGE_SIZE;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {};

    auto json = m.toJSON();
    assert(json.find("sub_pages_per_frame") == std::string::npos);
    assert(json.find("frame_tables") == std::string::npos);

    std::printf("  PASS: testManifestSeekableOmittedWhenZero\n");
}

int main() {
    std::printf("test_seekable\n");

    // Codec tests.
    testEncodeDecodeRoundtrip();
    testExtractPageFromFrame();
    testSparsePages();
    testEmptyPages();
    testSinglePage();
    testFrameOffsets();

    // Manifest tests.
    testManifestSeekableRoundtrip();
    testManifestLegacyBackwardCompat();
    testManifestSeekableOmittedWhenZero();

    std::printf("  All 9 seekable tests passed.\n");
    return 0;
}
