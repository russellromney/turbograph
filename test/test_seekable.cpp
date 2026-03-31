// Tests for seekable multi-frame encoding/decoding and manifest serialization.
// Covers: roundtrips, partial frames, edge cases, regression tests for bugs.

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
        page[0] = static_cast<uint8_t>(i + 1);
        pages.push_back(std::move(page));
    }

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.size() == 3);
    assert(!result.blob.empty());

    // Frame 0: pages 0-3.
    auto& e0 = result.frameTable[0];
    assert(e0.pageCount == 4);
    std::vector<uint8_t> f0(result.blob.begin() + e0.offset,
        result.blob.begin() + e0.offset + e0.len);
    auto raw0 = decodeFrame(f0, e0.pageCount, PAGE_SIZE);
    assert(raw0.size() == 4 * PAGE_SIZE);
    assert(raw0[0] == 1);
    assert(raw0[3 * PAGE_SIZE] == 4);

    // Frame 2: pages 8-9 (partial).
    auto& e2 = result.frameTable[2];
    assert(e2.pageCount == 2); // Regression: partial frame stores actual count.
    std::vector<uint8_t> f2(result.blob.begin() + e2.offset,
        result.blob.begin() + e2.offset + e2.len);
    auto raw2 = decodeFrame(f2, e2.pageCount, PAGE_SIZE);
    assert(raw2.size() == 2 * PAGE_SIZE);
    assert(raw2[0] == 9);
    assert(raw2[PAGE_SIZE] == 10);

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
    auto& e = result.frameTable[0];
    std::vector<uint8_t> frameData(result.blob.begin() + e.offset,
        result.blob.begin() + e.offset + e.len);
    auto rawFrame = decodeFrame(frameData, e.pageCount, PAGE_SIZE);

    auto p0 = extractPageFromFrame(rawFrame, 0, PAGE_SIZE);
    assert(p0.has_value() && (*p0)[0] == 10);

    auto p3 = extractPageFromFrame(rawFrame, 3, PAGE_SIZE);
    assert(p3.has_value() && (*p3)[0] == 13);

    auto p4 = extractPageFromFrame(rawFrame, 4, PAGE_SIZE);
    assert(!p4.has_value());

    std::printf("  PASS: testExtractPageFromFrame\n");
}

static void testSparsePages() {
    std::vector<std::optional<std::vector<uint8_t>>> pages(5);
    for (uint32_t i : {0, 2, 4}) {
        std::vector<uint8_t> page(PAGE_SIZE, 0);
        page[0] = static_cast<uint8_t>(i + 1);
        pages[i] = std::move(page);
    }

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.size() == 2);

    auto& e0 = result.frameTable[0];
    std::vector<uint8_t> f0(result.blob.begin() + e0.offset,
        result.blob.begin() + e0.offset + e0.len);
    auto raw0 = decodeFrame(f0, e0.pageCount, PAGE_SIZE);
    assert(raw0[0] == 1);
    assert(raw0[PAGE_SIZE] == 0);     // Page 1 zero-filled.
    assert(raw0[2 * PAGE_SIZE] == 3);
    assert(raw0[3 * PAGE_SIZE] == 0); // Page 3 zero-filled.

    std::printf("  PASS: testSparsePages\n");
}

static void testEmptyPages() {
    std::vector<std::optional<std::vector<uint8_t>>> pages(10);
    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.empty());
    assert(result.blob.empty());
    std::printf("  PASS: testEmptyPages\n");
}

static void testSinglePage() {
    std::vector<std::optional<std::vector<uint8_t>>> pages(1);
    pages[0] = std::vector<uint8_t>(PAGE_SIZE, 0xAB);

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.size() == 1);
    assert(result.frameTable[0].pageCount == 1);

    auto& e = result.frameTable[0];
    std::vector<uint8_t> frameData(result.blob.begin() + e.offset,
        result.blob.begin() + e.offset + e.len);
    auto raw = decodeFrame(frameData, e.pageCount, PAGE_SIZE);
    assert(raw.size() == PAGE_SIZE);
    assert(raw[0] == 0xAB);

    std::printf("  PASS: testSinglePage\n");
}

static void testFrameOffsets() {
    std::vector<std::optional<std::vector<uint8_t>>> pages;
    for (uint32_t i = 0; i < 12; i++) {
        pages.push_back(std::vector<uint8_t>(PAGE_SIZE, static_cast<uint8_t>(i)));
    }

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.size() == 3);
    assert(result.frameTable[0].offset == 0);

    for (size_t i = 1; i < result.frameTable.size(); i++) {
        assert(result.frameTable[i].offset ==
            result.frameTable[i-1].offset + result.frameTable[i-1].len);
    }

    auto last = result.frameTable.back();
    assert(result.blob.size() == last.offset + last.len);

    std::printf("  PASS: testFrameOffsets\n");
}

// --- Regression: partial frame page count stored in FrameEntry ---

static void testPartialFramePageCount() {
    // 7 pages, subPagesPerFrame=4 -> frame 0 has 4 pages, frame 1 has 3 pages.
    std::vector<std::optional<std::vector<uint8_t>>> pages;
    for (uint32_t i = 0; i < 7; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, 0);
        page[0] = static_cast<uint8_t>(i + 1);
        pages.push_back(std::move(page));
    }

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.size() == 2);
    assert(result.frameTable[0].pageCount == 4);
    assert(result.frameTable[1].pageCount == 3); // NOT 4!

    // Decode partial frame using stored pageCount.
    auto& e1 = result.frameTable[1];
    std::vector<uint8_t> f1(result.blob.begin() + e1.offset,
        result.blob.begin() + e1.offset + e1.len);
    auto raw1 = decodeFrame(f1, e1.pageCount, PAGE_SIZE);
    assert(raw1.size() == 3 * PAGE_SIZE);
    assert(raw1[0] == 5);
    assert(raw1[PAGE_SIZE] == 6);
    assert(raw1[2 * PAGE_SIZE] == 7);

    std::printf("  PASS: testPartialFramePageCount\n");
}

// --- Regression: decodeFrame uses zstd content size, not caller estimate ---

static void testDecodeFrameWrongCallerEstimate() {
    // Encode a frame with 2 pages, but pass pagesInFrame=4 to decodeFrame.
    // decodeFrame should use zstd's embedded content size and succeed.
    std::vector<std::optional<std::vector<uint8_t>>> pages(2);
    pages[0] = std::vector<uint8_t>(PAGE_SIZE, 0xCC);
    pages[1] = std::vector<uint8_t>(PAGE_SIZE, 0xDD);

    auto result = encodeSeekable(pages, PAGE_SIZE, SUB_PAGES, 3);
    assert(result.frameTable.size() == 1);
    assert(result.frameTable[0].pageCount == 2);

    auto& e = result.frameTable[0];
    std::vector<uint8_t> frameData(result.blob.begin() + e.offset,
        result.blob.begin() + e.offset + e.len);

    // Pass wrong pagesInFrame (4 instead of 2). Should still work because
    // decodeFrame reads zstd content size from the frame header.
    auto raw = decodeFrame(frameData, 4, PAGE_SIZE);
    assert(raw.size() == 2 * PAGE_SIZE); // Actual content, not 4*PAGE_SIZE.
    assert(raw[0] == 0xCC);
    assert(raw[PAGE_SIZE] == 0xDD);

    std::printf("  PASS: testDecodeFrameWrongCallerEstimate\n");
}

// --- Edge case: subPagesPerFrame=1 (one page per frame) ---

static void testSubPagesPerFrameOne() {
    std::vector<std::optional<std::vector<uint8_t>>> pages;
    for (uint32_t i = 0; i < 5; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, 0);
        page[0] = static_cast<uint8_t>(i + 1);
        pages.push_back(std::move(page));
    }

    auto result = encodeSeekable(pages, PAGE_SIZE, /*subPagesPerFrame=*/1, 3);
    assert(result.frameTable.size() == 5); // One frame per page.

    for (uint32_t i = 0; i < 5; i++) {
        assert(result.frameTable[i].pageCount == 1);
        auto& e = result.frameTable[i];
        std::vector<uint8_t> fd(result.blob.begin() + e.offset,
            result.blob.begin() + e.offset + e.len);
        auto raw = decodeFrame(fd, e.pageCount, PAGE_SIZE);
        assert(raw.size() == PAGE_SIZE);
        assert(raw[0] == static_cast<uint8_t>(i + 1));
    }

    std::printf("  PASS: testSubPagesPerFrameOne\n");
}

// --- Edge case: pagesPerGroup not divisible by subPagesPerFrame ---

static void testNonDivisibleGroupSize() {
    // 10 pages, subPagesPerFrame=3 -> frames of 3, 3, 3, 1.
    std::vector<std::optional<std::vector<uint8_t>>> pages;
    for (uint32_t i = 0; i < 10; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, 0);
        page[0] = static_cast<uint8_t>(i + 1);
        pages.push_back(std::move(page));
    }

    auto result = encodeSeekable(pages, PAGE_SIZE, /*subPagesPerFrame=*/3, 3);
    assert(result.frameTable.size() == 4);
    assert(result.frameTable[0].pageCount == 3);
    assert(result.frameTable[1].pageCount == 3);
    assert(result.frameTable[2].pageCount == 3);
    assert(result.frameTable[3].pageCount == 1); // Remainder.

    // Verify last frame (single page).
    auto& last = result.frameTable[3];
    std::vector<uint8_t> fd(result.blob.begin() + last.offset,
        result.blob.begin() + last.offset + last.len);
    auto raw = decodeFrame(fd, last.pageCount, PAGE_SIZE);
    assert(raw.size() == PAGE_SIZE);
    assert(raw[0] == 10);

    std::printf("  PASS: testNonDivisibleGroupSize\n");
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
        {{0, 1000, 4}, {1000, 900, 4}, {1900, 950, 2}},
        {{0, 1100, 4}, {1100, 1050, 3}}
    };

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->version == 5);
    assert(parsed->subPagesPerFrame == 4);
    assert(parsed->isSeekable());
    assert(parsed->frameTables.size() == 2);
    assert(parsed->frameTables[0].size() == 3);
    assert(parsed->frameTables[0][0].offset == 0);
    assert(parsed->frameTables[0][0].len == 1000);
    assert(parsed->frameTables[0][0].pageCount == 4);
    assert(parsed->frameTables[0][2].pageCount == 2); // Partial frame.
    assert(parsed->frameTables[1][1].pageCount == 3);

    std::printf("  PASS: testManifestSeekableRoundtrip\n");
}

static void testManifestPageCountRoundtrip() {
    // Regression: pageCount must survive JSON roundtrip.
    Manifest m;
    m.version = 1;
    m.pageCount = 10;
    m.pageSize = PAGE_SIZE;
    m.pagesPerGroup = 8;
    m.subPagesPerFrame = 3;
    m.pageGroupKeys = {"pg/0_v1"};
    m.frameTables = {
        {{0, 500, 3}, {500, 400, 3}, {900, 300, 2}}
    };

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->frameTables[0][0].pageCount == 3);
    assert(parsed->frameTables[0][1].pageCount == 3);
    assert(parsed->frameTables[0][2].pageCount == 2);

    std::printf("  PASS: testManifestPageCountRoundtrip\n");
}

static void testManifestLegacyBackwardCompat() {
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

// --- Regression: old 2-element frame entries (no pageCount) parse with pageCount=0 ---

static void testManifestOldTwoElementFrameEntry() {
    // Simulate a manifest written before pageCount was added to FrameEntry.
    std::string json = R"({"version":1,"page_count":10,"page_size":4096,)"
        R"("pages_per_group":8,"page_group_keys":["pg/0_v1"],)"
        R"("sub_pages_per_frame":4,"frame_tables":[[[0,500],[500,400]]]})";

    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->frameTables.size() == 1);
    assert(parsed->frameTables[0].size() == 2);
    assert(parsed->frameTables[0][0].offset == 0);
    assert(parsed->frameTables[0][0].len == 500);
    assert(parsed->frameTables[0][0].pageCount == 0); // Missing = 0.
    assert(parsed->frameTables[0][1].offset == 500);
    assert(parsed->frameTables[0][1].len == 400);
    assert(parsed->frameTables[0][1].pageCount == 0);

    std::printf("  PASS: testManifestOldTwoElementFrameEntry\n");
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

    // Regression tests.
    testPartialFramePageCount();
    testDecodeFrameWrongCallerEstimate();

    // Edge cases.
    testSubPagesPerFrameOne();
    testNonDivisibleGroupSize();

    // Manifest tests.
    testManifestSeekableRoundtrip();
    testManifestPageCountRoundtrip();
    testManifestLegacyBackwardCompat();
    testManifestSeekableOmittedWhenZero();
    testManifestOldTwoElementFrameEntry();

    std::printf("  All 15 seekable tests passed.\n");
    return 0;
}
