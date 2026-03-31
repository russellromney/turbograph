// Unit tests for prefetch logic and PageBitmap-based cache tracking.
// No S3 required — pure in-memory/disk tests.

#include "page_bitmap.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <vector>

using namespace lbug::tiered;

static std::filesystem::path tmpDir() {
    auto dir = std::filesystem::temp_directory_path() / "tiered_prefetch_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

// --- Bitmap-based cache tracking tests ---

static void testBitmapPresent() {
    auto dir = tmpDir();
    PageBitmap bm(dir / "page_bitmap");

    bm.markPresent(42);
    assert(bm.isPresent(42));
    assert(!bm.isPresent(43));

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testBitmapPresent\n");
}

static void testBitmapAfterClear() {
    auto dir = tmpDir();
    PageBitmap bm(dir / "page_bitmap");

    bm.markPresent(1);
    bm.markPresent(2);
    assert(bm.isPresent(1));
    assert(bm.isPresent(2));

    bm.clear();
    assert(!bm.isPresent(1));
    assert(!bm.isPresent(2));

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testBitmapAfterClear\n");
}

static void testBitmapMarkRange() {
    auto dir = tmpDir();
    PageBitmap bm(dir / "page_bitmap");

    bm.markRange(0, 512); // One full page group worth.
    for (uint64_t i = 0; i < 512; i++) {
        assert(bm.isPresent(i));
    }
    assert(!bm.isPresent(512));

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testBitmapMarkRange\n");
}

static void testBitmapClearRange() {
    auto dir = tmpDir();
    PageBitmap bm(dir / "page_bitmap");

    bm.markRange(0, 512);
    assert(bm.presentCount() == 512);

    bm.clearRange(0, 512);
    assert(bm.presentCount() == 0);
    assert(!bm.isPresent(0));
    assert(!bm.isPresent(511));

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testBitmapClearRange\n");
}

// --- Prefetch math tests ---

// Simulates the hop-based prefetch count calculation from readOnePage.
static uint64_t computePrefetchCount(uint8_t consecutiveMisses,
    uint64_t totalPageGroups, const std::vector<float>& hops) {
    float hopSum = 0;
    for (auto v : hops) hopSum += v;
    bool fractionMode = (hopSum <= 1.01f);

    auto hopIdx = static_cast<size_t>(consecutiveMisses - 1);

    if (hopIdx >= hops.size()) {
        return totalPageGroups; // Implicit final hop: everything.
    } else if (fractionMode) {
        return std::max(uint64_t{1},
            static_cast<uint64_t>(hops[hopIdx] * totalPageGroups));
    } else {
        return static_cast<uint64_t>(hops[hopIdx]);
    }
}

static void testHopScheduleDefault() {
    // Default hops: {0.01, 0.1} = 3 hops — 1% / 10% / everything.
    std::vector<float> hops = {0.01f, 0.1f};
    uint64_t totalPGs = 1000;

    // Miss 1: hop 0 → 1% of 1000 = 10.
    assert(computePrefetchCount(1, totalPGs, hops) == 10);
    // Miss 2: hop 1 → 10% of 1000 = 100.
    assert(computePrefetchCount(2, totalPGs, hops) == 100);
    // Miss 3: implicit final hop → everything.
    assert(computePrefetchCount(3, totalPGs, hops) == 1000);
    // Miss 10: still everything.
    assert(computePrefetchCount(10, totalPGs, hops) == 1000);

    std::printf("  PASS: testHopScheduleDefault\n");
}

static void testHopScheduleAbsolute() {
    // Absolute mode: {4, 16, 64}.
    std::vector<float> hops = {4.0f, 16.0f, 64.0f};
    uint64_t totalPGs = 500;

    assert(computePrefetchCount(1, totalPGs, hops) == 4);
    assert(computePrefetchCount(2, totalPGs, hops) == 16);
    assert(computePrefetchCount(3, totalPGs, hops) == 64);
    // Miss 4: beyond hops → everything.
    assert(computePrefetchCount(4, totalPGs, hops) == 500);

    std::printf("  PASS: testHopScheduleAbsolute\n");
}

static void testHopScheduleSmallDB() {
    // Small DB: 5 page groups. Fraction mode clamps to at least 1.
    std::vector<float> hops = {0.01f, 0.1f};
    uint64_t totalPGs = 5;

    assert(computePrefetchCount(1, totalPGs, hops) == 1); // 0.01 * 5 = 0.05, clamped to 1.
    assert(computePrefetchCount(2, totalPGs, hops) == 1); // 0.1 * 5 = 0.5, clamped to 1.
    assert(computePrefetchCount(3, totalPGs, hops) == 5); // Everything.

    std::printf("  PASS: testHopScheduleSmallDB\n");
}

// --- Large batch bitmap test ---

static void testBitmapHandlesManyPages() {
    auto dir = tmpDir();
    PageBitmap bm(dir / "page_bitmap");

    // Mark 200 page groups worth of pages (200 * 512 = 102400 pages).
    for (uint64_t pg = 0; pg < 200; pg++) {
        auto start = pg * 512;
        bm.markRange(start, 512);
    }

    assert(bm.presentCount() == 200 * 512);

    // Spot check.
    assert(bm.isPresent(0));
    assert(bm.isPresent(511));
    assert(bm.isPresent(512));
    assert(bm.isPresent(200 * 512 - 1));
    assert(!bm.isPresent(200 * 512));

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testBitmapHandlesManyPages\n");
}

int main() {
    std::printf("=== Prefetch Unit Tests ===\n");
    testBitmapPresent();
    testBitmapAfterClear();
    testBitmapMarkRange();
    testBitmapClearRange();
    testHopScheduleDefault();
    testHopScheduleAbsolute();
    testHopScheduleSmallDB();
    testBitmapHandlesManyPages();

    std::printf("All prefetch unit tests passed.\n");
    return 0;
}
