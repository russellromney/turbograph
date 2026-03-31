#include "page_bitmap.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <filesystem>

using namespace lbug::tiered;

static std::filesystem::path tmpBitmapPath() {
    auto dir = std::filesystem::temp_directory_path() / "tiered_bitmap_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir / "page_bitmap";
}

static void testMarkAndCheck() {
    auto path = tmpBitmapPath();
    PageBitmap bm(path);

    assert(!bm.isPresent(0));
    assert(!bm.isPresent(1));
    assert(!bm.isPresent(100));

    bm.markPresent(0);
    assert(bm.isPresent(0));
    assert(!bm.isPresent(1));

    bm.markPresent(7);
    assert(bm.isPresent(7));
    assert(!bm.isPresent(6));

    bm.markPresent(8);
    assert(bm.isPresent(8));

    bm.markPresent(1000);
    assert(bm.isPresent(1000));
    assert(!bm.isPresent(999));

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testMarkAndCheck\n");
}

static void testMarkRange() {
    auto path = tmpBitmapPath();
    PageBitmap bm(path);

    bm.markRange(10, 20); // Pages 10..29.

    for (uint64_t i = 0; i < 10; i++) {
        assert(!bm.isPresent(i));
    }
    for (uint64_t i = 10; i < 30; i++) {
        assert(bm.isPresent(i));
    }
    assert(!bm.isPresent(30));

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testMarkRange\n");
}

static void testClear() {
    auto path = tmpBitmapPath();
    PageBitmap bm(path);

    bm.markRange(0, 100);
    assert(bm.presentCount() == 100);

    bm.clear();
    assert(bm.presentCount() == 0);

    for (uint64_t i = 0; i < 100; i++) {
        assert(!bm.isPresent(i));
    }

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testClear\n");
}

static void testResize() {
    auto path = tmpBitmapPath();
    PageBitmap bm(path);

    bm.resize(1000);
    assert(bm.pageCount() >= 1000);

    // All should be absent after resize.
    for (uint64_t i = 0; i < 1000; i++) {
        assert(!bm.isPresent(i));
    }

    bm.markPresent(999);
    assert(bm.isPresent(999));

    // Resize larger — existing bits preserved.
    bm.resize(2000);
    assert(bm.isPresent(999));
    assert(!bm.isPresent(1500));

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testResize\n");
}

static void testPresentCount() {
    auto path = tmpBitmapPath();
    PageBitmap bm(path);

    assert(bm.presentCount() == 0);

    bm.markPresent(0);
    assert(bm.presentCount() == 1);

    bm.markPresent(0); // Idempotent.
    assert(bm.presentCount() == 1);

    bm.markRange(10, 5);
    assert(bm.presentCount() == 6); // 0 + 10,11,12,13,14.

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testPresentCount\n");
}

static void testPersistAndLoad() {
    auto path = tmpBitmapPath();

    // Write with one bitmap instance.
    {
        PageBitmap bm(path);
        bm.markPresent(0);
        bm.markPresent(7);
        bm.markPresent(8);
        bm.markPresent(100);
        bm.markRange(200, 50);
        bm.persist();
    }

    // Load with a new bitmap instance.
    {
        PageBitmap bm(path);
        assert(bm.isPresent(0));
        assert(bm.isPresent(7));
        assert(bm.isPresent(8));
        assert(bm.isPresent(100));
        assert(!bm.isPresent(99));
        for (uint64_t i = 200; i < 250; i++) {
            assert(bm.isPresent(i));
        }
        assert(!bm.isPresent(250));
    }

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testPersistAndLoad\n");
}

static void testLoadFromEmptyDir() {
    auto path = tmpBitmapPath();
    // No bitmap file exists yet.
    PageBitmap bm(path);
    assert(bm.presentCount() == 0);
    assert(!bm.isPresent(0));

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testLoadFromEmptyDir\n");
}

static void testLargeBitmap() {
    auto path = tmpBitmapPath();
    PageBitmap bm(path);

    // 128K pages = 500MB DB worth (at 4KB pages).
    uint64_t pageCount = 128 * 1024;
    bm.resize(pageCount);

    // Mark every other page.
    for (uint64_t i = 0; i < pageCount; i += 2) {
        bm.markPresent(i);
    }
    assert(bm.presentCount() == pageCount / 2);

    // Verify.
    for (uint64_t i = 0; i < pageCount; i++) {
        if (i % 2 == 0) {
            assert(bm.isPresent(i));
        } else {
            assert(!bm.isPresent(i));
        }
    }

    // Persist and reload.
    bm.persist();
    {
        PageBitmap bm2(path);
        assert(bm2.presentCount() == pageCount / 2);
        assert(bm2.isPresent(0));
        assert(!bm2.isPresent(1));
        assert(bm2.isPresent(pageCount - 2));
    }

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testLargeBitmap\n");
}

static void testClearRange() {
    auto path = tmpBitmapPath();
    PageBitmap bm(path);

    bm.markRange(10, 20); // Pages 10..29.

    // Clear pages 15..24 (10 pages in the middle).
    bm.clearRange(15, 10);

    // Pages 10-14 still present.
    for (uint64_t i = 10; i < 15; i++) {
        assert(bm.isPresent(i));
    }
    // Pages 15-24 cleared.
    for (uint64_t i = 15; i < 25; i++) {
        assert(!bm.isPresent(i));
    }
    // Pages 25-29 still present.
    for (uint64_t i = 25; i < 30; i++) {
        assert(bm.isPresent(i));
    }

    assert(bm.presentCount() == 10);

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testClearRange\n");
}

static void testClearRangeEntireRange() {
    auto path = tmpBitmapPath();
    PageBitmap bm(path);

    bm.markRange(0, 512);
    assert(bm.presentCount() == 512);

    bm.clearRange(0, 512);
    assert(bm.presentCount() == 0);

    for (uint64_t i = 0; i < 512; i++) {
        assert(!bm.isPresent(i));
    }

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testClearRangeEntireRange\n");
}

static void testClearRangeBeyondSize() {
    auto path = tmpBitmapPath();
    PageBitmap bm(path);

    bm.markRange(0, 10);

    // Clear range extending beyond the bitmap size — should not crash.
    bm.clearRange(5, 100);

    for (uint64_t i = 0; i < 5; i++) {
        assert(bm.isPresent(i));
    }
    for (uint64_t i = 5; i < 10; i++) {
        assert(!bm.isPresent(i));
    }

    assert(bm.presentCount() == 5);

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testClearRangeBeyondSize\n");
}

static void testClearRangePersistAndLoad() {
    auto path = tmpBitmapPath();

    {
        PageBitmap bm(path);
        bm.markRange(0, 100);
        bm.clearRange(20, 30); // Clear 20..49.
        bm.persist();
    }

    {
        PageBitmap bm(path);
        for (uint64_t i = 0; i < 20; i++) assert(bm.isPresent(i));
        for (uint64_t i = 20; i < 50; i++) assert(!bm.isPresent(i));
        for (uint64_t i = 50; i < 100; i++) assert(bm.isPresent(i));
        assert(bm.presentCount() == 70);
    }

    std::filesystem::remove_all(path.parent_path());
    std::printf("  PASS: testClearRangePersistAndLoad\n");
}

int main() {
    std::printf("=== PageBitmap Tests ===\n");
    testMarkAndCheck();
    testMarkRange();
    testClear();
    testResize();
    testPresentCount();
    testPersistAndLoad();
    testLoadFromEmptyDir();
    testLargeBitmap();
    testClearRange();
    testClearRangeEntireRange();
    testClearRangeBeyondSize();
    testClearRangePersistAndLoad();
    std::printf("All PageBitmap tests passed.\n");
    return 0;
}
