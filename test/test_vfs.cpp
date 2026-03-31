// VFS unit tests — exercise TieredFileSystem directly without Kuzu or S3.
//
// Tests the real VFS code paths: write → dirty page → read, write → sync → read
// from local file via bitmap/pread, partial page writes, cross-page I/O, truncate.
//
// S3 uploads will fail (dummy credentials) but that's fine — we're testing the
// local file + bitmap path. The sync path writes pages to the local file and
// marks the bitmap BEFORE attempting S3 upload.
//
// Uses small pagesPerGroup=4 so we can fill a page group without writing 512 pages.

#include "tiered_file_system.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>

using namespace lbug::tiered;
using namespace lbug::common;

static constexpr uint32_t PAGE_SIZE = 4096;
static constexpr uint32_t PAGES_PER_GROUP = 4;

static std::filesystem::path tmpDir() {
    auto dir = std::filesystem::temp_directory_path() / "tiered_vfs_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

static TieredConfig makeConfig(const std::filesystem::path& dir) {
    TieredConfig cfg;
    cfg.s3 = {"https://localhost:1", "fake-bucket", "test/vfs", "auto", "fake-ak", "fake-sk", 1};
    cfg.cacheDir = (dir / "cache").string();
    cfg.dataFilePath = (dir / "data.kz").string();
    cfg.pageSize = PAGE_SIZE;
    cfg.pagesPerGroup = PAGES_PER_GROUP;
    cfg.compressionLevel = 3;
    return cfg;
}

// --- Test: write full page, read it back (dirty page path) ---

static void testWriteReadDirty() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write a full page at offset 0.
    std::vector<uint8_t> page0(PAGE_SIZE, 0xAA);
    fi->writeFile(page0.data(), PAGE_SIZE, 0);

    // Read it back — should come from dirty map.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    assert(buf == page0);

    // Write page 1 with different data.
    std::vector<uint8_t> page1(PAGE_SIZE, 0xBB);
    fi->writeFile(page1.data(), PAGE_SIZE, PAGE_SIZE);

    // Both pages should be readable.
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    assert(buf == page0);
    fi->readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    assert(buf == page1);

    // File size should reflect 2 pages.
    assert(fi->getFileSize() == 2 * PAGE_SIZE);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWriteReadDirty\n");
}

// --- Test: write, sync, read back from local file via bitmap ---

static void testWriteSyncRead() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 4 pages (fills one page group).
    for (int i = 0; i < 4; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 1));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }

    // Sync — writes pages to local file + marks bitmap.
    // S3 upload will fail (fake credentials), but local file is updated first.
    fi->syncFile();

    // Read back — should come from local file via bitmap (dirty map is cleared by sync).
    for (int i = 0; i < 4; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        for (auto b : buf) {
            assert(b == static_cast<uint8_t>(i + 1));
        }
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWriteSyncRead\n");
}

// --- Test: partial page write (read-modify-write) ---

static void testPartialPageWrite() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write a full page of 0xAA.
    std::vector<uint8_t> page(PAGE_SIZE, 0xAA);
    fi->writeFile(page.data(), PAGE_SIZE, 0);

    // Overwrite bytes 100-199 with 0xBB (partial page write).
    std::vector<uint8_t> patch(100, 0xBB);
    fi->writeFile(patch.data(), 100, 100);

    // Read back full page — should be patched.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (int i = 0; i < 100; i++) assert(buf[i] == 0xAA);
    for (int i = 100; i < 200; i++) assert(buf[i] == 0xBB);
    for (int i = 200; i < PAGE_SIZE; i++) assert(buf[i] == 0xAA);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testPartialPageWrite\n");
}

// --- Test: cross-page read spanning two pages ---

static void testCrossPageRead() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    std::vector<uint8_t> page0(PAGE_SIZE, 0xAA);
    std::vector<uint8_t> page1(PAGE_SIZE, 0xBB);
    fi->writeFile(page0.data(), PAGE_SIZE, 0);
    fi->writeFile(page1.data(), PAGE_SIZE, PAGE_SIZE);

    // Read 200 bytes spanning the page boundary.
    std::vector<uint8_t> buf(200, 0);
    fi->readFromFile(buf.data(), 200, PAGE_SIZE - 100);
    for (int i = 0; i < 100; i++) assert(buf[i] == 0xAA);
    for (int i = 100; i < 200; i++) assert(buf[i] == 0xBB);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testCrossPageRead\n");
}

// --- Test: cross-page write spanning two pages ---

static void testCrossPageWrite() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 2 pages of zeros.
    std::vector<uint8_t> zeros(2 * PAGE_SIZE, 0x00);
    fi->writeFile(zeros.data(), 2 * PAGE_SIZE, 0);

    // Write 200 bytes of 0xDD spanning the page boundary.
    std::vector<uint8_t> patch(200, 0xDD);
    fi->writeFile(patch.data(), 200, PAGE_SIZE - 100);

    // Verify page 0.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (uint32_t i = 0; i < PAGE_SIZE - 100; i++) assert(buf[i] == 0x00);
    for (uint32_t i = PAGE_SIZE - 100; i < PAGE_SIZE; i++) assert(buf[i] == 0xDD);

    // Verify page 1.
    fi->readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    for (int i = 0; i < 100; i++) assert(buf[i] == 0xDD);
    for (uint32_t i = 100; i < PAGE_SIZE; i++) assert(buf[i] == 0x00);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testCrossPageWrite\n");
}

// --- Test: truncate ---

static void testTruncate() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 5 pages.
    for (int i = 0; i < 5; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 1));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    assert(fi->getFileSize() == 5 * PAGE_SIZE);

    // Truncate to 2 pages.
    fi->truncate(2 * PAGE_SIZE);
    assert(fi->getFileSize() == 2 * PAGE_SIZE);

    // Pages 0 and 1 still readable.
    std::vector<uint8_t> buf(PAGE_SIZE);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0x01);
    fi->readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    for (auto b : buf) assert(b == 0x02);

    // Pages beyond truncation return zeros.
    fi->readFromFile(buf.data(), PAGE_SIZE, 2 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x00);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testTruncate\n");
}

// --- Test: write → sync → dirty pages cleared → read from local file ---
// This verifies that sync actually moves data from dirty map to local file.

static void testSyncClearsDirtyPages() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    std::vector<uint8_t> page(PAGE_SIZE, 0x42);
    fi->writeFile(page.data(), PAGE_SIZE, 0);

    // Before sync: data is in dirty map.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    assert(buf == page);

    // Sync moves data to local file + bitmap.
    fi->syncFile();

    // After sync: dirty map is empty, data comes from local file.
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    assert(buf == page);

    // Write a new page after sync — should go to dirty map.
    std::vector<uint8_t> page2(PAGE_SIZE, 0x99);
    fi->writeFile(page2.data(), PAGE_SIZE, PAGE_SIZE);
    fi->readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    assert(buf == page2);

    // Original page still readable from local file.
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    assert(buf == page);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testSyncClearsDirtyPages\n");
}

// --- Test: multi-page write at non-zero offset ---

static void testMultiPageWriteOffset() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write page 0 with 0xFF.
    std::vector<uint8_t> page0(PAGE_SIZE, 0xFF);
    fi->writeFile(page0.data(), PAGE_SIZE, 0);

    // Write 3 pages at once starting at page 1.
    std::vector<uint8_t> data(3 * PAGE_SIZE, 0xCC);
    fi->writeFile(data.data(), 3 * PAGE_SIZE, PAGE_SIZE);

    assert(fi->getFileSize() == 4 * PAGE_SIZE);

    // Page 0 should still be 0xFF.
    std::vector<uint8_t> buf(PAGE_SIZE);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xFF);

    // Pages 1-3 should be 0xCC.
    for (int i = 1; i <= 3; i++) {
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        for (auto b : buf) assert(b == 0xCC);
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testMultiPageWriteOffset\n");
}

// --- Test: bitmap state after sync ---
// Verifies that after sync, the bitmap reflects which pages are in the local file.

static void testBitmapStateAfterSync() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    auto& ti = fi->cast<TieredFileInfo>();

    // Write pages 0,1,2 — skip page 3.
    for (int i = 0; i < 3; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }

    // Before sync: bitmap should be empty (pages only in dirty map).
    assert(!ti.bitmap->isPresent(0));
    assert(!ti.bitmap->isPresent(1));
    assert(!ti.bitmap->isPresent(2));

    fi->syncFile();

    // After sync: bitmap should mark pages 0,1,2 as present.
    assert(ti.bitmap->isPresent(0));
    assert(ti.bitmap->isPresent(1));
    assert(ti.bitmap->isPresent(2));
    assert(!ti.bitmap->isPresent(3)); // Never written.

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testBitmapStateAfterSync\n");
}

// --- Test: clearCacheAll makes all reads miss the bitmap ---
// After clearCache, reads for pages that were synced to local file should
// go to S3. Since S3 is fake, they'll return zeros. This tests the bitmap
// clearing path — the actual S3 cold read is tested in test_vfs_s3.cpp.

static void testClearCacheInvalidatesBitmap() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    auto& ti = fi->cast<TieredFileInfo>();

    std::vector<uint8_t> page(PAGE_SIZE, 0x42);
    fi->writeFile(page.data(), PAGE_SIZE, 0);
    fi->syncFile();

    assert(ti.bitmap->isPresent(0));

    // Clear cache (bitmap).
    vfs.clearCacheAll();
    assert(!ti.bitmap->isPresent(0));

    // Read after clear — page is NOT in dirty map, NOT in bitmap.
    // S3 fetch will fail (fake creds), so we get zeros.
    std::vector<uint8_t> buf(PAGE_SIZE, 0xFF);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0x00);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testClearCacheInvalidatesBitmap\n");
}

// --- Test: getFileSize is consistent ---

static void testFileSizeConsistency() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // New file: size 0.
    assert(fi->getFileSize() == 0);

    // Write page 0.
    std::vector<uint8_t> page(PAGE_SIZE, 0x01);
    fi->writeFile(page.data(), PAGE_SIZE, 0);
    assert(fi->getFileSize() == PAGE_SIZE);

    // Write page 5 (sparse — pages 1-4 never written).
    fi->writeFile(page.data(), PAGE_SIZE, 5 * PAGE_SIZE);
    assert(fi->getFileSize() == 6 * PAGE_SIZE);

    // Truncate to 3 pages.
    fi->truncate(3 * PAGE_SIZE);
    assert(fi->getFileSize() == 3 * PAGE_SIZE);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testFileSizeConsistency\n");
}

// --- Test: second open reuses bitmap and local file from first session ---

static void testReopenPreservesLocalData() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);

    // Session 1: write + sync.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        std::vector<uint8_t> page(PAGE_SIZE, 0x77);
        fi->writeFile(page.data(), PAGE_SIZE, 0);
        fi->syncFile();
    }

    // Session 2: open same VFS, read back.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        auto& ti = fi->cast<TieredFileInfo>();

        // Bitmap should be loaded from disk.
        assert(ti.bitmap->isPresent(0));

        // Data should be readable from local file.
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, 0);
        for (auto b : buf) assert(b == 0x77);
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testReopenPreservesLocalData\n");
}

// --- Test: evictLocalGroup clears bitmap and group state ---

static void testEvictLocalGroup() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    auto& ti = fi->cast<TieredFileInfo>();

    // Write 8 pages (2 page groups of 4 pages each).
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 1));
        fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
    }
    fi->syncFile();

    // Both groups' pages should be in bitmap.
    for (int i = 0; i < 8; i++) {
        assert(ti.bitmap->isPresent(i));
    }

    // Evict page group 0 (pages 0-3).
    vfs.evictLocalGroup(0);

    // Pages 0-3 should no longer be present in bitmap.
    for (int i = 0; i < 4; i++) {
        assert(!ti.bitmap->isPresent(i));
    }
    // Pages 4-7 (group 1) should still be present.
    for (int i = 4; i < 8; i++) {
        assert(ti.bitmap->isPresent(i));
    }

    // Reading evicted pages should return zeros (S3 is fake, fetch fails).
    for (int i = 0; i < 4; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0xFF);
        fi->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        for (auto b : buf) assert(b == 0x00);
    }

    // Reading non-evicted pages should still return original data.
    for (int i = 4; i < 8; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(i + 1));
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEvictLocalGroup\n");
}

int main() {
    std::printf("=== VFS Unit Tests ===\n");
    testWriteReadDirty();
    testWriteSyncRead();
    testPartialPageWrite();
    testCrossPageRead();
    testCrossPageWrite();
    testTruncate();
    testSyncClearsDirtyPages();
    testMultiPageWriteOffset();
    testBitmapStateAfterSync();
    testClearCacheInvalidatesBitmap();
    testFileSizeConsistency();
    testReopenPreservesLocalData();
    testEvictLocalGroup();
    std::printf("All VFS unit tests passed.\n");
    return 0;
}
