// Cold-open tests: reproduce the "table 0 doesn't exist" benchmark crash.
//
// The bug: creating a fresh TieredFileSystem with an empty cache dir against
// data that was previously synced to S3, then reading pages back. This is
// exactly what a cold benchmark does.
//
// These tests progressively narrow down where the failure occurs:
// 1. Basic cold restart (write, destroy TFS, fresh TFS, read) -- small data
// 2. Cold restart with large page groups (pagesPerGroup=2048)
// 3. Cold restart with seekable frames enabled
// 4. Cold restart with different cache dir (simulating benchmark's temp dirs)
// 5. Cold restart reading page 0 first (simulating Database header read)
// 6. Cold restart with sequential reads (simulating BufferedFileReader)
// 7. Multiple cold restarts in a row (simulating benchmark iterations)
// 8. Cold restart with getFileSize check (simulating FileHandle construction)
//
// Requires Tigris credentials in environment.

#include "tiered_file_system.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <numeric>

using namespace lbug::tiered;
using namespace lbug::common;

static constexpr uint32_t PAGE_SIZE = 4096;

static S3Config s3ConfigFromEnv() {
    auto ak = std::getenv("TIGRIS_STORAGE_ACCESS_KEY_ID");
    auto sk = std::getenv("TIGRIS_STORAGE_SECRET_ACCESS_KEY");
    auto ep = std::getenv("TIGRIS_STORAGE_ENDPOINT");
    if (!ak || !sk || !ep) {
        std::printf("SKIP: Tigris credentials not set\n");
        std::exit(0);
    }
    return S3Config{ep, "cinch-data", "test/cold-open", "auto", ak, sk};
}

static std::filesystem::path tmpDir(const char* suffix = "") {
    auto dir = std::filesystem::temp_directory_path() /
        (std::string("cold_open_test") + suffix);
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

static TieredConfig makeConfig(const std::filesystem::path& dir, uint32_t ppg = 4) {
    TieredConfig cfg;
    cfg.s3 = s3ConfigFromEnv();
    cfg.cacheDir = (dir / "cache").string();
    cfg.dataFilePath = (dir / "data.kz").string();
    cfg.pageSize = PAGE_SIZE;
    cfg.pagesPerGroup = ppg;
    cfg.compressionLevel = 3;
    cfg.subPagesPerFrame = 4;
    return cfg;
}

static void cleanupS3(const TieredConfig& cfg) {
    S3Client s3(cfg.s3);
    s3.deleteObject(cfg.s3.prefix + "/manifest.json");
    auto keys = s3.listObjects(cfg.s3.prefix + "/pg/");
    for (auto& k : keys) s3.deleteObject(k);
}

// Fill page with a recognizable pattern: byte 0 = marker, rest = pageIdx.
static std::vector<uint8_t> makePage(uint8_t marker, uint32_t pageIdx) {
    std::vector<uint8_t> page(PAGE_SIZE, 0);
    page[0] = marker;
    page[1] = static_cast<uint8_t>(pageIdx & 0xFF);
    page[2] = static_cast<uint8_t>((pageIdx >> 8) & 0xFF);
    return page;
}

static void verifyPage(const uint8_t* buf, uint8_t marker, uint32_t pageIdx) {
    if (buf[0] != marker || buf[1] != static_cast<uint8_t>(pageIdx & 0xFF)) {
        std::fprintf(stderr, "    VERIFY FAIL: page %u: got [%02x %02x %02x], expected [%02x %02x %02x]\n",
            pageIdx, buf[0], buf[1], buf[2],
            marker, static_cast<uint8_t>(pageIdx & 0xFF),
            static_cast<uint8_t>((pageIdx >> 8) & 0xFF));
    }
    assert(buf[0] == marker);
    assert(buf[1] == static_cast<uint8_t>(pageIdx & 0xFF));
    assert(buf[2] == static_cast<uint8_t>((pageIdx >> 8) & 0xFF));
}

// --- Test 1: Basic cold restart, small data (4 pages, ppg=4) ---
static void testBasicColdRestart() {
    auto dir = tmpDir("_basic");
    auto cfg = makeConfig(dir, 4);
    cleanupS3(cfg);

    // Session 1: write 4 pages + sync.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        for (uint32_t i = 0; i < 4; i++) {
            auto page = makePage(0xA0, i);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    // Wipe ALL local state.
    std::filesystem::remove_all(cfg.cacheDir);

    // Session 2: fresh VFS, read all pages from S3.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        vfs.s3().resetCounters();

        for (uint32_t i = 0; i < 4; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0);
            fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
            verifyPage(buf.data(), 0xA0, i);
        }
        assert(vfs.s3().fetchCount.load() > 0);
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testBasicColdRestart\n");
}

// --- Test 2: Cold restart with large page groups (ppg=2048) ---
// This matches the benchmark config. Write enough pages to span 1+ groups.
static void testColdRestartLargeGroups() {
    auto dir = tmpDir("_large");
    auto cfg = makeConfig(dir, 2048);
    cleanupS3(cfg);

    uint32_t numPages = 100; // ~400KB, all in one page group.

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        for (uint32_t i = 0; i < numPages; i++) {
            auto page = makePage(0xB0, i);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    std::filesystem::remove_all(cfg.cacheDir);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        vfs.s3().resetCounters();

        for (uint32_t i = 0; i < numPages; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0xFF);
            fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
            if (buf[0] != 0xB0) {
                std::fprintf(stderr, "    Page %u: first bytes = [%02x %02x %02x %02x], all-zero=%s\n",
                    i, buf[0], buf[1], buf[2], buf[3],
                    std::all_of(buf.begin(), buf.end(), [](uint8_t b){ return b == 0; }) ? "YES" : "no");
            }
            verifyPage(buf.data(), 0xB0, i);
        }
        assert(vfs.s3().fetchCount.load() > 0);
        std::printf("    Large groups: %u pages, %llu S3 fetches\n",
            numPages, (unsigned long long)vfs.s3().fetchCount.load());
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testColdRestartLargeGroups\n");
}

// --- Test 3: Cold restart with different cache dir ---
// This is what the benchmark does: session 1 uses cacheDir A, session 2 uses cacheDir B.
static void testColdRestartDifferentCacheDir() {
    auto dir1 = tmpDir("_dir1");
    auto dir2 = tmpDir("_dir2");
    auto cfg1 = makeConfig(dir1, 4);
    auto cfg2 = cfg1;
    cfg2.cacheDir = (dir2 / "cache").string();
    cleanupS3(cfg1);

    // Session 1: write + sync.
    {
        TieredFileSystem vfs(cfg1);
        auto fi = vfs.openFile(cfg1.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        for (uint32_t i = 0; i < 8; i++) {
            auto page = makePage(0xC0, i);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    // Session 2: DIFFERENT cache dir, same S3 prefix. No local data at all.
    {
        TieredFileSystem vfs(cfg2);
        auto fi = vfs.openFile(cfg2.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        vfs.s3().resetCounters();

        for (uint32_t i = 0; i < 8; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0);
            fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
            verifyPage(buf.data(), 0xC0, i);
        }
        assert(vfs.s3().fetchCount.load() > 0);
    }

    cleanupS3(cfg1);
    std::filesystem::remove_all(dir1);
    std::filesystem::remove_all(dir2);
    std::printf("  PASS: testColdRestartDifferentCacheDir\n");
}

// --- Test 4: Cold restart reading page 0 first ---
// Kuzu reads page 0 (database header) first during Database construction.
static void testColdRestartPage0First() {
    auto dir = tmpDir("_page0");
    auto cfg = makeConfig(dir, 2048);
    cleanupS3(cfg);

    uint32_t numPages = 50;

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        for (uint32_t i = 0; i < numPages; i++) {
            auto page = makePage(0xD0, i);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    std::filesystem::remove_all(cfg.cacheDir);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        vfs.s3().resetCounters();

        // Read page 0 specifically (this is what Kuzu does first).
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, 0);
        verifyPage(buf.data(), 0xD0, 0);

        // Then read some pages deeper in the file (catalog/metadata pages).
        fi->readFromFile(buf.data(), PAGE_SIZE, 10 * PAGE_SIZE);
        verifyPage(buf.data(), 0xD0, 10);

        fi->readFromFile(buf.data(), PAGE_SIZE, 30 * PAGE_SIZE);
        verifyPage(buf.data(), 0xD0, 30);

        assert(vfs.s3().fetchCount.load() > 0);
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testColdRestartPage0First\n");
}

// --- Test 5: Cold restart with sequential byte reads ---
// Kuzu's BufferedFileReader reads in byte-level chunks, not page-aligned.
// Test that sub-page and cross-page reads work on a cold VFS.
static void testColdRestartSequentialByteReads() {
    auto dir = tmpDir("_seqread");
    auto cfg = makeConfig(dir, 2048);
    cleanupS3(cfg);

    uint32_t numPages = 10;

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        for (uint32_t i = 0; i < numPages; i++) {
            auto page = makePage(0xE0, i);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    std::filesystem::remove_all(cfg.cacheDir);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        // Read first 3 bytes of page 0 (sub-page read).
        uint8_t small[3] = {};
        fi->readFromFile(small, 3, 0);
        assert(small[0] == 0xE0);
        assert(small[1] == 0x00);
        assert(small[2] == 0x00);

        // Read 8KB crossing page boundary (pages 0-1).
        std::vector<uint8_t> cross(PAGE_SIZE * 2, 0);
        fi->readFromFile(cross.data(), PAGE_SIZE * 2, 0);
        assert(cross[0] == 0xE0);                // Page 0 marker.
        assert(cross[PAGE_SIZE] == 0xE0);         // Page 1 marker.
        assert(cross[PAGE_SIZE + 1] == 0x01);     // Page 1 index low byte.

        // Read from middle of page 5.
        uint8_t mid[4] = {};
        fi->readFromFile(mid, 4, 5 * PAGE_SIZE + 100);
        // Bytes 100-103 of page 5 should be 0 (makePage only writes bytes 0-2).
        assert(mid[0] == 0);
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testColdRestartSequentialByteReads\n");
}

// --- Test 6: getFileSize on cold VFS returns correct value ---
// FileHandle construction calls getFileSize to compute numPages.
static void testColdRestartFileSize() {
    auto dir = tmpDir("_filesize");
    auto cfg = makeConfig(dir, 2048);
    cleanupS3(cfg);

    uint32_t numPages = 50;

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        for (uint32_t i = 0; i < numPages; i++) {
            auto page = makePage(0xF0, i);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    std::filesystem::remove_all(cfg.cacheDir);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        auto size = fi->getFileSize();
        assert(size == numPages * PAGE_SIZE);
        std::printf("    Cold file size: %llu (expected %u)\n",
            (unsigned long long)size, numPages * PAGE_SIZE);
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testColdRestartFileSize\n");
}

// --- Test 7: Multiple cold restarts in a row ---
// Simulates the benchmark running cold queries repeatedly.
static void testMultipleColdRestarts() {
    auto dir = tmpDir("_multi");
    auto cfg = makeConfig(dir, 4);
    cleanupS3(cfg);

    // Initial write.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        for (uint32_t i = 0; i < 8; i++) {
            auto page = makePage(0x10, i);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    // 5 cold restarts in a row, each with a fresh cache dir.
    for (int restart = 0; restart < 5; restart++) {
        auto coldDir = tmpDir(("_multi_cold_" + std::to_string(restart)).c_str());
        auto coldCfg = cfg;
        coldCfg.cacheDir = (coldDir / "cache").string();

        TieredFileSystem vfs(coldCfg);
        auto fi = vfs.openFile(coldCfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        vfs.s3().resetCounters();

        for (uint32_t i = 0; i < 8; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0);
            fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
            verifyPage(buf.data(), 0x10, i);
        }

        assert(vfs.s3().fetchCount.load() > 0);
        std::filesystem::remove_all(coldDir);
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testMultipleColdRestarts\n");
}

// --- Test 8: fileOrPathExists on cold VFS ---
// Database constructor calls fileOrPathExists before opening.
static void testColdFileOrPathExists() {
    auto dir = tmpDir("_exists");
    auto cfg = makeConfig(dir, 4);
    cleanupS3(cfg);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        auto page = makePage(0x20, 0);
        fi->writeFile(page.data(), PAGE_SIZE, 0);
        fi->syncFile();
    }

    std::filesystem::remove_all(cfg.cacheDir);

    {
        TieredFileSystem vfs(cfg);
        // Before openFile, check if the file exists (checks S3 manifest).
        bool exists = vfs.fileOrPathExists(cfg.dataFilePath);
        assert(exists);

        // Also verify a non-existent path returns false.
        bool notExists = vfs.fileOrPathExists("/nonexistent/path.kz");
        assert(!notExists);
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testColdFileOrPathExists\n");
}

// --- Test 9: Cold restart with seekable frames ---
// Verify seekable encoded data survives a cold restart.
static void testColdRestartSeekable() {
    auto dir = tmpDir("_seekable");
    auto cfg = makeConfig(dir, 8);
    cfg.subPagesPerFrame = 2;
    cleanupS3(cfg);

    uint32_t numPages = 8;

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        for (uint32_t i = 0; i < numPages; i++) {
            auto page = makePage(0x30, i);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    std::filesystem::remove_all(cfg.cacheDir);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        vfs.s3().resetCounters();

        for (uint32_t i = 0; i < numPages; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0);
            fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
            verifyPage(buf.data(), 0x30, i);
        }
        assert(vfs.s3().fetchCount.load() > 0);
        std::printf("    Seekable cold: %u pages, %llu S3 fetches\n",
            numPages, (unsigned long long)vfs.s3().fetchCount.load());
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testColdRestartSeekable\n");
}

// --- Test 10: Cold restart reads interleaved with getFileSize ---
// Simulates Kuzu's FileHandle: getFileSize, then read pages.
static void testColdRestartSizeAndRead() {
    auto dir = tmpDir("_sizeread");
    auto cfg = makeConfig(dir, 2048);
    cleanupS3(cfg);

    uint32_t numPages = 30;

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        for (uint32_t i = 0; i < numPages; i++) {
            auto page = makePage(0x40, i);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    // Fully cold: different cache dir.
    auto coldDir = tmpDir("_sizeread_cold");
    auto coldCfg = cfg;
    coldCfg.cacheDir = (coldDir / "cache").string();

    {
        TieredFileSystem vfs(coldCfg);
        auto fi = vfs.openFile(coldCfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        // Step 1: getFileSize (Kuzu does this in FileHandle constructor).
        auto size = fi->getFileSize();
        assert(size == numPages * PAGE_SIZE);

        // Step 2: read page 0 (header).
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, 0);
        verifyPage(buf.data(), 0x40, 0);

        // Step 3: read a page deeper in the file.
        fi->readFromFile(buf.data(), PAGE_SIZE, 20 * PAGE_SIZE);
        verifyPage(buf.data(), 0x40, 20);
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::filesystem::remove_all(coldDir);
    std::printf("  PASS: testColdRestartSizeAndRead\n");
}

int main() {
    std::printf("=== Cold-Open Tests ===\n");

    testBasicColdRestart();
    testColdRestartLargeGroups();
    testColdRestartDifferentCacheDir();
    testColdRestartPage0First();
    testColdRestartSequentialByteReads();
    testColdRestartFileSize();
    testMultipleColdRestarts();
    testColdFileOrPathExists();
    testColdRestartSeekable();
    testColdRestartSizeAndRead();

    std::printf("  All 10 cold-open tests passed.\n");
    return 0;
}
