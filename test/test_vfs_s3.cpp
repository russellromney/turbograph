// VFS integration tests — exercise TieredFileSystem end-to-end with real S3 (Tigris).
//
// Tests the immutable page group model: each sync produces new versioned S3 keys.
// Old keys are never overwritten — only cleaned up by explicit eviction.
//
// Requires Tigris credentials in environment:
//   TIGRIS_STORAGE_ACCESS_KEY_ID, TIGRIS_STORAGE_SECRET_ACCESS_KEY, TIGRIS_STORAGE_ENDPOINT
//
// Uses small pagesPerGroup=4 so we fill page groups without writing 2048 pages.

#include "tiered_file_system.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>

using namespace lbug::tiered;
using namespace lbug::common;

static constexpr uint32_t PAGE_SIZE = 4096;
static constexpr uint32_t PAGES_PER_GROUP = 4;

static S3Config s3ConfigFromEnv() {
    auto ak = std::getenv("TIGRIS_STORAGE_ACCESS_KEY_ID");
    auto sk = std::getenv("TIGRIS_STORAGE_SECRET_ACCESS_KEY");
    auto ep = std::getenv("TIGRIS_STORAGE_ENDPOINT");
    if (!ak || !sk || !ep) {
        std::printf("SKIP: Tigris credentials not set\n");
        std::exit(0);
    }
    return S3Config{ep, "cinch-data", "test/vfs-integration", "auto", ak, sk};
}

static std::filesystem::path tmpDir() {
    auto dir = std::filesystem::temp_directory_path() / "tiered_vfs_s3_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

static TieredConfig makeConfig(const std::filesystem::path& dir) {
    TieredConfig cfg;
    cfg.s3 = s3ConfigFromEnv();
    cfg.cacheDir = (dir / "cache").string();
    cfg.dataFilePath = (dir / "data.kz").string();
    cfg.pageSize = PAGE_SIZE;
    cfg.pagesPerGroup = PAGES_PER_GROUP;
    cfg.compressionLevel = 3;
    return cfg;
}

// Clean up ALL S3 objects under the test prefix using LIST.
static void cleanupS3(const TieredConfig& cfg) {
    S3Client s3(cfg.s3);
    s3.deleteObject(cfg.s3.prefix + "/manifest.json");
    // Delete all page group objects (immutable keys have unpredictable names).
    auto pgKeys = s3.listObjects(cfg.s3.prefix + "/pg/");
    for (auto& key : pgKeys) {
        s3.deleteObject(key);
    }
}

// --- Test: write -> sync -> clear cache -> read from S3 (cold restart) ---

static void testColdReadFromS3() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        for (int i = 0; i < 4; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0x10 + i));
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();

        // Verify warm read.
        for (int i = 0; i < 4; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0);
            fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
            for (auto b : buf) assert(b == static_cast<uint8_t>(0x10 + i));
        }

        // Clear cache -> cold read from S3.
        vfs.clearCache();
        vfs.s3().resetCounters();

        for (int i = 0; i < 4; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0);
            fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
            for (auto b : buf) assert(b == static_cast<uint8_t>(0x10 + i));
        }

        assert(vfs.s3().fetchCount.load() > 0);
        std::printf("    S3 fetches after cold read: %llu (%lluKB)\n",
            (unsigned long long)vfs.s3().fetchCount.load(),
            (unsigned long long)(vfs.s3().fetchBytes.load() / 1024));
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testColdReadFromS3\n");
}

// --- Test: warm reads produce zero S3 fetches ---

static void testWarmReadNoS3() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    std::vector<uint8_t> page(PAGE_SIZE, 0xAA);
    fi->writeFile(page.data(), PAGE_SIZE, 0);
    fi->syncFile();

    vfs.s3().resetCounters();

    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xAA);

    assert(vfs.s3().fetchCount.load() == 0);

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWarmReadNoS3\n");
}

// --- Test: page group prefetch brings in neighbors ---

static void testPageGroupPrefetch() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    auto& ti = fi->cast<TieredFileInfo>();

    for (int i = 0; i < 4; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 1));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    vfs.clearCache();
    assert(!ti.bitmap->isPresent(0));

    // Read only page 2 -> should fetch entire page group.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 2 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x03);

    assert(ti.bitmap->isPresent(0));
    assert(ti.bitmap->isPresent(1));
    assert(ti.bitmap->isPresent(2));
    assert(ti.bitmap->isPresent(3));

    // Other pages should come from local file now.
    vfs.s3().resetCounters();
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0x01);
    fi->readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    for (auto b : buf) assert(b == 0x02);
    fi->readFromFile(buf.data(), PAGE_SIZE, 3 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x04);
    assert(vfs.s3().fetchCount.load() == 0);

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testPageGroupPrefetch\n");
}

// --- Test: cold restart — new VFS instance reads from S3 ---

static void testColdRestartNewInstance() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    // Session 1: Write + sync.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        for (int i = 0; i < 4; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0x50 + i));
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    // Wipe ALL local state. Only S3 remains.
    std::filesystem::remove_all(cfg.cacheDir);

    // Session 2: Fresh VFS, all reads from S3.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        vfs.s3().resetCounters();

        for (int i = 0; i < 4; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0);
            fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
            for (auto b : buf) assert(b == static_cast<uint8_t>(0x50 + i));
        }

        assert(vfs.s3().fetchCount.load() > 0);
        std::printf("    S3 fetches on cold restart: %llu (%lluKB)\n",
            (unsigned long long)vfs.s3().fetchCount.load(),
            (unsigned long long)(vfs.s3().fetchBytes.load() / 1024));
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testColdRestartNewInstance\n");
}

// --- Test: multiple page groups ---

static void testMultiplePageGroups() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    for (int i = 0; i < 12; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    vfs.clearCache();
    vfs.s3().resetCounters();

    for (int i = 0; i < 12; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0xFF);
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(i));
    }

    assert(vfs.s3().fetchCount.load() > 0);
    std::printf("    S3 fetches for 3 page groups: %llu (%lluKB)\n",
        (unsigned long long)vfs.s3().fetchCount.load(),
        (unsigned long long)(vfs.s3().fetchBytes.load() / 1024));

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testMultiplePageGroups\n");
}

// --- Test: S3 counter accuracy ---

static void testS3CounterAccuracy() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    vfs.clearCache();
    vfs.s3().resetCounters();

    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);

    auto count1 = vfs.s3().fetchCount.load();
    auto bytes1 = vfs.s3().fetchBytes.load();
    assert(count1 > 0);
    assert(bytes1 > 0);
    std::printf("    After reading pg0: fetches=%llu bytes=%lluKB\n",
        (unsigned long long)count1, (unsigned long long)(bytes1 / 1024));

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testS3CounterAccuracy\n");
}

// --- Test: overwrite synced page then re-read ---

static void testOverwriteSyncedPage() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    std::vector<uint8_t> page1(PAGE_SIZE, 0xAA);
    fi->writeFile(page1.data(), PAGE_SIZE, 0);
    fi->syncFile();

    std::vector<uint8_t> page2(PAGE_SIZE, 0xBB);
    fi->writeFile(page2.data(), PAGE_SIZE, 0);
    fi->syncFile();

    // Clear and cold read -> should get 0xBB (from v2 page group).
    vfs.clearCache();
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xBB);

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testOverwriteSyncedPage\n");
}

// --- Test: immutable page group keys (core safety property) ---
// Overwriting pages produces a NEW S3 key. Old key is NOT deleted.

static void testImmutablePageGroupKeys() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    S3Client s3(cfg.s3);

    // Session 1: Write pages, sync.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        for (int i = 0; i < 4; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, 0xAA);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    // Verify: manifest v1 exists, has one page group key.
    auto manifest1 = s3.getManifest();
    assert(manifest1.has_value());
    assert(manifest1->version == 1);
    assert(!manifest1->pageGroupKeys.empty());
    auto key_v1 = manifest1->pageGroupKeys[0];
    assert(key_v1.find("_v1") != std::string::npos);
    assert(s3.getObject(key_v1).has_value());

    // Session 2: Overwrite same pages, sync again.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        for (int i = 0; i < 4; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, 0xBB);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    // Verify: manifest v2, new key, AND old key still exists (not overwritten).
    auto manifest2 = s3.getManifest();
    assert(manifest2.has_value());
    assert(manifest2->version == 2);
    auto key_v2 = manifest2->pageGroupKeys[0];
    assert(key_v2.find("_v2") != std::string::npos);
    assert(key_v2 != key_v1);
    assert(s3.getObject(key_v2).has_value());
    assert(s3.getObject(key_v1).has_value()); // Old key still there!

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testImmutablePageGroupKeys\n");
}

// --- Test: eviction cleans up orphaned page groups ---

static void testEvictionCleansOrphans() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    S3Client s3(cfg.s3);

    // Write + sync (creates v1 keys).
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    for (int i = 0; i < 4; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, 0xAA);
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Overwrite + sync (creates v2 keys, v1 keys become orphans).
    for (int i = 0; i < 4; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, 0xBB);
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Before eviction: both v1 and v2 keys exist.
    auto allKeys = s3.listObjects(cfg.s3.prefix + "/pg/");
    assert(allKeys.size() >= 2);

    // Evict stale.
    auto deleted = vfs.evictStalePageGroups();
    assert(deleted > 0);

    // After eviction: only current manifest's keys remain.
    auto remainingKeys = s3.listObjects(cfg.s3.prefix + "/pg/");
    auto manifest = s3.getManifest();
    assert(manifest.has_value());
    for (auto& key : remainingKeys) {
        bool found = false;
        for (auto& validKey : manifest->pageGroupKeys) {
            if (key == validKey) { found = true; break; }
        }
        assert(found);
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEvictionCleansOrphans\n");
}

// --- Test: cold read data integrity across multiple page groups ---
// Write 3 page groups worth of data (12 pages), wipe local, read all back.

static void testColdFetchDataIntegrity() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    const int TOTAL_PAGES = PAGES_PER_GROUP * 3; // 12 pages

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        for (int i = 0; i < TOTAL_PAGES; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i & 0xFF));
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    std::filesystem::remove_all(cfg.cacheDir);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        vfs.s3().resetCounters();

        for (int i = 0; i < TOTAL_PAGES; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0xFF);
            fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
            uint8_t expected = static_cast<uint8_t>(i & 0xFF);
            for (auto b : buf) {
                assert(b == expected);
            }
        }

        std::printf("    Cold read %d pages: %llu S3 fetches (%lluKB)\n",
            TOTAL_PAGES,
            (unsigned long long)vfs.s3().fetchCount.load(),
            (unsigned long long)(vfs.s3().fetchBytes.load() / 1024));
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testColdFetchDataIntegrity\n");
}

// --- Test: cold restart — wipe all local state, read from S3 ---

static void testColdRestart() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        for (int i = 0; i < 16; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0x30 + i));
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }
        fi->syncFile();
    }

    std::filesystem::remove_all(cfg.cacheDir);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        vfs.s3().resetCounters();

        for (int i = 0; i < 16; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0);
            fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
            for (auto b : buf) assert(b == static_cast<uint8_t>(0x30 + i));
        }

        std::printf("    Cold restart: %llu fetches (%lluKB)\n",
            (unsigned long long)vfs.s3().fetchCount.load(),
            (unsigned long long)(vfs.s3().fetchBytes.load() / 1024));
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testColdRestart\n");
}

// --- Test: evict local group, cold read from S3 verifies data integrity ---

static void testEvictAndColdReadFromS3() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    auto& ti = fi->cast<TieredFileInfo>();

    // Write 8 pages (2 page groups of 4 pages).
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0xA0 + i));
        fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
    }
    fi->syncFile();

    // Verify all pages are warm.
    for (int i = 0; i < 8; i++) {
        assert(ti.bitmap->isPresent(i));
    }

    // Evict only page group 0 (pages 0-3).
    vfs.evictLocalGroup(0);

    // Pages 0-3 evicted, pages 4-7 still warm.
    for (int i = 0; i < 4; i++) assert(!ti.bitmap->isPresent(i));
    for (int i = 4; i < 8; i++) assert(ti.bitmap->isPresent(i));

    vfs.s3().resetCounters();

    // Read evicted pages — should fetch from S3 with correct data.
    for (int i = 0; i < 4; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(0xA0 + i));
    }

    // Should have triggered at least one S3 fetch.
    assert(vfs.s3().fetchCount.load() > 0);
    std::printf("    S3 fetches after evict+read: %llu\n",
        (unsigned long long)vfs.s3().fetchCount.load());

    // Read warm pages (group 1) — should NOT trigger additional S3 fetches.
    auto fetchesBefore = vfs.s3().fetchCount.load();
    for (int i = 4; i < 8; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(0xA0 + i));
    }
    assert(vfs.s3().fetchCount.load() == fetchesBefore);

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEvictAndColdReadFromS3\n");
}

// --- Test: evict group → write one page → sync → cold read preserves all pages ---
// Regression test: flushPendingPageGroups must merge with existing S3 data when
// the local bitmap is incomplete (e.g. after eviction). Without the merge, the
// sync would upload only the locally-present pages, silently dropping the rest.

static void testEvictWriteSyncColdRead() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    auto& ti = fi->cast<TieredFileInfo>();

    // Write 8 pages (2 page groups) with distinct data.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0xC0 + i));
        fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
    }
    fi->syncFile();

    // All 8 pages synced to S3 and locally cached.
    for (int i = 0; i < 8; i++) assert(ti.bitmap->isPresent(i));

    // Evict page group 0 (pages 0-3). Bitmap cleared, local data gone.
    vfs.evictLocalGroup(0);
    for (int i = 0; i < 4; i++) assert(!ti.bitmap->isPresent(i));
    for (int i = 4; i < 8; i++) assert(ti.bitmap->isPresent(i));

    // Write ONLY page 1 in the evicted group. Pages 0, 2, 3 are NOT in bitmap.
    // (writeFile marks dirty but doesn't set bitmap — that happens in syncFile.)
    std::vector<uint8_t> newPage1(PAGE_SIZE, 0xFF);
    fi->writeFile(newPage1.data(), PAGE_SIZE, 1 * PAGE_SIZE);

    // Sync. This triggers flushPendingPageGroups for group 0 with only page 1
    // in the bitmap. The fix must fetch the old S3 page group and merge pages
    // 0, 2, 3 from S3 with page 1 from local.
    fi->syncFile();

    // Wipe ALL local state. Only S3 remains.
    std::filesystem::remove_all(cfg.cacheDir);

    // Fresh VFS, all reads from S3.
    TieredFileSystem vfs2(cfg);
    auto fi2 = vfs2.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Page 0: original 0xC0 (merged from old S3 group).
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi2->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xC0);

    // Page 1: overwritten 0xFF (from local write).
    fi2->readFromFile(buf.data(), PAGE_SIZE, 1 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xFF);

    // Page 2: original 0xC2 (merged from old S3 group).
    fi2->readFromFile(buf.data(), PAGE_SIZE, 2 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xC2);

    // Page 3: original 0xC3 (merged from old S3 group).
    fi2->readFromFile(buf.data(), PAGE_SIZE, 3 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xC3);

    // Pages 4-7: original data (group 1, untouched).
    for (int i = 4; i < 8; i++) {
        fi2->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(0xC0 + i));
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEvictWriteSyncColdRead\n");
}

// --- Test: clearCache → write one page → sync → cold read preserves all pages ---
// Same bug as evict variant, but via clearCache (clears ALL groups at once).
// This is the exact pattern the benchmark uses: clearCache() then run a query
// that touches some pages, causing Kuzu buffer pool to flush dirty pages.

static void testClearCacheWriteSyncColdRead() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 4 pages and sync.
    for (int i = 0; i < 4; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0xD0 + i));
        fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
    }
    fi->syncFile();

    // Clear all cached pages.
    vfs.clearCache();

    // Write ONLY page 2. Pages 0, 1, 3 are not in bitmap.
    std::vector<uint8_t> newPage2(PAGE_SIZE, 0xEE);
    fi->writeFile(newPage2.data(), PAGE_SIZE, 2 * PAGE_SIZE);
    fi->syncFile();

    // Wipe local state, cold read from S3.
    std::filesystem::remove_all(cfg.cacheDir);

    TieredFileSystem vfs2(cfg);
    auto fi2 = vfs2.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    std::vector<uint8_t> buf(PAGE_SIZE, 0);

    fi2->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xD0); // Preserved from S3 merge.

    fi2->readFromFile(buf.data(), PAGE_SIZE, 1 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xD1); // Preserved from S3 merge.

    fi2->readFromFile(buf.data(), PAGE_SIZE, 2 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xEE); // Overwritten locally.

    fi2->readFromFile(buf.data(), PAGE_SIZE, 3 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xD3); // Preserved from S3 merge.

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testClearCacheWriteSyncColdRead\n");
}

// --- Test: multiple groups with partial eviction ---
// Evict group 1 (middle), write to group 0 and group 2. Sync. Verify group 1
// data survives (it was on S3, not dirty, shouldn't be in pending set at all).

static void testPartialEvictionMultipleGroups() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 12 pages (3 page groups).
    for (int i = 0; i < 12; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i));
        fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
    }
    fi->syncFile();

    // Evict only group 1 (pages 4-7).
    vfs.evictLocalGroup(1);

    // Write to page 0 (group 0) and page 8 (group 2). Don't touch group 1.
    std::vector<uint8_t> newP0(PAGE_SIZE, 0xAA);
    fi->writeFile(newP0.data(), PAGE_SIZE, 0);
    std::vector<uint8_t> newP8(PAGE_SIZE, 0xBB);
    fi->writeFile(newP8.data(), PAGE_SIZE, 8 * PAGE_SIZE);
    fi->syncFile();

    // Cold restart.
    std::filesystem::remove_all(cfg.cacheDir);

    TieredFileSystem vfs2(cfg);
    auto fi2 = vfs2.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    std::vector<uint8_t> buf(PAGE_SIZE, 0);

    // Group 0: page 0 overwritten, pages 1-3 original.
    fi2->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xAA);
    for (int i = 1; i < 4; i++) {
        fi2->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(i));
    }

    // Group 1: all original (evicted but not dirty, so never in pending set).
    for (int i = 4; i < 8; i++) {
        fi2->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(i));
    }

    // Group 2: page 8 overwritten, pages 9-11 original.
    fi2->readFromFile(buf.data(), PAGE_SIZE, 8 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xBB);
    for (int i = 9; i < 12; i++) {
        fi2->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(i));
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testPartialEvictionMultipleGroups\n");
}

// --- Test: evict all → write one page → sync → cold read ---
// Extreme case: all groups evicted, single page written. All other pages must
// come from S3 merge.

static void testEvictAllWriteOneSyncColdRead() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 8 pages (2 groups).
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0x40 + i));
        fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
    }
    fi->syncFile();

    // Evict both groups.
    vfs.evictLocalGroup(0);
    vfs.evictLocalGroup(1);

    // Write only page 6 (group 1, index 2 within group).
    std::vector<uint8_t> newP6(PAGE_SIZE, 0x99);
    fi->writeFile(newP6.data(), PAGE_SIZE, 6 * PAGE_SIZE);
    fi->syncFile();

    // Cold restart.
    std::filesystem::remove_all(cfg.cacheDir);

    TieredFileSystem vfs2(cfg);
    auto fi2 = vfs2.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    std::vector<uint8_t> buf(PAGE_SIZE, 0);

    // Group 0: all original (evicted, not dirty → old S3 key preserved).
    for (int i = 0; i < 4; i++) {
        fi2->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(0x40 + i));
    }

    // Group 1: pages 4,5,7 original (merged from S3), page 6 overwritten.
    fi2->readFromFile(buf.data(), PAGE_SIZE, 4 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x44);
    fi2->readFromFile(buf.data(), PAGE_SIZE, 5 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x45);
    fi2->readFromFile(buf.data(), PAGE_SIZE, 6 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x99); // Overwritten.
    fi2->readFromFile(buf.data(), PAGE_SIZE, 7 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x47);

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEvictAllWriteOneSyncColdRead\n");
}

// --- Test: double sync after partial eviction is idempotent ---
// Evict, write, sync, then sync again with no new writes. Second sync should
// not corrupt data (pendingPageGroups should be empty).

static void testDoubleSyncAfterPartialEviction() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    for (int i = 0; i < 4; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0x70 + i));
        fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
    }
    fi->syncFile();

    vfs.evictLocalGroup(0);

    std::vector<uint8_t> newP0(PAGE_SIZE, 0xDD);
    fi->writeFile(newP0.data(), PAGE_SIZE, 0);
    fi->syncFile(); // First sync: merge with S3.
    fi->syncFile(); // Second sync: no dirty pages, should be no-op.

    // Cold restart.
    std::filesystem::remove_all(cfg.cacheDir);

    TieredFileSystem vfs2(cfg);
    auto fi2 = vfs2.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi2->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xDD);
    fi2->readFromFile(buf.data(), PAGE_SIZE, 1 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x71);
    fi2->readFromFile(buf.data(), PAGE_SIZE, 2 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x72);
    fi2->readFromFile(buf.data(), PAGE_SIZE, 3 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x73);

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testDoubleSyncAfterPartialEviction\n");
}

// --- Test: read triggers fetch, then sync doesn't corrupt ---
// Read from a cold group (fetch from S3 into local cache), then write to a
// different page in the same group, then sync. The fetched pages should be
// preserved.

static void testFetchThenWriteSyncPreservesData() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        for (int i = 0; i < 4; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0xA0 + i));
            fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        }
        fi->syncFile();
    }

    // New session with wiped cache (all cold).
    std::filesystem::remove_all(cfg.cacheDir);

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Read page 0 → fetches entire group from S3, populates local cache + bitmap.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xA0);

    // Now write page 3. All 4 pages should be in bitmap (from the fetch).
    std::vector<uint8_t> newP3(PAGE_SIZE, 0xCC);
    fi->writeFile(newP3.data(), PAGE_SIZE, 3 * PAGE_SIZE);
    fi->syncFile();

    // Cold restart again.
    std::filesystem::remove_all(cfg.cacheDir);

    TieredFileSystem vfs2(cfg);
    auto fi2 = vfs2.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    fi2->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xA0); // Fetched, not overwritten.
    fi2->readFromFile(buf.data(), PAGE_SIZE, 1 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xA1);
    fi2->readFromFile(buf.data(), PAGE_SIZE, 2 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xA2);
    fi2->readFromFile(buf.data(), PAGE_SIZE, 3 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xCC); // Overwritten.

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testFetchThenWriteSyncPreservesData\n");
}

// --- Test: clearCache persists empty bitmap to disk ---
// Regression: clearCache() must persist the cleared bitmap. Without this,
// a subsequent VFS instance loads the stale on-disk bitmap (from the initial
// checkpoint), sees all pages as present, and reads zeros from the truncated
// local file — silently corrupting the database.

static void testClearCachePersistsBitmap() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    // Session 1: Write all pages, sync (persists bitmap with all pages present).
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        for (int i = 0; i < 8; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0xB0 + i));
            fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        }
        fi->syncFile(); // Persists bitmap: all 8 pages present.
    }

    // Session 2: Open, clearCache, destroy. This simulates one cold benchmark query.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        auto& ti = fi->cast<TieredFileInfo>();

        // Bitmap loaded from disk: all 8 pages present.
        for (int i = 0; i < 8; i++) assert(ti.bitmap->isPresent(i));

        vfs.clearCache();

        // In-memory bitmap cleared.
        for (int i = 0; i < 8; i++) assert(!ti.bitmap->isPresent(i));
    }

    // Session 3: Fresh VFS. Bitmap should be empty on disk (clearCache persisted it).
    // All reads must come from S3.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        auto& ti = fi->cast<TieredFileInfo>();

        // Critical assertion: bitmap loaded from disk must be EMPTY.
        for (int i = 0; i < 8; i++) assert(!ti.bitmap->isPresent(i));

        vfs.s3().resetCounters();

        // Read all pages — must come from S3.
        for (int i = 0; i < 8; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0);
            fi->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
            for (auto b : buf) assert(b == static_cast<uint8_t>(0xB0 + i));
        }

        assert(vfs.s3().fetchCount.load() > 0);
        std::printf("    S3 fetches after stale bitmap fix: %llu\n",
            (unsigned long long)vfs.s3().fetchCount.load());
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testClearCachePersistsBitmap\n");
}

// --- Test: sequential cold queries don't corrupt each other ---
// Simulates the benchmark pattern: repeated open → clearCache → query → close.
// Each iteration must read correct data from S3, not stale zeros from the
// previous iteration's truncated local file.

static void testSequentialColdQueriesNoCorruption() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    cleanupS3(cfg);

    // Initial data load.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        for (int i = 0; i < 8; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0x50 + i));
            fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
        }
        fi->syncFile();
    }

    // Simulate 3 sequential cold queries (like the benchmark).
    for (int iter = 0; iter < 3; iter++) {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        vfs.clearCache();
        vfs.s3().resetCounters();

        // Read all pages — must come from S3 with correct data.
        for (int i = 0; i < 8; i++) {
            std::vector<uint8_t> buf(PAGE_SIZE, 0);
            fi->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(i) * PAGE_SIZE);
            for (auto b : buf) assert(b == static_cast<uint8_t>(0x50 + i));
        }

        assert(vfs.s3().fetchCount.load() > 0);
        std::printf("    iter=%d S3 fetches: %llu\n", iter,
            (unsigned long long)vfs.s3().fetchCount.load());
    }

    cleanupS3(cfg);
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testSequentialColdQueriesNoCorruption\n");
}

int main() {
    std::printf("=== VFS S3 Integration Tests ===\n");
    testColdReadFromS3();
    testWarmReadNoS3();
    testPageGroupPrefetch();
    testColdRestartNewInstance();
    testMultiplePageGroups();
    testS3CounterAccuracy();
    testOverwriteSyncedPage();
    testImmutablePageGroupKeys();
    testEvictionCleansOrphans();
    testColdFetchDataIntegrity();
    testColdRestart();

    std::printf("\n--- Eviction Tests ---\n");
    testEvictAndColdReadFromS3();
    testEvictWriteSyncColdRead();
    testClearCacheWriteSyncColdRead();
    testPartialEvictionMultipleGroups();
    testEvictAllWriteOneSyncColdRead();
    testDoubleSyncAfterPartialEviction();
    testFetchThenWriteSyncPreservesData();
    testClearCachePersistsBitmap();
    testSequentialColdQueriesNoCorruption();

    std::printf("All VFS S3 integration tests passed.\n");
    return 0;
}
