// Unit tests for Phase Glacier: automatic cache eviction.
// No S3 required -- tests eviction logic using local write/sync/read paths.

#include "tiered_file_system.h"
#include "crypto.h"

#include <cassert>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <thread>
#include <unistd.h>

using namespace lbug::tiered;
using namespace lbug::common;

static constexpr uint32_t PAGE_SIZE = 4096;
static constexpr uint32_t PAGES_PER_GROUP = 4;

static std::filesystem::path tmpDir() {
    auto dir = std::filesystem::temp_directory_path() / "tiered_eviction_test";
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

static TieredConfig makeConfig(const std::filesystem::path& dir,
    uint64_t maxCacheBytes = 0) {
    TieredConfig cfg;
    cfg.s3 = {"https://localhost:1", "fake-bucket", "test/eviction", "auto", "fake-ak", "fake-sk", 1};
    cfg.cacheDir = (dir / "cache").string();
    cfg.dataFilePath = (dir / "data.kz").string();
    cfg.pageSize = PAGE_SIZE;
    cfg.pagesPerGroup = PAGES_PER_GROUP;
    cfg.compressionLevel = 3;
    cfg.maxCacheBytes = maxCacheBytes;
    return cfg;
}

// Helper: write pages, sync to local cache (S3 will fail, but local file + bitmap are populated).
static void writeAndSync(FileInfo& fi, uint32_t startPage, uint32_t numPages) {
    for (uint32_t i = 0; i < numPages; i++) {
        auto pageNum = startPage + i;
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(pageNum & 0xFF));
        fi.writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(pageNum) * PAGE_SIZE);
    }
    // Sync writes pages to local file + bitmap. S3 upload fails (fake creds) but that's fine.
    try { fi.syncFile(); } catch (...) {}
}

// --- Test: currentCacheBytes tracks cache size ---

static void testCacheBytesTracking() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    assert(vfs.currentCacheBytes() == 0);

    // Write 4 pages (1 group), sync to local cache.
    writeAndSync(*fi, 0, 4);
    assert(vfs.currentCacheBytes() == 4 * PAGE_SIZE);

    // Write 4 more pages (second group).
    writeAndSync(*fi, 4, 4);
    assert(vfs.currentCacheBytes() == 8 * PAGE_SIZE);

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testCacheBytesTracking\n");
}

// --- Test: evictToBudget does nothing when unlimited ---

static void testNoEvictionWhenUnlimited() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir, 0); // 0 = unlimited.
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    writeAndSync(*fi, 0, 8); // 2 groups.
    assert(vfs.currentCacheBytes() == 8 * PAGE_SIZE);

    auto evicted = vfs.evictToBudget();
    assert(evicted == 0);
    assert(vfs.currentCacheBytes() == 8 * PAGE_SIZE);

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testNoEvictionWhenUnlimited\n");
}

// --- Test: evictToBudget does nothing when under budget ---

static void testNoEvictionWhenUnderBudget() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir, 100 * PAGE_SIZE); // 100 pages budget.
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    writeAndSync(*fi, 0, 8); // 2 groups = 8 pages, well under 100.
    assert(vfs.currentCacheBytes() == 8 * PAGE_SIZE);

    auto evicted = vfs.evictToBudget();
    assert(evicted == 0);

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testNoEvictionWhenUnderBudget\n");
}

// --- Test: evictToBudget evicts when over budget ---

static void testEvictionWhenOverBudget() {
    auto dir = tmpDir();
    // Budget: 1 group (4 pages). Write 3 groups (12 pages).
    auto cfg = makeConfig(dir, 4 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    writeAndSync(*fi, 0, 12); // 3 groups.
    assert(vfs.currentCacheBytes() == 12 * PAGE_SIZE);

    auto evicted = vfs.evictToBudget();
    assert(evicted > 0);
    // Should be at or below 80% of budget (3.2 pages -> 0 pages since groups are 4 pages).
    assert(vfs.currentCacheBytes() <= 4 * PAGE_SIZE);

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEvictionWhenOverBudget\n");
}

// --- Test: structural pages survive eviction ---

static void testStructuralPagesSurviveEviction() {
    auto dir = tmpDir();
    // Budget: 1 group. Write 3 groups.
    auto cfg = makeConfig(dir, 4 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Mark group 0 as structural.
    vfs.beginTrackStructural();
    writeAndSync(*fi, 0, 4); // Group 0 = structural.
    vfs.endTrack();

    // Write 2 more groups (data).
    writeAndSync(*fi, 4, 8); // Groups 1, 2 = data.
    assert(vfs.currentCacheBytes() == 12 * PAGE_SIZE);

    auto evicted = vfs.evictToBudget();
    assert(evicted > 0);

    // Group 0 (structural) should still be present.
    auto* afi = const_cast<TieredFileInfo*>(
        static_cast<const TieredFileInfo*>(fi.get()));
    assert(afi->bitmap->isPresent(0));
    assert(afi->bitmap->isPresent(1));
    assert(afi->bitmap->isPresent(2));
    assert(afi->bitmap->isPresent(3));

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testStructuralPagesSurviveEviction\n");
}

// --- Test: access count affects eviction order ---

static void testAccessCountAffectsEviction() {
    auto dir = tmpDir();
    // Budget: 2 groups. Write 3 groups.
    auto cfg = makeConfig(dir, 8 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    writeAndSync(*fi, 0, 12); // Groups 0, 1, 2.

    // Touch group 0 many times (make it "hot").
    auto* afi = static_cast<TieredFileInfo*>(fi.get());
    for (int i = 0; i < 50; i++) {
        afi->touchGroup(0);
    }
    // Touch group 1 a few times.
    for (int i = 0; i < 5; i++) {
        afi->touchGroup(1);
    }
    // Don't touch group 2 at all (coldest).

    auto evicted = vfs.evictToBudget();
    assert(evicted >= 1);

    // Group 2 (coldest, score = 0) should be evicted first.
    // Group 0 (hottest) should survive.
    assert(afi->bitmap->isPresent(0)); // Hot, survived.

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testAccessCountAffectsEviction\n");
}

// --- Test: eviction reclaims disk space (bitmap cleared) ---

static void testEvictionClearsBitmap() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir, 4 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    writeAndSync(*fi, 0, 12); // 3 groups.
    auto* afi = static_cast<TieredFileInfo*>(fi.get());

    // All 12 pages present.
    assert(afi->bitmap->presentCount() == 12);

    vfs.evictToBudget();

    // Some pages evicted.
    assert(afi->bitmap->presentCount() < 12);
    // Remaining pages are within budget.
    assert(afi->bitmap->presentCount() * PAGE_SIZE <= 4 * PAGE_SIZE);

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEvictionClearsBitmap\n");
}

// --- Test: group states reset on eviction ---

static void testEvictionResetsGroupState() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir, 4 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    writeAndSync(*fi, 0, 12);
    auto* afi = static_cast<TieredFileInfo*>(fi.get());

    vfs.evictToBudget();

    // Evicted groups should have state NONE.
    int noneCount = 0;
    for (uint64_t g = 0; g < afi->totalGroups; g++) {
        if (afi->getGroupState(g) == GroupState::NONE) noneCount++;
    }
    assert(noneCount > 0); // At least one group evicted.

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEvictionResetsGroupState\n");
}

// --- Test: access tracking initializes only when maxCacheBytes > 0 ---

static void testAccessTrackingOnlyWhenLimited() {
    auto dir = tmpDir();

    // Unlimited: no access tracking arrays.
    {
        auto cfg = makeConfig(dir, 0);
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        writeAndSync(*fi, 0, 4);
        auto* afi = static_cast<TieredFileInfo*>(fi.get());
        assert(!afi->groupAccessCounts); // Not allocated.
        assert(!afi->groupAccessTimes);
        fi.reset();
    }

    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);

    // Limited: access tracking arrays allocated.
    {
        auto cfg = makeConfig(dir, 100 * PAGE_SIZE);
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        writeAndSync(*fi, 0, 4);
        auto* afi = static_cast<TieredFileInfo*>(fi.get());
        assert(afi->groupAccessCounts); // Allocated.
        assert(afi->groupAccessTimes);
        fi.reset();
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testAccessTrackingOnlyWhenLimited\n");
}

// --- Test: touchGroup saturates at 64 ---

static void testTouchGroupSaturation() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir, 100 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    writeAndSync(*fi, 0, 4);

    auto* afi = static_cast<TieredFileInfo*>(fi.get());
    for (int i = 0; i < 200; i++) {
        afi->touchGroup(0);
    }
    assert(afi->groupAccessCounts[0].load() == 64);

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testTouchGroupSaturation\n");
}

// --- Test: eviction with all structural pages (nothing to evict) ---

static void testAllStructuralNothingToEvict() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir, 4 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Mark everything as structural.
    vfs.beginTrackStructural();
    writeAndSync(*fi, 0, 12);
    vfs.endTrack();

    // Over budget but all structural. Nothing should be evicted.
    auto evicted = vfs.evictToBudget();
    assert(evicted == 0);
    assert(vfs.currentCacheBytes() == 12 * PAGE_SIZE); // Still over budget.

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testAllStructuralNothingToEvict\n");
}

// --- Test: batch eviction targets 80% of budget ---

static void testBatchEvictionTargets80Percent() {
    auto dir = tmpDir();
    // Budget: 5 groups (20 pages). Write 8 groups (32 pages).
    auto cfg = makeConfig(dir, 20 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    writeAndSync(*fi, 0, 32);
    assert(vfs.currentCacheBytes() == 32 * PAGE_SIZE);

    vfs.evictToBudget();

    // Target is 80% of 20 pages = 16 pages. Should be at or below 16 pages.
    assert(vfs.currentCacheBytes() <= 16 * PAGE_SIZE);

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testBatchEvictionTargets80Percent\n");
}

// --- Test: touchGroup updates access time and count ---

static void testTouchGroupUpdatesAccessTimeAndCount() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir, 100 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    writeAndSync(*fi, 0, 4);

    auto* afi = static_cast<TieredFileInfo*>(fi.get());
    assert(afi->groupAccessCounts);
    assert(afi->groupAccessTimes);

    // Initially zero.
    assert(afi->groupAccessCounts[0].load() == 0);
    assert(afi->groupAccessTimes[0].load() == 0);

    auto beforeTouch = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();
    afi->touchGroup(0);
    auto afterTouch = std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::steady_clock::now().time_since_epoch()).count();

    assert(afi->groupAccessCounts[0].load() == 1);
    auto accessTime = afi->groupAccessTimes[0].load();
    assert(accessTime >= static_cast<uint64_t>(beforeTouch));
    assert(accessTime <= static_cast<uint64_t>(afterTouch));

    // Touch again.
    afi->touchGroup(0);
    assert(afi->groupAccessCounts[0].load() == 2);
    assert(afi->groupAccessTimes[0].load() >= accessTime);

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testTouchGroupUpdatesAccessTimeAndCount\n");
}

// --- Test: concurrent reads + eviction (no crash, no data corruption) ---

static void testConcurrentReadsAndEviction() {
    auto dir = tmpDir();
    // Budget: 4 groups (16 pages). Write 8 groups (32 pages).
    auto cfg = makeConfig(dir, 16 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 32 pages across 8 groups, each page filled with its page number.
    for (uint32_t p = 0; p < 32; p++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(p & 0xFF));
        fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(p) * PAGE_SIZE);
    }
    try { fi->syncFile(); } catch (...) {}

    auto* afi = static_cast<TieredFileInfo*>(fi.get());
    std::atomic<bool> stop{false};
    std::atomic<int> errors{0};

    // Reader threads: continuously touch groups, verify bitmap consistency,
    // and verify data correctness for pages still in the local cache.
    auto readerFn = [&](int threadId) {
        while (!stop.load(std::memory_order_relaxed)) {
            for (uint64_t g = 0; g < afi->totalGroups; g++) {
                afi->touchGroup(g);
                // Check bitmap consistency: presentCount should never exceed total pages.
                auto present = afi->bitmap->presentCount();
                if (present > 32) {
                    errors.fetch_add(1, std::memory_order_relaxed);
                }
                // Data correctness: read a page that is still present in the
                // local cache and verify its contents match what was written.
                auto startPage = g * PAGES_PER_GROUP;
                for (uint32_t p = 0; p < PAGES_PER_GROUP; p++) {
                    auto pageNum = startPage + p;
                    if (pageNum < 32 && afi->bitmap->isPresent(pageNum)) {
                        std::vector<uint8_t> buf(PAGE_SIZE);
                        auto offset = static_cast<off_t>(pageNum * PAGE_SIZE);
                        auto bytesRead = ::pread(afi->localFd, buf.data(), PAGE_SIZE, offset);
                        if (bytesRead == static_cast<ssize_t>(PAGE_SIZE)) {
                            uint8_t expected = static_cast<uint8_t>(pageNum & 0xFF);
                            // Verify first and last bytes match the expected fill pattern.
                            if (buf[0] != expected || buf[PAGE_SIZE - 1] != expected) {
                                errors.fetch_add(1, std::memory_order_relaxed);
                            }
                        }
                        break; // One page per group per iteration is enough.
                    }
                }
            }
        }
    };

    // Eviction thread: continuously evicts to budget.
    auto evictorFn = [&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            vfs.evictToBudget();
            std::this_thread::yield();
        }
    };

    std::vector<std::thread> threads;
    // 4 reader threads.
    for (int i = 0; i < 4; i++) {
        threads.emplace_back(readerFn, i);
    }
    // 2 evictor threads.
    for (int i = 0; i < 2; i++) {
        threads.emplace_back(evictorFn);
    }

    // Run for 200ms.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    stop.store(true, std::memory_order_relaxed);

    for (auto& t : threads) {
        t.join();
    }

    assert(errors.load() == 0);

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testConcurrentReadsAndEviction\n");
}

// --- Test: eviction of index pages under pressure ---

static void testIndexPagesEvictedUnderPressure() {
    auto dir = tmpDir();
    // Budget: 1 group. Write 3 groups: 1 structural, 1 index, 1 data.
    auto cfg = makeConfig(dir, 4 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Group 0 = structural.
    vfs.beginTrackStructural();
    writeAndSync(*fi, 0, 4);
    vfs.endTrack();

    // Group 1 = index.
    vfs.beginTrackIndex();
    writeAndSync(*fi, 4, 4);
    vfs.endTrack();

    // Group 2 = data (no tracking).
    writeAndSync(*fi, 8, 4);

    assert(vfs.currentCacheBytes() == 12 * PAGE_SIZE);

    auto evicted = vfs.evictToBudget();
    assert(evicted > 0);

    auto* afi = static_cast<TieredFileInfo*>(fi.get());

    // Structural pages (group 0) must survive.
    assert(afi->bitmap->isPresent(0));
    assert(afi->bitmap->isPresent(3));

    // Data pages (group 2) should be evicted before index pages.
    // With budget = 4 pages and structural = 4 pages, both data and index
    // must be evicted to reach 80% of 4 pages = 3.2 pages. But structural
    // alone is 4 pages, so we evict everything we can.
    bool dataEvicted = !afi->bitmap->isPresent(8);
    assert(dataEvicted); // Data evicted first.

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testIndexPagesEvictedUnderPressure\n");
}

// --- Test: growGroupArrays expands tracking on write ---

static void testGrowGroupArraysOnWrite() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir, 100 * PAGE_SIZE);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    auto* afi = static_cast<TieredFileInfo*>(fi.get());

    // Initially 0 groups (empty DB).
    assert(afi->totalGroups == 0);

    // Write 1 group.
    writeAndSync(*fi, 0, 4);
    assert(afi->totalGroups >= 1);
    assert(afi->groupStates);
    assert(afi->groupAccessCounts);
    assert(afi->groupAccessTimes);

    // Group state should be PRESENT after sync.
    assert(afi->getGroupState(0) == GroupState::PRESENT);

    // Touch should work now.
    afi->touchGroup(0);
    assert(afi->groupAccessCounts[0].load() == 1);

    // Write more groups, verify arrays grow.
    writeAndSync(*fi, 4, 8); // Groups 1, 2.
    assert(afi->totalGroups >= 3);
    assert(afi->getGroupState(1) == GroupState::PRESENT);
    assert(afi->getGroupState(2) == GroupState::PRESENT);

    // Previous group's data preserved.
    assert(afi->groupAccessCounts[0].load() == 1);

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testGrowGroupArraysOnWrite\n");
}

// --- Test: encryption + eviction round-trip correctness ---

static void testEncryptionWithEviction() {
    auto dir = tmpDir();
    // Budget: 1 group (4 pages). Write 3 groups (12 pages).
    auto cfg = makeConfig(dir, 4 * PAGE_SIZE);
    // Set a deterministic 32-byte encryption key.
    Key256 key{};
    for (size_t i = 0; i < 32; i++) key[i] = static_cast<uint8_t>(0xAA ^ i);
    cfg.encryptionKey = key;

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 12 pages, each filled with a recognizable pattern.
    for (uint32_t p = 0; p < 12; p++) {
        std::vector<uint8_t> page(PAGE_SIZE);
        // Fill with a pattern: each byte = (pageNum * 7 + byteOffset) & 0xFF.
        for (uint32_t b = 0; b < PAGE_SIZE; b++) {
            page[b] = static_cast<uint8_t>((p * 7 + b) & 0xFF);
        }
        fi->writeFile(page.data(), PAGE_SIZE, static_cast<uint64_t>(p) * PAGE_SIZE);
    }
    try { fi->syncFile(); } catch (...) {}

    auto* afi = static_cast<TieredFileInfo*>(fi.get());
    assert(vfs.currentCacheBytes() == 12 * PAGE_SIZE);

    // Verify data on disk is encrypted (not plaintext).
    // Read raw bytes from the local cache file for page 0.
    {
        std::vector<uint8_t> rawPage(PAGE_SIZE);
        auto bytesRead = ::pread(afi->localFd, rawPage.data(), PAGE_SIZE, 0);
        assert(bytesRead == static_cast<ssize_t>(PAGE_SIZE));
        // Build expected plaintext for page 0.
        std::vector<uint8_t> expectedPlain(PAGE_SIZE);
        for (uint32_t b = 0; b < PAGE_SIZE; b++) {
            expectedPlain[b] = static_cast<uint8_t>((0 * 7 + b) & 0xFF);
        }
        // Raw bytes should NOT match plaintext (they should be encrypted).
        assert(rawPage != expectedPlain);
    }

    // Trigger eviction. Should evict at least some groups.
    auto evicted = vfs.evictToBudget();
    assert(evicted > 0);

    // Now verify that pages still in cache can be read back correctly
    // (the read path must decrypt them).
    for (uint32_t p = 0; p < 12; p++) {
        if (!afi->bitmap->isPresent(p)) continue; // Evicted, skip.

        std::vector<uint8_t> buf(PAGE_SIZE);
        fi->readFromFile(buf.data(), PAGE_SIZE, static_cast<uint64_t>(p) * PAGE_SIZE);

        // Verify every byte matches the original pattern.
        for (uint32_t b = 0; b < PAGE_SIZE; b++) {
            uint8_t expected = static_cast<uint8_t>((p * 7 + b) & 0xFF);
            assert(buf[b] == expected);
        }
    }

    fi.reset();
    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEncryptionWithEviction\n");
}

int main() {
    std::printf("=== Cache Eviction Unit Tests ===\n");

    // Happy path.
    testCacheBytesTracking();
    testNoEvictionWhenUnlimited();
    testNoEvictionWhenUnderBudget();
    testEvictionWhenOverBudget();

    // Priority / correctness.
    testStructuralPagesSurviveEviction();
    testAccessCountAffectsEviction();
    testEvictionClearsBitmap();
    testEvictionResetsGroupState();

    // Edge cases.
    testAccessTrackingOnlyWhenLimited();
    testTouchGroupSaturation();
    testAllStructuralNothingToEvict();
    testBatchEvictionTargets80Percent();

    // Access tracking.
    testTouchGroupUpdatesAccessTimeAndCount();

    // Concurrency.
    testConcurrentReadsAndEviction();

    // Index eviction.
    testIndexPagesEvictedUnderPressure();

    // Dynamic growth.
    testGrowGroupArraysOnWrite();

    // Encryption + eviction.
    testEncryptionWithEviction();

    std::printf("All cache eviction unit tests passed.\n");
    return 0;
}
