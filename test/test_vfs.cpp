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

#include "crypto.h"
#include "manifest.h"
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
// With S3Primary ordering, the manifest is only persisted
// locally after a successful S3 upload. With fake S3 (uploads fail),
// flushPendingPageGroups returns early, so the manifest stays at version 0
// with pageCount=0. On reopen, the VFS sees a fresh database.
//
// This test now verifies the S3Primary invariant: data written in session 1
// but not uploaded to S3 is NOT available in session 2 (because the manifest
// does not reflect it). The local cache file has the bytes, but the VFS
// correctly does not serve them since the manifest says pageCount=0.
// This is the correct behavior: local state must not exceed what S3 knows.
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

    // Session 2: open same VFS.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        // With fake S3, manifest stays at version 0, pageCount=0.
        // The VFS should report version 0.
        assert(vfs.getManifestVersion() == 0);
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

// --- Test: encrypted write -> sync -> read round-trip ---

static TieredConfig makeEncryptedConfig(const std::filesystem::path& dir) {
    auto cfg = makeConfig(dir);
    Key256 key;
    for (int i = 0; i < 32; i++) key[i] = static_cast<uint8_t>(i + 0x10);
    cfg.encryptionKey = key;
    return cfg;
}

static void testEncryptedWriteSyncRead() {
    auto dir = tmpDir();
    auto cfg = makeEncryptedConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write a known pattern.
    std::vector<uint8_t> writeData(PAGE_SIZE, 0xAA);
    fi->writeFile(writeData.data(), PAGE_SIZE, 0);

    // Read back from dirty page (plaintext, no encryption involved yet).
    std::vector<uint8_t> readDirty(PAGE_SIZE);
    fi->readFromFile(readDirty.data(), PAGE_SIZE, 0);
    assert(readDirty == writeData);

    // Sync to local cache (triggers CTR encrypt on write).
    fi->syncFile();

    // Read through VFS after sync (should CTR decrypt and return plaintext).
    std::vector<uint8_t> readBack(PAGE_SIZE);
    fi->readFromFile(readBack.data(), PAGE_SIZE, 0);
    assert(readBack == writeData);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEncryptedWriteSyncRead\n");
}

static void testEncryptedWrongKeyReadsGarbage() {
    auto dir = tmpDir();
    auto cfg = makeEncryptedConfig(dir);

    // Write and sync with the correct key.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
        std::vector<uint8_t> writeData(PAGE_SIZE, 0xBB);
        fi->writeFile(writeData.data(), PAGE_SIZE, 0);
        fi->syncFile();
    }

    // Reopen with a different key.
    Key256 wrongKey;
    for (int i = 0; i < 32; i++) wrongKey[i] = static_cast<uint8_t>(0xFF - i);
    cfg.encryptionKey = wrongKey;

    TieredFileSystem vfs2(cfg);
    auto fi2 = vfs2.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::READ_ONLY));

    std::vector<uint8_t> readBack(PAGE_SIZE);
    fi2->readFromFile(readBack.data(), PAGE_SIZE, 0);

    std::vector<uint8_t> expected(PAGE_SIZE, 0xBB);
    assert(readBack != expected); // Wrong key = garbage.

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testEncryptedWrongKeyReadsGarbage\n");
}

static void testEncryptedManifestFlag() {
    // Verify manifest records encrypted=true.
    Manifest m;
    m.version = 1;
    m.pageCount = 100;
    m.pageSize = 4096;
    m.pagesPerGroup = 4;
    m.encrypted = true;

    auto json = m.toJSON();
    assert(json.find("\"encrypted\":true") != std::string::npos);

    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->encrypted);

    // Unencrypted manifest should not have the flag.
    m.encrypted = false;
    json = m.toJSON();
    assert(json.find("encrypted") == std::string::npos);

    parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(!parsed->encrypted);

    std::printf("  PASS: testEncryptedManifestFlag\n");
}

static void testWirePureManifestApplyAndRead() {
    Manifest original;
    original.version = 42;
    original.pageCount = 0;
    original.pageSize = 4096;
    original.pagesPerGroup = 2048;
    original.journalSeq = 12345;
    original.encrypted = true;
    original.subPagesPerFrame = 4;

    auto body = original.toMsgpack();
    std::vector<uint8_t> tagged;
    tagged.reserve(body.size() + 1);
    tagged.push_back(0x01);
    tagged.insert(tagged.end(), body.begin(), body.end());

    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    auto result = vfs.setManifestBytes(tagged);
    assert(result.first == 0);
    assert(result.second.empty());

    auto readBack = vfs.manifestBytes();
    assert(!readBack.empty());
    assert(readBack[0] == 0x01);

    auto parsed = Manifest::fromMsgpack(
        std::vector<uint8_t>(readBack.begin() + 1, readBack.end()));
    assert(parsed.has_value());
    assert(parsed->version == 42);
    assert(parsed->pageCount == 0);
    assert(parsed->pageSize == 4096);
    assert(parsed->pagesPerGroup == 2048);
    assert(parsed->pageGroupKeys.empty());
    assert(parsed->journalSeq == 12345);
    assert(parsed->encrypted == true);
    assert(parsed->subPagesPerFrame == 4);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWirePureManifestApplyAndRead\n");
}

static void testWireHybridManifestApplyAndRead() {
    Manifest original;
    original.version = 7;
    original.pageCount = 0;
    original.pageSize = 4096;
    original.pagesPerGroup = 2048;
    original.journalSeq = 99;

    HybridPayload hybrid;
    hybrid.turbograph = original;
    hybrid.graphstream_journal_seq = 555;
    hybrid.graphstream_segment_prefix = "gs/mydb/";

    auto body = hybrid.toMsgpack();
    std::vector<uint8_t> tagged;
    tagged.reserve(body.size() + 1);
    tagged.push_back(0x02);
    tagged.insert(tagged.end(), body.begin(), body.end());

    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    auto result = vfs.setManifestBytes(tagged);

    assert(result.first == 555);
    assert(result.second == "gs/mydb/");

    auto readBack = vfs.manifestBytes();
    assert(readBack[0] == 0x01);
    auto parsed = Manifest::fromMsgpack(
        std::vector<uint8_t>(readBack.begin() + 1, readBack.end()));
    assert(parsed.has_value());
    assert(parsed->version == 7);
    assert(parsed->journalSeq == 99);
    assert(parsed->pageGroupKeys.empty());

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWireHybridManifestApplyAndRead\n");
}

static void testWireSubframeOverridesDecodeWithoutActiveFile() {
    Manifest original;
    original.version = 5;
    original.pageCount = 50;
    original.pageSize = 4096;
    original.pagesPerGroup = 2048;
    original.pageGroupKeys = {"pg/0_v5"};

    std::unordered_map<size_t, SubframeOverride> ovr;
    ovr[0] = SubframeOverride{"pg/0_f0_v5", FrameEntry{0, 8192, 2}};
    ovr[3] = SubframeOverride{"pg/0_f3_v5", FrameEntry{8192, 4096, 1}};
    original.subframeOverrides.push_back(ovr);

    auto body = original.toMsgpack();
    std::vector<uint8_t> tagged;
    tagged.reserve(body.size() + 1);
    tagged.push_back(0x01);
    tagged.insert(tagged.end(), body.begin(), body.end());

    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);
    auto decoded = vfs.decodeManifestBytes(tagged);
    assert(!decoded.hybrid);
    assert(decoded.manifest.subframeOverrides.size() == 1);
    assert(decoded.manifest.subframeOverrides[0].size() == 2);
    assert(decoded.manifest.subframeOverrides[0].at(0).key == "pg/0_f0_v5");
    assert(decoded.manifest.subframeOverrides[0].at(0).entry.len == 8192);
    assert(decoded.manifest.subframeOverrides[0].at(3).key == "pg/0_f3_v5");
    assert(decoded.manifest.subframeOverrides[0].at(3).entry.pageCount == 1);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWireSubframeOverridesDecodeWithoutActiveFile\n");
}

static void testWireManifestBytesWithGraphstreamDelta() {
    Manifest original;
    original.version = 3;
    original.pageCount = 0;
    original.pageSize = 4096;
    original.pagesPerGroup = 2048;

    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    auto body = original.toMsgpack();
    std::vector<uint8_t> tagged;
    tagged.reserve(body.size() + 1);
    tagged.push_back(0x01);
    tagged.insert(tagged.end(), body.begin(), body.end());
    vfs.setManifestBytes(tagged);

    auto hybridBytes = vfs.manifestBytesWithGraphstreamDelta(777, "prefix/segments/");
    assert(!hybridBytes.empty());
    assert(hybridBytes[0] == 0x02);

    auto parsed = HybridPayload::fromMsgpack(
        std::vector<uint8_t>(hybridBytes.begin() + 1, hybridBytes.end()));
    assert(parsed.has_value());
    assert(parsed->turbograph.version == 3);
    assert(parsed->graphstream_journal_seq == 777);
    assert(parsed->graphstream_segment_prefix == "prefix/segments/");

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWireManifestBytesWithGraphstreamDelta\n");
}

static void testWireNoopSkipsRemotePreflight() {
    Manifest original;
    original.version = 0;
    original.pageCount = PAGES_PER_GROUP;
    original.pageSize = PAGE_SIZE;
    original.pagesPerGroup = PAGES_PER_GROUP;
    original.pageGroupKeys = {"missing/pg/0_v0"};

    HybridPayload hybrid;
    hybrid.turbograph = original;
    hybrid.graphstream_journal_seq = 321;
    hybrid.graphstream_segment_prefix = "gs/noop/";

    auto body = hybrid.toMsgpack();
    std::vector<uint8_t> tagged;
    tagged.reserve(body.size() + 1);
    tagged.push_back(0x02);
    tagged.insert(tagged.end(), body.begin(), body.end());

    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    auto result = vfs.setManifestBytes(tagged);
    assert(result.first == 321);
    assert(result.second == "gs/noop/");
    assert(vfs.getManifestVersion() == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWireNoopSkipsRemotePreflight\n");
}

static void testWirePreflightMissingObjectsWithoutActiveFile() {
    Manifest original;
    original.version = 9;
    original.pageCount = 4;
    original.pageSize = 4096;
    original.pagesPerGroup = 4;
    original.pageGroupKeys = {"missing/pg/0_v9"};

    auto body = original.toMsgpack();
    std::vector<uint8_t> tagged;
    tagged.reserve(body.size() + 1);
    tagged.push_back(0x01);
    tagged.insert(tagged.end(), body.begin(), body.end());

    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    try {
        vfs.preflightManifestBytes(tagged);
        assert(false && "preflightManifestBytes should reject missing objects");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("missing page objects") != std::string::npos);
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWirePreflightMissingObjectsWithoutActiveFile\n");
}

// Empty payload should be rejected by setManifestBytes.
static void testWireEmptyPayloadRejected() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    try {
        vfs.setManifestBytes({});
        assert(false && "setManifestBytes should reject empty payload");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("empty") != std::string::npos);
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWireEmptyPayloadRejected\n");
}

// Unknown tag byte should be rejected.
static void testWireWrongTagRejected() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    std::vector<uint8_t> bad = {0x03, 0x93, 0x01, 0x02, 0x03}; // tag 0x03 = unknown
    try {
        vfs.setManifestBytes(bad);
        assert(false && "setManifestBytes should reject unknown tag");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("unknown wire tag") != std::string::npos);
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWireWrongTagRejected\n");
}

// Malformed msgpack body should be rejected.
static void testWireMalformedMsgpackRejected() {
    auto dir = tmpDir();
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Tag 0x01 (pure) followed by an incomplete msgpack map header.
    std::vector<uint8_t> bad = {0x01, 0x8A};
    try {
        vfs.setManifestBytes(bad);
        assert(false && "setManifestBytes should reject malformed msgpack");
    } catch (const std::runtime_error& e) {
        assert(std::string(e.what()).find("failed to decode") != std::string::npos);
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWireMalformedMsgpackRejected\n");
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
    testEncryptedWriteSyncRead();
    testEncryptedWrongKeyReadsGarbage();
    testEncryptedManifestFlag();
    testWirePureManifestApplyAndRead();
    testWireHybridManifestApplyAndRead();
    testWireSubframeOverridesDecodeWithoutActiveFile();
    testWireManifestBytesWithGraphstreamDelta();
    testWireNoopSkipsRemotePreflight();
    testWirePreflightMissingObjectsWithoutActiveFile();
    testWireEmptyPayloadRejected();
    testWireWrongTagRejected();
    testWireMalformedMsgpackRejected();
    std::printf("All VFS unit tests passed.\n");
    return 0;
}
