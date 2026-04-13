// Phase GraphDrift: subframe override tests.
//
// Tests the override write path, override read path, auto-compaction, and
// manifest round-trip with subframe overrides.
//
// Uses small pagesPerGroup=8, subPagesPerFrame=2 so:
//   - 4 frames per group
//   - effectiveThreshold = 4/4 = 1 (auto), so any single dirty frame uses override
//   - We use overrideThreshold=2 to test the override vs full-rewrite boundary
//
// S3 uploads will fail (dummy credentials), so we test via the local
// cache path. doSyncFile writes to local cache first, then attempts S3 upload.
// The override decision logic and manifest updates still run locally.

#include "chunk_codec.h"
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
static constexpr uint32_t PAGES_PER_GROUP = 8;
static constexpr uint32_t SUB_PAGES_PER_FRAME = 2;

static std::filesystem::path tmpDir(const char* suffix = "drift") {
    auto dir = std::filesystem::temp_directory_path() / ("tiered_drift_test_" + std::string(suffix));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

static TieredConfig makeConfig(const std::filesystem::path& dir,
    uint32_t overrideThreshold = 2,
    uint32_t compactionThreshold = 8) {
    TieredConfig cfg;
    cfg.s3 = {"https://localhost:1", "fake-bucket", "test/drift", "auto", "fake-ak", "fake-sk", 1};
    cfg.cacheDir = (dir / "cache").string();
    cfg.dataFilePath = (dir / "data.kz").string();
    cfg.pageSize = PAGE_SIZE;
    cfg.pagesPerGroup = PAGES_PER_GROUP;
    cfg.subPagesPerFrame = SUB_PAGES_PER_FRAME;
    cfg.compressionLevel = 3;
    cfg.overrideThreshold = overrideThreshold;
    cfg.compactionThreshold = compactionThreshold;
    return cfg;
}

// --- Test: manifest round-trip with subframe overrides ---

static void testManifestRoundTrip() {
    Manifest m;
    m.version = 5;
    m.pageCount = 100;
    m.pageSize = PAGE_SIZE;
    m.pagesPerGroup = PAGES_PER_GROUP;
    m.subPagesPerFrame = SUB_PAGES_PER_FRAME;
    m.pageGroupKeys = {"test/pg/0_v1", "test/pg/1_v1"};
    m.frameTables.resize(2);
    m.frameTables[0] = {{0, 100, 2}, {100, 90, 2}};
    m.frameTables[1] = {{0, 80, 2}, {80, 85, 2}};

    // Add overrides for group 0, frames 1 and 3.
    m.subframeOverrides.resize(2);
    SubframeOverride ov0;
    ov0.key = "test/pg/0_f1_v3";
    ov0.entry = {0, 50, 2};
    m.subframeOverrides[0][1] = ov0;

    SubframeOverride ov1;
    ov1.key = "test/pg/0_f3_v4";
    ov1.entry = {0, 45, 2};
    m.subframeOverrides[0][3] = ov1;

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->version == 5);
    assert(parsed->pageCount == 100);
    assert(parsed->subPagesPerFrame == SUB_PAGES_PER_FRAME);
    assert(parsed->pageGroupKeys.size() == 2);
    assert(parsed->frameTables.size() == 2);

    // Check overrides parsed correctly.
    assert(parsed->subframeOverrides.size() >= 2);
    assert(parsed->subframeOverrides[0].size() == 2);
    assert(parsed->subframeOverrides[0].count(1));
    assert(parsed->subframeOverrides[0].at(1).key == "test/pg/0_f1_v3");
    assert(parsed->subframeOverrides[0].at(1).entry.len == 50);
    assert(parsed->subframeOverrides[0].at(1).entry.pageCount == 2);
    assert(parsed->subframeOverrides[0].count(3));
    assert(parsed->subframeOverrides[0].at(3).key == "test/pg/0_f3_v4");
    assert(parsed->subframeOverrides[0].at(3).entry.len == 45);

    // Group 1 should have no overrides.
    assert(parsed->subframeOverrides[1].empty());

    std::printf("  PASS: testManifestRoundTrip\n");
}

// --- Test: manifest backward compat (no subframe_overrides field) ---

static void testManifestBackwardCompat() {
    // Minimal manifest JSON without subframe_overrides.
    std::string json = R"({"version":1,"page_count":16,"page_size":4096,"pages_per_group":8,"page_group_keys":["pg/0_v1","pg/1_v1"],"sub_pages_per_frame":2,"frame_tables":[[[0,100,2],[100,90,2]],[[0,80,2]]]})";
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->version == 1);
    assert(parsed->pageGroupKeys.size() == 2);
    // subframeOverrides should be initialized (empty maps for each group).
    assert(parsed->subframeOverrides.size() >= 2);
    assert(parsed->subframeOverrides[0].empty());
    assert(parsed->subframeOverrides[1].empty());

    std::printf("  PASS: testManifestBackwardCompat\n");
}

// --- Test: manifest with empty overrides (all groups clean) ---

static void testManifestEmptyOverrides() {
    Manifest m;
    m.version = 1;
    m.pageCount = 16;
    m.pageSize = PAGE_SIZE;
    m.pagesPerGroup = PAGES_PER_GROUP;
    m.subPagesPerFrame = SUB_PAGES_PER_FRAME;
    m.pageGroupKeys = {"pg/0_v1"};
    m.frameTables = {{{0, 100, 2}, {100, 90, 2}}};
    m.subframeOverrides.resize(1); // Empty map for group 0.

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    // subframe_overrides should not appear in JSON (all empty).
    assert(json.find("subframe_overrides") == std::string::npos);
    assert(parsed->subframeOverrides.size() >= 1);
    assert(parsed->subframeOverrides[0].empty());

    std::printf("  PASS: testManifestEmptyOverrides\n");
}

// --- Test: normalizeOverrides ---

static void testNormalizeOverrides() {
    Manifest m;
    m.pageGroupKeys = {"a", "b", "c"};
    m.subframeOverrides.resize(1); // Only 1, but 3 groups.
    m.normalizeOverrides();
    assert(m.subframeOverrides.size() == 3);
    assert(m.subframeOverrides[0].empty());
    assert(m.subframeOverrides[1].empty());
    assert(m.subframeOverrides[2].empty());

    std::printf("  PASS: testNormalizeOverrides\n");
}

// --- Test: write few pages, sync, verify override decision (local path) ---
//
// This tests the write -> sync -> read-back cycle with the override path.
// S3 uploads fail, but the local cache path still works.
// We verify data integrity through the dirty -> local-file -> read path.

static void testWriteSyncReadWithOverrides() {
    auto dir = tmpDir("sync_override");
    auto cfg = makeConfig(dir, 2, 0); // threshold=2, no compaction.
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write all 8 pages (fills one group) to establish a base.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 1));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Read back all 8 pages.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        for (auto b : buf) {
            assert(b == static_cast<uint8_t>(i + 1));
        }
    }

    // Now write only 1 page (page 3) -- should trigger override path (1 frame < 2 threshold).
    std::vector<uint8_t> newPage3(PAGE_SIZE, 0xFF);
    fi->writeFile(newPage3.data(), PAGE_SIZE, 3 * PAGE_SIZE);
    fi->syncFile();

    // Read back -- page 3 should be updated, others unchanged.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        if (i == 3) {
            for (auto b : buf) assert(b == 0xFF);
        } else {
            for (auto b : buf) assert(b == static_cast<uint8_t>(i + 1));
        }
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testWriteSyncReadWithOverrides\n");
}

// --- Test: write enough pages to exceed threshold, verify full rewrite ---

static void testFullRewriteThreshold() {
    auto dir = tmpDir("full_rewrite");
    auto cfg = makeConfig(dir, 2, 0); // threshold=2, no compaction.
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write all 8 pages to establish a base.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 1));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Write 4 pages spanning all 4 frames (one page per frame = 4 dirty frames >= 2 threshold).
    for (int i = 0; i < 8; i += 2) {
        std::vector<uint8_t> page(PAGE_SIZE, 0xCC);
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Read back all pages and verify data integrity.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        if (i % 2 == 0) {
            for (auto b : buf) assert(b == 0xCC);
        } else {
            for (auto b : buf) assert(b == static_cast<uint8_t>(i + 1));
        }
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testFullRewriteThreshold\n");
}

// --- Test: override then full rewrite ---

static void testOverrideThenFullRewrite() {
    auto dir = tmpDir("override_then_full");
    auto cfg = makeConfig(dir, 2, 0);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write all 8 pages.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 10));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Override: write 1 page.
    std::vector<uint8_t> overridePage(PAGE_SIZE, 0xAA);
    fi->writeFile(overridePage.data(), PAGE_SIZE, 0);
    fi->syncFile();

    // Full rewrite: write all 8 pages.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 100));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Read back.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        for (auto b : buf) {
            assert(b == static_cast<uint8_t>(i + 100));
        }
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testOverrideThenFullRewrite\n");
}

// --- Test: multiple groups with mixed override/full-rewrite ---

static void testMultipleGroupsMixed() {
    auto dir = tmpDir("multi_group");
    auto cfg = makeConfig(dir, 2, 0);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 16 pages (2 groups).
    for (int i = 0; i < 16; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 1));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Group 0: write 1 page (override path).
    std::vector<uint8_t> p0(PAGE_SIZE, 0xDD);
    fi->writeFile(p0.data(), PAGE_SIZE, 2 * PAGE_SIZE);

    // Group 1: write 5 pages (4 frames dirty >= 2 threshold -> full rewrite).
    for (int i = 8; i < 13; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, 0xEE);
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Verify group 0: page 2 changed, others same.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        if (i == 2) {
            for (auto b : buf) assert(b == 0xDD);
        } else {
            for (auto b : buf) assert(b == static_cast<uint8_t>(i + 1));
        }
    }

    // Verify group 1: pages 8-12 changed, 13-15 same.
    for (int i = 8; i < 16; i++) {
        std::vector<uint8_t> buf(PAGE_SIZE, 0);
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        if (i < 13) {
            for (auto b : buf) assert(b == 0xEE);
        } else {
            for (auto b : buf) assert(b == static_cast<uint8_t>(i + 1));
        }
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testMultipleGroupsMixed\n");
}

// --- Test: S3 client overrideFrameKey naming ---

static void testOverrideFrameKeyNaming() {
    S3Config s3cfg;
    s3cfg.endpoint = "https://localhost:1";
    s3cfg.bucket = "test-bucket";
    s3cfg.prefix = "db/tenant1";
    s3cfg.region = "auto";
    s3cfg.accessKey = "ak";
    s3cfg.secretKey = "sk";
    s3cfg.poolSize = 1;
    S3Client client(s3cfg);

    auto key = client.overrideFrameKey(5, 3, 42);
    assert(key == "db/tenant1/pg/5_f3_v42");

    auto key2 = client.overrideFrameKey(0, 0, 1);
    assert(key2 == "db/tenant1/pg/0_f0_v1");

    std::printf("  PASS: testOverrideFrameKeyNaming\n");
}

// --- Test: seekable encode single frame (used for override) ---

static void testEncodeSingleFrame() {
    // Encode a single frame (2 pages), decode, verify.
    std::vector<std::optional<std::vector<uint8_t>>> pages(2);
    pages[0] = std::vector<uint8_t>(PAGE_SIZE, 0x11);
    pages[1] = std::vector<uint8_t>(PAGE_SIZE, 0x22);

    auto result = encodeSeekable(pages, PAGE_SIZE, 2, 3);
    assert(!result.blob.empty());
    assert(result.frameTable.size() == 1);
    assert(result.frameTable[0].pageCount == 2);

    // Decode the frame.
    auto rawFrame = decodeFrame(result.blob, 2, PAGE_SIZE);
    assert(rawFrame.size() == 2 * PAGE_SIZE);

    // Extract pages.
    auto p0 = extractPageFromFrame(rawFrame, 0, PAGE_SIZE);
    auto p1 = extractPageFromFrame(rawFrame, 1, PAGE_SIZE);
    assert(p0.has_value() && p1.has_value());
    for (auto b : *p0) assert(b == 0x11);
    for (auto b : *p1) assert(b == 0x22);

    std::printf("  PASS: testEncodeSingleFrame\n");
}

// --- Test: multiple sequential overrides accumulate correctly ---

static void testSequentialOverrides() {
    auto dir = tmpDir("seq_overrides");
    auto cfg = makeConfig(dir, 2, 0); // No compaction.
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write all 8 pages.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Override 1: write page 0.
    std::vector<uint8_t> ov1(PAGE_SIZE, 0xA1);
    fi->writeFile(ov1.data(), PAGE_SIZE, 0);
    fi->syncFile();

    // Override 2: write page 4.
    std::vector<uint8_t> ov2(PAGE_SIZE, 0xA2);
    fi->writeFile(ov2.data(), PAGE_SIZE, 4 * PAGE_SIZE);
    fi->syncFile();

    // Override 3: write page 0 again (overwrite previous override).
    std::vector<uint8_t> ov3(PAGE_SIZE, 0xA3);
    fi->writeFile(ov3.data(), PAGE_SIZE, 0);
    fi->syncFile();

    // Verify all pages.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xA3);

    fi->readFromFile(buf.data(), PAGE_SIZE, 4 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xA2);

    for (int i = 1; i < 8; i++) {
        if (i == 4) continue;
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(i));
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testSequentialOverrides\n");
}

// --- Test: overrideThreshold=0 (auto) calculation ---

static void testAutoThreshold() {
    auto dir = tmpDir("auto_threshold");
    // Auto threshold: 4 frames / 4 = 1, so ANY dirty frame count >= 1 triggers full rewrite.
    // This means overrides are never used with auto threshold and small groups.
    // Set pagesPerGroup=32, subPagesPerFrame=2 -> 16 frames, threshold=4.
    auto cfg = makeConfig(dir, 0, 0);
    cfg.pagesPerGroup = 32;
    cfg.subPagesPerFrame = 2;
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 32 pages.
    for (int i = 0; i < 32; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Write 2 pages in same frame -> 1 dirty frame < 4 threshold -> override.
    std::vector<uint8_t> p0(PAGE_SIZE, 0xBB);
    fi->writeFile(p0.data(), PAGE_SIZE, 0);
    std::vector<uint8_t> p1(PAGE_SIZE, 0xBC);
    fi->writeFile(p1.data(), PAGE_SIZE, PAGE_SIZE);
    fi->syncFile();

    // Verify data.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xBB);
    fi->readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    for (auto b : buf) assert(b == 0xBC);

    // Other pages unchanged.
    for (int i = 2; i < 32; i++) {
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(i));
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testAutoThreshold\n");
}

// --- Test: disabled overrides (overrideThreshold very high) ---

static void testDisabledOverrides() {
    auto dir = tmpDir("disabled_overrides");
    // Set threshold so high that overrides never trigger: 9999.
    // But effectiveThreshold is based on overrideThreshold config, not auto.
    // Actually: the decision is dirtyFrameSet.size() < effectiveThreshold.
    // With threshold=9999, any dirty frame count < 9999 uses override path.
    // To disable overrides, we use legacy format (subPagesPerFrame=0).
    auto cfg = makeConfig(dir, 2, 0);
    cfg.subPagesPerFrame = 0; // Legacy format, no overrides possible.
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write 8 pages.
    for (int i = 0; i < 8; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 1));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    fi->syncFile();

    // Write 1 page -- with legacy format, full rewrite always happens.
    std::vector<uint8_t> newPage(PAGE_SIZE, 0xFE);
    fi->writeFile(newPage.data(), PAGE_SIZE, 0);
    fi->syncFile();

    // Verify.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xFE);

    for (int i = 1; i < 8; i++) {
        fi->readFromFile(buf.data(), PAGE_SIZE, i * PAGE_SIZE);
        for (auto b : buf) assert(b == static_cast<uint8_t>(i + 1));
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testDisabledOverrides\n");
}

// --- Test: write to new (empty) group -- no existing frame table, no override ---

static void testNewGroupNoOverride() {
    auto dir = tmpDir("new_group");
    auto cfg = makeConfig(dir, 2, 0);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write a single page to an empty database. No existing frame table,
    // so override path should not be used.
    std::vector<uint8_t> page(PAGE_SIZE, 0x42);
    fi->writeFile(page.data(), PAGE_SIZE, 0);
    fi->syncFile();

    // Read back.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    fi->readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0x42);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testNewGroupNoOverride\n");
}

// --- Test: SubframeOverride struct defaults ---

static void testSubframeOverrideDefaults() {
    SubframeOverride ov;
    assert(ov.key.empty());
    assert(ov.entry.offset == 0);
    assert(ov.entry.len == 0);
    assert(ov.entry.pageCount == 0);
    std::printf("  PASS: testSubframeOverrideDefaults\n");
}

// --- Test: manifest JSON with multiple group overrides ---

static void testManifestMultiGroupOverrides() {
    Manifest m;
    m.version = 10;
    m.pageCount = 24;
    m.pageSize = PAGE_SIZE;
    m.pagesPerGroup = PAGES_PER_GROUP;
    m.subPagesPerFrame = SUB_PAGES_PER_FRAME;
    m.pageGroupKeys = {"pg/0_v1", "pg/1_v1", "pg/2_v1"};
    m.frameTables = {
        {{0, 100, 2}, {100, 90, 2}},
        {{0, 80, 2}},
        {{0, 70, 2}, {70, 65, 2}}
    };

    m.subframeOverrides.resize(3);
    m.subframeOverrides[0][0] = {"pg/0_f0_v5", {0, 55, 2}};
    m.subframeOverrides[2][1] = {"pg/2_f1_v7", {0, 60, 2}};

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->subframeOverrides.size() >= 3);
    assert(parsed->subframeOverrides[0].size() == 1);
    assert(parsed->subframeOverrides[0].at(0).key == "pg/0_f0_v5");
    assert(parsed->subframeOverrides[1].empty());
    assert(parsed->subframeOverrides[2].size() == 1);
    assert(parsed->subframeOverrides[2].at(1).key == "pg/2_f1_v7");

    std::printf("  PASS: testManifestMultiGroupOverrides\n");
}

int main() {
    std::printf("=== Phase GraphDrift Tests ===\n");

    // Manifest tests.
    testManifestRoundTrip();
    testManifestBackwardCompat();
    testManifestEmptyOverrides();
    testNormalizeOverrides();
    testSubframeOverrideDefaults();
    testManifestMultiGroupOverrides();

    // Codec tests.
    testEncodeSingleFrame();
    testOverrideFrameKeyNaming();

    // VFS write/read tests.
    testWriteSyncReadWithOverrides();
    testFullRewriteThreshold();
    testOverrideThenFullRewrite();
    testMultipleGroupsMixed();
    testSequentialOverrides();
    testAutoThreshold();
    testDisabledOverrides();
    testNewGroupNoOverride();

    std::printf("All Phase GraphDrift tests passed.\n");
    return 0;
}
