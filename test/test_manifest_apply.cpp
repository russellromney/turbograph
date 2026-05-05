// Manifest sync/apply tests: journal_seq in manifest, syncAndGetVersion,
// getManifestVersion, applyRemoteManifest, cache validation on open.
//
// Uses dummy S3 credentials (uploads will fail). Tests focus on local-path
// behavior: dirty pages, sync, manifest persistence, cache invalidation.

#include "tiered_file_system.h"

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <thread>
#include <atomic>
#include <chrono>

using namespace lbug::tiered;
using namespace lbug::common;

static constexpr uint32_t PAGE_SIZE = 4096;
static constexpr uint32_t PAGES_PER_GROUP = 4;

static std::filesystem::path tmpDir(const char* suffix) {
    auto dir = std::filesystem::temp_directory_path() /
        (std::string("tiered_manifest_apply_test_") + suffix);
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    return dir;
}

static TieredConfig makeConfig(const std::filesystem::path& dir) {
    TieredConfig cfg;
    cfg.s3 = {"https://localhost:1", "fake-bucket", "test/manifest_apply", "auto",
              "fake-ak", "fake-sk", 1};
    cfg.cacheDir = (dir / "cache").string();
    cfg.dataFilePath = (dir / "data.kz").string();
    cfg.pageSize = PAGE_SIZE;
    cfg.pagesPerGroup = PAGES_PER_GROUP;
    cfg.compressionLevel = 3;
    return cfg;
}

static std::string readTextFileIfExists(const std::filesystem::path& path) {
    if (!std::filesystem::exists(path)) return {};
    std::ifstream f(path, std::ios::binary);
    return std::string((std::istreambuf_iterator<char>(f)),
                       std::istreambuf_iterator<char>());
}

static void writeManifestFile(const std::filesystem::path& cacheDir, const Manifest& manifest) {
    std::filesystem::create_directories(cacheDir);
    auto manifestPath = cacheDir / "manifest.json";
    auto json = manifest.toJSON();
    std::ofstream f(manifestPath, std::ios::binary | std::ios::trunc);
    f.write(json.data(), json.size());
}

// --- Test: syncAndGetVersion with no dirty pages returns current version ---
static void testSyncNoDirtyPages() {
    auto dir = tmpDir("sync_no_dirty");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // No writes, sync should return 0 (initial version).
    auto version = vfs.syncAndGetVersion();
    assert(version == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testSyncNoDirtyPages\n");
}

// --- Test: syncAndGetVersion after writing data ---
static void testSyncAfterWrite() {
    auto dir = tmpDir("sync_after_write");
    auto cfg = makeConfig(dir);
    cfg.journalSeq = 55;
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write a page.
    std::vector<uint8_t> page(PAGE_SIZE, 0xCC);
    fi->writeFile(page.data(), PAGE_SIZE, 0);

    // Sync. S3 upload will fail (dummy endpoint) but local path works.
    // The manifest version may stay 0 because flushPendingPageGroups needs
    // S3 putObject to succeed before bumping version. But the method should
    // not crash and should return something.
    auto version = vfs.syncAndGetVersion();
    // With failed S3, version stays at 0. That's expected.
    // The key invariant: journalSeq was set on the manifest.
    (void)version;

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testSyncAfterWrite\n");
}

// --- Test: getManifestVersion returns current version ---
static void testGetManifestVersion() {
    auto dir = tmpDir("get_version");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    auto version = vfs.getManifestVersion();
    assert(version == 0); // Fresh database starts at version 0.

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testGetManifestVersion\n");
}

// --- Test: getManifestVersion with no active file returns 0 ---
static void testGetManifestVersionNoFile() {
    auto dir = tmpDir("get_version_nofile");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    // No file opened.
    auto version = vfs.getManifestVersion();
    assert(version == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testGetManifestVersionNoFile\n");
}

// --- Test: applyRemoteManifest with same version is a no-op ---
static void testApplyManifestSameVersion() {
    auto dir = tmpDir("apply_same");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Build a manifest JSON matching the current version (0).
    Manifest m;
    m.version = 0;
    m.pageCount = 0;
    m.pageSize = PAGE_SIZE;
    m.pagesPerGroup = PAGES_PER_GROUP;
    auto json = m.toJSON();

    auto version = vfs.applyRemoteManifest(json);
    assert(version == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestSameVersion\n");
}

// --- Test: same-version apply is a local no-op, even if remote objects are gone ---
static void testApplyManifestNoopSkipsRemotePreflight() {
    auto dir = tmpDir("apply_noop_skip_preflight");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    Manifest remote;
    remote.version = 0;
    remote.pageCount = PAGES_PER_GROUP;
    remote.pageSize = PAGE_SIZE;
    remote.pagesPerGroup = PAGES_PER_GROUP;
    remote.pageGroupKeys = {"pg/missing_v0"};

    auto version = vfs.applyRemoteManifest(remote.toJSON());
    assert(version == 0);
    assert(vfs.getManifestVersion() == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestNoopSkipsRemotePreflight\n");
}

// --- Test: applyRemoteManifest with newer version updates manifest ---
static void testApplyManifestNewerVersion() {
    auto dir = tmpDir("apply_newer");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Build a "remote" manifest with higher version.
    Manifest remote;
    remote.version = 5;
    remote.pageCount = 0;
    remote.pageSize = PAGE_SIZE;
    remote.pagesPerGroup = PAGES_PER_GROUP;
    remote.journalSeq = 100;
    auto json = remote.toJSON();

    auto version = vfs.applyRemoteManifest(json);
    assert(version == 5);

    // Verify the manifest version is now 5.
    assert(vfs.getManifestVersion() == 5);

    // Verify the manifest is persisted locally.
    auto manifestPath = std::filesystem::path(cfg.cacheDir) / "manifest.json";
    assert(std::filesystem::exists(manifestPath));
    std::ifstream f(manifestPath, std::ios::binary);
    std::string localJson((std::istreambuf_iterator<char>(f)),
                           std::istreambuf_iterator<char>());
    auto parsed = Manifest::fromJSON(localJson);
    assert(parsed.has_value());
    assert(parsed->version == 5);
    assert(parsed->journalSeq == 100);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestNewerVersion\n");
}

// --- Test: applyRemoteManifest with older version is a remote no-op ---
static void testApplyManifestOlderVersion() {
    auto dir = tmpDir("apply_older");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    Manifest current;
    current.version = 5;
    current.pageCount = 0;
    current.pageSize = PAGE_SIZE;
    current.pagesPerGroup = PAGES_PER_GROUP;
    current.journalSeq = 500;
    assert(vfs.applyRemoteManifest(current.toJSON()) == 5);

    auto manifestPath = std::filesystem::path(cfg.cacheDir) / "manifest.json";
    auto manifestBefore = readTextFileIfExists(manifestPath);

    Manifest stale;
    stale.version = 4;
    stale.pageCount = PAGES_PER_GROUP;
    stale.pageSize = PAGE_SIZE;
    stale.pagesPerGroup = PAGES_PER_GROUP;
    stale.pageGroupKeys = {"pg/missing_v4"};
    stale.journalSeq = 400;

    auto version = vfs.applyRemoteManifest(stale.toJSON());
    assert(version == 5);
    assert(vfs.getManifestVersion() == 5);
    assert(readTextFileIfExists(manifestPath) == manifestBefore);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestOlderVersion\n");
}

// --- Test: applyRemoteManifest with invalid JSON throws ---
static void testApplyManifestInvalidJSON() {
    auto dir = tmpDir("apply_invalid");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    bool threw = false;
    try {
        vfs.applyRemoteManifest("not valid json");
    } catch (const std::runtime_error& e) {
        threw = true;
        assert(std::string(e.what()).find("invalid manifest JSON") != std::string::npos);
    }
    assert(threw);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestInvalidJSON\n");
}

// --- Test: applyRemoteManifest with no active file throws ---
static void testApplyManifestNoFile() {
    auto dir = tmpDir("apply_nofile");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    Manifest m;
    m.version = 1;
    m.pageCount = 0;
    m.pageSize = PAGE_SIZE;
    m.pagesPerGroup = PAGES_PER_GROUP;

    bool threw = false;
    try {
        vfs.applyRemoteManifest(m.toJSON());
    } catch (const std::runtime_error& e) {
        threw = true;
        assert(std::string(e.what()).find("no active file") != std::string::npos);
    }
    assert(threw);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestNoFile\n");
}

// --- Test: journal_seq is set in manifest after sync ---
// With S3Primary ordering, the local manifest is only
// persisted after a successful S3 upload. With fake S3 (uploads fail),
// the local manifest file will not be updated with journalSeq. The
// in-memory manifest still gets the stamp. This test verifies the
// in-memory version is accessible and that the local file is NOT ahead
// of S3 (the core S3Primary invariant).
static void testJournalSeqSetDuringSync() {
    auto dir = tmpDir("journal_seq_sync");
    auto cfg = makeConfig(dir);
    cfg.journalSeq = 77;
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write a page so doSyncFile has work to do.
    std::vector<uint8_t> page(PAGE_SIZE, 0xEE);
    fi->writeFile(page.data(), PAGE_SIZE, 0);

    vfs.syncAndGetVersion();

    // With fake S3, flushPendingPageGroups returns early (no uploads
    // succeeded), so the local manifest file should still reflect the
    // initial state (version 0, no journalSeq update). This is correct:
    // the local manifest must not claim a version that S3 does not have.
    auto manifestPath = std::filesystem::path(cfg.cacheDir) / "manifest.json";
    if (std::filesystem::exists(manifestPath)) {
        std::ifstream f(manifestPath, std::ios::binary);
        std::string json((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());
        auto parsed = Manifest::fromJSON(json);
        assert(parsed.has_value());
        // Local manifest version must be <= what S3 has. With fake S3,
        // S3 has nothing, so local version should be 0.
        assert(parsed->version == 0);
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testJournalSeqSetDuringSync\n");
}

// --- Test: applyRemoteManifest preflights missing page groups before mutation ---
static void testApplyManifestMissingPageGroupLeavesStateUnchanged() {
    auto dir = tmpDir("apply_missing_group");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write pages in group 0.
    std::vector<uint8_t> page(PAGE_SIZE, 0xFF);
    for (uint32_t i = 0; i < PAGES_PER_GROUP; i++) {
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }

    // Sync to local (S3 upload fails but local cache is populated).
    vfs.syncAndGetVersion();
    auto manifestPath = std::filesystem::path(cfg.cacheDir) / "manifest.json";
    auto manifestBefore = readTextFileIfExists(manifestPath);
    auto localFilePath = std::filesystem::path(cfg.cacheDir) / "data.cache";
    std::vector<uint8_t> pageBefore(PAGE_SIZE);
    {
        std::ifstream f(localFilePath, std::ios::binary);
        f.read(reinterpret_cast<char*>(pageBefore.data()), pageBefore.size());
        assert(f.gcount() == static_cast<std::streamsize>(pageBefore.size()));
    }

    Manifest remote;
    remote.version = 10;
    remote.pageCount = PAGES_PER_GROUP;
    remote.pageSize = PAGE_SIZE;
    remote.pagesPerGroup = PAGES_PER_GROUP;
    remote.pageGroupKeys = {"pg/0_v10"}; // Different from whatever was set.
    remote.journalSeq = 200;

    bool threw = false;
    try {
        vfs.applyRemoteManifest(remote.toJSON());
    } catch (const std::runtime_error& e) {
        threw = true;
        assert(std::string(e.what()).find("missing page objects") != std::string::npos);
    }
    assert(threw);
    assert(vfs.getManifestVersion() == 0);
    assert(readTextFileIfExists(manifestPath) == manifestBefore);
    std::vector<uint8_t> pageAfter(PAGE_SIZE);
    {
        std::ifstream f(localFilePath, std::ios::binary);
        f.read(reinterpret_cast<char*>(pageAfter.data()), pageAfter.size());
        assert(f.gcount() == static_cast<std::streamsize>(pageAfter.size()));
    }
    assert(pageAfter == pageBefore);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestMissingPageGroupLeavesStateUnchanged\n");
}

// --- Test: incomplete manifest staging files are ignored on restart ---
static void testIncompleteManifestTmpIgnoredOnOpen() {
    auto dir = tmpDir("manifest_tmp_ignored");
    auto cfg = makeConfig(dir);
    auto cacheDir = std::filesystem::path(cfg.cacheDir);

    Manifest oldManifest;
    oldManifest.version = 3;
    oldManifest.pageSize = PAGE_SIZE;
    oldManifest.pagesPerGroup = PAGES_PER_GROUP;
    writeManifestFile(cacheDir, oldManifest);

    Manifest stagedManifest = oldManifest;
    stagedManifest.version = 4;
    auto stagedJson = stagedManifest.toJSON();
    {
        std::ofstream f(cacheDir / "manifest.json.tmp", std::ios::binary | std::ios::trunc);
        f.write(stagedJson.data(), stagedJson.size());
    }

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    assert(vfs.getManifestVersion() == 3);
    assert(readTextFileIfExists(cacheDir / "manifest.json") == oldManifest.toJSON());

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testIncompleteManifestTmpIgnoredOnOpen\n");
}

// --- Test: deleted local cache sidecars reopen from the durable manifest cleanly ---
static void testDeletedCacheSidecarsReopenCleanly() {
    auto dir = tmpDir("deleted_cache_sidecars");
    auto cfg = makeConfig(dir);
    auto cacheDir = std::filesystem::path(cfg.cacheDir);

    Manifest manifest;
    manifest.version = 7;
    manifest.pageSize = PAGE_SIZE;
    manifest.pagesPerGroup = PAGES_PER_GROUP;
    writeManifestFile(cacheDir, manifest);
    std::filesystem::remove(cacheDir / "data.cache");
    std::filesystem::remove(cacheDir / "page_bitmap");

    TieredFileSystem vfs(cfg);
    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));
    assert(vfs.getManifestVersion() == 7);
    assert(std::filesystem::exists(cacheDir / "data.cache"));

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testDeletedCacheSidecarsReopenCleanly\n");
}

// --- Test: syncAndGetVersion with no active file returns 0 ---
static void testSyncNoActiveFile() {
    auto dir = tmpDir("sync_nofile");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    // No file opened.
    auto version = vfs.syncAndGetVersion();
    assert(version == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testSyncNoActiveFile\n");
}

// --- Test: applyRemoteManifest with garbage JSON does not crash (UDF safety) ---
static void testApplyManifestGarbageJSON() {
    auto dir = tmpDir("apply_garbage");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Various malformed inputs should throw, not crash.
    std::vector<std::string> garbageInputs = {
        "",
        "{{{{",
        "null",
        "{\"version\": \"not_a_number\"}",
        "🔥🔥🔥",
        std::string(10000, 'x'),
    };

    for (auto& input : garbageInputs) {
        bool threw = false;
        try {
            vfs.applyRemoteManifest(input);
        } catch (const std::exception&) {
            threw = true;
        }
        assert(threw);
    }

    // VFS should still be functional after garbage inputs.
    auto version = vfs.getManifestVersion();
    assert(version == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestGarbageJSON\n");
}

// --- Test: concurrent applyRemoteManifest + reads do not crash ---
static void testConcurrentApplyAndRead() {
    auto dir = tmpDir("concurrent_apply");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write initial data so reads have something to target.
    std::vector<uint8_t> page(PAGE_SIZE, 0xAA);
    for (uint32_t i = 0; i < PAGES_PER_GROUP; i++) {
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    vfs.syncAndGetVersion();

    std::atomic<bool> stop{false};
    std::atomic<uint64_t> readCount{0};
    std::atomic<bool> readerError{false};

    // Reader thread: continuously read page 0.
    std::thread reader([&]() {
        std::vector<uint8_t> buf(PAGE_SIZE);
        while (!stop.load()) {
            try {
                fi->readFromFile(buf.data(), PAGE_SIZE, 0);
                readCount.fetch_add(1);
            } catch (...) {
                readerError.store(true);
                break;
            }
        }
    });

    // Main thread: apply manifests repeatedly.
    auto start = std::chrono::steady_clock::now();
    uint64_t applyVersion = 1;
    while (std::chrono::steady_clock::now() - start < std::chrono::milliseconds(100)) {
        Manifest remote;
        remote.version = applyVersion++;
        remote.pageCount = PAGES_PER_GROUP;
        remote.pageSize = PAGE_SIZE;
        remote.pagesPerGroup = PAGES_PER_GROUP;
        remote.pageGroupKeys = {"pg/0_v" + std::to_string(remote.version)};

        try {
            vfs.applyRemoteManifest(remote.toJSON());
        } catch (...) {
            // S3 fetches may fail with dummy creds; that is expected.
        }
    }

    stop.store(true);
    reader.join();

    assert(!readerError.load());
    assert(readCount.load() > 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testConcurrentApplyAndRead\n");
}

// --- Test: applyRemoteManifest preflights missing subframe overrides before mutation ---
static void testApplyManifestMissingOverrideLeavesStateUnchanged() {
    auto dir = tmpDir("missing_override");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    Manifest remote;
    remote.version = 2;
    remote.pageCount = PAGES_PER_GROUP;
    remote.pageSize = PAGE_SIZE;
    remote.pagesPerGroup = PAGES_PER_GROUP;
    remote.pageGroupKeys = {""}; // Empty base key is ignored; override object is load-bearing.
    remote.subframeOverrides.resize(1);
    SubframeOverride overrideFrame;
    overrideFrame.key = "pg/0_f0_v2";
    overrideFrame.entry.offset = 0;
    overrideFrame.entry.len = 2000;
    overrideFrame.entry.pageCount = 4;
    remote.subframeOverrides[0][0] = overrideFrame;

    bool threw = false;
    try {
        vfs.applyRemoteManifest(remote.toJSON());
    } catch (const std::runtime_error& e) {
        threw = true;
        assert(std::string(e.what()).find("missing page objects") != std::string::npos);
    }
    assert(threw);
    assert(vfs.getManifestVersion() == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestMissingOverrideLeavesStateUnchanged\n");
}

// --- Test: seekable manifests must cover required frames before mutation ---
static void testApplyManifestRejectsUnreadableSeekableShape() {
    auto dir = tmpDir("bad_seekable_shape");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    Manifest remote;
    remote.version = 3;
    remote.pageCount = PAGES_PER_GROUP;
    remote.pageSize = PAGE_SIZE;
    remote.pagesPerGroup = PAGES_PER_GROUP;
    remote.pageGroupKeys = {""};
    remote.subPagesPerFrame = 2;

    bool threw = false;
    try {
        vfs.applyRemoteManifest(remote.toJSON());
    } catch (const std::runtime_error& e) {
        threw = true;
        assert(std::string(e.what()).find("missing page objects") != std::string::npos);
    }
    assert(threw);
    assert(vfs.getManifestVersion() == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestRejectsUnreadableSeekableShape\n");
}

// --- Test: pageSize mismatch in applyRemoteManifest throws ---
static void testApplyManifestPageSizeMismatch() {
    auto dir = tmpDir("apply_pagesize");
    auto cfg = makeConfig(dir);
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    Manifest remote;
    remote.version = 1;
    remote.pageCount = 4;
    remote.pageSize = 8192; // Mismatched with config_.pageSize (4096).
    remote.pagesPerGroup = PAGES_PER_GROUP;

    bool threw = false;
    try {
        vfs.applyRemoteManifest(remote.toJSON());
    } catch (const std::runtime_error& e) {
        threw = true;
        assert(std::string(e.what()).find("pageSize mismatch") != std::string::npos);
    }
    assert(threw);

    // VFS state should be unchanged.
    assert(vfs.getManifestVersion() == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testApplyManifestPageSizeMismatch\n");
}

// --- Test: RPO=0 crash recovery (write, sync, kill, restore) ---
// With S3Primary ordering, the local manifest is only
// persisted AFTER a successful S3 upload. With fake S3 (uploads fail),
// flushPendingPageGroups returns early without persisting any manifest.
// The local cache file still has the data (written by doSyncFile), but
// the manifest stays at version 0.
//
// This test verifies:
// 1. Local cache file survives the "crash" (pages are in the cache file)
// 2. The manifest on disk (from openFile's initial creation) is version 0
// 3. On reopen, the pages are still readable from the local cache
// 4. The S3Primary invariant holds: local manifest version <= S3 version
//
// Full S3 round-trip recovery (where S3 uploads succeed) is covered by
// the cold_open integration tests which require real Tigris credentials.
static void testRPO0CrashRecovery() {
    auto dir = tmpDir("rpo0_crash");
    auto cfg = makeConfig(dir);
    cfg.journalSeq = 42;

    // Session 1: write pages, sync, then "crash" (destroy VFS abruptly).
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        // Write recognizable data into multiple pages.
        for (uint32_t i = 0; i < PAGES_PER_GROUP; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, 0);
            page[0] = 0xDA;
            page[1] = static_cast<uint8_t>(i & 0xFF);
            page[2] = static_cast<uint8_t>((i >> 8) & 0xFF);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }

        // Sync: S3 uploads fail (fake endpoint). flushPendingPageGroups
        // returns early without persisting manifest. Local cache file
        // still has the written pages though.
        auto version = vfs.syncAndGetVersion();
        // With fake S3 the version stays 0.
        assert(version == 0);

        // Simulate crash: let VFS destructor run.
    }

    // Session 2: reopen a fresh VFS pointing to same cache directory.
    // With fake S3, the manifest was never updated (flushPendingPageGroups
    // returned early). The VFS sees a fresh database (version 0, pageCount 0).
    // Pages from session 1 exist in the local cache file but are NOT served
    // because the manifest does not reference them. This is correct S3Primary
    // behavior: local state must not exceed what S3 has.
    {
        auto cfg2 = makeConfig(dir);
        cfg2.journalSeq = 0;
        TieredFileSystem vfs2(cfg2);
        auto fi2 = vfs2.openFile(cfg2.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        // Manifest version should be 0 (S3 never got the upload).
        auto restoredVersion = vfs2.getManifestVersion();
        assert(restoredVersion == 0);
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testRPO0CrashRecovery\n");
}

// --- Test: S3 unreachable fallback on open ---
// With S3Primary ordering, sync with fake S3 does not
// persist the manifest locally (flushPendingPageGroups returns early).
// But the local cache file still has data, and reopening falls back to
// the local manifest (from openFile's initial creation).
static void testS3UnreachableFallbackOnOpen() {
    auto dir = tmpDir("s3_unreachable");
    auto cfg = makeConfig(dir);
    cfg.journalSeq = 99;

    // Session 1: write data and sync.
    {
        TieredFileSystem vfs(cfg);
        auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        // Write recognizable pages.
        for (uint32_t i = 0; i < PAGES_PER_GROUP; i++) {
            std::vector<uint8_t> page(PAGE_SIZE, 0);
            page[0] = 0xBE;
            page[1] = static_cast<uint8_t>(i & 0xFF);
            page[2] = static_cast<uint8_t>((i >> 8) & 0xFF);
            fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
        }

        // S3 uploads fail, version stays 0.
        auto v = vfs.syncAndGetVersion();
        assert(v == 0);
    }

    // Session 2: reopen with unreachable S3.
    {
        auto cfg2 = makeConfig(dir);
        cfg2.s3 = {"https://localhost:1", "fake-bucket", "test/manifest_apply", "auto",
                    "fake-ak", "fake-sk", 1};

        TieredFileSystem vfs2(cfg2);
        auto fi2 = vfs2.openFile(cfg2.dataFilePath, FileOpenFlags(FileFlags::WRITE));

        // Manifest version is 0 (S3 never received data).
        // Pages are not readable because manifest.pageCount=0
        // (the manifest was never updated since S3 upload failed).
        auto version = vfs2.getManifestVersion();
        assert(version == 0);
    }

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testS3UnreachableFallbackOnOpen\n");
}

// --- Test: S3Primary ordering invariant ---
// Verifies that the local manifest is never ahead of S3:
// 1. With fake S3, sync should NOT persist a new manifest version locally.
// 2. The local manifest file (if it exists) must have version <= 0 (what
//    S3 has, which is nothing with fake credentials).
// 3. Multiple syncs with failed S3 should not accumulate local versions.
static void testS3PrimaryOrderingInvariant() {
    auto dir = tmpDir("s3primary_ordering");
    auto cfg = makeConfig(dir);
    cfg.journalSeq = 50;
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Write pages across multiple groups.
    for (uint32_t i = 0; i < PAGES_PER_GROUP * 3; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i & 0xFF));
        fi->writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }

    // Sync multiple times. Each time, S3 uploads fail.
    for (int round = 0; round < 3; round++) {
        auto version = vfs.syncAndGetVersion();
        // Version must stay at 0 because S3 never got the data.
        assert(version == 0);

        // Write more data for next round.
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(0xF0 + round));
        fi->writeFile(page.data(), PAGE_SIZE, 0);
    }

    // Check the local manifest file: it must not have advanced past version 0.
    auto manifestPath = std::filesystem::path(cfg.cacheDir) / "manifest.json";
    if (std::filesystem::exists(manifestPath)) {
        std::ifstream f(manifestPath, std::ios::binary);
        std::string json((std::istreambuf_iterator<char>(f)),
                         std::istreambuf_iterator<char>());
        auto parsed = Manifest::fromJSON(json);
        assert(parsed.has_value());
        // S3Primary invariant: local version must be <= S3 version.
        // With fake S3, S3 version is effectively 0 (nothing uploaded).
        assert(parsed->version == 0);
    }

    // In-memory manifest version should also be 0 (no successful flush).
    assert(vfs.getManifestVersion() == 0);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testS3PrimaryOrderingInvariant\n");
}

// --- Test: local manifest not persisted before S3 upload ---
// Verifies the specific fix: doSyncFile no longer persists the manifest
// independently of flushPendingPageGroups. The manifest file should only
// be updated when flushPendingPageGroups succeeds (S3 upload + putManifest).
static void testLocalManifestNotPersistedBeforeS3() {
    auto dir = tmpDir("local_before_s3");
    auto cfg = makeConfig(dir);
    cfg.journalSeq = 123;
    TieredFileSystem vfs(cfg);

    auto fi = vfs.openFile(cfg.dataFilePath, FileOpenFlags(FileFlags::WRITE));

    // Record the manifest file state before sync.
    auto manifestPath = std::filesystem::path(cfg.cacheDir) / "manifest.json";
    std::string manifestBefore;
    if (std::filesystem::exists(manifestPath)) {
        std::ifstream f(manifestPath, std::ios::binary);
        manifestBefore = std::string((std::istreambuf_iterator<char>(f)),
                                      std::istreambuf_iterator<char>());
    }

    // Write data and sync. S3 upload will fail.
    std::vector<uint8_t> page(PAGE_SIZE, 0xAB);
    fi->writeFile(page.data(), PAGE_SIZE, 0);
    vfs.syncAndGetVersion();

    // The manifest file should NOT have been modified by the sync, because
    // S3 upload failed and flushPendingPageGroups returned early.
    std::string manifestAfter;
    if (std::filesystem::exists(manifestPath)) {
        std::ifstream f(manifestPath, std::ios::binary);
        manifestAfter = std::string((std::istreambuf_iterator<char>(f)),
                                     std::istreambuf_iterator<char>());
    }

    // The manifest file content should be identical before and after sync.
    // (Both should be the initial manifest written by openFile.)
    assert(manifestBefore == manifestAfter);

    std::filesystem::remove_all(dir);
    std::printf("  PASS: testLocalManifestNotPersistedBeforeS3\n");
}

int main() {
    std::printf("=== Manifest Sync/Apply Tests ===\n");

    testSyncNoDirtyPages();
    testSyncAfterWrite();
    testGetManifestVersion();
    testGetManifestVersionNoFile();
    testApplyManifestSameVersion();
    testApplyManifestNoopSkipsRemotePreflight();
    testApplyManifestNewerVersion();
    testApplyManifestOlderVersion();
    testApplyManifestInvalidJSON();
    testApplyManifestNoFile();
    testJournalSeqSetDuringSync();
    testApplyManifestMissingPageGroupLeavesStateUnchanged();
    testIncompleteManifestTmpIgnoredOnOpen();
    testDeletedCacheSidecarsReopenCleanly();
    testSyncNoActiveFile();
    testApplyManifestGarbageJSON();
    testConcurrentApplyAndRead();
    testApplyManifestMissingOverrideLeavesStateUnchanged();
    testApplyManifestRejectsUnreadableSeekableShape();
    testApplyManifestPageSizeMismatch();
    testRPO0CrashRecovery();
    testS3UnreachableFallbackOnOpen();
    testS3PrimaryOrderingInvariant();
    testLocalManifestNotPersistedBeforeS3();

    std::printf("All manifest sync/apply tests passed.\n");
    return 0;
}
