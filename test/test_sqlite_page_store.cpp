#include "sqlite_page_store.h"

#include <cassert>
#include <cstdio>
#include <cstdlib>
#include <filesystem>
#include <optional>
#include <string>
#include <vector>
#include <unistd.h>

using lbug::tiered::GraphPageHint;
using lbug::tiered::SqlitePageStore;

static std::string dbPath(const char* name) {
    auto dir = std::filesystem::temp_directory_path() / "turbograph_sqlite_page_store_tests";
    std::filesystem::create_directories(dir);
    auto path = dir / name;
    std::filesystem::remove(path);
    std::filesystem::remove(std::string(path) + "-wal");
    std::filesystem::remove(std::string(path) + "-shm");
    return path.string();
}

static void testFullAndPartialWritesRoundTrip() {
    SqlitePageStore store(dbPath("roundtrip.db"), 256);
    std::vector<uint8_t> page(256);
    for (size_t i = 0; i < page.size(); i++) {
        page[i] = static_cast<uint8_t>(i);
    }

    store.write("graph.kz", page.data(), page.size(), 0);

    std::vector<uint8_t> patch = {0xA0, 0xA1, 0xA2, 0xA3};
    store.write("graph.kz", patch.data(), patch.size(), 254);

    std::vector<uint8_t> got(260, 0);
    store.read("graph.kz", got.data(), got.size(), 0);

    assert(got[0] == 0);
    assert(got[253] == 253);
    assert(got[254] == 0xA0);
    assert(got[255] == 0xA1);
    assert(got[256] == 0xA2);
    assert(got[257] == 0xA3);
    assert(store.fileSize("graph.kz") == 258);
    std::printf("  PASS: testFullAndPartialWritesRoundTrip\n");
}

static void testMissingPagesReadAsZerosAndPersistAfterReopen() {
    auto path = dbPath("reopen.db");
    {
        SqlitePageStore store(path, 128);
        std::vector<uint8_t> data(16, 0xCC);
        store.write("graph.kz", data.data(), data.size(), 128 * 3 + 8);
    }
    {
        SqlitePageStore store(path, 128);
        std::vector<uint8_t> got(128, 0xFF);
        store.read("graph.kz", got.data(), got.size(), 0);
        for (auto byte : got) {
            assert(byte == 0);
        }
        store.read("graph.kz", got.data(), 16, 128 * 3 + 8);
        for (int i = 0; i < 16; i++) {
            assert(got[i] == 0xCC);
        }
        assert(store.fileSize("graph.kz") == 408);
    }
    std::printf("  PASS: testMissingPagesReadAsZerosAndPersistAfterReopen\n");
}

static void testTruncateDropsTailPagesAndZeroesPartialTail() {
    SqlitePageStore store(dbPath("truncate.db"), 64);
    std::vector<uint8_t> data(192, 0xDD);
    store.write("graph.kz", data.data(), data.size(), 0);

    store.truncate("graph.kz", 70);

    std::vector<uint8_t> got(128, 0);
    store.read("graph.kz", got.data(), got.size(), 0);
    for (int i = 0; i < 70; i++) {
        assert(got[i] == 0xDD);
    }
    for (int i = 70; i < 128; i++) {
        assert(got[i] == 0);
    }
    assert(store.fileSize("graph.kz") == 70);
    std::printf("  PASS: testTruncateDropsTailPagesAndZeroesPartialTail\n");
}

static void testGraphPageHintsRoundTrip() {
    SqlitePageStore store(dbPath("hints.db"), 128);
    std::vector<uint8_t> page(128, 0x11);
    store.writePage("graph.kz", 4, page.data(), page.size());
    store.writePage("graph.kz", 7, page.data(), page.size());

    store.setHint("graph.kz", 4, GraphPageHint{"rel", 42, "csr:42"});
    store.setHint("graph.kz", 7, GraphPageHint{"rel", 42, "csr:42"});

    auto hint = store.hint("graph.kz", 4);
    assert(hint.has_value());
    assert(hint->regionKind == "rel");
    assert(hint->regionId == 42);
    assert(hint->localityKey == "csr:42");

    auto pages = store.pagesForHint("graph.kz", "rel", 42);
    assert((pages == std::vector<uint64_t>{4, 7}));
    std::printf("  PASS: testGraphPageHintsRoundTrip\n");
}

static void testContainerPageSizeDefaultsToFitGraphBlob() {
    SqlitePageStore store(dbPath("page_size.db"), 4096);
    assert(store.pageSize() == 4096);
    assert(store.sqlitePageSize() == 65536);
    assert(store.walAutoCheckpointPages() == 1000);
    assert(store.sqliteCacheSizePages() == 0);
    assert(store.sqliteSynchronous() == 1); // NORMAL
    std::printf("  PASS: testContainerPageSizeDefaultsToFitGraphBlob\n");
}

static void testSqlitePragmasAreConfigurable() {
    SqlitePageStore store(dbPath("pragmas.db"), 4096, std::nullopt, std::nullopt,
        32768, 17, 42, "FULL");
    assert(store.sqlitePageSize() == 32768);
    assert(store.walAutoCheckpointPages() == 17);
    assert(store.sqliteCacheSizePages() == 42);
    assert(store.sqliteSynchronous() == 2); // FULL
    std::printf("  PASS: testSqlitePragmasAreConfigurable\n");
}

static void testBuiltinSqliteVfsName() {
    SqlitePageStore store(dbPath("unix_vfs.db"), 256, std::string("unix"));
    std::vector<uint8_t> data(256, 0xEF);
    store.write("graph.kz", data.data(), data.size(), 0);
    std::vector<uint8_t> got(256, 0);
    store.read("graph.kz", got.data(), got.size(), 0);
    assert(got == data);
    std::printf("  PASS: testBuiltinSqliteVfsName\n");
}

static void testZeroLengthFileStillExists() {
    SqlitePageStore store(dbPath("exists.db"), 256);
    assert(!store.fileExists("empty.kz"));
    store.truncate("empty.kz", 0);
    assert(store.fileExists("empty.kz"));
    assert(store.fileSize("empty.kz") == 0);
    std::printf("  PASS: testZeroLengthFileStillExists\n");
}

static void testActualTurboliteVfsSmoke() {
    auto extension = std::getenv("TURBOGRAPH_TURBOLITE_EXTENSION");
    if (extension == nullptr || std::string(extension).empty()) {
        std::printf("  SKIP: testActualTurboliteVfsSmoke "
                    "(set TURBOGRAPH_TURBOLITE_EXTENSION)\n");
        return;
    }

    auto base = std::filesystem::temp_directory_path() /
                ("turbograph_turbolite_vfs_page_store_" + std::to_string(getpid()));
    std::filesystem::remove_all(base);
    std::filesystem::create_directories(base);
    auto cacheDir = base / "turbolite-cache";
    std::filesystem::create_directories(cacheDir);
    setenv("TURBOLITE_CACHE_DIR", cacheDir.string().c_str(), 1);

    auto path = (base / "page-store.sqlite").string();
    {
        SqlitePageStore store(path, 4096, std::string("turbolite"),
            std::string(extension), 65536, 100, 0, "NORMAL");
        std::vector<uint8_t> page(4096, 0x42);
        store.write("graph.kz", page.data(), page.size(), 0);
        store.write("graph.kz", page.data(), page.size(), page.size() * 9);
        assert(store.sqlitePageSize() == 65536);
        assert(store.walAutoCheckpointPages() == 100);
        assert(store.fileSize("graph.kz") == page.size() * 10);
    }
    {
        SqlitePageStore store(path, 4096, std::string("turbolite"),
            std::string(extension), 65536, 100, 0, "NORMAL");
        std::vector<uint8_t> got(4096, 0);
        store.read("graph.kz", got.data(), got.size(), 4096 * 9);
        for (auto byte : got) {
            assert(byte == 0x42);
        }
    }

    std::filesystem::remove_all(base);
    std::printf("  PASS: testActualTurboliteVfsSmoke\n");
}

int main() {
    std::printf("SqlitePageStore tests...\n");
    testFullAndPartialWritesRoundTrip();
    testMissingPagesReadAsZerosAndPersistAfterReopen();
    testTruncateDropsTailPagesAndZeroesPartialTail();
    testGraphPageHintsRoundTrip();
    testContainerPageSizeDefaultsToFitGraphBlob();
    testSqlitePragmasAreConfigurable();
    testBuiltinSqliteVfsName();
    testZeroLengthFileStillExists();
    testActualTurboliteVfsSmoke();
    std::printf("All SqlitePageStore tests passed.\n");
    return 0;
}
