#include "sqlite_graph_file_system.h"

#include <cassert>
#include <cstdio>
#include <filesystem>
#include <string>
#include <vector>

using namespace lbug::common;
using namespace lbug::tiered;

static std::string dbPath(const char* name) {
    auto dir = std::filesystem::temp_directory_path() / "turbograph_sqlite_graph_fs_tests";
    std::filesystem::create_directories(dir);
    auto path = dir / name;
    std::filesystem::remove(path);
    std::filesystem::remove(std::string(path) + "-wal");
    std::filesystem::remove(std::string(path) + "-shm");
    return path.string();
}

static SqliteGraphFileSystem makeFs(const char* name, uint32_t pageSize = 4096) {
    SqliteGraphFileSystemConfig cfg;
    cfg.sqlitePath = dbPath(name);
    cfg.graphPageSize = pageSize;
    return SqliteGraphFileSystem(std::move(cfg));
}

static void testFileInfoReadWriteTruncate() {
    auto fs = makeFs("rw.db", 256);
    auto fi = fs.openFile("graph.kz", FileOpenFlags(FileFlags::WRITE | FileFlags::CREATE_IF_NOT_EXISTS));

    std::vector<uint8_t> page(256, 0xAA);
    fi->writeFile(page.data(), page.size(), 0);

    std::vector<uint8_t> patch(8, 0xBB);
    fi->writeFile(patch.data(), patch.size(), 252);

    std::vector<uint8_t> got(260, 0);
    fi->readFromFile(got.data(), got.size(), 0);
    for (int i = 0; i < 252; i++) assert(got[i] == 0xAA);
    for (int i = 252; i < 260; i++) assert(got[i] == 0xBB);
    assert(fi->getFileSize() == 260);

    fi->truncate(128);
    assert(fi->getFileSize() == 128);
    std::fill(got.begin(), got.end(), 0xCC);
    fi->readFromFile(got.data(), got.size(), 0);
    for (int i = 0; i < 128; i++) assert(got[i] == 0xAA);
    for (size_t i = 128; i < got.size(); i++) assert(got[i] == 0);

    std::printf("  PASS: testFileInfoReadWriteTruncate\n");
}

static void testReopenReadOnlyAndSequentialRead() {
    auto path = dbPath("reopen_fs.db");
    {
        SqliteGraphFileSystemConfig cfg;
        cfg.sqlitePath = path;
        cfg.graphPageSize = 128;
        SqliteGraphFileSystem fs(std::move(cfg));
        auto fi = fs.openFile("graph.kz", FileOpenFlags(FileFlags::WRITE | FileFlags::CREATE_IF_NOT_EXISTS));
        std::vector<uint8_t> data(300);
        for (size_t i = 0; i < data.size(); i++) data[i] = static_cast<uint8_t>(i & 0xFF);
        fi->writeFile(data.data(), data.size(), 0);
        assert(fs.fileOrPathExists("graph.kz"));
    }
    {
        SqliteGraphFileSystemConfig cfg;
        cfg.sqlitePath = path;
        cfg.graphPageSize = 128;
        SqliteGraphFileSystem fs(std::move(cfg));
        auto fi = fs.openFile("graph.kz", FileOpenFlags(FileFlags::READ_ONLY));
        std::vector<uint8_t> got(32, 0);
        assert(fi->seek(250, SEEK_SET) == 250);
        auto n = fi->readFile(got.data(), got.size());
        assert(n == 32);
        for (int i = 0; i < 32; i++) assert(got[i] == static_cast<uint8_t>((250 + i) & 0xFF));
    }

    std::printf("  PASS: testReopenReadOnlyAndSequentialRead\n");
}

static void testGraphHintsVisibleThroughShimStore() {
    auto fs = makeFs("hints_fs.db", 128);
    auto fi = fs.openFile("graph.kz", FileOpenFlags(FileFlags::WRITE | FileFlags::CREATE_IF_NOT_EXISTS));
    std::vector<uint8_t> page(128, 0x44);
    fi->writeFile(page.data(), page.size(), 128 * 5);

    fs.pageStore().setHint("graph.kz", 5, GraphPageHint{"rel", 99, "csr:99"});
    auto pages = fs.pageStore().pagesForHint("graph.kz", "rel", 99);
    assert((pages == std::vector<uint64_t>{5}));

    std::printf("  PASS: testGraphHintsVisibleThroughShimStore\n");
}

static void testGraphAndSqlitePageSizesCanDiverge() {
    SqliteGraphFileSystemConfig cfg;
    cfg.sqlitePath = dbPath("page_size_fs.db");
    cfg.graphPageSize = 4096;
    cfg.sqlitePageSize = 8192;
    SqliteGraphFileSystem fs(std::move(cfg));
    assert(fs.pageStore().pageSize() == 4096);
    assert(fs.pageStore().sqlitePageSize() == 8192);
    std::printf("  PASS: testGraphAndSqlitePageSizesCanDiverge\n");
}

static void testCreateEmptyFileExists() {
    auto fs = makeFs("empty_fs.db", 256);
    auto fi = fs.openFile("empty.kz", FileOpenFlags(FileFlags::WRITE | FileFlags::CREATE_IF_NOT_EXISTS));
    assert(fi->getFileSize() == 0);
    assert(fs.fileOrPathExists("empty.kz"));
    auto matches = fs.glob(nullptr, "empty.kz");
    assert((matches == std::vector<std::string>{"empty.kz"}));
    std::printf("  PASS: testCreateEmptyFileExists\n");
}

static void testCanHandleOnlyConfiguredDataFile() {
    SqliteGraphFileSystemConfig cfg;
    cfg.sqlitePath = dbPath("routing_fs.db");
    cfg.dataFilePath = "/tmp/graph/data.kz";
    SqliteGraphFileSystem fs(std::move(cfg));
    assert(fs.canHandleFile("/tmp/graph/data.kz"));
    assert(!fs.canHandleFile("/tmp/graph/data.kz.wal"));
    assert(!fs.canHandleFile("/tmp/graph/shadow"));
    std::printf("  PASS: testCanHandleOnlyConfiguredDataFile\n");
}

static void testCanonicalGraphIdSurvivesLocalPathChange() {
    auto path = dbPath("canonical_id_fs.db");
    {
        SqliteGraphFileSystemConfig cfg;
        cfg.sqlitePath = path;
        cfg.dataFilePath = "/tmp/graph-a/data.kz";
        cfg.dataFileId = "graph:stable-id";
        cfg.graphPageSize = 128;
        SqliteGraphFileSystem fs(std::move(cfg));

        auto fi = fs.openFile("/tmp/graph-a/data.kz",
            FileOpenFlags(FileFlags::WRITE | FileFlags::CREATE_IF_NOT_EXISTS));
        std::vector<uint8_t> data(300);
        for (size_t i = 0; i < data.size(); i++) data[i] = static_cast<uint8_t>((i * 7) & 0xFF);
        fi->writeFile(data.data(), data.size(), 0);
        assert(fs.fileOrPathExists("/tmp/graph-a/data.kz"));
        assert(!fs.pageStore().fileExists("/tmp/graph-a/data.kz"));
        assert(fs.pageStore().fileExists("graph:stable-id"));
    }
    {
        SqliteGraphFileSystemConfig cfg;
        cfg.sqlitePath = path;
        cfg.dataFilePath = "/tmp/graph-b/data.kz";
        cfg.dataFileId = "graph:stable-id";
        cfg.graphPageSize = 128;
        SqliteGraphFileSystem fs(std::move(cfg));

        assert(fs.canHandleFile("/tmp/graph-b/data.kz"));
        assert(fs.fileOrPathExists("/tmp/graph-b/data.kz"));
        auto fi = fs.openFile("/tmp/graph-b/data.kz", FileOpenFlags(FileFlags::READ_ONLY));
        std::vector<uint8_t> got(300);
        fi->readFromFile(got.data(), got.size(), 0);
        for (size_t i = 0; i < got.size(); i++) assert(got[i] == static_cast<uint8_t>((i * 7) & 0xFF));
    }

    std::printf("  PASS: testCanonicalGraphIdSurvivesLocalPathChange\n");
}

int main() {
    std::printf("SqliteGraphFileSystem tests...\n");
    testFileInfoReadWriteTruncate();
    testReopenReadOnlyAndSequentialRead();
    testGraphHintsVisibleThroughShimStore();
    testGraphAndSqlitePageSizesCanDiverge();
    testCreateEmptyFileExists();
    testCanHandleOnlyConfiguredDataFile();
    testCanonicalGraphIdSurvivesLocalPathChange();
    std::printf("All SqliteGraphFileSystem tests passed.\n");
    return 0;
}
