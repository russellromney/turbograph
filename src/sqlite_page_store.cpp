#include "sqlite_page_store.h"

#include <algorithm>
#include <cctype>
#include <cstring>
#include <string_view>
#include <stdexcept>

#include <sqlite3.h>

namespace lbug {
namespace tiered {

namespace {

[[noreturn]] void throwSqlite(sqlite3* db, const std::string& context) {
    throw std::runtime_error(context + ": " + sqlite3_errmsg(db));
}

class Statement {
public:
    Statement(sqlite3* db, const char* sql) : db_(db) {
        if (sqlite3_prepare_v2(db_, sql, -1, &stmt_, nullptr) != SQLITE_OK) {
            throwSqlite(db_, std::string("prepare ") + sql);
        }
    }

    ~Statement() {
        if (stmt_) {
            sqlite3_finalize(stmt_);
        }
    }

    sqlite3_stmt* get() { return stmt_; }

    bool stepRow() {
        auto rc = sqlite3_step(stmt_);
        if (rc == SQLITE_ROW) return true;
        if (rc == SQLITE_DONE) return false;
        throwSqlite(db_, "sqlite step");
    }

    void stepDone() {
        auto rc = sqlite3_step(stmt_);
        if (rc != SQLITE_DONE) {
            throwSqlite(db_, "sqlite step done");
        }
    }

private:
    sqlite3* db_;
    sqlite3_stmt* stmt_ = nullptr;
};

void bindText(sqlite3* db, sqlite3_stmt* stmt, int idx, const std::string& value) {
    if (sqlite3_bind_text(stmt, idx, value.data(), static_cast<int>(value.size()),
            SQLITE_TRANSIENT) != SQLITE_OK) {
        throwSqlite(db, "bind text");
    }
}

void bindU64(sqlite3* db, sqlite3_stmt* stmt, int idx, uint64_t value) {
    if (sqlite3_bind_int64(stmt, idx, static_cast<sqlite3_int64>(value)) != SQLITE_OK) {
        throwSqlite(db, "bind int64");
    }
}

std::string normalizeSynchronous(std::string value) {
    std::transform(value.begin(), value.end(), value.begin(),
        [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    if (value == "0") return "OFF";
    if (value == "1") return "NORMAL";
    if (value == "2") return "FULL";
    if (value == "3") return "EXTRA";
    if (value == "OFF" || value == "NORMAL" || value == "FULL" ||
        value == "EXTRA") {
        return value;
    }
    throw std::invalid_argument("unsupported SQLite synchronous mode: " + value);
}

bool endsWith(std::string_view value, std::string_view suffix) {
    return value.size() >= suffix.size() &&
           value.substr(value.size() - suffix.size()) == suffix;
}

std::string normalizeExtensionPathForSqlite(std::string path) {
#if defined(__APPLE__)
    if (endsWith(path, ".dylib")) {
        path.resize(path.size() - std::string_view(".dylib").size());
    }
#elif defined(__linux__)
    if (endsWith(path, ".so")) {
        path.resize(path.size() - std::string_view(".so").size());
    }
#endif
    return path;
}

} // namespace

SqlitePageStore::SqlitePageStore(std::string path, uint32_t pageSize,
    std::optional<std::string> vfsName,
    std::optional<std::string> loadableExtensionPath, uint32_t sqlitePageSize,
    uint32_t walAutoCheckpointPages, int32_t cacheSizePages, std::string synchronous)
    : path_(std::move(path)), pageSize_(pageSize) {
    if (pageSize_ == 0) {
        throw std::invalid_argument("SqlitePageStore pageSize must be non-zero");
    }
    open(std::move(vfsName), std::move(loadableExtensionPath), sqlitePageSize,
        walAutoCheckpointPages, cacheSizePages, synchronous);
    migrate();
}

SqlitePageStore::~SqlitePageStore() {
    if (db_) {
        sqlite3_close(db_);
    }
}

void SqlitePageStore::registerLoadableExtension(const std::string& path) {
#if defined(TURBOGRAPH_SQLITE_ENABLE_LOAD_EXTENSION)
    auto sqlitePath = normalizeExtensionPathForSqlite(path);
    sqlite3* bootstrap = nullptr;
    if (sqlite3_open(":memory:", &bootstrap) != SQLITE_OK) {
        throwSqlite(bootstrap, "open sqlite extension bootstrap");
    }
    if (sqlite3_enable_load_extension(bootstrap, 1) != SQLITE_OK) {
        auto msg = std::string("enable sqlite load extension: ") + sqlite3_errmsg(bootstrap);
        sqlite3_close(bootstrap);
        throw std::runtime_error(msg);
    }
    char* err = nullptr;
    auto rc = sqlite3_load_extension(bootstrap, sqlitePath.c_str(), "sqlite3_turbolite_init",
        &err);
    if (rc != SQLITE_OK) {
        std::string msg = err ? err : sqlite3_errmsg(bootstrap);
        sqlite3_free(err);
        sqlite3_close(bootstrap);
        throw std::runtime_error("load sqlite extension " + path + ": " + msg);
    }
    sqlite3_close(bootstrap);
#else
    throw std::runtime_error(
        "load sqlite extension " + path +
        ": sqlite3_load_extension is not exposed by this SQLite SDK; "
        "register the VFS before constructing SqlitePageStore or build with "
        "TURBOGRAPH_SQLITE_ENABLE_LOAD_EXTENSION against a SQLite that exposes it");
#endif
}

void SqlitePageStore::open(std::optional<std::string> vfsName,
    std::optional<std::string> loadableExtensionPath, uint32_t sqlitePageSize,
    uint32_t walAutoCheckpointPages, int32_t cacheSizePages,
    const std::string& synchronous) {
    if (loadableExtensionPath) {
        registerLoadableExtension(*loadableExtensionPath);
    }
    auto flags = SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_NOMUTEX;
    const char* vfs = vfsName ? vfsName->c_str() : nullptr;
    if (sqlite3_open_v2(path_.c_str(), &db_, flags, vfs) != SQLITE_OK) {
        throwSqlite(db_, "open sqlite page store");
    }
    // Use the largest standard SQLite page by default. Ladybug pages are 4KiB
    // BLOBs; an 8KiB SQLite page only avoids overflow, while 64KiB lets one
    // container page pack many graph pages and wastes much less space on
    // SQLite row/B-tree overhead. Keep it configurable for latency experiments.
    auto targetSqlitePageSize = sqlitePageSize != 0 ? sqlitePageSize : 65536u;
    exec(("PRAGMA page_size = " + std::to_string(targetSqlitePageSize)).c_str());
    exec("PRAGMA journal_mode = WAL");
    exec(("PRAGMA wal_autocheckpoint = " +
          std::to_string(walAutoCheckpointPages)).c_str());
    exec(("PRAGMA cache_size = " + std::to_string(cacheSizePages)).c_str());
    exec(("PRAGMA synchronous = " + normalizeSynchronous(synchronous)).c_str());
    exec("PRAGMA foreign_keys = ON");
}

uint32_t SqlitePageStore::sqlitePageSize() const {
    Statement stmt(db_, "PRAGMA page_size");
    if (!stmt.stepRow()) {
        return 0;
    }
    return static_cast<uint32_t>(sqlite3_column_int(stmt.get(), 0));
}

uint32_t SqlitePageStore::walAutoCheckpointPages() const {
    Statement stmt(db_, "PRAGMA wal_autocheckpoint");
    if (!stmt.stepRow()) {
        return 0;
    }
    return static_cast<uint32_t>(sqlite3_column_int(stmt.get(), 0));
}

int32_t SqlitePageStore::sqliteCacheSizePages() const {
    Statement stmt(db_, "PRAGMA cache_size");
    if (!stmt.stepRow()) {
        return 0;
    }
    return sqlite3_column_int(stmt.get(), 0);
}

int32_t SqlitePageStore::sqliteSynchronous() const {
    Statement stmt(db_, "PRAGMA synchronous");
    if (!stmt.stepRow()) {
        return 0;
    }
    return sqlite3_column_int(stmt.get(), 0);
}

bool SqlitePageStore::fileExists(const std::string& fileId) const {
    Statement stmt(db_, "SELECT 1 FROM graph_files WHERE file_id = ?");
    bindText(db_, stmt.get(), 1, fileId);
    return stmt.stepRow();
}

void SqlitePageStore::migrate() {
    exec(R"SQL(
        CREATE TABLE IF NOT EXISTS graph_files (
            file_id TEXT PRIMARY KEY,
            size_bytes INTEGER NOT NULL,
            mtime_ms INTEGER NOT NULL DEFAULT (
                CAST(strftime('%s', 'now') AS INTEGER) * 1000
            )
        );
    )SQL");

    exec(R"SQL(
        CREATE TABLE IF NOT EXISTS graph_pages (
            file_id TEXT NOT NULL,
            page_no INTEGER NOT NULL,
            bytes BLOB NOT NULL,
            PRIMARY KEY (file_id, page_no),
            FOREIGN KEY (file_id) REFERENCES graph_files(file_id) ON DELETE CASCADE
        ) WITHOUT ROWID;
    )SQL");

    exec(R"SQL(
        CREATE TABLE IF NOT EXISTS graph_page_hints (
            file_id TEXT NOT NULL,
            page_no INTEGER NOT NULL,
            region_kind TEXT NOT NULL,
            region_id INTEGER,
            locality_key TEXT,
            PRIMARY KEY (file_id, page_no),
            FOREIGN KEY (file_id, page_no)
                REFERENCES graph_pages(file_id, page_no) ON DELETE CASCADE
        ) WITHOUT ROWID;
    )SQL");

    exec("CREATE INDEX IF NOT EXISTS graph_page_hints_lookup "
         "ON graph_page_hints(file_id, region_kind, region_id, page_no)");
}

void SqlitePageStore::exec(const char* sql) const {
    char* err = nullptr;
    auto rc = sqlite3_exec(db_, sql, nullptr, nullptr, &err);
    if (rc != SQLITE_OK) {
        std::string msg = err ? err : sqlite3_errmsg(db_);
        sqlite3_free(err);
        throw std::runtime_error("sqlite exec: " + msg);
    }
}

void SqlitePageStore::beginImmediate() {
    exec("BEGIN IMMEDIATE");
}

void SqlitePageStore::commit() {
    exec("COMMIT");
}

void SqlitePageStore::rollbackNoThrow() {
    try {
        exec("ROLLBACK");
    } catch (...) {
    }
}

uint64_t SqlitePageStore::fileSize(const std::string& fileId) const {
    Statement stmt(db_, "SELECT size_bytes FROM graph_files WHERE file_id = ?");
    bindText(db_, stmt.get(), 1, fileId);
    if (!stmt.stepRow()) {
        return 0;
    }
    return static_cast<uint64_t>(sqlite3_column_int64(stmt.get(), 0));
}

void SqlitePageStore::upsertFileSize(const std::string& fileId, uint64_t sizeBytes) {
    Statement stmt(db_, R"SQL(
        INSERT INTO graph_files(file_id, size_bytes)
        VALUES (?, ?)
        ON CONFLICT(file_id) DO UPDATE SET
            size_bytes = excluded.size_bytes,
            mtime_ms = CAST(strftime('%s', 'now') AS INTEGER) * 1000
    )SQL");
    bindText(db_, stmt.get(), 1, fileId);
    bindU64(db_, stmt.get(), 2, sizeBytes);
    stmt.stepDone();
}

void SqlitePageStore::truncate(const std::string& fileId, uint64_t sizeBytes) {
    beginImmediate();
    try {
        upsertFileSize(fileId, sizeBytes);
        auto keepPages = (sizeBytes + pageSize_ - 1) / pageSize_;
        Statement deletePages(db_,
            "DELETE FROM graph_pages WHERE file_id = ? AND page_no >= ?");
        bindText(db_, deletePages.get(), 1, fileId);
        bindU64(db_, deletePages.get(), 2, keepPages);
        deletePages.stepDone();

        if (sizeBytes > 0 && sizeBytes % pageSize_ != 0) {
            auto lastPageNo = sizeBytes / pageSize_;
            auto lastPage = readPage(fileId, lastPageNo);
            std::fill(lastPage.begin() + (sizeBytes % pageSize_), lastPage.end(), 0);
            writePage(fileId, lastPageNo, lastPage.data(), pageSize_);
            upsertFileSize(fileId, sizeBytes);
        }
        commit();
    } catch (...) {
        rollbackNoThrow();
        throw;
    }
}

std::vector<uint8_t> SqlitePageStore::readPage(const std::string& fileId,
    uint64_t pageNo) const {
    Statement stmt(db_, "SELECT bytes FROM graph_pages WHERE file_id = ? AND page_no = ?");
    bindText(db_, stmt.get(), 1, fileId);
    bindU64(db_, stmt.get(), 2, pageNo);
    if (!stmt.stepRow()) {
        return std::vector<uint8_t>(pageSize_, 0);
    }
    auto* blob = static_cast<const uint8_t*>(sqlite3_column_blob(stmt.get(), 0));
    auto len = sqlite3_column_bytes(stmt.get(), 0);
    std::vector<uint8_t> page(pageSize_, 0);
    if (blob && len > 0) {
        std::memcpy(page.data(), blob, std::min<uint32_t>(pageSize_, len));
    }
    return page;
}

void SqlitePageStore::writePage(const std::string& fileId, uint64_t pageNo,
    const uint8_t* data, uint32_t len) {
    if (len > pageSize_) {
        throw std::invalid_argument("writePage len exceeds pageSize");
    }
    std::vector<uint8_t> page(pageSize_, 0);
    std::memcpy(page.data(), data, len);

    upsertFileSize(fileId, std::max(fileSize(fileId), (pageNo + 1) * pageSize_));
    Statement stmt(db_, R"SQL(
        INSERT INTO graph_pages(file_id, page_no, bytes)
        VALUES (?, ?, ?)
        ON CONFLICT(file_id, page_no) DO UPDATE SET bytes = excluded.bytes
    )SQL");
    bindText(db_, stmt.get(), 1, fileId);
    bindU64(db_, stmt.get(), 2, pageNo);
    if (sqlite3_bind_blob(stmt.get(), 3, page.data(), static_cast<int>(page.size()),
            SQLITE_TRANSIENT) != SQLITE_OK) {
        throwSqlite(db_, "bind blob");
    }
    stmt.stepDone();
}

void SqlitePageStore::read(const std::string& fileId, void* buffer, uint64_t numBytes,
    uint64_t offset) const {
    auto* dst = static_cast<uint8_t*>(buffer);
    uint64_t done = 0;
    while (done < numBytes) {
        auto fileOffset = offset + done;
        auto pageNo = fileOffset / pageSize_;
        auto offsetInPage = fileOffset % pageSize_;
        auto n = std::min<uint64_t>(pageSize_ - offsetInPage, numBytes - done);
        auto page = readPage(fileId, pageNo);
        std::memcpy(dst + done, page.data() + offsetInPage, n);
        done += n;
    }
}

void SqlitePageStore::write(const std::string& fileId, const uint8_t* buffer,
    uint64_t numBytes, uint64_t offset) {
    auto oldSize = fileSize(fileId);
    beginImmediate();
    try {
        uint64_t done = 0;
        while (done < numBytes) {
            auto fileOffset = offset + done;
            auto pageNo = fileOffset / pageSize_;
            auto offsetInPage = fileOffset % pageSize_;
            auto n = std::min<uint64_t>(pageSize_ - offsetInPage, numBytes - done);
            if (offsetInPage == 0 && n == pageSize_) {
                writePage(fileId, pageNo, buffer + done, pageSize_);
            } else {
                auto page = readPage(fileId, pageNo);
                std::memcpy(page.data() + offsetInPage, buffer + done, n);
                writePage(fileId, pageNo, page.data(), pageSize_);
            }
            done += n;
        }
        upsertFileSize(fileId, std::max(oldSize, offset + numBytes));
        commit();
    } catch (...) {
        rollbackNoThrow();
        throw;
    }
}

void SqlitePageStore::setHint(const std::string& fileId, uint64_t pageNo,
    GraphPageHint hint) {
    Statement stmt(db_, R"SQL(
        INSERT INTO graph_page_hints(file_id, page_no, region_kind, region_id, locality_key)
        VALUES (?, ?, ?, ?, ?)
        ON CONFLICT(file_id, page_no) DO UPDATE SET
            region_kind = excluded.region_kind,
            region_id = excluded.region_id,
            locality_key = excluded.locality_key
    )SQL");
    bindText(db_, stmt.get(), 1, fileId);
    bindU64(db_, stmt.get(), 2, pageNo);
    bindText(db_, stmt.get(), 3, hint.regionKind);
    if (hint.regionId < 0) {
        sqlite3_bind_null(stmt.get(), 4);
    } else {
        sqlite3_bind_int64(stmt.get(), 4, hint.regionId);
    }
    bindText(db_, stmt.get(), 5, hint.localityKey);
    stmt.stepDone();
}

std::optional<GraphPageHint> SqlitePageStore::hint(const std::string& fileId,
    uint64_t pageNo) const {
    Statement stmt(db_, R"SQL(
        SELECT region_kind, COALESCE(region_id, -1), COALESCE(locality_key, '')
        FROM graph_page_hints
        WHERE file_id = ? AND page_no = ?
    )SQL");
    bindText(db_, stmt.get(), 1, fileId);
    bindU64(db_, stmt.get(), 2, pageNo);
    if (!stmt.stepRow()) {
        return std::nullopt;
    }
    GraphPageHint out;
    out.regionKind = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 0));
    out.regionId = sqlite3_column_int64(stmt.get(), 1);
    out.localityKey = reinterpret_cast<const char*>(sqlite3_column_text(stmt.get(), 2));
    return out;
}

std::vector<uint64_t> SqlitePageStore::pagesForHint(const std::string& fileId,
    const std::string& regionKind, std::optional<int64_t> regionId) const {
    const char* sql = regionId
        ? "SELECT page_no FROM graph_page_hints WHERE file_id = ? AND region_kind = ? "
          "AND region_id = ? ORDER BY page_no"
        : "SELECT page_no FROM graph_page_hints WHERE file_id = ? AND region_kind = ? "
          "ORDER BY page_no";
    Statement stmt(db_, sql);
    bindText(db_, stmt.get(), 1, fileId);
    bindText(db_, stmt.get(), 2, regionKind);
    if (regionId) {
        sqlite3_bind_int64(stmt.get(), 3, *regionId);
    }
    std::vector<uint64_t> pages;
    while (stmt.stepRow()) {
        pages.push_back(static_cast<uint64_t>(sqlite3_column_int64(stmt.get(), 0)));
    }
    return pages;
}

} // namespace tiered
} // namespace lbug
