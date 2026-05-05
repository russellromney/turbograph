#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

struct sqlite3;

namespace lbug {
namespace tiered {

struct GraphPageHint {
    std::string regionKind;
    int64_t regionId = -1;
    std::string localityKey;
};

// Spike-only backing store for "graph pages inside SQLite".
//
// This is intentionally lower level than TieredFileSystem: it proves the
// storage substrate shape before we try to impersonate Ladybug/Kuzu's full
// FileSystem API. Pass vfsName = "turbolite" later to put the SQLite file
// itself behind turbolite.
class SqlitePageStore {
public:
    explicit SqlitePageStore(std::string path, uint32_t pageSize = 4096,
        std::optional<std::string> vfsName = std::nullopt,
        std::optional<std::string> loadableExtensionPath = std::nullopt,
        uint32_t sqlitePageSize = 0, uint32_t walAutoCheckpointPages = 1000,
        int32_t cacheSizePages = 0, std::string synchronous = "NORMAL");
    ~SqlitePageStore();

    SqlitePageStore(const SqlitePageStore&) = delete;
    SqlitePageStore& operator=(const SqlitePageStore&) = delete;
    SqlitePageStore(SqlitePageStore&&) = delete;
    SqlitePageStore& operator=(SqlitePageStore&&) = delete;

    uint32_t pageSize() const { return pageSize_; }
    uint32_t sqlitePageSize() const;
    uint32_t walAutoCheckpointPages() const;
    int32_t sqliteCacheSizePages() const;
    int32_t sqliteSynchronous() const;

    bool fileExists(const std::string& fileId) const;
    uint64_t fileSize(const std::string& fileId) const;
    void truncate(const std::string& fileId, uint64_t sizeBytes);

    std::vector<uint8_t> readPage(const std::string& fileId, uint64_t pageNo) const;
    void writePage(const std::string& fileId, uint64_t pageNo, const uint8_t* data,
        uint32_t len);

    void read(const std::string& fileId, void* buffer, uint64_t numBytes,
        uint64_t offset) const;
    void write(const std::string& fileId, const uint8_t* buffer, uint64_t numBytes,
        uint64_t offset);

    void setHint(const std::string& fileId, uint64_t pageNo, GraphPageHint hint);
    std::optional<GraphPageHint> hint(const std::string& fileId, uint64_t pageNo) const;
    std::vector<uint64_t> pagesForHint(const std::string& fileId,
        const std::string& regionKind, std::optional<int64_t> regionId = std::nullopt) const;

private:
    void registerLoadableExtension(const std::string& path);
    void open(std::optional<std::string> vfsName,
        std::optional<std::string> loadableExtensionPath, uint32_t sqlitePageSize,
        uint32_t walAutoCheckpointPages, int32_t cacheSizePages,
        const std::string& synchronous);
    void migrate();
    void exec(const char* sql) const;
    void beginImmediate();
    void commit();
    void rollbackNoThrow();
    void upsertFileSize(const std::string& fileId, uint64_t sizeBytes);

    std::string path_;
    uint32_t pageSize_;
    sqlite3* db_ = nullptr;
};

} // namespace tiered
} // namespace lbug
