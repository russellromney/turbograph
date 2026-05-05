#pragma once

#include "common/file_system/file_system.h"
#include "sqlite_page_store.h"

#include <memory>
#include <optional>
#include <string>
#include <vector>

namespace lbug {
namespace tiered {

struct SqliteGraphFileSystemConfig {
    std::string sqlitePath;
    std::string dataFilePath;
    uint32_t graphPageSize = 4096;
    uint32_t sqlitePageSize = 0;
    uint32_t sqliteWalAutoCheckpointPages = 1000;
    int32_t sqliteCacheSizePages = 0;
    std::string sqliteSynchronous = "NORMAL";
    std::optional<std::string> sqliteVfsName;
    std::optional<std::string> sqliteLoadableExtensionPath;
};

struct SqliteGraphFileInfo : public common::FileInfo {
    SqliteGraphFileInfo(std::string path, common::FileSystem* fs, bool readOnly)
        : FileInfo(std::move(path), fs), readOnly(readOnly) {}

    bool readOnly = false;
    uint64_t cursor = 0;
};

// Spike FileSystem: presents Ladybug/Kuzu files while persisting their pages as
// SQLite rows. The SQLite database can later be opened through turbolite via
// `sqliteVfsName`.
class SqliteGraphFileSystem : public common::FileSystem {
public:
    explicit SqliteGraphFileSystem(SqliteGraphFileSystemConfig config);

    std::unique_ptr<common::FileInfo> openFile(const std::string& path,
        common::FileOpenFlags flags, main::ClientContext* context = nullptr) override;

    std::vector<std::string> glob(main::ClientContext* context,
        const std::string& path) const override;

    bool canHandleFile(const std::string_view path) const override;
    void syncFile(const common::FileInfo& fileInfo) const override;
    bool fileOrPathExists(const std::string& path,
        main::ClientContext* context = nullptr) override;

    SqlitePageStore& pageStore() { return store_; }
    const SqlitePageStore& pageStore() const { return store_; }

protected:
    void readFromFile(common::FileInfo& fileInfo, void* buffer, uint64_t numBytes,
        uint64_t position) const override;

    int64_t readFile(common::FileInfo& fileInfo, void* buf, size_t numBytes) const override;

    void writeFile(common::FileInfo& fileInfo, const uint8_t* buffer, uint64_t numBytes,
        uint64_t offset) const override;

    int64_t seek(common::FileInfo& fileInfo, uint64_t offset, int whence) const override;

    uint64_t getFileSize(const common::FileInfo& fileInfo) const override;

    void truncate(common::FileInfo& fileInfo, uint64_t size) const override;

private:
    std::string dataFilePath_;
    mutable SqlitePageStore store_;
};

} // namespace tiered
} // namespace lbug
