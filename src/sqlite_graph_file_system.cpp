#include "sqlite_graph_file_system.h"

#include <algorithm>
#include <cstring>
#include <stdexcept>

namespace lbug {
namespace tiered {

namespace {

bool hasFlag(common::FileOpenFlags flags, uint8_t flag) {
    return (flags.flags & flag) != 0;
}

SqliteGraphFileInfo& asSqliteInfo(common::FileInfo& fileInfo) {
    return fileInfo.cast<SqliteGraphFileInfo>();
}

const SqliteGraphFileInfo& asSqliteInfo(const common::FileInfo& fileInfo) {
    return fileInfo.constCast<SqliteGraphFileInfo>();
}

} // namespace

SqliteGraphFileSystem::SqliteGraphFileSystem(SqliteGraphFileSystemConfig config)
    : FileSystem(), dataFilePath_(std::move(config.dataFilePath)),
      dataFileId_(std::move(config.dataFileId)),
      store_(std::move(config.sqlitePath), config.graphPageSize,
                        std::move(config.sqliteVfsName),
                        std::move(config.sqliteLoadableExtensionPath),
                        config.sqlitePageSize,
                        config.sqliteWalAutoCheckpointPages,
                        config.sqliteCacheSizePages,
                        std::move(config.sqliteSynchronous)) {}

std::string SqliteGraphFileSystem::fileIdForPath(const std::string& path) const {
    if (!dataFileId_.empty() && path == dataFilePath_) {
        return dataFileId_;
    }
    return path;
}

std::unique_ptr<common::FileInfo> SqliteGraphFileSystem::openFile(
    const std::string& path, common::FileOpenFlags flags, main::ClientContext*) {
    auto readOnly = hasFlag(flags, common::FileFlags::READ_ONLY) &&
                    !hasFlag(flags, common::FileFlags::WRITE);

    if (hasFlag(flags, common::FileFlags::CREATE_AND_TRUNCATE_IF_EXISTS)) {
        if (readOnly) {
            throw std::runtime_error("cannot truncate a read-only SqliteGraphFileSystem file");
        }
        store_.truncate(fileIdForPath(path), 0);
    } else if (!readOnly && hasFlag(flags, common::FileFlags::CREATE_IF_NOT_EXISTS) &&
               !fileOrPathExists(path)) {
        store_.truncate(fileIdForPath(path), 0);
    }

    if (readOnly && !fileOrPathExists(path)) {
        throw std::runtime_error("SqliteGraphFileSystem file does not exist: " + path);
    }

    return std::make_unique<SqliteGraphFileInfo>(fileIdForPath(path), this, readOnly);
}

std::vector<std::string> SqliteGraphFileSystem::glob(main::ClientContext*,
    const std::string& path) const {
    if (store_.fileExists(fileIdForPath(path))) {
        return {path};
    }
    return {};
}

bool SqliteGraphFileSystem::canHandleFile(const std::string_view path) const {
    return dataFilePath_.empty() || path == dataFilePath_;
}

void SqliteGraphFileSystem::syncFile(const common::FileInfo&) const {
    // Durability is delegated to SQLite. A future turbolite-backed trial should
    // add an explicit checkpoint/sync hook here.
}

bool SqliteGraphFileSystem::fileOrPathExists(const std::string& path,
    main::ClientContext*) {
    return store_.fileExists(fileIdForPath(path));
}

void SqliteGraphFileSystem::readFromFile(common::FileInfo& fileInfo, void* buffer,
    uint64_t numBytes, uint64_t position) const {
    store_.read(fileInfo.path, buffer, numBytes, position);
}

int64_t SqliteGraphFileSystem::readFile(common::FileInfo& fileInfo, void* buf,
    size_t numBytes) const {
    auto& info = asSqliteInfo(fileInfo);
    auto size = store_.fileSize(info.path);
    if (info.cursor >= size) {
        return 0;
    }
    auto toRead = std::min<uint64_t>(numBytes, size - info.cursor);
    store_.read(info.path, buf, toRead, info.cursor);
    info.cursor += toRead;
    return static_cast<int64_t>(toRead);
}

void SqliteGraphFileSystem::writeFile(common::FileInfo& fileInfo, const uint8_t* buffer,
    uint64_t numBytes, uint64_t offset) const {
    const auto& info = asSqliteInfo(fileInfo);
    if (info.readOnly) {
        throw std::runtime_error("cannot write read-only SqliteGraphFileSystem file");
    }
    store_.write(fileInfo.path, buffer, numBytes, offset);
}

int64_t SqliteGraphFileSystem::seek(common::FileInfo& fileInfo, uint64_t offset,
    int whence) const {
    auto& info = asSqliteInfo(fileInfo);
    uint64_t next = 0;
    if (whence == SEEK_SET) {
        next = offset;
    } else if (whence == SEEK_CUR) {
        next = info.cursor + offset;
    } else if (whence == SEEK_END) {
        next = store_.fileSize(info.path) + offset;
    } else {
        throw std::runtime_error("unsupported SqliteGraphFileSystem seek mode");
    }
    info.cursor = next;
    return static_cast<int64_t>(info.cursor);
}

uint64_t SqliteGraphFileSystem::getFileSize(const common::FileInfo& fileInfo) const {
    const auto& info = asSqliteInfo(fileInfo);
    return store_.fileSize(info.path);
}

void SqliteGraphFileSystem::truncate(common::FileInfo& fileInfo, uint64_t size) const {
    const auto& info = asSqliteInfo(fileInfo);
    if (info.readOnly) {
        throw std::runtime_error("cannot truncate read-only SqliteGraphFileSystem file");
    }
    store_.truncate(fileInfo.path, size);
}

} // namespace tiered
} // namespace lbug
