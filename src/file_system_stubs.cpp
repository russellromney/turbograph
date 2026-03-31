// Minimal stubs for FileSystem + FileInfo + Exception — just enough for test
// linking without pulling in the full Kuzu dependency chain (string_utils →
// utf8proc → ...).  All non-overridden FileSystem virtual methods abort
// immediately since TieredFileSystem overrides everything the tests actually
// call.

#include "common/file_system/file_info.h"
#include "common/file_system/file_system.h"

#include <cstdlib>

namespace lbug {
namespace common {

// --- Exception (needed by KU_UNREACHABLE / KU_ASSERT in inline headers) ---

Exception::Exception(std::string msg) : exception(), exception_message_(std::move(msg)) {}

// --- FileInfo delegation (file_info.cpp equivalent) ---

uint64_t FileInfo::getFileSize() const { return fileSystem->getFileSize(*this); }

void FileInfo::readFromFile(void* buffer, uint64_t numBytes, uint64_t position) {
    fileSystem->readFromFile(*this, buffer, numBytes, position);
}

int64_t FileInfo::readFile(void* buf, size_t nbyte) {
    return fileSystem->readFile(*this, buf, nbyte);
}

void FileInfo::writeFile(const uint8_t* buffer, uint64_t numBytes, uint64_t offset) {
    fileSystem->writeFile(*this, buffer, numBytes, offset);
}

void FileInfo::syncFile() const { fileSystem->syncFile(*this); }

int64_t FileInfo::seek(uint64_t offset, int whence) {
    return fileSystem->seek(*this, offset, whence);
}

void FileInfo::reset() { fileSystem->reset(*this); }

void FileInfo::truncate(uint64_t size) { fileSystem->truncate(*this, size); }

bool FileInfo::canPerformSeek() const { return fileSystem->canPerformSeek(); }

// --- FileSystem base-class stubs (vtable entries for non-overridden virtuals) ---

void FileSystem::overwriteFile(const std::string&, const std::string&) { std::abort(); }
void FileSystem::copyFile(const std::string&, const std::string&) { std::abort(); }
void FileSystem::createDir(const std::string&) const { std::abort(); }
void FileSystem::removeFileIfExists(const std::string&, const main::ClientContext*) { std::abort(); }
bool FileSystem::fileOrPathExists(const std::string&, main::ClientContext*) { std::abort(); }

std::string FileSystem::expandPath(main::ClientContext*, const std::string& path) const {
    return path;
}

std::string FileSystem::joinPath(const std::string& base, const std::string& part) {
    return base + "/" + part;
}

std::string FileSystem::getFileExtension(const std::filesystem::path& path) {
    return path.extension().string();
}

bool FileSystem::isCompressedFile(const std::filesystem::path&) { return false; }

std::string FileSystem::getFileName(const std::filesystem::path& path) {
    return path.filename().string();
}

void FileSystem::writeFile(FileInfo&, const uint8_t*, uint64_t, uint64_t) const { std::abort(); }
void FileSystem::truncate(FileInfo&, uint64_t) const { std::abort(); }

void FileSystem::reset(FileInfo& fileInfo) { fileInfo.seek(0, SEEK_SET); }

bool FileSystem::isGZIPCompressed(const std::filesystem::path&) { return false; }

} // namespace common
} // namespace lbug
