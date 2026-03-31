#pragma once

#include <cstdint>
#include <filesystem>
#include <mutex>
#include <vector>

namespace lbug {
namespace tiered {

// Bit vector tracking which pages exist in the local cache file.
// 1 bit per page. Thread-safe via mutex.
// Persisted to disk as raw bytes at {cacheDir}/page_bitmap.
class PageBitmap {
public:
    explicit PageBitmap(std::filesystem::path diskPath);

    bool isPresent(uint64_t page) const;
    void markPresent(uint64_t page);
    void markRange(uint64_t start, uint64_t count);
    void clearRange(uint64_t start, uint64_t count);
    void clear();
    void resize(uint64_t pageCount);
    void persist() const;

    uint64_t presentCount() const;
    uint64_t pageCount() const;

private:
    std::filesystem::path diskPath_;
    mutable std::mutex mu_;
    std::vector<uint8_t> bits_; // 1 byte per 8 pages.
    uint64_t pageCount_ = 0;
};

} // namespace tiered
} // namespace lbug
