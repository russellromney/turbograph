#include "page_bitmap.h"

#include <fstream>

namespace lbug {
namespace tiered {

PageBitmap::PageBitmap(std::filesystem::path diskPath) : diskPath_(std::move(diskPath)) {
    if (std::filesystem::exists(diskPath_)) {
        std::ifstream f(diskPath_, std::ios::binary | std::ios::ate);
        auto size = static_cast<size_t>(f.tellg());
        if (size > 0) {
            f.seekg(0);
            bits_.resize(size);
            f.read(reinterpret_cast<char*>(bits_.data()), size);
            pageCount_ = size * 8;
        }
    }
}

bool PageBitmap::isPresent(uint64_t page) const {
    std::lock_guard lock(mu_);
    auto byteIdx = page / 8;
    auto bitIdx = page % 8;
    if (byteIdx >= bits_.size()) {
        return false;
    }
    return (bits_[byteIdx] >> bitIdx) & 1;
}

void PageBitmap::markPresent(uint64_t page) {
    std::lock_guard lock(mu_);
    auto byteIdx = page / 8;
    auto bitIdx = page % 8;
    if (byteIdx >= bits_.size()) {
        bits_.resize(byteIdx + 1, 0);
        pageCount_ = (byteIdx + 1) * 8;
    }
    bits_[byteIdx] |= (1 << bitIdx);
}

void PageBitmap::markRange(uint64_t start, uint64_t count) {
    std::lock_guard lock(mu_);
    auto end = start + count;
    auto neededBytes = (end + 7) / 8;
    if (neededBytes > bits_.size()) {
        bits_.resize(neededBytes, 0);
        pageCount_ = neededBytes * 8;
    }
    for (uint64_t page = start; page < end; page++) {
        auto byteIdx = page / 8;
        auto bitIdx = page % 8;
        bits_[byteIdx] |= (1 << bitIdx);
    }
}

void PageBitmap::clearRange(uint64_t start, uint64_t count) {
    std::lock_guard lock(mu_);
    auto end = start + count;
    for (uint64_t page = start; page < end; page++) {
        auto byteIdx = page / 8;
        auto bitIdx = page % 8;
        if (byteIdx < bits_.size()) {
            bits_[byteIdx] &= ~(1 << bitIdx);
        }
    }
}

void PageBitmap::clear() {
    std::lock_guard lock(mu_);
    std::fill(bits_.begin(), bits_.end(), 0);
}

void PageBitmap::resize(uint64_t pageCount) {
    std::lock_guard lock(mu_);
    auto neededBytes = (pageCount + 7) / 8;
    bits_.resize(neededBytes, 0);
    pageCount_ = pageCount;
}

void PageBitmap::persist() const {
    std::lock_guard lock(mu_);
    std::filesystem::create_directories(diskPath_.parent_path());
    auto tmpPath = diskPath_;
    tmpPath += ".tmp";
    {
        std::ofstream f(tmpPath, std::ios::binary | std::ios::trunc);
        f.write(reinterpret_cast<const char*>(bits_.data()), bits_.size());
    }
    std::filesystem::rename(tmpPath, diskPath_);
}

uint64_t PageBitmap::presentCount() const {
    std::lock_guard lock(mu_);
    uint64_t count = 0;
    for (auto byte : bits_) {
        // popcount via Brian Kernighan's algorithm.
        while (byte) {
            byte &= byte - 1;
            count++;
        }
    }
    return count;
}

uint64_t PageBitmap::pageCount() const {
    std::lock_guard lock(mu_);
    return pageCount_;
}

} // namespace tiered
} // namespace lbug
