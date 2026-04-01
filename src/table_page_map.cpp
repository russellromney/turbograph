#include "table_page_map.h"

namespace lbug {
namespace tiered {

void TablePageMap::addInterval(uint32_t startPage, uint32_t numPages, uint32_t tableId, bool isRel) {
    if (numPages == 0) return;
    intervals_.push_back({startPage, startPage + numPages, tableId, isRel});
    if (tableId > maxTableId_) {
        maxTableId_ = tableId;
    }
}

void TablePageMap::finalize() {
    // Sort by startPage for binary search.
    std::sort(intervals_.begin(), intervals_.end(),
        [](const Interval& a, const Interval& b) {
            return a.startPage < b.startPage;
        });
}

TablePageMap::LookupResult TablePageMap::lookup(uint32_t pageNum) const {
    if (intervals_.empty()) {
        return {0, false, false};
    }

    // Binary search: find the last interval where startPage <= pageNum.
    // upper_bound finds first interval with startPage > pageNum, then we step back.
    auto it = std::upper_bound(intervals_.begin(), intervals_.end(), pageNum,
        [](uint32_t page, const Interval& interval) {
            return page < interval.startPage;
        });

    if (it == intervals_.begin()) {
        // pageNum is before all intervals.
        return {0, false, false};
    }

    --it;
    // Check if pageNum falls within this interval.
    if (pageNum >= it->startPage && pageNum < it->endPage) {
        return {it->tableId, it->isRelationship, true};
    }

    return {0, false, false};
}

// --- TableMissCounters ---

TableMissCounters::TableMissCounters(uint32_t maxTableId)
    : size_(maxTableId + 1),
      counts_(std::make_unique<std::atomic<uint8_t>[]>(maxTableId + 1)) {
    for (uint32_t i = 0; i < size_; i++) {
        counts_[i].store(0, std::memory_order_relaxed);
    }
}

uint8_t TableMissCounters::get(uint32_t tableId) const {
    if (tableId >= size_) return 0;
    return counts_[tableId].load(std::memory_order_relaxed);
}

void TableMissCounters::increment(uint32_t tableId) {
    if (tableId >= size_) return;
    auto current = counts_[tableId].load(std::memory_order_relaxed);
    if (current < 255) {
        counts_[tableId].store(current + 1, std::memory_order_relaxed);
    }
}

void TableMissCounters::reset(uint32_t tableId) {
    if (tableId >= size_) return;
    counts_[tableId].store(0, std::memory_order_relaxed);
}

} // namespace tiered
} // namespace lbug
