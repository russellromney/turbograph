#pragma once

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <memory>
#include <vector>

namespace lbug {
namespace tiered {

// Maps page numbers to table IDs and table types (node vs relationship).
// Built from Kuzu/LadybugDB metadata after Database construction.
// Used by readOnePage() to select per-table prefetch schedules.
//
// Internally stores sorted, non-overlapping intervals for binary search lookup.
// Thread-safe for concurrent reads after finalize(). Not thread-safe for writes.
class TablePageMap {
public:
    struct Interval {
        uint32_t startPage;  // Inclusive.
        uint32_t endPage;    // Exclusive.
        uint32_t tableId;
        bool isRelationship; // true = rel table (scan schedule), false = node table (lookup).
    };

    struct LookupResult {
        uint32_t tableId;
        bool isRelationship;
        bool found;
    };

    // Add a page range belonging to a table. Call before finalize().
    void addInterval(uint32_t startPage, uint32_t numPages, uint32_t tableId, bool isRel);

    // Sort intervals and prepare for lookup. Must call after all addInterval() calls.
    void finalize();

    // Binary search for the table owning a given page. O(log n).
    LookupResult lookup(uint32_t pageNum) const;

    // Number of intervals (for testing).
    size_t size() const { return intervals_.size(); }

    // Maximum table ID seen (for pre-allocating per-table miss counters).
    uint32_t maxTableId() const { return maxTableId_; }

    // Access intervals (for testing).
    const std::vector<Interval>& intervals() const { return intervals_; }

private:
    std::vector<Interval> intervals_;
    uint32_t maxTableId_ = 0;
};

// Per-table miss counter array. Pre-allocated after TablePageMap is built.
// Lock-free: each counter is an atomic uint8_t indexed by table ID.
class TableMissCounters {
public:
    explicit TableMissCounters(uint32_t maxTableId);

    uint8_t get(uint32_t tableId) const;
    void increment(uint32_t tableId);  // Saturating increment (max 255).
    void reset(uint32_t tableId);

private:
    uint32_t size_;
    std::unique_ptr<std::atomic<uint8_t>[]> counts_;
};

} // namespace tiered
} // namespace lbug
