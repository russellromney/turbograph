// Unit tests for TablePageMap interval map and TableMissCounters.
// No S3 required -- pure in-memory tests.

#include "table_page_map.h"

#include <cassert>
#include <cstdio>
#include <thread>
#include <vector>

using namespace lbug::tiered;

// --- TablePageMap tests ---

static void testEmptyMapLookup() {
    TablePageMap map;
    map.finalize();

    auto r = map.lookup(0);
    assert(!r.found);
    assert(map.size() == 0);

    std::printf("  PASS: testEmptyMapLookup\n");
}

static void testSingleInterval() {
    TablePageMap map;
    // Node table 0 owns pages 100-199.
    map.addInterval(100, 100, 0, false);
    map.finalize();

    assert(map.size() == 1);

    // Before interval.
    assert(!map.lookup(99).found);

    // Start of interval.
    auto r = map.lookup(100);
    assert(r.found);
    assert(r.tableId == 0);
    assert(!r.isRelationship);

    // Middle of interval.
    r = map.lookup(150);
    assert(r.found);
    assert(r.tableId == 0);

    // Last page of interval.
    r = map.lookup(199);
    assert(r.found);
    assert(r.tableId == 0);

    // Just after interval.
    assert(!map.lookup(200).found);

    std::printf("  PASS: testSingleInterval\n");
}

static void testMultipleTablesDisjoint() {
    TablePageMap map;
    // Node table 0: pages 0-99.
    map.addInterval(0, 100, 0, false);
    // Rel table 1: pages 200-499.
    map.addInterval(200, 300, 1, true);
    // Node table 2: pages 1000-1099.
    map.addInterval(1000, 100, 2, false);
    map.finalize();

    assert(map.size() == 3);
    assert(map.maxTableId() == 2);

    // Node table 0.
    auto r = map.lookup(0);
    assert(r.found && r.tableId == 0 && !r.isRelationship);
    r = map.lookup(99);
    assert(r.found && r.tableId == 0);

    // Gap between tables.
    assert(!map.lookup(100).found);
    assert(!map.lookup(199).found);

    // Rel table 1.
    r = map.lookup(200);
    assert(r.found && r.tableId == 1 && r.isRelationship);
    r = map.lookup(499);
    assert(r.found && r.tableId == 1);
    assert(!map.lookup(500).found);

    // Node table 2.
    r = map.lookup(1000);
    assert(r.found && r.tableId == 2 && !r.isRelationship);
    assert(!map.lookup(1100).found);

    std::printf("  PASS: testMultipleTablesDisjoint\n");
}

static void testOverlappingIntervalsSameTable() {
    // A single table may have multiple column chunks with different PageRanges.
    // These should all resolve to the same table.
    TablePageMap map;
    map.addInterval(0, 50, 3, false);   // Column A.
    map.addInterval(50, 50, 3, false);  // Column B (adjacent).
    map.addInterval(200, 30, 3, false); // Column C (non-adjacent).
    map.finalize();

    assert(map.size() == 3);

    auto r = map.lookup(25);
    assert(r.found && r.tableId == 3);
    r = map.lookup(75);
    assert(r.found && r.tableId == 3);
    r = map.lookup(210);
    assert(r.found && r.tableId == 3);

    // Gap.
    assert(!map.lookup(100).found);
    assert(!map.lookup(199).found);

    std::printf("  PASS: testOverlappingIntervalsSameTable\n");
}

static void testUnsortedInput() {
    // Intervals added out of order should still work after finalize.
    TablePageMap map;
    map.addInterval(1000, 100, 5, true);
    map.addInterval(0, 50, 3, false);
    map.addInterval(500, 200, 4, true);
    map.finalize();

    auto r = map.lookup(25);
    assert(r.found && r.tableId == 3);
    r = map.lookup(600);
    assert(r.found && r.tableId == 4 && r.isRelationship);
    r = map.lookup(1050);
    assert(r.found && r.tableId == 5 && r.isRelationship);

    std::printf("  PASS: testUnsortedInput\n");
}

static void testZeroPageInterval() {
    // Zero-page intervals should be silently ignored.
    TablePageMap map;
    map.addInterval(100, 0, 1, false); // Zero pages, should be skipped.
    map.addInterval(200, 50, 2, true);
    map.finalize();

    assert(map.size() == 1); // Only the non-zero interval.
    assert(!map.lookup(100).found);
    assert(map.lookup(200).found);

    std::printf("  PASS: testZeroPageInterval\n");
}

static void testLargePageNumbers() {
    // Stress test with large page numbers (realistic for big databases).
    TablePageMap map;
    // 16GB DB / 4KB pages = ~4M pages. 2048 pages/group = ~2000 groups.
    map.addInterval(0, 100000, 0, false);        // Node table: 100K pages.
    map.addInterval(100000, 3000000, 1, true);    // Rel table: 3M pages.
    map.addInterval(3200000, 500000, 2, false);   // Node table: 500K pages.
    map.finalize();

    auto r = map.lookup(50000);
    assert(r.found && r.tableId == 0 && !r.isRelationship);
    r = map.lookup(2000000);
    assert(r.found && r.tableId == 1 && r.isRelationship);
    r = map.lookup(3500000);
    assert(r.found && r.tableId == 2 && !r.isRelationship);

    // Between intervals.
    assert(!map.lookup(3100000).found);

    std::printf("  PASS: testLargePageNumbers\n");
}

static void testMaxTableId() {
    TablePageMap map;
    map.addInterval(0, 10, 7, false);
    map.addInterval(10, 10, 42, true);
    map.addInterval(20, 10, 3, false);
    map.finalize();

    assert(map.maxTableId() == 42);

    std::printf("  PASS: testMaxTableId\n");
}

// --- TableMissCounters tests ---

static void testMissCounterBasic() {
    TableMissCounters counters(10);

    assert(counters.get(0) == 0);
    assert(counters.get(5) == 0);

    counters.increment(5);
    assert(counters.get(5) == 1);

    counters.increment(5);
    counters.increment(5);
    assert(counters.get(5) == 3);

    counters.reset(5);
    assert(counters.get(5) == 0);

    // Other counters unaffected.
    assert(counters.get(0) == 0);

    std::printf("  PASS: testMissCounterBasic\n");
}

static void testMissCounterSaturation() {
    TableMissCounters counters(1);

    // Saturating increment at 255.
    for (int i = 0; i < 300; i++) {
        counters.increment(0);
    }
    assert(counters.get(0) == 255);

    // Reset works from saturated.
    counters.reset(0);
    assert(counters.get(0) == 0);

    std::printf("  PASS: testMissCounterSaturation\n");
}

static void testMissCounterOutOfBounds() {
    TableMissCounters counters(5);

    // Out-of-bounds operations should be no-ops.
    counters.increment(10); // Beyond size.
    assert(counters.get(10) == 0);

    counters.reset(10); // No crash.

    std::printf("  PASS: testMissCounterOutOfBounds\n");
}

static void testMissCounterMultipleTables() {
    TableMissCounters counters(3);

    // Simulate interleaved misses on different tables.
    counters.increment(0); // Node table.
    counters.increment(0);
    counters.increment(1); // Rel table.
    counters.increment(0);
    counters.increment(1);
    counters.increment(1);

    assert(counters.get(0) == 3);
    assert(counters.get(1) == 3);
    assert(counters.get(2) == 0);

    // Reset one table, others unaffected.
    counters.reset(0);
    assert(counters.get(0) == 0);
    assert(counters.get(1) == 3);

    std::printf("  PASS: testMissCounterMultipleTables\n");
}

// --- Prefetch schedule selection test ---

static void testScheduleSelectionByTableType() {
    // Simulate the per-table schedule selection logic from readOnePage.
    TablePageMap map;
    map.addInterval(0, 100, 0, false);      // Node table.
    map.addInterval(100, 500, 1, true);      // Rel table.
    map.finalize();

    TableMissCounters counters(map.maxTableId());

    // Lookup a node table page.
    auto r = map.lookup(50);
    assert(r.found && !r.isRelationship);

    // Lookup a rel table page.
    r = map.lookup(300);
    assert(r.found && r.isRelationship);

    // Lookup a page not in any table (structural).
    r = map.lookup(700);
    assert(!r.found);

    std::printf("  PASS: testScheduleSelectionByTableType\n");
}

// --- Per-table prefetch count computation ---

static uint64_t computePerTablePrefetch(uint8_t misses, uint64_t totalPGs,
    const std::vector<float>& hops) {
    float hopSum = 0;
    for (auto v : hops) hopSum += v;
    bool fractionMode = (hopSum <= 1.01f);

    uint8_t missCount = misses > 0 ? misses : 1;
    auto hopIdx = static_cast<size_t>(missCount - 1);

    if (hopIdx >= hops.size()) {
        return totalPGs;
    } else if (fractionMode) {
        return std::max(uint64_t{1},
            static_cast<uint64_t>(hops[hopIdx] * totalPGs));
    } else {
        return static_cast<uint64_t>(hops[hopIdx]);
    }
}

static void testRelTableGetsScanSchedule() {
    // Rel tables should use scan schedule: {0.3, 0.3, 0.4}.
    std::vector<float> scanHops = {0.3f, 0.3f, 0.4f};
    uint64_t totalPGs = 100;

    // Miss 1: 30% of 100 = 30.
    assert(computePerTablePrefetch(1, totalPGs, scanHops) == 30);
    // Miss 2: 30% = 30.
    assert(computePerTablePrefetch(2, totalPGs, scanHops) == 30);
    // Miss 3: 40% = 40.
    assert(computePerTablePrefetch(3, totalPGs, scanHops) == 40);
    // Miss 4: everything.
    assert(computePerTablePrefetch(4, totalPGs, scanHops) == 100);

    std::printf("  PASS: testRelTableGetsScanSchedule\n");
}

static void testNodeTableGetsLookupSchedule() {
    // Node tables should use lookup schedule: {0.0, 0.0, 0.0}.
    std::vector<float> lookupHops = {0.0f, 0.0f, 0.0f};
    uint64_t totalPGs = 100;

    // Miss 1-3: 0% but clamped to 1.
    assert(computePerTablePrefetch(1, totalPGs, lookupHops) == 1);
    assert(computePerTablePrefetch(2, totalPGs, lookupHops) == 1);
    assert(computePerTablePrefetch(3, totalPGs, lookupHops) == 1);
    // Miss 4: implicit final hop = everything.
    assert(computePerTablePrefetch(4, totalPGs, lookupHops) == 100);

    std::printf("  PASS: testNodeTableGetsLookupSchedule\n");
}

// --- Edge case / failure mode tests ---

static void testOverlappingIntervalsDifferentTables() {
    // Two different tables have intervals that partially overlap in page space.
    // The binary search returns the first interval whose startPage <= pageNum.
    // With sorted intervals, the earlier-starting table wins for overlapping pages.
    TablePageMap map;
    map.addInterval(0, 100, 0, false);   // Table 0: pages 0-99.
    map.addInterval(50, 100, 1, true);   // Table 1: pages 50-149 (overlaps with table 0).
    map.finalize();

    // Page 0-49: table 0 (only table 0 covers these).
    auto r = map.lookup(25);
    assert(r.found && r.tableId == 0);

    // Page 50-99: overlapping region. Binary search finds table 0 (starts at 0 <= 50).
    // Table 1 starts at 50 but table 0's interval (0-100) also contains page 50.
    // upper_bound finds first interval with start > 50 = table 1 (start=50 is NOT > 50).
    // Actually upper_bound finds start > 50, so it skips both 0 and 50, returns end.
    // Then --it gives us the last interval with start <= 50... which is table 1 (start=50).
    // Table 1 contains page 75 (50 <= 75 < 150). So table 1 wins for overlapping pages.
    r = map.lookup(75);
    assert(r.found);
    // The exact table depends on sort order. Both have start <= 75.
    // After sort by startPage: [{0,100,t0}, {50,150,t1}].
    // upper_bound(75): first with start > 75 = end. --it = {50,150,t1}. t1 wins.
    assert(r.tableId == 1);

    // Page 100-149: only table 1.
    r = map.lookup(120);
    assert(r.found && r.tableId == 1);

    std::printf("  PASS: testOverlappingIntervalsDifferentTables\n");
}

static void testAdjacentIntervals() {
    // Two intervals that are exactly adjacent (no gap, no overlap).
    TablePageMap map;
    map.addInterval(0, 100, 0, false);
    map.addInterval(100, 100, 1, true);
    map.finalize();

    // Last page of first interval.
    auto r = map.lookup(99);
    assert(r.found && r.tableId == 0);

    // First page of second interval.
    r = map.lookup(100);
    assert(r.found && r.tableId == 1);

    std::printf("  PASS: testAdjacentIntervals\n");
}

static void testSinglePageInterval() {
    // Interval with exactly 1 page.
    TablePageMap map;
    map.addInterval(42, 1, 7, false);
    map.finalize();

    assert(!map.lookup(41).found);
    auto r = map.lookup(42);
    assert(r.found && r.tableId == 7);
    assert(!map.lookup(43).found);

    std::printf("  PASS: testSinglePageInterval\n");
}

static void testManyIntervalsBinarySearchPerf() {
    // Stress test binary search with many intervals.
    TablePageMap map;
    // 1000 intervals, each 100 pages, 50-page gap between them.
    for (uint32_t i = 0; i < 1000; i++) {
        uint32_t start = i * 150;
        map.addInterval(start, 100, i, i % 2 == 1);
    }
    map.finalize();
    assert(map.size() == 1000);

    // Look up pages in first, middle, and last intervals.
    auto r = map.lookup(50);
    assert(r.found && r.tableId == 0 && !r.isRelationship);

    r = map.lookup(500 * 150 + 50);
    assert(r.found && r.tableId == 500 && !r.isRelationship);

    r = map.lookup(999 * 150 + 50);
    assert(r.found && r.tableId == 999 && r.isRelationship);

    // Gap page.
    assert(!map.lookup(105).found); // Between interval 0 (0-99) and 1 (150-249).

    std::printf("  PASS: testManyIntervalsBinarySearchPerf\n");
}

static void testMaxTableIdZero() {
    // Only table ID 0.
    TablePageMap map;
    map.addInterval(0, 10, 0, false);
    map.finalize();
    assert(map.maxTableId() == 0);

    // TableMissCounters with maxTableId=0 should have size 1.
    TableMissCounters counters(0);
    counters.increment(0);
    assert(counters.get(0) == 1);

    std::printf("  PASS: testMaxTableIdZero\n");
}

static void testMissCounterConcurrentAccess() {
    // Multiple threads incrementing different counters.
    TableMissCounters counters(10);
    std::vector<std::thread> threads;
    for (uint32_t t = 0; t < 4; t++) {
        threads.emplace_back([&counters, t]() {
            for (int i = 0; i < 100; i++) {
                counters.increment(t);
            }
        });
    }
    for (auto& th : threads) th.join();

    // Each counter should be 100 (relaxed ordering, but single-writer per counter).
    for (uint32_t t = 0; t < 4; t++) {
        assert(counters.get(t) == 100);
    }
    // Untouched counters should be 0.
    assert(counters.get(5) == 0);

    std::printf("  PASS: testMissCounterConcurrentAccess\n");
}

static void testFinalizeIdempotent() {
    // Calling finalize multiple times should be safe.
    TablePageMap map;
    map.addInterval(200, 50, 2, true);
    map.addInterval(0, 50, 0, false);
    map.addInterval(100, 50, 1, false);
    map.finalize();
    map.finalize(); // Second finalize.
    map.finalize(); // Third.

    auto r = map.lookup(25);
    assert(r.found && r.tableId == 0);
    r = map.lookup(125);
    assert(r.found && r.tableId == 1);
    r = map.lookup(225);
    assert(r.found && r.tableId == 2);

    std::printf("  PASS: testFinalizeIdempotent\n");
}

static void testLookupBeforeFinalize() {
    // Lookup before finalize works if intervals happen to be in order.
    // (Not guaranteed to work if unsorted, but should not crash.)
    TablePageMap map;
    map.addInterval(0, 50, 0, false);
    // Don't call finalize.
    auto r = map.lookup(25);
    assert(r.found && r.tableId == 0);

    std::printf("  PASS: testLookupBeforeFinalize\n");
}

int main() {
    std::printf("=== TablePageMap Unit Tests ===\n");

    // TablePageMap tests: happy path.
    testEmptyMapLookup();
    testSingleInterval();
    testMultipleTablesDisjoint();
    testOverlappingIntervalsSameTable();
    testUnsortedInput();
    testLargePageNumbers();
    testMaxTableId();

    // TablePageMap tests: edge cases.
    testZeroPageInterval();
    testSinglePageInterval();
    testAdjacentIntervals();
    testOverlappingIntervalsDifferentTables();
    testManyIntervalsBinarySearchPerf();
    testMaxTableIdZero();
    testFinalizeIdempotent();
    testLookupBeforeFinalize();

    // TableMissCounters tests: happy path + failure modes.
    testMissCounterBasic();
    testMissCounterSaturation();
    testMissCounterOutOfBounds();
    testMissCounterMultipleTables();
    testMissCounterConcurrentAccess();

    // Schedule selection tests.
    testScheduleSelectionByTableType();
    testRelTableGetsScanSchedule();
    testNodeTableGetsLookupSchedule();

    std::printf("All TablePageMap unit tests passed.\n");
    return 0;
}
