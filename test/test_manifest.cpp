#include "manifest.h"

#include <cassert>
#include <cstdio>

using namespace lbug::tiered;

static void testRoundTrip() {
    Manifest m;
    m.version = 42;
    m.pageCount = 800000;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"prefix/pg/0_v1", "prefix/pg/1_v1", "prefix/pg/2_v42"};

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->version == 42);
    assert(parsed->pageCount == 800000);
    assert(parsed->pageSize == 4096);
    assert(parsed->pagesPerGroup == 2048);
    assert(parsed->pageGroupKeys.size() == 3);
    assert(parsed->pageGroupKeys[0] == "prefix/pg/0_v1");
    assert(parsed->pageGroupKeys[1] == "prefix/pg/1_v1");
    assert(parsed->pageGroupKeys[2] == "prefix/pg/2_v42");
    std::printf("  PASS: testRoundTrip\n");
}

static void testEmptyManifest() {
    Manifest m;
    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->version == 0);
    assert(parsed->pageCount == 0);
    assert(parsed->pageSize == 0);
    assert(parsed->pagesPerGroup == 0);
    assert(parsed->pageGroupKeys.empty());
    std::printf("  PASS: testEmptyManifest\n");
}

static void testInvalidJSON() {
    assert(!Manifest::fromJSON("not json").has_value());
    assert(!Manifest::fromJSON("{}").has_value());
    assert(!Manifest::fromJSON("{\"version\":1}").has_value());
    std::printf("  PASS: testInvalidJSON\n");
}

static void testLargeValues() {
    Manifest m;
    m.version = 999999999;
    m.pageCount = 50000000; // 50M pages * 4KB = 200GB.
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    for (uint64_t i = 0; i < 3052; i++) {
        m.pageGroupKeys.push_back("prefix/pg/" + std::to_string(i) + "_v999999999");
    }

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->version == 999999999);
    assert(parsed->pageCount == 50000000);
    assert(parsed->pagesPerGroup == 2048);
    assert(parsed->pageGroupKeys.size() == 3052);
    assert(parsed->pageGroupKeys[0] == "prefix/pg/0_v999999999");
    assert(parsed->pageGroupKeys[3051] == "prefix/pg/3051_v999999999");
    std::printf("  PASS: testLargeValues\n");
}

static void testImmutableKeyNaming() {
    Manifest m;
    m.version = 7;
    m.pageCount = 100;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"p/pg/0_v3", "p/pg/1_v5", "p/pg/2_v7"};

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->pageGroupKeys[0] == "p/pg/0_v3");
    assert(parsed->pageGroupKeys[1] == "p/pg/1_v5");
    assert(parsed->pageGroupKeys[2] == "p/pg/2_v7");
    std::printf("  PASS: testImmutableKeyNaming\n");
}

// Phase GraphZenith: journal_seq round-trip
static void testJournalSeqRoundTrip() {
    Manifest m;
    m.version = 10;
    m.pageCount = 100;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"pg/0_v10"};
    m.journalSeq = 42;

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->journalSeq == 42);
    std::printf("  PASS: testJournalSeqRoundTrip\n");
}

// Phase GraphZenith: journal_seq defaults to 0 when missing
static void testJournalSeqDefaultsToZero() {
    // JSON without journal_seq field
    std::string json = R"({"version":5,"page_count":50,"page_size":4096,"pages_per_group":2048,"page_group_keys":["pg/0_v5"]})";
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->journalSeq == 0);
    std::printf("  PASS: testJournalSeqDefaultsToZero\n");
}

// Phase GraphZenith: journal_seq = 0 is omitted from JSON (backward compat)
static void testJournalSeqZeroOmitted() {
    Manifest m;
    m.version = 1;
    m.pageCount = 10;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"pg/0_v1"};
    m.journalSeq = 0;

    auto json = m.toJSON();
    // journal_seq should not appear in output when 0.
    assert(json.find("journal_seq") == std::string::npos);
    std::printf("  PASS: testJournalSeqZeroOmitted\n");
}

// Phase GraphZenith: journal_seq > 0 appears in JSON
static void testJournalSeqNonZeroPresent() {
    Manifest m;
    m.version = 1;
    m.pageCount = 10;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"pg/0_v1"};
    m.journalSeq = 999;

    auto json = m.toJSON();
    assert(json.find("\"journal_seq\":999") != std::string::npos);

    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->journalSeq == 999);
    std::printf("  PASS: testJournalSeqNonZeroPresent\n");
}

// Phase GraphZenith: large journal_seq values
static void testJournalSeqLargeValue() {
    Manifest m;
    m.version = 1;
    m.pageCount = 10;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"pg/0_v1"};
    m.journalSeq = 18446744073709551000ULL; // Near u64 max

    auto json = m.toJSON();
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->journalSeq == 18446744073709551000ULL);
    std::printf("  PASS: testJournalSeqLargeValue\n");
}

int main() {
    std::printf("=== Manifest Tests ===\n");
    testRoundTrip();
    testEmptyManifest();
    testInvalidJSON();
    testLargeValues();
    testImmutableKeyNaming();
    testJournalSeqRoundTrip();
    testJournalSeqDefaultsToZero();
    testJournalSeqZeroOmitted();
    testJournalSeqNonZeroPresent();
    testJournalSeqLargeValue();
    std::printf("All manifest tests passed.\n");
    return 0;
}
