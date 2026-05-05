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

// Journal sequence round-trip.
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

// Journal sequence defaults to 0 when missing.
static void testJournalSeqDefaultsToZero() {
    // JSON without journal_seq field
    std::string json = R"({"version":5,"page_count":50,"page_size":4096,"pages_per_group":2048,"page_group_keys":["pg/0_v5"]})";
    auto parsed = Manifest::fromJSON(json);
    assert(parsed.has_value());
    assert(parsed->journalSeq == 0);
    std::printf("  PASS: testJournalSeqDefaultsToZero\n");
}

// Journal sequence 0 is omitted from JSON for backward compatibility.
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

// Non-zero journal sequence appears in JSON.
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

// Large journal sequence values.
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

// Msgpack pure manifest round-trip.
static void testMsgpackRoundTrip() {
    Manifest m;
    m.version = 42;
    m.pageCount = 800000;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"prefix/pg/0_v1", "prefix/pg/1_v1", "prefix/pg/2_v42"};
    m.subPagesPerFrame = 4;
    m.frameTables = {{{0, 8192, 2}, {8192, 4096, 1}}};
    m.encrypted = true;
    m.journalSeq = 12345;

    auto bytes = m.toMsgpack();
    auto parsed = Manifest::fromMsgpack(bytes);
    assert(parsed.has_value());
    assert(parsed->version == 42);
    assert(parsed->pageCount == 800000);
    assert(parsed->pageSize == 4096);
    assert(parsed->pagesPerGroup == 2048);
    assert(parsed->pageGroupKeys.size() == 3);
    assert(parsed->subPagesPerFrame == 4);
    assert(parsed->frameTables.size() == 1);
    assert(parsed->frameTables[0].size() == 2);
    assert(parsed->frameTables[0][0].offset == 0);
    assert(parsed->frameTables[0][0].len == 8192);
    assert(parsed->frameTables[0][0].pageCount == 2);
    assert(parsed->encrypted == true);
    assert(parsed->journalSeq == 12345);
    std::printf("  PASS: testMsgpackRoundTrip\n");
}

// Msgpack hybrid payload round-trip.
static void testHybridPayloadRoundTrip() {
    Manifest m;
    m.version = 7;
    m.pageCount = 100;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"pg/0_v7"};
    m.journalSeq = 99;

    HybridPayload h;
    h.turbograph = m;
    h.graphstream_journal_seq = 555;
    h.graphstream_segment_prefix = "gs/mydb/";

    auto bytes = h.toMsgpack();
    auto parsed = HybridPayload::fromMsgpack(bytes);
    assert(parsed.has_value());
    assert(parsed->turbograph.version == 7);
    assert(parsed->turbograph.journalSeq == 99);
    assert(parsed->graphstream_journal_seq == 555);
    assert(parsed->graphstream_segment_prefix == "gs/mydb/");
    std::printf("  PASS: testHybridPayloadRoundTrip\n");
}

// Msgpack subframe overrides survive round-trip.
static void testMsgpackSubframeOverrides() {
    Manifest m;
    m.version = 5;
    m.pageCount = 50;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"pg/0_v5"};

    std::unordered_map<size_t, SubframeOverride> ovr;
    ovr[2] = SubframeOverride{"pg/0_f2_v5", FrameEntry{0, 4096, 4}};
    m.subframeOverrides.push_back(ovr);

    auto bytes = m.toMsgpack();
    auto parsed = Manifest::fromMsgpack(bytes);
    assert(parsed.has_value());
    assert(parsed->subframeOverrides.size() == 1);
    assert(parsed->subframeOverrides[0].size() == 1);
    assert(parsed->subframeOverrides[0].at(2).key == "pg/0_f2_v5");
    assert(parsed->subframeOverrides[0].at(2).entry.pageCount == 4);
    std::printf("  PASS: testMsgpackSubframeOverrides\n");
}

// Wire tag byte round-trip through TieredFileSystem shape.
static void testWireTagDiscrimination() {
    // Pure manifest bytes should start with 0x01
    Manifest m;
    m.version = 1;
    m.pageCount = 10;
    m.pageSize = 4096;
    m.pagesPerGroup = 2048;
    m.pageGroupKeys = {"pg/0_v1"};

    auto pure = m.toMsgpack();
    // We test the payload shape directly: first byte is tag when wrapped by TieredFileSystem.
    // Here we just verify the payload itself is valid msgpack (not empty, starts with fixmap).
    assert(!pure.empty());
    assert((pure[0] & 0xF0) == 0x80); // fixmap or map16/map32
    std::printf("  PASS: testWireTagDiscrimination\n");
}

// Malformed msgpack: truncated input should return nullopt.
static void testMalformedMsgpackTruncated() {
    // A valid msgpack fixmap header for 10 fields, but truncated before any content.
    std::vector<uint8_t> bad = {0x8A};
    auto parsed = Manifest::fromMsgpack(bad);
    assert(!parsed.has_value());
    std::printf("  PASS: testMalformedMsgpackTruncated\n");
}

// Malformed msgpack: wrong type tag should return nullopt.
static void testMalformedMsgpackWrongTypeTag() {
    // A valid msgpack array header (0x91 = fixarray 1) followed by a string,
    // but the decoder expects a map.
    std::vector<uint8_t> bad;
    bad.push_back(0x91); // fixarray 1
    bad.push_back(0xA1); // fixstr 1
    bad.push_back('x');
    auto parsed = Manifest::fromMsgpack(bad);
    assert(!parsed.has_value());
    std::printf("  PASS: testMalformedMsgpackWrongTypeTag\n");
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
    testMsgpackRoundTrip();
    testHybridPayloadRoundTrip();
    testMsgpackSubframeOverrides();
    testWireTagDiscrimination();
    testMalformedMsgpackTruncated();
    testMalformedMsgpackWrongTypeTag();
    std::printf("All manifest tests passed.\n");
    return 0;
}
