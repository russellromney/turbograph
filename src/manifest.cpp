#include "manifest.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace lbug {
namespace tiered {

// ============================================================================
// Minimal msgpack encoder/decoder
// ============================================================================
// Self-contained implementation to avoid adding a dependency. Produces
// msgpack maps with string keys, compatible with rmp_serde for
// identically-shaped structs. The C++ Manifest schema (10 fields) is
// turbograph-specific and not interchangeable with Rust turbolite's
// Manifest schema (~20 fields) without a translation layer.

static void mpWriteU8(std::vector<uint8_t>& out, uint8_t v) {
    out.push_back(v);
}

static void mpWriteU16(std::vector<uint8_t>& out, uint16_t v) {
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v));
}

static void mpWriteU32(std::vector<uint8_t>& out, uint32_t v) {
    out.push_back(static_cast<uint8_t>(v >> 24));
    out.push_back(static_cast<uint8_t>(v >> 16));
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v));
}

static void mpWriteU64(std::vector<uint8_t>& out, uint64_t v) {
    out.push_back(static_cast<uint8_t>(v >> 56));
    out.push_back(static_cast<uint8_t>(v >> 48));
    out.push_back(static_cast<uint8_t>(v >> 40));
    out.push_back(static_cast<uint8_t>(v >> 32));
    out.push_back(static_cast<uint8_t>(v >> 24));
    out.push_back(static_cast<uint8_t>(v >> 16));
    out.push_back(static_cast<uint8_t>(v >> 8));
    out.push_back(static_cast<uint8_t>(v));
}

static void mpWriteUInt(std::vector<uint8_t>& out, uint64_t v) {
    if (v <= 0x7F) {
        mpWriteU8(out, static_cast<uint8_t>(v));
    } else if (v <= 0xFF) {
        out.push_back(0xCC);
        mpWriteU8(out, static_cast<uint8_t>(v));
    } else if (v <= 0xFFFF) {
        out.push_back(0xCD);
        mpWriteU16(out, static_cast<uint16_t>(v));
    } else if (v <= 0xFFFFFFFF) {
        out.push_back(0xCE);
        mpWriteU32(out, static_cast<uint32_t>(v));
    } else {
        out.push_back(0xCF);
        mpWriteU64(out, v);
    }
}

static void mpWriteStr(std::vector<uint8_t>& out, const std::string& s) {
    size_t n = s.size();
    if (n <= 31) {
        out.push_back(static_cast<uint8_t>(0xA0 | n));
    } else if (n <= 0xFF) {
        out.push_back(0xD9);
        mpWriteU8(out, static_cast<uint8_t>(n));
    } else if (n <= 0xFFFF) {
        out.push_back(0xDA);
        mpWriteU16(out, static_cast<uint16_t>(n));
    } else {
        out.push_back(0xDB);
        mpWriteU32(out, static_cast<uint32_t>(n));
    }
    out.insert(out.end(), s.begin(), s.end());
}

static void mpWriteArrayHeader(std::vector<uint8_t>& out, size_t n) {
    if (n <= 15) {
        out.push_back(static_cast<uint8_t>(0x90 | n));
    } else if (n <= 0xFFFF) {
        out.push_back(0xDC);
        mpWriteU16(out, static_cast<uint16_t>(n));
    } else {
        out.push_back(0xDD);
        mpWriteU32(out, static_cast<uint32_t>(n));
    }
}

static void mpWriteMapHeader(std::vector<uint8_t>& out, size_t n) {
    if (n <= 15) {
        out.push_back(static_cast<uint8_t>(0x80 | n));
    } else if (n <= 0xFFFF) {
        out.push_back(0xDE);
        mpWriteU16(out, static_cast<uint16_t>(n));
    } else {
        out.push_back(0xDF);
        mpWriteU32(out, static_cast<uint32_t>(n));
    }
}

static void mpWriteBool(std::vector<uint8_t>& out, bool v) {
    out.push_back(v ? 0xC3 : 0xC2);
}

static void mpWriteNil(std::vector<uint8_t>& out) {
    out.push_back(0xC0);
}

// Forward declaration for recursive Manifest serialization.
static void mpWriteManifest(std::vector<uint8_t>& out, const Manifest& m);

static void mpWriteFrameEntry(std::vector<uint8_t>& out, const FrameEntry& e) {
    mpWriteMapHeader(out, 3);
    mpWriteStr(out, "offset");
    mpWriteUInt(out, e.offset);
    mpWriteStr(out, "len");
    mpWriteUInt(out, e.len);
    mpWriteStr(out, "pageCount");
    mpWriteUInt(out, e.pageCount);
}

static void mpWriteSubframeOverride(std::vector<uint8_t>& out, const SubframeOverride& o) {
    mpWriteMapHeader(out, 2);
    mpWriteStr(out, "key");
    mpWriteStr(out, o.key);
    mpWriteStr(out, "entry");
    mpWriteFrameEntry(out, o.entry);
}

static void mpWriteManifest(std::vector<uint8_t>& out, const Manifest& m) {
    mpWriteMapHeader(out, 10);
    mpWriteStr(out, "version");
    mpWriteUInt(out, m.version);
    mpWriteStr(out, "pageCount");
    mpWriteUInt(out, m.pageCount);
    mpWriteStr(out, "pageSize");
    mpWriteUInt(out, m.pageSize);
    mpWriteStr(out, "pagesPerGroup");
    mpWriteUInt(out, m.pagesPerGroup);
    mpWriteStr(out, "pageGroupKeys");
    mpWriteArrayHeader(out, m.pageGroupKeys.size());
    for (const auto& key : m.pageGroupKeys) {
        mpWriteStr(out, key);
    }
    mpWriteStr(out, "frameTables");
    mpWriteArrayHeader(out, m.frameTables.size());
    for (const auto& group : m.frameTables) {
        mpWriteArrayHeader(out, group.size());
        for (const auto& entry : group) {
            mpWriteFrameEntry(out, entry);
        }
    }
    mpWriteStr(out, "subPagesPerFrame");
    mpWriteUInt(out, m.subPagesPerFrame);
    mpWriteStr(out, "subframeOverrides");
    mpWriteArrayHeader(out, m.subframeOverrides.size());
    for (const auto& group : m.subframeOverrides) {
        mpWriteMapHeader(out, group.size());
        for (const auto& [frameIdx, ov] : group) {
            mpWriteUInt(out, frameIdx);
            mpWriteSubframeOverride(out, ov);
        }
    }
    mpWriteStr(out, "encrypted");
    mpWriteBool(out, m.encrypted);
    mpWriteStr(out, "journalSeq");
    mpWriteUInt(out, m.journalSeq);
}

static void mpWriteHybridPayload(std::vector<uint8_t>& out, const HybridPayload& h) {
    mpWriteMapHeader(out, 3);
    mpWriteStr(out, "turbograph");
    mpWriteManifest(out, h.turbograph);
    mpWriteStr(out, "graphstream_journal_seq");
    mpWriteUInt(out, h.graphstream_journal_seq);
    mpWriteStr(out, "graphstream_segment_prefix");
    mpWriteStr(out, h.graphstream_segment_prefix);
}

// Decoder helpers
class MpReader {
public:
    const uint8_t* p;
    size_t len;

    explicit MpReader(const std::vector<uint8_t>& data)
        : p(data.data()), len(data.size()) {}

    bool empty() const { return len == 0; }

    uint8_t peek() const {
        if (len == 0) throw std::runtime_error("msgpack unexpected EOF");
        return *p;
    }

    uint8_t readU8() {
        if (len == 0) throw std::runtime_error("msgpack unexpected EOF");
        uint8_t v = *p;
        p++;
        len--;
        return v;
    }

    uint16_t readU16() {
        if (len < 2) throw std::runtime_error("msgpack unexpected EOF");
        uint16_t v = (static_cast<uint16_t>(p[0]) << 8) | p[1];
        p += 2;
        len -= 2;
        return v;
    }

    uint32_t readU32() {
        if (len < 4) throw std::runtime_error("msgpack unexpected EOF");
        uint32_t v = (static_cast<uint32_t>(p[0]) << 24)
                   | (static_cast<uint32_t>(p[1]) << 16)
                   | (static_cast<uint32_t>(p[2]) << 8)
                   | p[3];
        p += 4;
        len -= 4;
        return v;
    }

    uint64_t readU64() {
        if (len < 8) throw std::runtime_error("msgpack unexpected EOF");
        uint64_t v = (static_cast<uint64_t>(p[0]) << 56)
                   | (static_cast<uint64_t>(p[1]) << 48)
                   | (static_cast<uint64_t>(p[2]) << 40)
                   | (static_cast<uint64_t>(p[3]) << 32)
                   | (static_cast<uint64_t>(p[4]) << 24)
                   | (static_cast<uint64_t>(p[5]) << 16)
                   | (static_cast<uint64_t>(p[6]) << 8)
                   | p[7];
        p += 8;
        len -= 8;
        return v;
    }

    uint64_t readUInt() {
        uint8_t tag = readU8();
        if (tag <= 0x7F) return tag;
        switch (tag) {
            case 0xCC: return readU8();
            case 0xCD: return readU16();
            case 0xCE: return readU32();
            case 0xCF: return readU64();
            default: throw std::runtime_error("msgpack unexpected integer tag");
        }
    }

    std::string readStr() {
        uint8_t tag = readU8();
        size_t n = 0;
        if ((tag & 0xE0) == 0xA0) {
            n = tag & 0x1F;
        } else if (tag == 0xD9) {
            n = readU8();
        } else if (tag == 0xDA) {
            n = readU16();
        } else if (tag == 0xDB) {
            n = readU32();
        } else {
            throw std::runtime_error("msgpack unexpected string tag");
        }
        if (len < n) throw std::runtime_error("msgpack unexpected EOF in string");
        std::string s(reinterpret_cast<const char*>(p), n);
        p += n;
        len -= n;
        return s;
    }

    bool readBool() {
        uint8_t tag = readU8();
        if (tag == 0xC2) return false;
        if (tag == 0xC3) return true;
        throw std::runtime_error("msgpack unexpected bool tag");
    }

    size_t readArrayHeader() {
        uint8_t tag = readU8();
        if ((tag & 0xF0) == 0x90) return tag & 0x0F;
        if (tag == 0xDC) return readU16();
        if (tag == 0xDD) return readU32();
        throw std::runtime_error("msgpack unexpected array tag");
    }

    size_t readMapHeader() {
        uint8_t tag = readU8();
        if ((tag & 0xF0) == 0x80) return tag & 0x0F;
        if (tag == 0xDE) return readU16();
        if (tag == 0xDF) return readU32();
        throw std::runtime_error("msgpack unexpected map tag");
    }

    void skipValue() {
        uint8_t tag = readU8();
        if (tag <= 0x7F || tag == 0xCC || tag == 0xCD || tag == 0xCE || tag == 0xCF) {
            if (tag == 0xCC) { readU8(); }
            else if (tag == 0xCD) { readU16(); }
            else if (tag == 0xCE) { readU32(); }
            else if (tag == 0xCF) { readU64(); }
            return;
        }
        if ((tag & 0xE0) == 0xA0) {
            size_t n = tag & 0x1F;
            if (len < n) throw std::runtime_error("msgpack EOF");
            p += n; len -= n;
            return;
        }
        if (tag == 0xD9) { size_t n = readU8(); if (len < n) throw std::runtime_error("msgpack EOF"); p += n; len -= n; return; }
        if (tag == 0xDA) { size_t n = readU16(); if (len < n) throw std::runtime_error("msgpack EOF"); p += n; len -= n; return; }
        if (tag == 0xDB) { size_t n = readU32(); if (len < n) throw std::runtime_error("msgpack EOF"); p += n; len -= n; return; }
        if ((tag & 0xF0) == 0x80) {
            size_t n = tag & 0x0F;
            for (size_t i = 0; i < n * 2; i++) skipValue();
            return;
        }
        if (tag == 0xDE) { size_t n = readU16(); for (size_t i = 0; i < n * 2; i++) skipValue(); return; }
        if (tag == 0xDF) { size_t n = readU32(); for (size_t i = 0; i < n * 2; i++) skipValue(); return; }
        if ((tag & 0xF0) == 0x90) {
            size_t n = tag & 0x0F;
            for (size_t i = 0; i < n; i++) skipValue();
            return;
        }
        if (tag == 0xDC) { size_t n = readU16(); for (size_t i = 0; i < n; i++) skipValue(); return; }
        if (tag == 0xDD) { size_t n = readU32(); for (size_t i = 0; i < n; i++) skipValue(); return; }
        if (tag == 0xC0 || tag == 0xC2 || tag == 0xC3) return;
        throw std::runtime_error("msgpack skip unsupported tag");
    }
};

static FrameEntry mpReadFrameEntry(MpReader& r) {
    size_t n = r.readMapHeader();
    FrameEntry e;
    for (size_t i = 0; i < n; i++) {
        std::string key = r.readStr();
        if (key == "offset") e.offset = r.readUInt();
        else if (key == "len") e.len = static_cast<uint32_t>(r.readUInt());
        else if (key == "pageCount") e.pageCount = static_cast<uint32_t>(r.readUInt());
        else r.skipValue();
    }
    return e;
}

static SubframeOverride mpReadSubframeOverride(MpReader& r) {
    size_t n = r.readMapHeader();
    SubframeOverride o;
    for (size_t i = 0; i < n; i++) {
        std::string key = r.readStr();
        if (key == "key") o.key = r.readStr();
        else if (key == "entry") o.entry = mpReadFrameEntry(r);
        else r.skipValue();
    }
    return o;
}

static Manifest mpReadManifest(MpReader& r) {
    size_t n = r.readMapHeader();
    Manifest m;
    for (size_t i = 0; i < n; i++) {
        std::string key = r.readStr();
        if (key == "version") m.version = r.readUInt();
        else if (key == "pageCount") m.pageCount = r.readUInt();
        else if (key == "pageSize") m.pageSize = static_cast<uint32_t>(r.readUInt());
        else if (key == "pagesPerGroup") m.pagesPerGroup = static_cast<uint32_t>(r.readUInt());
        else if (key == "pageGroupKeys") {
            size_t arr = r.readArrayHeader();
            m.pageGroupKeys.reserve(arr);
            for (size_t j = 0; j < arr; j++) m.pageGroupKeys.push_back(r.readStr());
        } else if (key == "frameTables") {
            size_t arr = r.readArrayHeader();
            m.frameTables.reserve(arr);
            for (size_t g = 0; g < arr; g++) {
                size_t frames = r.readArrayHeader();
                std::vector<FrameEntry> group;
                group.reserve(frames);
                for (size_t f = 0; f < frames; f++) group.push_back(mpReadFrameEntry(r));
                m.frameTables.push_back(std::move(group));
            }
        } else if (key == "subPagesPerFrame") m.subPagesPerFrame = static_cast<uint32_t>(r.readUInt());
        else if (key == "subframeOverrides") {
            size_t arr = r.readArrayHeader();
            m.subframeOverrides.reserve(arr);
            for (size_t g = 0; g < arr; g++) {
                size_t mapn = r.readMapHeader();
                std::unordered_map<size_t, SubframeOverride> group;
                for (size_t j = 0; j < mapn; j++) {
                    size_t frameIdx = r.readUInt();
                    group[frameIdx] = mpReadSubframeOverride(r);
                }
                m.subframeOverrides.push_back(std::move(group));
            }
        } else if (key == "encrypted") m.encrypted = r.readBool();
        else if (key == "journalSeq") m.journalSeq = r.readUInt();
        else r.skipValue();
    }
    return m;
}

static HybridPayload mpReadHybridPayload(MpReader& r) {
    size_t n = r.readMapHeader();
    HybridPayload h;
    for (size_t i = 0; i < n; i++) {
        std::string key = r.readStr();
        if (key == "turbograph") h.turbograph = mpReadManifest(r);
        else if (key == "graphstream_journal_seq") h.graphstream_journal_seq = r.readUInt();
        else if (key == "graphstream_segment_prefix") h.graphstream_segment_prefix = r.readStr();
        else r.skipValue();
    }
    return h;
}

std::string Manifest::toJSON() const {
    std::string json = "{\"version\":";
    json += std::to_string(version);
    json += ",\"page_count\":";
    json += std::to_string(pageCount);
    json += ",\"page_size\":";
    json += std::to_string(pageSize);
    json += ",\"pages_per_group\":";
    json += std::to_string(pagesPerGroup);
    json += ",\"page_group_keys\":[";
    for (size_t i = 0; i < pageGroupKeys.size(); i++) {
        if (i > 0) json += ',';
        json += '"';
        json += pageGroupKeys[i];
        json += '"';
    }
    json += ']';

    // Seekable frame fields (omitted when not seekable for backward compat).
    if (subPagesPerFrame > 0) {
        json += ",\"sub_pages_per_frame\":";
        json += std::to_string(subPagesPerFrame);
        json += ",\"frame_tables\":[";
        for (size_t g = 0; g < frameTables.size(); g++) {
            if (g > 0) json += ',';
            json += '[';
            for (size_t f = 0; f < frameTables[g].size(); f++) {
                if (f > 0) json += ',';
                json += "[";
                json += std::to_string(frameTables[g][f].offset);
                json += ',';
                json += std::to_string(frameTables[g][f].len);
                json += ',';
                json += std::to_string(frameTables[g][f].pageCount);
                json += ']';
            }
            json += ']';
        }
        json += ']';
    }

    // Subframe overrides are omitted when empty for backward compatibility.
    if (!subframeOverrides.empty()) {
        bool hasAny = false;
        for (auto& ovMap : subframeOverrides) {
            if (!ovMap.empty()) { hasAny = true; break; }
        }
        if (hasAny) {
            json += ",\"subframe_overrides\":[";
            for (size_t g = 0; g < subframeOverrides.size(); g++) {
                if (g > 0) json += ',';
                auto& ovMap = subframeOverrides[g];
                if (ovMap.empty()) {
                    json += "{}";
                } else {
                    json += '{';
                    bool first = true;
                    for (auto& [frameIdx, ov] : ovMap) {
                        if (!first) json += ',';
                        first = false;
                        json += '"';
                        json += std::to_string(frameIdx);
                        json += "\":{\"key\":\"";
                        json += ov.key;
                        json += "\",\"offset\":";
                        json += std::to_string(ov.entry.offset);
                        json += ",\"len\":";
                        json += std::to_string(ov.entry.len);
                        json += ",\"pageCount\":";
                        json += std::to_string(ov.entry.pageCount);
                        json += '}';
                    }
                    json += '}';
                }
            }
            json += ']';
        }
    }

    if (encrypted) {
        json += ",\"encrypted\":true";
    }

    // journal_seq is omitted when 0 for backward compatibility.
    if (journalSeq > 0) {
        json += ",\"journal_seq\":";
        json += std::to_string(journalSeq);
    }

    json += '}';
    return json;
}

// Minimal JSON parser. No library dependency.
static bool findU64(const std::string& json, const char* key, uint64_t& out) {
    auto pos = json.find(key);
    if (pos == std::string::npos) {
        return false;
    }
    pos = json.find(':', pos);
    if (pos == std::string::npos) {
        return false;
    }
    pos++;
    while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t')) {
        pos++;
    }
    char* end = nullptr;
    out = std::strtoull(json.c_str() + pos, &end, 10);
    return end != json.c_str() + pos;
}

static bool findU32(const std::string& json, const char* key, uint32_t& out) {
    uint64_t val = 0;
    if (!findU64(json, key, val)) {
        return false;
    }
    out = static_cast<uint32_t>(val);
    return true;
}

static std::optional<std::vector<std::string>> findStringArray(
    const std::string& json, const char* key) {
    auto pos = json.find(key);
    if (pos == std::string::npos) return std::nullopt;
    pos = json.find('[', pos);
    if (pos == std::string::npos) return std::nullopt;
    pos++; // skip '['

    std::vector<std::string> result;
    while (pos < json.size()) {
        // Skip whitespace and commas.
        while (pos < json.size() && (json[pos] == ' ' || json[pos] == '\t' ||
               json[pos] == '\n' || json[pos] == '\r' || json[pos] == ',')) {
            pos++;
        }
        if (pos >= json.size() || json[pos] == ']') break;
        if (json[pos] != '"') return std::nullopt;
        pos++; // skip opening quote
        auto end = json.find('"', pos);
        if (end == std::string::npos) return std::nullopt;
        result.push_back(json.substr(pos, end - pos));
        pos = end + 1;
    }
    return result;
}

// Parse frame_tables: [[offset,len], ...] per group, nested in outer array.
static std::optional<std::vector<std::vector<FrameEntry>>> parseFrameTables(
    const std::string& json) {
    auto outerPos = json.find("\"frame_tables\"");
    if (outerPos == std::string::npos) return std::nullopt;
    outerPos = json.find('[', outerPos);
    if (outerPos == std::string::npos) return std::nullopt;
    outerPos++; // skip outer '['

    std::vector<std::vector<FrameEntry>> result;

    while (outerPos < json.size()) {
        // Skip whitespace and commas.
        while (outerPos < json.size() && (json[outerPos] == ' ' || json[outerPos] == '\t' ||
               json[outerPos] == '\n' || json[outerPos] == '\r' || json[outerPos] == ',')) {
            outerPos++;
        }
        if (outerPos >= json.size() || json[outerPos] == ']') break;
        if (json[outerPos] != '[') return std::nullopt;
        outerPos++; // skip group '['

        std::vector<FrameEntry> groupEntries;
        while (outerPos < json.size()) {
            while (outerPos < json.size() && (json[outerPos] == ' ' || json[outerPos] == '\t' ||
                   json[outerPos] == '\n' || json[outerPos] == '\r' || json[outerPos] == ',')) {
                outerPos++;
            }
            if (outerPos >= json.size() || json[outerPos] == ']') { outerPos++; break; }
            if (json[outerPos] != '[') return std::nullopt;
            outerPos++; // skip entry '['

            // Parse [offset, len, pageCount].
            FrameEntry entry;
            char* end = nullptr;
            entry.offset = std::strtoull(json.c_str() + outerPos, &end, 10);
            outerPos = end - json.c_str();
            while (outerPos < json.size() && (json[outerPos] == ',' || json[outerPos] == ' ')) {
                outerPos++;
            }
            entry.len = static_cast<uint32_t>(std::strtoul(json.c_str() + outerPos, &end, 10));
            outerPos = end - json.c_str();
            // pageCount is optional (backward compat with old 2-element entries).
            while (outerPos < json.size() && (json[outerPos] == ',' || json[outerPos] == ' ')) {
                outerPos++;
            }
            if (outerPos < json.size() && json[outerPos] != ']') {
                entry.pageCount = static_cast<uint32_t>(std::strtoul(json.c_str() + outerPos, &end, 10));
                outerPos = end - json.c_str();
            }
            while (outerPos < json.size() && json[outerPos] != ']') outerPos++;
            if (outerPos < json.size()) outerPos++; // skip ']'

            groupEntries.push_back(entry);
        }
        result.push_back(std::move(groupEntries));
    }
    return result;
}

void Manifest::normalizeOverrides() {
    while (subframeOverrides.size() < pageGroupKeys.size()) {
        subframeOverrides.emplace_back();
    }
}

// Parse subframe_overrides: array of objects, each object maps frame index (string key)
// to {key, offset, len, pageCount}.
static std::optional<std::vector<std::unordered_map<size_t, SubframeOverride>>>
parseSubframeOverrides(const std::string& json) {
    auto outerPos = json.find("\"subframe_overrides\"");
    if (outerPos == std::string::npos) return std::nullopt;
    outerPos = json.find('[', outerPos);
    if (outerPos == std::string::npos) return std::nullopt;
    outerPos++; // skip outer '['

    std::vector<std::unordered_map<size_t, SubframeOverride>> result;

    while (outerPos < json.size()) {
        // Skip whitespace and commas.
        while (outerPos < json.size() && (json[outerPos] == ' ' || json[outerPos] == '\t' ||
               json[outerPos] == '\n' || json[outerPos] == '\r' || json[outerPos] == ',')) {
            outerPos++;
        }
        if (outerPos >= json.size() || json[outerPos] == ']') break;
        if (json[outerPos] != '{') return std::nullopt;
        outerPos++; // skip '{'

        std::unordered_map<size_t, SubframeOverride> groupOverrides;

        while (outerPos < json.size()) {
            // Skip whitespace and commas.
            while (outerPos < json.size() && (json[outerPos] == ' ' || json[outerPos] == '\t' ||
                   json[outerPos] == '\n' || json[outerPos] == '\r' || json[outerPos] == ',')) {
                outerPos++;
            }
            if (outerPos >= json.size() || json[outerPos] == '}') { outerPos++; break; }

            // Parse frame index key (quoted string).
            if (json[outerPos] != '"') return std::nullopt;
            outerPos++;
            auto keyEnd = json.find('"', outerPos);
            if (keyEnd == std::string::npos) return std::nullopt;
            auto frameIdxStr = json.substr(outerPos, keyEnd - outerPos);
            size_t frameIdx = std::stoul(frameIdxStr);
            outerPos = keyEnd + 1;

            // Skip colon.
            while (outerPos < json.size() && (json[outerPos] == ' ' || json[outerPos] == ':')) {
                outerPos++;
            }

            // Parse value object: {key, offset, len, pageCount}.
            if (outerPos >= json.size() || json[outerPos] != '{') return std::nullopt;
            // Find matching closing brace.
            auto objStart = outerPos;
            int depth = 0;
            for (; outerPos < json.size(); outerPos++) {
                if (json[outerPos] == '{') depth++;
                else if (json[outerPos] == '}') {
                    depth--;
                    if (depth == 0) { outerPos++; break; }
                }
            }
            auto objStr = json.substr(objStart, outerPos - objStart);

            SubframeOverride ov;

            // Parse "key" field.
            auto keyPos = objStr.find("\"key\"");
            if (keyPos != std::string::npos) {
                keyPos = objStr.find('"', keyPos + 5);
                if (keyPos != std::string::npos) {
                    keyPos++;
                    auto end = objStr.find('"', keyPos);
                    if (end != std::string::npos) {
                        ov.key = objStr.substr(keyPos, end - keyPos);
                    }
                }
            }

            // Parse numeric fields.
            uint64_t tmpU64 = 0;
            if (findU64(objStr, "\"offset\"", tmpU64)) {
                ov.entry.offset = tmpU64;
            }
            uint32_t tmpU32 = 0;
            if (findU32(objStr, "\"len\"", tmpU32)) {
                ov.entry.len = tmpU32;
            }
            if (findU32(objStr, "\"pageCount\"", tmpU32)) {
                ov.entry.pageCount = tmpU32;
            }

            groupOverrides[frameIdx] = std::move(ov);
        }

        result.push_back(std::move(groupOverrides));
    }
    return result;
}

std::optional<Manifest> Manifest::fromJSON(const std::string& json) {
    Manifest m;
    if (!findU64(json, "\"version\"", m.version)) {
        return std::nullopt;
    }
    if (!findU64(json, "\"page_count\"", m.pageCount)) {
        return std::nullopt;
    }
    if (!findU32(json, "\"page_size\"", m.pageSize)) {
        return std::nullopt;
    }
    if (!findU32(json, "\"pages_per_group\"", m.pagesPerGroup)) {
        return std::nullopt;
    }
    auto keys = findStringArray(json, "\"page_group_keys\"");
    if (!keys.has_value()) {
        return std::nullopt;
    }
    m.pageGroupKeys = std::move(*keys);

    // Optional seekable frame fields (backward compat: missing = legacy).
    findU32(json, "\"sub_pages_per_frame\"", m.subPagesPerFrame);
    if (m.subPagesPerFrame > 0) {
        auto ft = parseFrameTables(json);
        if (ft.has_value()) {
            m.frameTables = std::move(*ft);
        }
    }

    // Optional subframe overrides. Missing means empty for backward compatibility.
    auto ovs = parseSubframeOverrides(json);
    if (ovs.has_value()) {
        m.subframeOverrides = std::move(*ovs);
    }
    m.normalizeOverrides();

    // Optional encrypted flag (backward compat: missing = false).
    m.encrypted = json.find("\"encrypted\":true") != std::string::npos;

    // Optional journal_seq. Missing means 0 for backward compatibility.
    findU64(json, "\"journal_seq\"", m.journalSeq);

    return m;
}

// ============================================================================
// Msgpack wire serialization
// ============================================================================

std::vector<uint8_t> Manifest::toMsgpack() const {
    std::vector<uint8_t> out;
    mpWriteManifest(out, *this);
    return out;
}

std::optional<Manifest> Manifest::fromMsgpack(const std::vector<uint8_t>& bytes) {
    try {
        MpReader r(bytes);
        Manifest m = mpReadManifest(r);
        return m;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

std::vector<uint8_t> HybridPayload::toMsgpack() const {
    std::vector<uint8_t> out;
    mpWriteHybridPayload(out, *this);
    return out;
}

std::optional<HybridPayload> HybridPayload::fromMsgpack(const std::vector<uint8_t>& bytes) {
    try {
        MpReader r(bytes);
        HybridPayload h = mpReadHybridPayload(r);
        return h;
    } catch (const std::exception&) {
        return std::nullopt;
    }
}

} // namespace tiered
} // namespace lbug
