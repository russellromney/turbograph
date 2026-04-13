#include "manifest.h"

#include <cstdio>
#include <cstdlib>
#include <cstring>

namespace lbug {
namespace tiered {

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

    // Phase GraphDrift: subframe overrides (omitted when empty for backward compat).
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

    // Phase GraphZenith: journal_seq (omitted when 0 for backward compat).
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

    // Phase GraphDrift: optional subframe overrides (backward compat: missing = empty).
    auto ovs = parseSubframeOverrides(json);
    if (ovs.has_value()) {
        m.subframeOverrides = std::move(*ovs);
    }
    m.normalizeOverrides();

    // Optional encrypted flag (backward compat: missing = false).
    m.encrypted = json.find("\"encrypted\":true") != std::string::npos;

    // Phase GraphZenith: optional journal_seq (backward compat: missing = 0).
    findU64(json, "\"journal_seq\"", m.journalSeq);

    return m;
}

} // namespace tiered
} // namespace lbug
