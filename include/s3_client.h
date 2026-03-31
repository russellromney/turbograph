#pragma once

#include "connection_pool.h"
#include "manifest.h"

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace httplib {
class Client;
}

namespace lbug {
namespace tiered {

struct S3Config {
    std::string endpoint;  // e.g. "https://fly.storage.tigris.dev"
    std::string bucket;
    std::string prefix;    // e.g. "graphs/{tenant_id}"
    std::string region;    // e.g. "auto"
    std::string accessKey;
    std::string secretKey;
    uint32_t poolSize = 16; // Connection pool size.
};

// Range request input.
struct RangeRequest {
    std::string key;
    uint64_t offset;
    uint64_t length;
    uint64_t tag; // Caller-defined identifier for correlating responses.
};

// Range request output.
struct RangeResponse {
    uint64_t tag;
    std::vector<uint8_t> data;
};

// Minimal S3 client for tiered storage. Uses httplib + SigV4.
// Connection pool for parallel operations.
class S3Client {
public:
    explicit S3Client(S3Config config);
    ~S3Client();

    // Object operations.
    std::optional<std::vector<uint8_t>> getObject(const std::string& key);
    bool putObject(const std::string& key, const uint8_t* data, uint64_t size);
    bool deleteObject(const std::string& key);

    // Byte-range GET of a single range from an object.
    std::optional<std::vector<uint8_t>> getObjectRange(
        const std::string& key, uint64_t offset, uint64_t length);

    // Parallel byte-range GETs. Each request can target any key.
    // Returns responses for all successful fetches. Missing tags = failed.
    std::vector<RangeResponse> getObjectRanges(const std::vector<RangeRequest>& requests);

    // Immutable page group key: {prefix}/pg/{groupId}_v{manifestVersion}.
    std::string pageGroupKey(uint64_t groupId, uint64_t manifestVersion) const;

    // List all objects under a prefix. Returns full S3 keys.
    std::vector<std::string> listObjects(const std::string& prefix);

    // Delete stale page groups not referenced by the current manifest.
    // Skips keys with version >= manifest.version (race protection).
    uint64_t evictStalePageGroups(const Manifest& manifest);

    // Manifest operations.
    std::optional<Manifest> getManifest();
    bool putManifest(const Manifest& manifest);

    // I/O counters for benchmarking.
    std::atomic<uint64_t> fetchCount{0};
    std::atomic<uint64_t> fetchBytes{0};
    void resetCounters() { fetchCount = 0; fetchBytes = 0; }

    // Expose prefix for callers that need to build S3 paths.
    const std::string& prefix() const { return config_.prefix; }

private:
    // SigV4 signing.
    struct SignedHeaders {
        std::string authorization;
        std::string date;
        std::string payloadHash;
        std::string host;
    };

    SignedHeaders signRequest(const std::string& method, const std::string& path,
        const std::string& payloadHash) const;

    // Execute a GET with optional Range header using a pooled connection.
    std::optional<std::vector<uint8_t>> doGet(const std::string& key,
        const std::string& rangeHeader = "");

    // Execute a PUT using a pooled connection.
    bool doPut(const std::string& key, const uint8_t* data, uint64_t size);

    // Execute a DELETE using a pooled connection.
    bool doDelete(const std::string& key);

    static std::string sha256Hex(const uint8_t* data, uint64_t size);
    static std::string sha256Hex(const std::string& data);
    static std::string currentTimestamp();
    static std::string currentDateStamp();

    // Sort query parameters alphabetically for SigV4 canonical request.
    static std::string sortQueryParams(const std::string& rawQuery);

    S3Config config_;
    std::shared_ptr<ConnectionPool> pool_;
    std::string host_; // Parsed from endpoint, cached.
};

} // namespace tiered
} // namespace lbug
