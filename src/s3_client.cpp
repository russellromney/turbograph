#include "s3_client.h"

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <thread>
#include <unordered_set>

#include "httplib.h"

#include <openssl/hmac.h>
#include <openssl/sha.h>

namespace lbug {
namespace tiered {

// --- Crypto helpers ---

static std::string bytesToHex(const unsigned char* bytes, size_t len) {
    std::string result;
    result.reserve(len * 2);
    static const char hexChars[] = "0123456789abcdef";
    for (size_t i = 0; i < len; i++) {
        result.push_back(hexChars[bytes[i] >> 4]);
        result.push_back(hexChars[bytes[i] & 0x0f]);
    }
    return result;
}

std::string S3Client::sha256Hex(const uint8_t* data, uint64_t size) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256(data, size, hash);
    return bytesToHex(hash, SHA256_DIGEST_LENGTH);
}

std::string S3Client::sha256Hex(const std::string& data) {
    return sha256Hex(reinterpret_cast<const uint8_t*>(data.c_str()), data.size());
}

static std::vector<uint8_t> hmacSHA256(const void* key, size_t keyLen, const std::string& msg) {
    unsigned char result[EVP_MAX_MD_SIZE];
    unsigned int resultLen = 0;
    HMAC(EVP_sha256(), key, static_cast<int>(keyLen),
        reinterpret_cast<const unsigned char*>(msg.c_str()), msg.size(), result, &resultLen);
    return std::vector<uint8_t>(result, result + resultLen);
}

// --- Timestamps ---

std::string S3Client::currentTimestamp() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[32];
    std::strftime(buf, sizeof(buf), "%Y%m%dT%H%M%SZ", &tm);
    return std::string(buf);
}

std::string S3Client::currentDateStamp() {
    auto now = std::chrono::system_clock::now();
    auto t = std::chrono::system_clock::to_time_t(now);
    std::tm tm{};
    gmtime_r(&t, &tm);
    char buf[16];
    std::strftime(buf, sizeof(buf), "%Y%m%d", &tm);
    return std::string(buf);
}

// --- Query string helpers ---

// URI-encode a string per SigV4: encode all characters except unreserved (A-Za-z0-9_.~-).
static std::string uriEncode(const std::string& s) {
    std::string result;
    result.reserve(s.size() * 2);
    for (unsigned char c : s) {
        if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') ||
            (c >= '0' && c <= '9') || c == '_' || c == '.' || c == '~' || c == '-') {
            result += static_cast<char>(c);
        } else {
            char hex[4];
            std::snprintf(hex, sizeof(hex), "%%%02X", c);
            result += hex;
        }
    }
    return result;
}

std::string S3Client::sortQueryParams(const std::string& rawQuery) {
    if (rawQuery.empty()) return {};
    // Split into key=value pairs, URI-encode each, sort alphabetically.
    std::vector<std::string> params;
    size_t pos = 0;
    while (pos < rawQuery.size()) {
        auto amp = rawQuery.find('&', pos);
        auto param = (amp == std::string::npos)
            ? rawQuery.substr(pos)
            : rawQuery.substr(pos, amp - pos);
        // URI-encode the value (part after '=').
        auto eq = param.find('=');
        if (eq != std::string::npos) {
            auto key = uriEncode(param.substr(0, eq));
            auto val = uriEncode(param.substr(eq + 1));
            params.push_back(key + "=" + val);
        } else {
            params.push_back(uriEncode(param) + "=");
        }
        if (amp == std::string::npos) break;
        pos = amp + 1;
    }
    std::sort(params.begin(), params.end());
    std::string result;
    for (size_t i = 0; i < params.size(); i++) {
        if (i > 0) result += '&';
        result += params[i];
    }
    return result;
}

// --- SigV4 Signing ---

S3Client::SignedHeaders S3Client::signRequest(const std::string& method, const std::string& path,
    const std::string& payloadHash) const {
    auto timestamp = currentTimestamp();
    auto dateStamp = timestamp.substr(0, 8);
    auto service = std::string("s3");
    auto credentialScope = dateStamp + "/" + config_.region + "/" + service + "/aws4_request";

    // Split path on '?' to handle query strings for LIST requests.
    std::string canonicalURI;
    std::string canonicalQueryString;
    auto qpos = path.find('?');
    if (qpos != std::string::npos) {
        canonicalURI = path.substr(0, qpos);
        canonicalQueryString = sortQueryParams(path.substr(qpos + 1));
    } else {
        canonicalURI = path;
    }

    auto canonicalHeaders =
        "host:" + host_ + "\n" + "x-amz-content-sha256:" + payloadHash + "\n" +
        "x-amz-date:" + timestamp + "\n";
    auto signedHeaders = std::string("host;x-amz-content-sha256;x-amz-date");

    auto canonicalRequest = method + "\n" + canonicalURI + "\n" + canonicalQueryString + "\n" +
                            canonicalHeaders + "\n" + signedHeaders + "\n" + payloadHash;

    // String to sign.
    auto stringToSign =
        "AWS4-HMAC-SHA256\n" + timestamp + "\n" + credentialScope + "\n" + sha256Hex(canonicalRequest);

    // Signing key.
    auto kDate =
        hmacSHA256(("AWS4" + config_.secretKey).c_str(), 4 + config_.secretKey.size(), dateStamp);
    auto kRegion = hmacSHA256(kDate.data(), kDate.size(), config_.region);
    auto kService = hmacSHA256(kRegion.data(), kRegion.size(), service);
    auto kSigning = hmacSHA256(kService.data(), kService.size(), "aws4_request");

    // Signature.
    auto signatureBytes = hmacSHA256(kSigning.data(), kSigning.size(), stringToSign);
    auto signature = bytesToHex(signatureBytes.data(), signatureBytes.size());

    auto authorization = "AWS4-HMAC-SHA256 Credential=" + config_.accessKey + "/" +
                         credentialScope + ", SignedHeaders=" + signedHeaders +
                         ", Signature=" + signature;

    return SignedHeaders{authorization, timestamp, payloadHash, host_};
}

// --- Constructor / Destructor ---

static std::string parseHost(const std::string& endpoint) {
    std::string host = endpoint;
    if (host.find("https://") == 0) {
        host = host.substr(8);
    } else if (host.find("http://") == 0) {
        host = host.substr(7);
    }
    if (!host.empty() && host.back() == '/') {
        host.pop_back();
    }
    return host;
}

S3Client::S3Client(S3Config config) : config_(std::move(config)) {
    host_ = parseHost(config_.endpoint);
    pool_ = std::make_shared<ConnectionPool>(config_.endpoint, config_.poolSize);
}

S3Client::~S3Client() = default;

// --- Core I/O primitives (pooled) ---

std::optional<std::vector<uint8_t>> S3Client::doGet(const std::string& key,
    const std::string& rangeHeader) {
    auto objectPath = "/" + config_.bucket + "/" + key;
    auto emptyHash = sha256Hex(reinterpret_cast<const uint8_t*>(""), 0);
    auto headers = signRequest("GET", objectPath, emptyHash);

    httplib::Headers httpHeaders = {{"Host", headers.host},
        {"x-amz-content-sha256", headers.payloadHash}, {"x-amz-date", headers.date},
        {"Authorization", headers.authorization}};

    if (!rangeHeader.empty()) {
        httpHeaders.emplace("Range", rangeHeader);
    }

    PooledClient client(*pool_);
    auto res = client->Get(objectPath, httpHeaders);
    if (!res) {
        return std::nullopt;
    }
    if (res->status == 404) {
        return std::nullopt;
    }
    // Accept both 200 (full object) and 206 (partial content / range request).
    if (res->status != 200 && res->status != 206) {
        return std::nullopt;
    }
    fetchCount.fetch_add(1, std::memory_order_relaxed);
    fetchBytes.fetch_add(res->body.size(), std::memory_order_relaxed);
    return std::vector<uint8_t>(res->body.begin(), res->body.end());
}

bool S3Client::doPut(const std::string& key, const uint8_t* data, uint64_t size) {
    auto objectPath = "/" + config_.bucket + "/" + key;
    auto payloadHash = sha256Hex(data, size);
    auto headers = signRequest("PUT", objectPath, payloadHash);

    httplib::Headers httpHeaders = {{"Host", headers.host},
        {"x-amz-content-sha256", headers.payloadHash}, {"x-amz-date", headers.date},
        {"Authorization", headers.authorization}};

    PooledClient client(*pool_);
    auto res = client->Put(objectPath, httpHeaders,
        reinterpret_cast<const char*>(data), size, "application/octet-stream");
    return res && res->status >= 200 && res->status < 300;
}

bool S3Client::doDelete(const std::string& key) {
    auto objectPath = "/" + config_.bucket + "/" + key;
    auto emptyHash = sha256Hex(reinterpret_cast<const uint8_t*>(""), 0);
    auto headers = signRequest("DELETE", objectPath, emptyHash);

    httplib::Headers httpHeaders = {{"Host", headers.host},
        {"x-amz-content-sha256", headers.payloadHash}, {"x-amz-date", headers.date},
        {"Authorization", headers.authorization}};

    PooledClient client(*pool_);
    auto res = client->Delete(objectPath, httpHeaders);
    return res && (res->status == 204 || res->status == 200 || res->status == 404);
}

// --- Object Operations ---

std::optional<std::vector<uint8_t>> S3Client::getObject(const std::string& key) {
    return doGet(key);
}

bool S3Client::putObject(const std::string& key, const uint8_t* data, uint64_t size) {
    return doPut(key, data, size);
}

bool S3Client::deleteObject(const std::string& key) {
    return doDelete(key);
}

// --- Range Request Operations ---

std::optional<std::vector<uint8_t>> S3Client::getObjectRange(
    const std::string& key, uint64_t offset, uint64_t length) {
    auto rangeStr = "bytes=" + std::to_string(offset) + "-" + std::to_string(offset + length - 1);
    return doGet(key, rangeStr);
}

std::vector<RangeResponse> S3Client::getObjectRanges(const std::vector<RangeRequest>& requests) {
    if (requests.empty()) {
        return {};
    }
    if (requests.size() == 1) {
        auto data = getObjectRange(requests[0].key, requests[0].offset, requests[0].length);
        if (data.has_value()) {
            return {{requests[0].tag, std::move(*data)}};
        }
        return {};
    }

    // Parallel range requests. Each thread acquires a pooled connection —
    // if pool is exhausted, threads block until a connection is available.
    // This provides natural backpressure without wave batching.
    std::vector<RangeResponse> results;
    std::mutex resultsMu;
    std::vector<std::thread> threads;
    threads.reserve(requests.size());

    for (auto& req : requests) {
        threads.emplace_back([this, &req, &results, &resultsMu]() {
            auto objectPath = "/" + config_.bucket + "/" + req.key;
            auto emptyHash = sha256Hex(reinterpret_cast<const uint8_t*>(""), 0);
            auto headers = signRequest("GET", objectPath, emptyHash);

            auto rangeStr = "bytes=" + std::to_string(req.offset) + "-" +
                            std::to_string(req.offset + req.length - 1);

            httplib::Headers httpHeaders = {{"Host", headers.host},
                {"x-amz-content-sha256", headers.payloadHash}, {"x-amz-date", headers.date},
                {"Authorization", headers.authorization}, {"Range", rangeStr}};

            PooledClient client(*pool_);
            auto res = client->Get(objectPath, httpHeaders);
            if (res && (res->status == 200 || res->status == 206)) {
                fetchCount.fetch_add(1, std::memory_order_relaxed);
                fetchBytes.fetch_add(res->body.size(), std::memory_order_relaxed);
                std::lock_guard lock(resultsMu);
                results.push_back(
                    {req.tag, std::vector<uint8_t>(res->body.begin(), res->body.end())});
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }
    return results;
}

// --- Immutable page group keys ---

std::string S3Client::pageGroupKey(uint64_t groupId, uint64_t manifestVersion) const {
    return config_.prefix + "/pg/" + std::to_string(groupId) +
           "_v" + std::to_string(manifestVersion);
}

std::string S3Client::overrideFrameKey(uint64_t groupId, size_t frameIdx,
    uint64_t manifestVersion) const {
    return config_.prefix + "/pg/" + std::to_string(groupId) +
           "_f" + std::to_string(frameIdx) +
           "_v" + std::to_string(manifestVersion);
}

// --- List Objects (S3 ListObjectsV2) ---

std::vector<std::string> S3Client::listObjects(const std::string& prefix) {
    std::vector<std::string> keys;
    std::string continuationToken;

    while (true) {
        auto queryPath = "/" + config_.bucket +
            "?list-type=2&prefix=" + prefix;
        if (!continuationToken.empty()) {
            queryPath += "&continuation-token=" + continuationToken;
        }

        auto emptyHash = sha256Hex(reinterpret_cast<const uint8_t*>(""), 0);
        auto headers = signRequest("GET", queryPath, emptyHash);

        httplib::Headers httpHeaders = {{"Host", headers.host},
            {"x-amz-content-sha256", headers.payloadHash},
            {"x-amz-date", headers.date},
            {"Authorization", headers.authorization}};

        PooledClient client(*pool_);
        auto res = client->Get(queryPath, httpHeaders);
        if (!res) {
            std::fprintf(stderr, "[S3::listObjects] no response for prefix=%s\n", prefix.c_str());
            break;
        }
        if (res->status != 200) {
            std::fprintf(stderr, "[S3::listObjects] status=%d prefix=%s body=%.300s\n",
                res->status, prefix.c_str(), res->body.c_str());
            break;
        }

        // Minimal XML: extract <Key>...</Key> values.
        auto& body = res->body;
        size_t pos = 0;
        while ((pos = body.find("<Key>", pos)) != std::string::npos) {
            pos += 5;
            auto end = body.find("</Key>", pos);
            if (end == std::string::npos) break;
            keys.push_back(body.substr(pos, end - pos));
            pos = end + 6;
        }

        // Handle pagination.
        if (body.find("<IsTruncated>true</IsTruncated>") != std::string::npos) {
            auto ntPos = body.find("<NextContinuationToken>");
            if (ntPos != std::string::npos) {
                ntPos += 23;
                auto ntEnd = body.find("</NextContinuationToken>", ntPos);
                if (ntEnd != std::string::npos) {
                    continuationToken = body.substr(ntPos, ntEnd - ntPos);
                    continue;
                }
            }
        }
        break;
    }
    return keys;
}

// --- Eviction ---

uint64_t S3Client::evictStalePageGroups(const Manifest& manifest) {
    auto pgPrefix = config_.prefix + "/pg/";
    auto allKeys = listObjects(pgPrefix);

    std::unordered_set<std::string> validKeys(
        manifest.pageGroupKeys.begin(), manifest.pageGroupKeys.end());

    // Phase GraphDrift: override keys are also valid.
    for (auto& ovMap : manifest.subframeOverrides) {
        for (auto& [_, ov] : ovMap) {
            validKeys.insert(ov.key);
        }
    }

    uint64_t deleted = 0;
    for (auto& key : allKeys) {
        if (validKeys.count(key)) continue;

        // Race protection: don't delete keys with version >= manifest version.
        // They may be in-flight from a concurrent sync.
        auto vPos = key.rfind("_v");
        if (vPos != std::string::npos) {
            auto keyVersion = std::strtoull(key.c_str() + vPos + 2, nullptr, 10);
            if (keyVersion >= manifest.version) continue;
        }

        if (deleteObject(key)) {
            deleted++;
        }
    }
    return deleted;
}

// --- Manifest Operations ---

std::optional<Manifest> S3Client::getManifest() {
    auto key = config_.prefix + "/manifest.json";
    auto data = getObject(key);
    if (!data.has_value()) {
        return std::nullopt;
    }
    auto json = std::string(data->begin(), data->end());
    return Manifest::fromJSON(json);
}

bool S3Client::putManifest(const Manifest& manifest) {
    auto key = config_.prefix + "/manifest.json";
    auto json = manifest.toJSON();
    // Retry up to 3 times.
    for (int attempt = 0; attempt < 3; attempt++) {
        if (putObject(key, reinterpret_cast<const uint8_t*>(json.c_str()), json.size())) {
            return true;
        }
        if (attempt < 2) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100 * (1 << attempt)));
        }
    }
    return false;
}

} // namespace tiered
} // namespace lbug
