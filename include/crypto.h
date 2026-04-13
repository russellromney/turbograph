#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>
#include <vector>

namespace lbug {
namespace tiered {

using Key256 = std::array<uint8_t, 32>;

// AES-256-CTR encryption/decryption for local cache pages.
// IV = page_num as 8-byte LE, zero-padded to 16 bytes.
// No size overhead: output length == input length.
// Deterministic: same key + page_num always produces same ciphertext.
void aes256_ctr(const uint8_t* input, uint8_t* output, size_t len,
    uint64_t page_num, const Key256& key);

// AES-256-GCM encryption for S3 frames.
// Prepends random 12-byte nonce, appends 16-byte auth tag.
// Output is 28 bytes larger than input.
std::vector<uint8_t> aes256_gcm_encrypt(
    const uint8_t* plaintext, size_t len, const Key256& key);

// AES-256-GCM decryption for S3 frames.
// Expects: [12-byte nonce][ciphertext][16-byte tag].
// Returns plaintext, or empty vector on auth failure.
std::vector<uint8_t> aes256_gcm_decrypt(
    const uint8_t* data, size_t len, const Key256& key);

// GCM overhead: 12-byte nonce + 16-byte auth tag.
constexpr size_t GCM_OVERHEAD = 28;
constexpr size_t GCM_NONCE_LEN = 12;
constexpr size_t GCM_TAG_LEN = 16;

// Parse a hex-encoded 64-char string into a 32-byte key.
// Returns nullopt on invalid input.
std::optional<Key256> parse_hex_key(const std::string& hex);

} // namespace tiered
} // namespace lbug
