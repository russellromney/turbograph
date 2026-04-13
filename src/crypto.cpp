#include "crypto.h"

#include <openssl/evp.h>
#include <openssl/rand.h>
#include <cstring>
#include <stdexcept>

namespace lbug {
namespace tiered {

// --- AES-256-CTR (local cache) ---

void aes256_ctr(const uint8_t* input, uint8_t* output, size_t len,
    uint64_t page_num, const Key256& key) {
    // IV = page_num as 8-byte LE, zero-padded to 16 bytes.
    uint8_t iv[16] = {};
    std::memcpy(iv, &page_num, sizeof(page_num));

    auto* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        throw std::runtime_error("EVP_CIPHER_CTX_new failed");
    }

    if (EVP_EncryptInit_ex(ctx, EVP_aes_256_ctr(), nullptr,
            key.data(), iv) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("EVP_EncryptInit_ex CTR failed");
    }

    int outLen = 0;
    if (EVP_EncryptUpdate(ctx, output, &outLen, input,
            static_cast<int>(len)) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("EVP_EncryptUpdate CTR failed");
    }

    int finalLen = 0;
    EVP_EncryptFinal_ex(ctx, output + outLen, &finalLen);
    EVP_CIPHER_CTX_free(ctx);
}

// --- AES-256-GCM (S3 frames) ---

std::vector<uint8_t> aes256_gcm_encrypt(
    const uint8_t* plaintext, size_t len, const Key256& key) {
    // Output: [12-byte nonce][ciphertext][16-byte tag]
    std::vector<uint8_t> output(GCM_NONCE_LEN + len + GCM_TAG_LEN);

    // Random nonce.
    if (RAND_bytes(output.data(), GCM_NONCE_LEN) != 1) {
        throw std::runtime_error("RAND_bytes failed");
    }

    auto* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        throw std::runtime_error("EVP_CIPHER_CTX_new failed");
    }

    if (EVP_EncryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr,
            key.data(), output.data()) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("EVP_EncryptInit_ex GCM failed");
    }

    int outLen = 0;
    if (EVP_EncryptUpdate(ctx, output.data() + GCM_NONCE_LEN, &outLen,
            plaintext, static_cast<int>(len)) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("EVP_EncryptUpdate GCM failed");
    }

    int finalLen = 0;
    if (EVP_EncryptFinal_ex(ctx, output.data() + GCM_NONCE_LEN + outLen,
            &finalLen) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("EVP_EncryptFinal_ex GCM failed");
    }

    // Extract auth tag.
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_GET_TAG, GCM_TAG_LEN,
            output.data() + GCM_NONCE_LEN + len) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        throw std::runtime_error("EVP_CTRL_GCM_GET_TAG failed");
    }

    EVP_CIPHER_CTX_free(ctx);
    return output;
}

std::vector<uint8_t> aes256_gcm_decrypt(
    const uint8_t* data, size_t len, const Key256& key) {
    if (len < GCM_OVERHEAD) {
        return {}; // Too short.
    }

    const uint8_t* nonce = data;
    const uint8_t* ciphertext = data + GCM_NONCE_LEN;
    size_t ctLen = len - GCM_OVERHEAD;
    const uint8_t* tag = data + GCM_NONCE_LEN + ctLen;

    auto* ctx = EVP_CIPHER_CTX_new();
    if (!ctx) {
        return {};
    }

    if (EVP_DecryptInit_ex(ctx, EVP_aes_256_gcm(), nullptr,
            key.data(), nonce) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        return {};
    }

    std::vector<uint8_t> plaintext(ctLen);
    int outLen = 0;
    if (EVP_DecryptUpdate(ctx, plaintext.data(), &outLen,
            ciphertext, static_cast<int>(ctLen)) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        return {};
    }

    // Set expected tag.
    if (EVP_CIPHER_CTX_ctrl(ctx, EVP_CTRL_GCM_SET_TAG, GCM_TAG_LEN,
            const_cast<uint8_t*>(tag)) != 1) {
        EVP_CIPHER_CTX_free(ctx);
        return {};
    }

    int finalLen = 0;
    if (EVP_DecryptFinal_ex(ctx, plaintext.data() + outLen, &finalLen) != 1) {
        // Auth tag mismatch: tampered or wrong key.
        EVP_CIPHER_CTX_free(ctx);
        return {};
    }

    EVP_CIPHER_CTX_free(ctx);
    return plaintext;
}

// --- Hex key parsing ---

std::optional<Key256> parse_hex_key(const std::string& hex) {
    if (hex.size() != 64) return std::nullopt;

    Key256 key;
    for (size_t i = 0; i < 32; i++) {
        auto hi = hex[i * 2];
        auto lo = hex[i * 2 + 1];

        auto fromHex = [](char c) -> int {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return 10 + c - 'a';
            if (c >= 'A' && c <= 'F') return 10 + c - 'A';
            return -1;
        };

        int h = fromHex(hi), l = fromHex(lo);
        if (h < 0 || l < 0) return std::nullopt;
        key[i] = static_cast<uint8_t>((h << 4) | l);
    }
    return key;
}

} // namespace tiered
} // namespace lbug
