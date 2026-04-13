#include "crypto.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <vector>

using namespace lbug::tiered;

static Key256 testKey() {
    Key256 key;
    for (int i = 0; i < 32; i++) key[i] = static_cast<uint8_t>(i);
    return key;
}

static Key256 wrongKey() {
    Key256 key;
    for (int i = 0; i < 32; i++) key[i] = static_cast<uint8_t>(255 - i);
    return key;
}

// --- CTR tests ---

static void testCtrRoundTrip() {
    auto key = testKey();
    std::vector<uint8_t> plaintext(4096, 0xAB);
    std::vector<uint8_t> encrypted(4096);
    std::vector<uint8_t> decrypted(4096);

    aes256_ctr(plaintext.data(), encrypted.data(), 4096, 42, key);
    assert(encrypted != plaintext); // Should be different.

    aes256_ctr(encrypted.data(), decrypted.data(), 4096, 42, key);
    assert(decrypted == plaintext); // Round-trip.

    std::printf("  PASS: testCtrRoundTrip\n");
}

static void testCtrDeterministic() {
    auto key = testKey();
    std::vector<uint8_t> plaintext(4096, 0xCD);
    std::vector<uint8_t> enc1(4096), enc2(4096);

    aes256_ctr(plaintext.data(), enc1.data(), 4096, 100, key);
    aes256_ctr(plaintext.data(), enc2.data(), 4096, 100, key);
    assert(enc1 == enc2); // Same key + page_num = same ciphertext.

    std::printf("  PASS: testCtrDeterministic\n");
}

static void testCtrDifferentPages() {
    auto key = testKey();
    std::vector<uint8_t> plaintext(4096, 0xEF);
    std::vector<uint8_t> enc1(4096), enc2(4096);

    aes256_ctr(plaintext.data(), enc1.data(), 4096, 0, key);
    aes256_ctr(plaintext.data(), enc2.data(), 4096, 1, key);
    assert(enc1 != enc2); // Different page_num = different ciphertext.

    std::printf("  PASS: testCtrDifferentPages\n");
}

static void testCtrWrongKey() {
    auto key = testKey();
    auto bad = wrongKey();
    std::vector<uint8_t> plaintext(4096, 0x42);
    std::vector<uint8_t> encrypted(4096);
    std::vector<uint8_t> decrypted(4096);

    aes256_ctr(plaintext.data(), encrypted.data(), 4096, 7, key);
    aes256_ctr(encrypted.data(), decrypted.data(), 4096, 7, bad);
    assert(decrypted != plaintext); // Wrong key = garbage.

    std::printf("  PASS: testCtrWrongKey\n");
}

static void testCtrInPlace() {
    auto key = testKey();
    std::vector<uint8_t> data(4096, 0x55);
    auto original = data;

    // Encrypt in-place.
    aes256_ctr(data.data(), data.data(), 4096, 99, key);
    assert(data != original);

    // Decrypt in-place.
    aes256_ctr(data.data(), data.data(), 4096, 99, key);
    assert(data == original);

    std::printf("  PASS: testCtrInPlace\n");
}

static void testCtrSmallBuffer() {
    auto key = testKey();
    std::vector<uint8_t> plaintext(16, 0xAA); // Minimum AES block.
    std::vector<uint8_t> encrypted(16);
    std::vector<uint8_t> decrypted(16);

    aes256_ctr(plaintext.data(), encrypted.data(), 16, 0, key);
    aes256_ctr(encrypted.data(), decrypted.data(), 16, 0, key);
    assert(decrypted == plaintext);

    std::printf("  PASS: testCtrSmallBuffer\n");
}

// --- GCM tests ---

static void testGcmRoundTrip() {
    auto key = testKey();
    std::vector<uint8_t> plaintext(1024, 0xBB);

    auto encrypted = aes256_gcm_encrypt(plaintext.data(), plaintext.size(), key);
    assert(encrypted.size() == plaintext.size() + GCM_OVERHEAD);

    auto decrypted = aes256_gcm_decrypt(encrypted.data(), encrypted.size(), key);
    assert(decrypted == plaintext);

    std::printf("  PASS: testGcmRoundTrip\n");
}

static void testGcmRandomNonce() {
    auto key = testKey();
    std::vector<uint8_t> plaintext(512, 0xCC);

    auto enc1 = aes256_gcm_encrypt(plaintext.data(), plaintext.size(), key);
    auto enc2 = aes256_gcm_encrypt(plaintext.data(), plaintext.size(), key);

    // Random nonces mean different ciphertexts.
    assert(enc1 != enc2);

    // Both decrypt correctly.
    auto dec1 = aes256_gcm_decrypt(enc1.data(), enc1.size(), key);
    auto dec2 = aes256_gcm_decrypt(enc2.data(), enc2.size(), key);
    assert(dec1 == plaintext);
    assert(dec2 == plaintext);

    std::printf("  PASS: testGcmRandomNonce\n");
}

static void testGcmWrongKey() {
    auto key = testKey();
    auto bad = wrongKey();
    std::vector<uint8_t> plaintext(256, 0xDD);

    auto encrypted = aes256_gcm_encrypt(plaintext.data(), plaintext.size(), key);
    auto decrypted = aes256_gcm_decrypt(encrypted.data(), encrypted.size(), bad);
    assert(decrypted.empty()); // Auth failure = empty.

    std::printf("  PASS: testGcmWrongKey\n");
}

static void testGcmTampered() {
    auto key = testKey();
    std::vector<uint8_t> plaintext(128, 0xEE);

    auto encrypted = aes256_gcm_encrypt(plaintext.data(), plaintext.size(), key);

    // Flip a byte in the ciphertext.
    encrypted[GCM_NONCE_LEN + 10] ^= 0xFF;

    auto decrypted = aes256_gcm_decrypt(encrypted.data(), encrypted.size(), key);
    assert(decrypted.empty()); // Auth tag mismatch.

    std::printf("  PASS: testGcmTampered\n");
}

static void testGcmTooShort() {
    auto key = testKey();
    std::vector<uint8_t> short_data(10); // Less than GCM_OVERHEAD.
    auto decrypted = aes256_gcm_decrypt(short_data.data(), short_data.size(), key);
    assert(decrypted.empty());

    std::printf("  PASS: testGcmTooShort\n");
}

static void testGcmEmptyPlaintext() {
    auto key = testKey();
    auto encrypted = aes256_gcm_encrypt(nullptr, 0, key);
    assert(encrypted.size() == GCM_OVERHEAD); // Just nonce + tag.

    auto decrypted = aes256_gcm_decrypt(encrypted.data(), encrypted.size(), key);
    assert(decrypted.empty()); // Empty plaintext.

    std::printf("  PASS: testGcmEmptyPlaintext\n");
}

static void testGcmLargePayload() {
    auto key = testKey();
    std::vector<uint8_t> plaintext(1024 * 1024, 0x77); // 1MB.

    auto encrypted = aes256_gcm_encrypt(plaintext.data(), plaintext.size(), key);
    auto decrypted = aes256_gcm_decrypt(encrypted.data(), encrypted.size(), key);
    assert(decrypted == plaintext);

    std::printf("  PASS: testGcmLargePayload\n");
}

// --- Hex key parsing ---

static void testParseHexKey() {
    // Valid 64-char hex string.
    auto key = parse_hex_key(
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f");
    assert(key.has_value());
    for (int i = 0; i < 32; i++) {
        assert((*key)[i] == static_cast<uint8_t>(i));
    }

    // Invalid: too short.
    assert(!parse_hex_key("0011223344").has_value());

    // Invalid: non-hex char.
    assert(!parse_hex_key(
        "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1eXY").has_value());

    // Valid: uppercase hex.
    auto upper = parse_hex_key(
        "FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00FF00");
    assert(upper.has_value());
    assert((*upper)[0] == 0xFF);
    assert((*upper)[1] == 0x00);

    std::printf("  PASS: testParseHexKey\n");
}

int main() {
    std::printf("=== Crypto Tests ===\n");

    testCtrRoundTrip();
    testCtrDeterministic();
    testCtrDifferentPages();
    testCtrWrongKey();
    testCtrInPlace();
    testCtrSmallBuffer();
    testGcmRoundTrip();
    testGcmRandomNonce();
    testGcmWrongKey();
    testGcmTampered();
    testGcmTooShort();
    testGcmEmptyPlaintext();
    testGcmLargePayload();
    testParseHexKey();

    std::printf("  All 14 crypto tests passed.\n");
    return 0;
}
