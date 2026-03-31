// Regression tests for page I/O decomposition logic.
// Tests that arbitrary (position, numBytes) reads/writes are correctly
// decomposed into page-aligned operations.
//
// These tests exercise the core algorithm without depending on Kuzu headers
// by reimplementing the decomposition logic in a test harness.

#include <cassert>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <optional>
#include <unordered_map>
#include <vector>

static constexpr uint32_t PAGE_SIZE = 4096;

// --- Simulated page store (mirrors TieredFileInfo state) ---

struct PageStore {
    uint32_t pageSize = PAGE_SIZE;
    std::unordered_map<uint64_t, std::vector<uint8_t>> pages; // pageNum -> raw page data.
    uint64_t pageCount = 0;

    std::vector<uint8_t> readOnePage(uint64_t pageNum) {
        auto it = pages.find(pageNum);
        if (it != pages.end()) {
            return it->second;
        }
        if (pageNum >= pageCount) {
            return std::vector<uint8_t>(pageSize, 0);
        }
        return std::vector<uint8_t>(pageSize, 0);
    }

    void writeOnePage(uint64_t pageNum, const uint8_t* data) {
        pages[pageNum] = std::vector<uint8_t>(data, data + pageSize);
        if (pageNum + 1 > pageCount) {
            pageCount = pageNum + 1;
        }
    }

    // Mirrors TieredFileSystem::readFromFile decomposition.
    void readFromFile(void* buffer, uint64_t numBytes, uint64_t position) {
        auto ps = static_cast<uint64_t>(pageSize);
        auto* dst = static_cast<uint8_t*>(buffer);
        uint64_t bytesRead = 0;

        while (bytesRead < numBytes) {
            auto fileOffset = position + bytesRead;
            auto pageNum = fileOffset / ps;
            auto offsetInPage = fileOffset % ps;
            auto bytesFromPage = std::min(ps - offsetInPage, numBytes - bytesRead);

            auto pageData = readOnePage(pageNum);
            std::memcpy(dst + bytesRead, pageData.data() + offsetInPage, bytesFromPage);
            bytesRead += bytesFromPage;
        }
    }

    // Mirrors TieredFileSystem::writeFile decomposition.
    void writeFile(const uint8_t* buffer, uint64_t numBytes, uint64_t offset) {
        auto ps = static_cast<uint64_t>(pageSize);
        uint64_t bytesWritten = 0;

        while (bytesWritten < numBytes) {
            auto fileOffset = offset + bytesWritten;
            auto pageNum = fileOffset / ps;
            auto offsetInPage = fileOffset % ps;
            auto bytesToPage = std::min(ps - offsetInPage, numBytes - bytesWritten);

            if (offsetInPage == 0 && bytesToPage == ps) {
                // Full page write.
                writeOnePage(pageNum, buffer + bytesWritten);
            } else {
                // Partial page write — read-modify-write.
                auto pageData = readOnePage(pageNum);
                std::memcpy(pageData.data() + offsetInPage, buffer + bytesWritten, bytesToPage);
                writeOnePage(pageNum, pageData.data());
            }
            bytesWritten += bytesToPage;
        }
    }

    void truncate(uint64_t size) {
        auto newPageCount = (size + pageSize - 1) / pageSize;
        for (auto it = pages.begin(); it != pages.end();) {
            if (it->first >= newPageCount) {
                it = pages.erase(it);
            } else {
                ++it;
            }
        }
        pageCount = newPageCount;
    }
};

// --- Test: Full page read/write (the normal BufferManager path) ---

static void testFullPageReadWrite() {
    PageStore store;

    // Write a full page at page 0.
    std::vector<uint8_t> page0(PAGE_SIZE, 0xAA);
    store.writeFile(page0.data(), PAGE_SIZE, 0);

    // Read it back.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    store.readFromFile(buf.data(), PAGE_SIZE, 0);
    assert(buf == page0);

    // Write page 1.
    std::vector<uint8_t> page1(PAGE_SIZE, 0xBB);
    store.writeFile(page1.data(), PAGE_SIZE, PAGE_SIZE);

    store.readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    assert(buf == page1);

    // Page 0 is still intact.
    store.readFromFile(buf.data(), PAGE_SIZE, 0);
    assert(buf == page0);

    assert(store.pageCount == 2);
    std::printf("  PASS: testFullPageReadWrite\n");
}

// --- Test: Sub-page read (regression for bug #1 — pageSize = numBytes) ---

static void testSubPageRead() {
    PageStore store;

    // Write page 0 with known pattern.
    std::vector<uint8_t> page0(PAGE_SIZE);
    for (uint32_t i = 0; i < PAGE_SIZE; i++) {
        page0[i] = static_cast<uint8_t>(i & 0xFF);
    }
    store.writeFile(page0.data(), PAGE_SIZE, 0);

    // Read 100 bytes from offset 50 (sub-page read).
    std::vector<uint8_t> buf(100, 0);
    store.readFromFile(buf.data(), 100, 50);

    // Verify the bytes match page0[50..150).
    for (int i = 0; i < 100; i++) {
        assert(buf[i] == page0[50 + i]);
    }

    // Read 100 bytes from offset 0.
    store.readFromFile(buf.data(), 100, 0);
    for (int i = 0; i < 100; i++) {
        assert(buf[i] == page0[i]);
    }

    std::printf("  PASS: testSubPageRead\n");
}

// --- Test: Sub-page write (partial page write with read-modify-write) ---

static void testSubPageWrite() {
    PageStore store;

    // Write a full page with 0xAA.
    std::vector<uint8_t> page0(PAGE_SIZE, 0xAA);
    store.writeFile(page0.data(), PAGE_SIZE, 0);

    // Overwrite bytes 100-199 with 0xBB.
    std::vector<uint8_t> patch(100, 0xBB);
    store.writeFile(patch.data(), 100, 100);

    // Read back the full page.
    std::vector<uint8_t> buf(PAGE_SIZE, 0);
    store.readFromFile(buf.data(), PAGE_SIZE, 0);

    // First 100 bytes should be 0xAA.
    for (int i = 0; i < 100; i++) {
        assert(buf[i] == 0xAA);
    }
    // Next 100 bytes should be 0xBB.
    for (int i = 100; i < 200; i++) {
        assert(buf[i] == 0xBB);
    }
    // Rest should be 0xAA.
    for (int i = 200; i < PAGE_SIZE; i++) {
        assert(buf[i] == 0xAA);
    }

    std::printf("  PASS: testSubPageWrite\n");
}

// --- Test: Multi-page write (regression for bug #2) ---

static void testMultiPageWrite() {
    PageStore store;

    // Write 3 pages at once (12288 bytes at offset 0).
    uint64_t totalBytes = 3 * PAGE_SIZE;
    std::vector<uint8_t> data(totalBytes);
    for (uint64_t i = 0; i < totalBytes; i++) {
        data[i] = static_cast<uint8_t>((i / PAGE_SIZE) + 1); // Page 0: 0x01, Page 1: 0x02, Page 2: 0x03.
    }
    store.writeFile(data.data(), totalBytes, 0);

    assert(store.pageCount == 3);

    // Read each page individually and verify.
    std::vector<uint8_t> buf(PAGE_SIZE);

    store.readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0x01);

    store.readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    for (auto b : buf) assert(b == 0x02);

    store.readFromFile(buf.data(), PAGE_SIZE, 2 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x03);

    std::printf("  PASS: testMultiPageWrite\n");
}

// --- Test: Multi-page write at non-zero offset ---

static void testMultiPageWriteOffset() {
    PageStore store;

    // Write page 0 first.
    std::vector<uint8_t> page0(PAGE_SIZE, 0xFF);
    store.writeFile(page0.data(), PAGE_SIZE, 0);

    // Write 2 pages starting at page 1.
    uint64_t totalBytes = 2 * PAGE_SIZE;
    std::vector<uint8_t> data(totalBytes, 0xCC);
    store.writeFile(data.data(), totalBytes, PAGE_SIZE);

    assert(store.pageCount == 3);

    // Page 0 should still be 0xFF.
    std::vector<uint8_t> buf(PAGE_SIZE);
    store.readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0xFF);

    // Pages 1 and 2 should be 0xCC.
    store.readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    for (auto b : buf) assert(b == 0xCC);
    store.readFromFile(buf.data(), PAGE_SIZE, 2 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0xCC);

    std::printf("  PASS: testMultiPageWriteOffset\n");
}

// --- Test: Cross-page read (read spanning two pages) ---

static void testCrossPageRead() {
    PageStore store;

    // Write 2 pages with distinct patterns.
    std::vector<uint8_t> page0(PAGE_SIZE, 0xAA);
    std::vector<uint8_t> page1(PAGE_SIZE, 0xBB);
    store.writeFile(page0.data(), PAGE_SIZE, 0);
    store.writeFile(page1.data(), PAGE_SIZE, PAGE_SIZE);

    // Read 200 bytes spanning the page boundary.
    uint64_t readStart = PAGE_SIZE - 100; // Last 100 bytes of page 0 + first 100 of page 1.
    std::vector<uint8_t> buf(200, 0);
    store.readFromFile(buf.data(), 200, readStart);

    for (int i = 0; i < 100; i++) {
        assert(buf[i] == 0xAA);
    }
    for (int i = 100; i < 200; i++) {
        assert(buf[i] == 0xBB);
    }

    std::printf("  PASS: testCrossPageRead\n");
}

// --- Test: Cross-page write (write spanning two pages) ---

static void testCrossPageWrite() {
    PageStore store;

    // Write 2 pages with 0x00.
    std::vector<uint8_t> zeros(2 * PAGE_SIZE, 0x00);
    store.writeFile(zeros.data(), 2 * PAGE_SIZE, 0);

    // Write 200 bytes of 0xDD spanning the page boundary.
    uint64_t writeStart = PAGE_SIZE - 100;
    std::vector<uint8_t> patch(200, 0xDD);
    store.writeFile(patch.data(), 200, writeStart);

    // Read each page and verify.
    std::vector<uint8_t> buf(PAGE_SIZE);

    store.readFromFile(buf.data(), PAGE_SIZE, 0);
    // First PAGE_SIZE - 100 bytes should be 0x00, last 100 should be 0xDD.
    for (uint32_t i = 0; i < PAGE_SIZE - 100; i++) {
        assert(buf[i] == 0x00);
    }
    for (uint32_t i = PAGE_SIZE - 100; i < PAGE_SIZE; i++) {
        assert(buf[i] == 0xDD);
    }

    store.readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    // First 100 bytes should be 0xDD, rest should be 0x00.
    for (int i = 0; i < 100; i++) {
        assert(buf[i] == 0xDD);
    }
    for (uint32_t i = 100; i < PAGE_SIZE; i++) {
        assert(buf[i] == 0x00);
    }

    std::printf("  PASS: testCrossPageWrite\n");
}

// --- Test: Truncate (regression for bug #4) ---

static void testTruncate() {
    PageStore store;

    // Write 5 pages.
    for (int i = 0; i < 5; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 1));
        store.writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }
    assert(store.pageCount == 5);

    // Truncate to 2 pages.
    store.truncate(2 * PAGE_SIZE);
    assert(store.pageCount == 2);

    // Pages 0 and 1 should still be readable.
    std::vector<uint8_t> buf(PAGE_SIZE);
    store.readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0x01);

    store.readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    for (auto b : buf) assert(b == 0x02);

    // Pages 2+ should be zeros (beyond pageCount).
    store.readFromFile(buf.data(), PAGE_SIZE, 2 * PAGE_SIZE);
    for (auto b : buf) assert(b == 0x00);

    std::printf("  PASS: testTruncate\n");
}

// --- Test: Truncate to zero ---

static void testTruncateToZero() {
    PageStore store;

    std::vector<uint8_t> page(PAGE_SIZE, 0xEE);
    store.writeFile(page.data(), PAGE_SIZE, 0);
    assert(store.pageCount == 1);

    store.truncate(0);
    assert(store.pageCount == 0);
    assert(store.pages.empty());

    // Reading page 0 should return zeros.
    std::vector<uint8_t> buf(PAGE_SIZE, 0xFF);
    store.readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0x00);

    std::printf("  PASS: testTruncateToZero\n");
}

// --- Test: Old bug — numBytes used as pageSize would give wrong page numbers ---

static void testPageNumCalculation() {
    // The old code did: pageNum = position / numBytes
    // With position=4096, numBytes=100: old pageNum = 40 (WRONG)
    // Correct: pageNum = position / PAGE_SIZE = 4096 / 4096 = 1
    PageStore store;

    // Write page 1 with known pattern.
    std::vector<uint8_t> page1(PAGE_SIZE, 0x42);
    store.writeFile(page1.data(), PAGE_SIZE, PAGE_SIZE);

    // Read 100 bytes from position 4096 (start of page 1).
    // Old code would compute pageNum = 4096 / 100 = 40 (wrong!)
    // New code computes pageNum = 4096 / 4096 = 1 (correct).
    std::vector<uint8_t> buf(100, 0);
    store.readFromFile(buf.data(), 100, PAGE_SIZE);
    for (auto b : buf) assert(b == 0x42);

    // Also test: read 4096 bytes from position 100 — crosses pages.
    // Old code: pageNum = 100 / 4096 = 0, only reads page 0.
    // New code: reads bytes 100..4195, spanning pages 0 and 1.
    std::vector<uint8_t> page0(PAGE_SIZE, 0xAA);
    store.writeFile(page0.data(), PAGE_SIZE, 0);

    std::vector<uint8_t> bigBuf(PAGE_SIZE, 0);
    store.readFromFile(bigBuf.data(), PAGE_SIZE, 100);

    // First (4096-100) = 3996 bytes from page 0 (starting at offset 100).
    for (int i = 0; i < 3996; i++) {
        assert(bigBuf[i] == 0xAA);
    }
    // Last 100 bytes from page 1 (starting at offset 0).
    for (int i = 3996; i < 4096; i++) {
        assert(bigBuf[i] == 0x42);
    }

    std::printf("  PASS: testPageNumCalculation\n");
}

// --- Test: Write then read with exact BufferedFileReader pattern ---
// BufferedFileReader reads in chunks of up to PAGE_SIZE at sequential positions.

static void testBufferedFileReaderPattern() {
    PageStore store;

    // Write 3 pages of data.
    for (int i = 0; i < 3; i++) {
        std::vector<uint8_t> page(PAGE_SIZE, static_cast<uint8_t>(i + 0x10));
        store.writeFile(page.data(), PAGE_SIZE, i * PAGE_SIZE);
    }

    // Simulate BufferedFileReader: read full PAGE_SIZE chunks.
    uint64_t fileSize = 3 * PAGE_SIZE;
    uint64_t offset = 0;
    int pageIdx = 0;
    while (offset < fileSize) {
        auto readSize = std::min(static_cast<uint64_t>(PAGE_SIZE), fileSize - offset);
        std::vector<uint8_t> buf(readSize, 0);
        store.readFromFile(buf.data(), readSize, offset);

        uint8_t expected = static_cast<uint8_t>(pageIdx + 0x10);
        for (auto b : buf) assert(b == expected);

        offset += readSize;
        pageIdx++;
    }

    std::printf("  PASS: testBufferedFileReaderPattern\n");
}

// --- Test: writePagesToFile pattern (multi-page write at page-aligned offset) ---

static void testWritePagesToFilePattern() {
    PageStore store;

    // Simulate FileHandle::writePagesToFile(buffer, size, startPageIdx)
    // where size = 5 * PAGE_SIZE, startPageIdx = 2.
    uint64_t numPages = 5;
    uint64_t totalBytes = numPages * PAGE_SIZE;
    uint64_t startPageIdx = 2;
    std::vector<uint8_t> buffer(totalBytes);
    for (uint64_t i = 0; i < totalBytes; i++) {
        buffer[i] = static_cast<uint8_t>((i / PAGE_SIZE) + 1);
    }

    store.writeFile(buffer.data(), totalBytes, startPageIdx * PAGE_SIZE);

    assert(store.pageCount == startPageIdx + numPages); // 7 pages.

    // Verify each page.
    std::vector<uint8_t> buf(PAGE_SIZE);
    for (uint64_t p = 0; p < numPages; p++) {
        store.readFromFile(buf.data(), PAGE_SIZE, (startPageIdx + p) * PAGE_SIZE);
        uint8_t expected = static_cast<uint8_t>(p + 1);
        for (auto b : buf) assert(b == expected);
    }

    // Pages 0 and 1 should be zeros (never written).
    store.readFromFile(buf.data(), PAGE_SIZE, 0);
    for (auto b : buf) assert(b == 0x00);
    store.readFromFile(buf.data(), PAGE_SIZE, PAGE_SIZE);
    for (auto b : buf) assert(b == 0x00);

    std::printf("  PASS: testWritePagesToFilePattern\n");
}

int main() {
    std::printf("=== Page I/O Decomposition Tests ===\n");
    testFullPageReadWrite();
    testSubPageRead();
    testSubPageWrite();
    testMultiPageWrite();
    testMultiPageWriteOffset();
    testCrossPageRead();
    testCrossPageWrite();
    testTruncate();
    testTruncateToZero();
    testPageNumCalculation();
    testBufferedFileReaderPattern();
    testWritePagesToFilePattern();
    std::printf("All page I/O tests passed.\n");
    return 0;
}
