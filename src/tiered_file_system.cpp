#include "tiered_file_system.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <sys/stat.h>
#include <fstream>
#include <stdexcept>
#include <unistd.h>

#ifdef __linux__
#include <linux/falloc.h>
#endif

#include <zstd.h>

namespace lbug {
namespace tiered {

// --- Compression helpers (used only for S3 encoding) ---

static std::vector<uint8_t> compressPage(const uint8_t* data, uint64_t size, int level) {
    auto maxSize = ZSTD_compressBound(size);
    std::vector<uint8_t> compressed(maxSize);
    auto actualSize = ZSTD_compress(compressed.data(), maxSize, data, size, level);
    if (ZSTD_isError(actualSize)) {
        throw std::runtime_error("zstd compress failed");
    }
    compressed.resize(actualSize);
    return compressed;
}

static std::vector<uint8_t> decompressPage(const uint8_t* data, uint64_t compressedSize,
    uint64_t pageSize) {
    std::vector<uint8_t> decompressed(pageSize);
    auto actualSize =
        ZSTD_decompress(decompressed.data(), pageSize, data, compressedSize);
    if (ZSTD_isError(actualSize)) {
        throw std::runtime_error("zstd decompress failed");
    }
    decompressed.resize(actualSize);
    return decompressed;
}

// --- TieredFileInfo ---

TieredFileInfo::TieredFileInfo(std::string path, common::FileSystem* fs,
    std::shared_ptr<S3Client> s3, std::unique_ptr<PageBitmap> bitmap, Manifest manifest,
    uint32_t pageSize, int compressionLevel,
    uint32_t pagesPerGroup, int localFd)
    : FileInfo(std::move(path), fs), s3(std::move(s3)), bitmap(std::move(bitmap)),
      localFd(localFd), manifest(std::move(manifest)), pageSize(pageSize),
      compressionLevel(compressionLevel), pagesPerGroup(pagesPerGroup) {}

TieredFileInfo::~TieredFileInfo() {
    if (fileSystem) {
        auto* vfs = static_cast<TieredFileSystem*>(fileSystem);
        // Drain prefetch queue and wait for in-flight workers to finish
        // before this TieredFileInfo is freed (workers hold raw pointers).
        vfs->drainPrefetchAndWait();
        vfs->clearActiveFileInfo(this);
    }
    if (localFd >= 0) {
        ::close(localFd);
    }
}

void TieredFileInfo::initGroupStates(uint64_t count) {
    totalGroups = count;
    if (count == 0) return;
    groupStates = std::make_unique<std::atomic<uint8_t>[]>(count);
    for (uint64_t i = 0; i < count; i++) {
        groupStates[i].store(static_cast<uint8_t>(GroupState::NONE),
            std::memory_order_relaxed);
    }
}

GroupState TieredFileInfo::getGroupState(uint64_t pgId) const {
    if (!groupStates || pgId >= totalGroups) return GroupState::NONE;
    return static_cast<GroupState>(
        groupStates[pgId].load(std::memory_order_acquire));
}

bool TieredFileInfo::tryClaimGroup(uint64_t pgId) {
    if (!groupStates || pgId >= totalGroups) return false;
    uint8_t expected = static_cast<uint8_t>(GroupState::NONE);
    return groupStates[pgId].compare_exchange_strong(expected,
        static_cast<uint8_t>(GroupState::FETCHING),
        std::memory_order_acq_rel);
}

void TieredFileInfo::markGroupPresent(uint64_t pgId) {
    if (!groupStates || pgId >= totalGroups) return;
    groupStates[pgId].store(static_cast<uint8_t>(GroupState::PRESENT),
        std::memory_order_release);
    // Lock + unlock to synchronize with waiters, then notify.
    { std::lock_guard<std::mutex> lock(groupCvMu); }
    groupCv.notify_all();
}

void TieredFileInfo::markGroupNone(uint64_t pgId) {
    if (!groupStates || pgId >= totalGroups) return;
    groupStates[pgId].store(static_cast<uint8_t>(GroupState::NONE),
        std::memory_order_release);
    { std::lock_guard<std::mutex> lock(groupCvMu); }
    groupCv.notify_all();
}

void TieredFileInfo::waitForGroup(uint64_t pgId) const {
    if (!groupStates || pgId >= totalGroups) return;
    std::unique_lock<std::mutex> lock(groupCvMu);
    groupCv.wait(lock, [&] {
        return getGroupState(pgId) != GroupState::FETCHING;
    });
}

void TieredFileInfo::resetGroupStates() {
    if (!groupStates) return;
    for (uint64_t i = 0; i < totalGroups; i++) {
        groupStates[i].store(static_cast<uint8_t>(GroupState::NONE),
            std::memory_order_relaxed);
    }
}

// --- TieredFileSystem ---

TieredFileSystem::TieredFileSystem(TieredConfig config) : config_(std::move(config)) {
    s3_ = std::make_shared<S3Client>(config_.s3);

    // Start prefetch worker threads.
    prefetchStop_.store(false);
    for (uint32_t i = 0; i < config_.prefetchThreads; i++) {
        prefetchWorkers_.emplace_back([this] { prefetchWorkerLoop(); });
    }
}

TieredFileSystem::~TieredFileSystem() {
    // Stop prefetch pool before flushing.
    drainPrefetchAndWait();
    {
        std::lock_guard<std::mutex> lock(prefetchMu_);
        prefetchStop_.store(true);
    }
    prefetchCv_.notify_all();
    for (auto& t : prefetchWorkers_) {
        if (t.joinable()) t.join();
    }

    // Final flush of any pending page groups.
    flushPendingPageGroups();
}

void TieredFileSystem::prefetchWorkerLoop() const {
    while (true) {
        PrefetchJob job;
        {
            std::unique_lock<std::mutex> lock(prefetchMu_);
            prefetchCv_.wait(lock, [&] {
                return prefetchStop_.load() || !prefetchQueue_.empty();
            });
            if (prefetchStop_.load() && prefetchQueue_.empty()) return;
            if (prefetchQueue_.empty()) continue;
            job = std::move(prefetchQueue_.front());
            prefetchQueue_.pop_front();
        }

        prefetchInFlight_.fetch_add(1);

        auto& ti = *job.ti;
        if (ti.tryClaimGroup(job.groupId)) {
            bool ok = fetchAndStoreGroup(ti, job.groupId);
            if (ok) {
                ti.markGroupPresent(job.groupId);
            } else {
                ti.markGroupNone(job.groupId);
            }
        }

        prefetchInFlight_.fetch_sub(1);
    }
}

void TieredFileSystem::submitPrefetch(TieredFileInfo& ti,
    const std::vector<uint64_t>& groupIds) const {
    if (groupIds.empty()) return;

    // Pre-filter to NONE groups only.
    std::vector<uint64_t> filtered;
    filtered.reserve(groupIds.size());
    for (auto pgId : groupIds) {
        if (ti.getGroupState(pgId) == GroupState::NONE) {
            filtered.push_back(pgId);
        }
    }
    if (filtered.empty()) return;

    std::lock_guard<std::mutex> lock(prefetchMu_);
    if (prefetchStop_.load()) return;

    for (auto pgId : filtered) {
        prefetchQueue_.push_back({&ti, pgId});
    }
    prefetchCv_.notify_all();
}

void TieredFileSystem::drainPrefetch() const {
    std::lock_guard<std::mutex> lock(prefetchMu_);
    prefetchQueue_.clear();
}

void TieredFileSystem::drainPrefetchAndWait() const {
    // Clear the queue, then spin until all in-flight workers finish.
    drainPrefetch();
    while (prefetchInFlight_.load(std::memory_order_acquire) > 0) {
        std::this_thread::yield();
    }
}

bool TieredFileSystem::canHandleFile(const std::string_view path) const {
    return path == config_.dataFilePath;
}

std::unique_ptr<common::FileInfo> TieredFileSystem::openFile(const std::string& path,
    common::FileOpenFlags /*flags*/, main::ClientContext* /*context*/) {
    auto manifestPath = std::filesystem::path(config_.cacheDir) / "manifest.json";
    auto bitmapPath = std::filesystem::path(config_.cacheDir) / "page_bitmap";
    auto localFilePath = std::filesystem::path(config_.cacheDir) / "data.cache";

    std::filesystem::create_directories(config_.cacheDir);

    // 1. Load manifest: disk first, then S3 fallback.
    Manifest m;
    bool manifestFound = false;
    if (std::filesystem::exists(manifestPath)) {
        std::ifstream f(manifestPath, std::ios::binary);
        std::string json((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        auto parsed = Manifest::fromJSON(json);
        if (parsed.has_value()) {
            m = *parsed;
            manifestFound = true;
        }
    }
    if (!manifestFound) {
        auto s3Manifest = s3_->getManifest();
        if (s3Manifest.has_value()) {
            m = *s3Manifest;
            manifestFound = true;
            std::ofstream f(manifestPath, std::ios::binary | std::ios::trunc);
            auto json = m.toJSON();
            f.write(json.data(), json.size());
        } else {
            m.pageSize = config_.pageSize;
            m.pagesPerGroup = config_.pagesPerGroup;
        }
    }
    if (m.pageSize == 0) m.pageSize = config_.pageSize;
    if (m.pagesPerGroup == 0) m.pagesPerGroup = config_.pagesPerGroup;

    if (m.pageSize != config_.pageSize) {
        throw std::runtime_error(
            "Manifest pageSize " + std::to_string(m.pageSize) +
            " != config " + std::to_string(config_.pageSize));
    }
    if (m.pagesPerGroup != config_.pagesPerGroup) {
        throw std::runtime_error(
            "Manifest pagesPerGroup " + std::to_string(m.pagesPerGroup) +
            " != config " + std::to_string(config_.pagesPerGroup));
    }

    {
        std::lock_guard lock(existsMu_);
        existsCached_ = true;
        existsResult_ = manifestFound;
    }

    auto bitmap = std::make_unique<PageBitmap>(bitmapPath);
    if (m.pageCount > 0) {
        bitmap->resize(m.pageCount);
    }

    int localFd = ::open(localFilePath.c_str(), O_RDWR | O_CREAT, 0644);
    if (localFd < 0) {
        throw std::runtime_error("Failed to open local cache file: " + localFilePath.string());
    }

    if (m.pageCount > 0) {
        auto targetSize = static_cast<off_t>(m.pageCount * m.pageSize);
        if (::ftruncate(localFd, targetSize) != 0) {
            ::close(localFd);
            throw std::runtime_error("Failed to extend local cache file");
        }
    }

    auto info = std::make_unique<TieredFileInfo>(path, this, s3_, std::move(bitmap), m,
        m.pageSize, config_.compressionLevel,
        m.pagesPerGroup, localFd);

    // Initialize per-group fetch states.
    if (m.pageCount > 0) {
        auto totalGroups = (m.pageCount + m.pagesPerGroup - 1) / m.pagesPerGroup;
        info->initGroupStates(totalGroups);
    }

    activeFileInfo_.store(info.get());
    return info;
}

std::vector<std::string> TieredFileSystem::glob(main::ClientContext* /*context*/,
    const std::string& /*path*/) const {
    return {};
}

bool TieredFileSystem::fileOrPathExists(const std::string& path, main::ClientContext* /*context*/) {
    if (!canHandleFile(path)) {
        return false;
    }
    {
        std::lock_guard lock(existsMu_);
        if (existsCached_) {
            return existsResult_;
        }
    }
    auto manifest = s3_->getManifest();
    std::lock_guard lock(existsMu_);
    existsCached_ = true;
    existsResult_ = manifest.has_value();
    return existsResult_;
}

uint64_t TieredFileSystem::getFileSize(const common::FileInfo& fileInfo) const {
    auto& ti = fileInfo.constCast<TieredFileInfo>();
    std::lock_guard lock(ti.manifestMu);
    return ti.manifest.pageCount * ti.manifest.pageSize;
}

int64_t TieredFileSystem::seek(common::FileInfo& /*fileInfo*/, uint64_t /*offset*/,
    int /*whence*/) const {
    return 0;
}

int64_t TieredFileSystem::readFile(common::FileInfo& /*fileInfo*/, void* /*buf*/,
    size_t /*numBytes*/) const {
    return 0;
}

void TieredFileSystem::truncate(common::FileInfo& fileInfo, uint64_t size) const {
    auto& ti = fileInfo.cast<TieredFileInfo>();
    auto newPageCount = (size + ti.pageSize - 1) / ti.pageSize;

    {
        std::lock_guard lock(ti.dirtyMu);
        for (auto it = ti.dirtyPages.begin(); it != ti.dirtyPages.end();) {
            if (it->first >= newPageCount) {
                it = ti.dirtyPages.erase(it);
            } else {
                ++it;
            }
        }
    }

    {
        std::unique_lock lock(ti.manifestMu);
        ti.manifest.pageCount = newPageCount;
    }

    if (ti.localFd >= 0) {
        ::ftruncate(ti.localFd, static_cast<off_t>(newPageCount * ti.pageSize));
    }
}

// --- Page classification tracking ---

static void initTrackBitmap(std::unique_ptr<PageBitmap>& bm,
    const std::string& path, uint64_t pageCount) {
    bm = std::make_unique<PageBitmap>(std::filesystem::path(path));
    if (pageCount > 0) bm->resize(pageCount);
}

void TieredFileSystem::beginTrackStructural() {
    auto* afi = activeFileInfo_.load();
    if (!afi) return;
    std::lock_guard lock(afi->trackMu);
    initTrackBitmap(afi->structuralPages,
        config_.cacheDir + "/structural_bitmap", afi->manifest.pageCount);
    afi->trackMode = TieredFileInfo::TrackMode::STRUCTURAL;
}

void TieredFileSystem::beginTrackIndex() {
    auto* afi = activeFileInfo_.load();
    if (!afi) return;
    std::lock_guard lock(afi->trackMu);
    initTrackBitmap(afi->indexPages,
        config_.cacheDir + "/index_bitmap", afi->manifest.pageCount);
    afi->trackMode = TieredFileInfo::TrackMode::INDEX;
}

void TieredFileSystem::endTrack() {
    auto* afi = activeFileInfo_.load();
    if (!afi) return;
    std::lock_guard lock(afi->trackMu);
    afi->trackMode = TieredFileInfo::TrackMode::NONE;
}

// --- Selective cache eviction ---

// Helper: rebuild bitmap keeping only pages present in the given preserve bitmaps.
static void rebuildBitmap(PageBitmap& bitmap,
    PageBitmap* preserve1, PageBitmap* preserve2) {
    auto pageCount = bitmap.pageCount();
    bitmap.clear();
    for (uint64_t p = 0; p < pageCount; p++) {
        if ((preserve1 && preserve1->isPresent(p)) ||
            (preserve2 && preserve2->isPresent(p))) {
            bitmap.markPresent(p);
        }
    }
    bitmap.persist();
}

void TieredFileSystem::clearCacheAll() {
    drainPrefetchAndWait();
    auto* afi = activeFileInfo_.load();
    if (!afi) return;

    afi->resetGroupStates();
    afi->consecutiveMisses = 0;
    { std::lock_guard lock(afi->slingshotMu); afi->slingshotSubmitted.clear(); }
    if (afi->bitmap) {
        afi->bitmap->clear();
        afi->bitmap->persist();
    }

    // Reclaim all disk space: truncate to 0, then re-extend as sparse.
    if (afi->localFd >= 0) {
        struct stat st;
        if (::fstat(afi->localFd, &st) == 0 && st.st_size > 0) {
            ::ftruncate(afi->localFd, 0);
            ::ftruncate(afi->localFd, st.st_size);
        }
    }
}

void TieredFileSystem::clearCacheKeepStructural() {
    drainPrefetchAndWait();
    auto* afi = activeFileInfo_.load();
    if (!afi) return;

    afi->resetGroupStates();
    afi->consecutiveMisses = 0;
    { std::lock_guard lock(afi->slingshotMu); afi->slingshotSubmitted.clear(); }

    if (afi->bitmap && afi->structuralPages) {
        rebuildBitmap(*afi->bitmap, afi->structuralPages.get(), nullptr);
    } else if (afi->bitmap) {
        afi->bitmap->clear();
        afi->bitmap->persist();
    }
}

void TieredFileSystem::clearCacheKeepIndex() {
    drainPrefetchAndWait();
    auto* afi = activeFileInfo_.load();
    if (!afi) return;

    afi->resetGroupStates();
    afi->consecutiveMisses = 0;
    { std::lock_guard lock(afi->slingshotMu); afi->slingshotSubmitted.clear(); }

    if (afi->bitmap) {
        rebuildBitmap(*afi->bitmap,
            afi->structuralPages.get(), afi->indexPages.get());
    }
}

void TieredFileSystem::evictLocalGroup(uint64_t pageGroupId) {
    auto* afi = activeFileInfo_.load();
    if (!afi) return;
    auto& ti = *afi;

    // 1. Reset group state to NONE.
    ti.markGroupNone(pageGroupId);

    // 2. Clear bitmap for all pages in this group.
    auto startPage = pageGroupId * ti.pagesPerGroup;
    ti.bitmap->clearRange(startPage, ti.pagesPerGroup);

    // 3. Punch hole in local cache file to reclaim NVMe blocks.
    auto offset = static_cast<off_t>(startPage * ti.pageSize);
    auto len = static_cast<off_t>(ti.pagesPerGroup * ti.pageSize);
#ifdef __linux__
    ::fallocate(ti.localFd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, len);
#endif
}

// --- fetchAndStoreFrame: fetch a single seekable frame via range GET ---

bool TieredFileSystem::fetchAndStoreFrame(TieredFileInfo& ti, uint64_t pageGroupId,
    uint32_t frameIdx, std::vector<uint8_t>* requestedPageOut,
    uint32_t requestedLocalIdx) const {

    std::string pgKey;
    std::vector<FrameEntry> frameTable;
    uint32_t subPPF = 0;
    {
        std::lock_guard lock(ti.manifestMu);
        if (pageGroupId >= ti.manifest.pageGroupKeys.size()) return false;
        pgKey = ti.manifest.pageGroupKeys[pageGroupId];
        if (pageGroupId < ti.manifest.frameTables.size()) {
            frameTable = ti.manifest.frameTables[pageGroupId];
        }
        subPPF = ti.manifest.subPagesPerFrame;
    }
    if (pgKey.empty() || frameIdx >= frameTable.size() || subPPF == 0) return false;

    auto& entry = frameTable[frameIdx];
    if (entry.len == 0) return false;

    auto compressedFrame = ti.s3->getObjectRange(pgKey, entry.offset, entry.len);
    if (!compressedFrame.has_value()) return false;

    // Validate we got the expected number of bytes.
    if (compressedFrame->size() != entry.len) return false;

    // Use the page count stored at encode time (authoritative for partial frames).
    auto groupStartPage = pageGroupId * ti.pagesPerGroup;
    auto frameStartLocal = frameIdx * subPPF;
    uint32_t pagesInFrame = entry.pageCount > 0 ? entry.pageCount : subPPF;

    auto rawFrame = decodeFrame(*compressedFrame, pagesInFrame, ti.pageSize);

    // Write each page in the frame to local file.
    for (uint32_t i = 0; i < pagesInFrame; i++) {
        auto localIdx = frameStartLocal + i;
        auto absPageNum = groupStartPage + localIdx;
        auto pageOffset = static_cast<size_t>(i) * ti.pageSize;
        if (pageOffset + ti.pageSize > rawFrame.size()) break;

        auto fileOffset = static_cast<off_t>(absPageNum * ti.pageSize);
        ::pwrite(ti.localFd, rawFrame.data() + pageOffset, ti.pageSize, fileOffset);
        ti.bitmap->markPresent(absPageNum);

        if (requestedPageOut && localIdx == requestedLocalIdx) {
            *requestedPageOut = std::vector<uint8_t>(
                rawFrame.data() + pageOffset,
                rawFrame.data() + pageOffset + ti.pageSize);
        }
    }

    return true;
}

// --- fetchAndStoreGroup: fetch a page group from S3, store locally ---

bool TieredFileSystem::fetchAndStoreGroup(TieredFileInfo& ti, uint64_t pageGroupId,
    std::vector<uint8_t>* requestedPageOut, uint32_t requestedLocalIdx) const {

    // Check if this group has a seekable frame table.
    bool hasFrameTable = false;
    uint32_t subPPF = 0;
    {
        std::lock_guard lock(ti.manifestMu);
        subPPF = ti.manifest.subPagesPerFrame;
        hasFrameTable = subPPF > 0 &&
            pageGroupId < ti.manifest.frameTables.size() &&
            !ti.manifest.frameTables[pageGroupId].empty();
    }

    if (hasFrameTable) {
        // Seekable path: fetch only the frame containing the requested page.
        // Prefetch workers call this with requestedLocalIdx=0 and no output,
        // so they fetch all frames via the full-object fallback below.
        if (requestedPageOut != nullptr && requestedLocalIdx < ti.pagesPerGroup) {
            auto frameIdx = requestedLocalIdx / subPPF;
            return fetchAndStoreFrame(ti, pageGroupId, frameIdx,
                requestedPageOut, requestedLocalIdx);
        }
        // Prefetch path (or out-of-bounds): fetch full object and decode all frames.
    }

    // Legacy or prefetch path: full object GET.
    std::string pgKey;
    {
        std::lock_guard lock(ti.manifestMu);
        if (pageGroupId >= ti.manifest.pageGroupKeys.size()) return false;
        pgKey = ti.manifest.pageGroupKeys[pageGroupId];
    }
    if (pgKey.empty()) return false;

    auto blob = ti.s3->getObject(pgKey);
    if (!blob.has_value()) return false;

    if (hasFrameTable) {
        // Seekable blob: concatenated compressed frames. Decode all frames.
        std::vector<FrameEntry> frameTable;
        {
            std::lock_guard lock(ti.manifestMu);
            frameTable = ti.manifest.frameTables[pageGroupId];
        }

        for (uint32_t f = 0; f < frameTable.size(); f++) {
            auto& entry = frameTable[f];
            if (entry.len == 0) continue;
            if (entry.offset + entry.len > blob->size()) break;

            auto frameStartLocal = f * subPPF;
            auto groupStartPage = pageGroupId * ti.pagesPerGroup;

            // Use page count from frame entry (authoritative for partial frames).
            uint32_t pagesInFrame = entry.pageCount > 0 ? entry.pageCount : subPPF;

            std::vector<uint8_t> frameData(
                blob->begin() + entry.offset,
                blob->begin() + entry.offset + entry.len);
            auto rawFrame = decodeFrame(frameData, pagesInFrame, ti.pageSize);

            for (uint32_t i = 0; i < pagesInFrame; i++) {
                auto localIdx = frameStartLocal + i;
                auto absPageNum = groupStartPage + localIdx;
                auto pageOffset = static_cast<size_t>(i) * ti.pageSize;
                if (pageOffset + ti.pageSize > rawFrame.size()) break;

                auto fileOffset = static_cast<off_t>(absPageNum * ti.pageSize);
                ::pwrite(ti.localFd, rawFrame.data() + pageOffset, ti.pageSize, fileOffset);
                ti.bitmap->markPresent(absPageNum);

                if (requestedPageOut && localIdx == requestedLocalIdx) {
                    *requestedPageOut = std::vector<uint8_t>(
                        rawFrame.data() + pageOffset,
                        rawFrame.data() + pageOffset + ti.pageSize);
                }
            }
        }
        return true;
    }

    // Legacy path: per-page compressed with offset header.
    for (uint32_t i = 0; i < ti.pagesPerGroup; i++) {
        auto compressedPage = extractPage(*blob, i, ti.pagesPerGroup);
        if (!compressedPage.has_value() || compressedPage->empty()) continue;

        auto rawPage = decompressPage(compressedPage->data(),
            compressedPage->size(), ti.pageSize);
        if (rawPage.size() != ti.pageSize) continue;

        auto absPageNum = pageGroupId * ti.pagesPerGroup + i;
        auto fileOffset = static_cast<off_t>(absPageNum * ti.pageSize);
        ::pwrite(ti.localFd, rawPage.data(), ti.pageSize, fileOffset);
        ti.bitmap->markPresent(absPageNum);

        if (requestedPageOut && i == requestedLocalIdx) {
            *requestedPageOut = std::move(rawPage);
        }
    }

    return true;
}

// --- readOnePage: sync fetch + async prefetch ---

std::vector<uint8_t> TieredFileSystem::readOnePage(TieredFileInfo& ti, uint64_t pageNum) const {
    // Track page classification: mark page as structural or index depending on mode.
    {
        std::lock_guard lock(ti.trackMu);
        if (ti.trackMode == TieredFileInfo::TrackMode::STRUCTURAL && ti.structuralPages) {
            ti.structuralPages->markPresent(pageNum);
        } else if (ti.trackMode == TieredFileInfo::TrackMode::INDEX && ti.indexPages) {
            ti.indexPages->markPresent(pageNum);
        }
    }

    // 1. Check dirty pages (raw, uncompressed).
    {
        std::lock_guard lock(ti.dirtyMu);
        auto it = ti.dirtyPages.find(pageNum);
        if (it != ti.dirtyPages.end()) {
            return it->second;
        }
    }

    // 2. Bounds check — page beyond manifest means zeros.
    {
        std::lock_guard lock(ti.manifestMu);
        if (pageNum >= ti.manifest.pageCount) {
            return std::vector<uint8_t>(ti.pageSize, 0);
        }
    }

    // 3. Check bitmap — if page is in local file, pread it.
    if (ti.bitmap->isPresent(pageNum)) {
        ti.consecutiveMisses = 0;
        std::vector<uint8_t> page(ti.pageSize);
        auto offset = static_cast<off_t>(pageNum * ti.pageSize);
        auto bytesRead = ::pread(ti.localFd, page.data(), ti.pageSize, offset);
        if (bytesRead == static_cast<ssize_t>(ti.pageSize)) {
            return page;
        }
        // Fall through to S3 on read failure.
    }

    // 4. Cache miss — need to fetch this page group from S3.
    auto pageGroupId = pageNum / ti.pagesPerGroup;
    auto localIdx = static_cast<uint32_t>(pageNum % ti.pagesPerGroup);

    // 5. Check/claim group state.
    // If groupStates not initialized (DB started empty, grew via writes),
    // skip the coordination loop and fetch directly.
    bool skipGroupTracking = !ti.groupStates || pageGroupId >= ti.totalGroups;

    if (!skipGroupTracking) {
        while (true) {
            auto state = ti.getGroupState(pageGroupId);

            if (state == GroupState::PRESENT) {
                // Group was fetched but bitmap didn't have this specific page.
                // Page doesn't exist in S3 data (empty slot in group).
                return std::vector<uint8_t>(ti.pageSize, 0);
            }

            if (state == GroupState::FETCHING) {
                // Background thread is fetching this group. Wait for it.
                ti.waitForGroup(pageGroupId);
                // Group is now PRESENT or NONE (if fetch failed).
                if (ti.getGroupState(pageGroupId) == GroupState::PRESENT) {
                    if (ti.bitmap->isPresent(pageNum)) {
                        std::vector<uint8_t> page(ti.pageSize);
                        auto offset = static_cast<off_t>(pageNum * ti.pageSize);
                        ::pread(ti.localFd, page.data(), ti.pageSize, offset);
                        return page;
                    }
                    return std::vector<uint8_t>(ti.pageSize, 0);
                }
                // Fetch failed (NONE). Retry — we'll try to claim it ourselves.
                continue;
            }

            // state == NONE. Try to claim it.
            if (ti.tryClaimGroup(pageGroupId)) {
                break; // We own it — proceed to fetch.
            }
            // CAS failed — someone else claimed it. Loop back to check state.
        }
    }

    // 6. Fetch the group synchronously.
    std::vector<uint8_t> requestedPage;
    bool ok = fetchAndStoreGroup(ti, pageGroupId, &requestedPage, localIdx);

    if (ok) {
        // Only mark PRESENT if we fetched the FULL group (legacy or prefetch path).
        // Seekable single-frame fetches leave other frames unfetched, so the group
        // must stay NONE to allow subsequent frame fetches for other pages.
        bool fetchedFullGroup = true;
        {
            std::lock_guard lock(ti.manifestMu);
            auto subPPF = ti.manifest.subPagesPerFrame;
            fetchedFullGroup = !(subPPF > 0 &&
                pageGroupId < ti.manifest.frameTables.size() &&
                !ti.manifest.frameTables[pageGroupId].empty());
        }
        if (!skipGroupTracking && fetchedFullGroup) {
            ti.markGroupPresent(pageGroupId);
        } else if (!skipGroupTracking) {
            // Seekable frame fetch: we got one frame but the group has more.
            // Submit the full group to the prefetch pool as a background job.
            // The prefetch worker will fetch the entire S3 object and decode
            // all frames, populating the local cache for subsequent page reads.
            // Group stays NONE so the prefetch worker can claim it via tryClaimGroup.
            ti.markGroupNone(pageGroupId);
            {
                std::lock_guard lock(ti.slingshotMu);
                if (ti.slingshotSubmitted.insert(pageGroupId).second) {
                    submitPrefetch(ti, {pageGroupId});
                }
            }
        }
    } else {
        if (!skipGroupTracking) ti.markGroupNone(pageGroupId);
        return std::vector<uint8_t>(ti.pageSize, 0);
    }

    // 7. Compute prefetch groups and submit asynchronously (only with group tracking).
    if (!skipGroupTracking) {
        if (ti.consecutiveMisses < 255) {
            ti.consecutiveMisses++;
        }

        uint64_t totalPageGroups = 0;
        {
            std::lock_guard lock(ti.manifestMu);
            totalPageGroups =
                (ti.manifest.pageCount + ti.pagesPerGroup - 1) / ti.pagesPerGroup;
        }

        const auto& hops = config_.prefetchHops;
        float hopSum = 0;
        for (auto v : hops) hopSum += v;
        bool fractionMode = (hopSum <= 1.01f);

        auto hopIdx = static_cast<size_t>(ti.consecutiveMisses - 1);
        uint64_t prefetchCount;
        if (hopIdx >= hops.size()) {
            prefetchCount = totalPageGroups;
        } else if (fractionMode) {
            prefetchCount = std::max(uint64_t{1},
                static_cast<uint64_t>(hops[hopIdx] * totalPageGroups));
        } else {
            prefetchCount = static_cast<uint64_t>(hops[hopIdx]);
        }

        // Build prefetch list — fan out from current group, skip PRESENT/FETCHING.
        std::vector<uint64_t> prefetchGroups;
        uint64_t added = 0;
        for (uint64_t dist = 1; added < prefetchCount; dist++) {
            bool anyAdded = false;
            if (pageGroupId + dist < totalPageGroups) {
                if (ti.getGroupState(pageGroupId + dist) == GroupState::NONE) {
                    prefetchGroups.push_back(pageGroupId + dist);
                    added++;
                    anyAdded = true;
                }
            }
            if (dist <= pageGroupId) {
                if (ti.getGroupState(pageGroupId - dist) == GroupState::NONE) {
                    prefetchGroups.push_back(pageGroupId - dist);
                    added++;
                    anyAdded = true;
                }
            }
            if (!anyAdded && pageGroupId + dist >= totalPageGroups && dist > pageGroupId) {
                break;
            }
        }

        submitPrefetch(ti, prefetchGroups);
    }

    // 8. Return the requested page.
    if (!requestedPage.empty()) {
        return requestedPage;
    }

    // Page wasn't in the group (empty slot). Check bitmap in case
    // it was written by a different path.
    if (ti.bitmap->isPresent(pageNum)) {
        std::vector<uint8_t> page(ti.pageSize);
        auto offset = static_cast<off_t>(pageNum * ti.pageSize);
        ::pread(ti.localFd, page.data(), ti.pageSize, offset);
        return page;
    }

    return std::vector<uint8_t>(ti.pageSize, 0);
}

void TieredFileSystem::writeOnePage(TieredFileInfo& ti, uint64_t pageNum,
    const uint8_t* data) const {
    {
        std::lock_guard lock(ti.dirtyMu);
        ti.dirtyPages[pageNum] = std::vector<uint8_t>(data, data + ti.pageSize);
    }

    {
        std::unique_lock lock(ti.manifestMu);
        auto newCount = pageNum + 1;
        if (newCount > ti.manifest.pageCount) {
            ti.manifest.pageCount = newCount;
        }
    }
}

// --- Read Path ---

void TieredFileSystem::readFromFile(common::FileInfo& fileInfo, void* buffer, uint64_t numBytes,
    uint64_t position) const {
    auto& ti = fileInfo.cast<TieredFileInfo>();
    auto ps = static_cast<uint64_t>(ti.pageSize);
    auto* dst = static_cast<uint8_t*>(buffer);
    uint64_t bytesRead = 0;

    while (bytesRead < numBytes) {
        auto fileOffset = position + bytesRead;
        auto pageNum = fileOffset / ps;
        auto offsetInPage = fileOffset % ps;
        auto bytesFromPage = std::min(ps - offsetInPage, numBytes - bytesRead);

        auto pageData = readOnePage(ti, pageNum);
        std::memcpy(dst + bytesRead, pageData.data() + offsetInPage, bytesFromPage);
        bytesRead += bytesFromPage;
    }
}

// --- Write Path ---

void TieredFileSystem::writeFile(common::FileInfo& fileInfo, const uint8_t* buffer,
    uint64_t numBytes, uint64_t offset) const {
    auto& ti = fileInfo.cast<TieredFileInfo>();
    auto ps = static_cast<uint64_t>(ti.pageSize);
    uint64_t bytesWritten = 0;

    while (bytesWritten < numBytes) {
        auto fileOffset = offset + bytesWritten;
        auto pageNum = fileOffset / ps;
        auto offsetInPage = fileOffset % ps;
        auto bytesToPage = std::min(ps - offsetInPage, numBytes - bytesWritten);

        if (offsetInPage == 0 && bytesToPage == ps) {
            writeOnePage(ti, pageNum, buffer + bytesWritten);
        } else {
            auto pageData = readOnePage(ti, pageNum);
            std::memcpy(pageData.data() + offsetInPage, buffer + bytesWritten, bytesToPage);
            writeOnePage(ti, pageNum, pageData.data());
        }
        bytesWritten += bytesToPage;
    }
}

// --- Sync Path ---

void TieredFileSystem::syncFile(const common::FileInfo& fileInfo) const {
    auto& ti = const_cast<TieredFileInfo&>(fileInfo.constCast<TieredFileInfo>());
    doSyncFile(ti);
}

void TieredFileSystem::doSyncFile(TieredFileInfo& ti) const {
    std::unordered_map<uint64_t, std::vector<uint8_t>> dirtySnapshot;
    {
        std::lock_guard lock(ti.dirtyMu);
        if (ti.dirtyPages.empty()) {
            return;
        }
        dirtySnapshot = std::move(ti.dirtyPages);
        ti.dirtyPages.clear();
    }

    uint64_t maxPageNum = 0;
    for (auto& [pageNum, _] : dirtySnapshot) {
        if (pageNum > maxPageNum) maxPageNum = pageNum;
    }
    {
        auto neededSize = static_cast<off_t>((maxPageNum + 1) * ti.pageSize);
        struct stat st;
        if (::fstat(ti.localFd, &st) == 0 && st.st_size < neededSize) {
            ::ftruncate(ti.localFd, neededSize);
        }
    }

    for (auto& [pageNum, rawPage] : dirtySnapshot) {
        auto fileOffset = static_cast<off_t>(pageNum * ti.pageSize);
        ::pwrite(ti.localFd, rawPage.data(), ti.pageSize, fileOffset);
        ti.bitmap->markPresent(pageNum);
    }

    std::unordered_set<uint64_t> affectedPageGroups;
    for (auto& [pageNum, _] : dirtySnapshot) {
        affectedPageGroups.insert(pageNum / ti.pagesPerGroup);
    }

    {
        std::lock_guard lock(ti.pendingMu);
        for (auto pgId : affectedPageGroups) {
            ti.pendingPageGroups.insert(pgId);
        }
    }

    ti.bitmap->persist();

    {
        Manifest m;
        {
            std::lock_guard lock(ti.manifestMu);
            m = ti.manifest;
        }
        auto manifestPath = std::filesystem::path(config_.cacheDir) / "manifest.json";
        auto json = m.toJSON();
        std::ofstream mf(manifestPath, std::ios::binary | std::ios::trunc);
        mf.write(json.data(), json.size());
    }

    flushPendingPageGroups();
}

void TieredFileSystem::flushPendingPageGroups() const {
    auto* afi = activeFileInfo_.load();
    if (!afi) {
        return;
    }

    auto& ti = *afi;

    std::unordered_set<uint64_t> pending;
    {
        std::lock_guard lock(ti.pendingMu);
        if (ti.pendingPageGroups.empty()) {
            return;
        }
        pending = std::move(ti.pendingPageGroups);
        ti.pendingPageGroups.clear();
    }

    uint64_t nextVersion;
    {
        std::lock_guard lock(ti.manifestMu);
        nextVersion = ti.manifest.version + 1;
    }

    std::unordered_map<uint64_t, std::string> newKeys;
    bool anyUploaded = false;

    bool useSeekable = config_.subPagesPerFrame > 0;
    std::unordered_map<uint64_t, std::vector<FrameEntry>> newFrameTables;

    for (auto pgId : pending) {
        // Read raw pages from local cache file.
        std::vector<std::optional<std::vector<uint8_t>>> rawPages(ti.pagesPerGroup);
        bool anyPresent = false;
        bool allPresent = true;

        for (uint32_t i = 0; i < ti.pagesPerGroup; i++) {
            auto absPageNum = pgId * ti.pagesPerGroup + i;
            if (!ti.bitmap->isPresent(absPageNum)) {
                allPresent = false;
                continue;
            }
            std::vector<uint8_t> rawPage(ti.pageSize);
            auto fileOffset = static_cast<off_t>(absPageNum * ti.pageSize);
            auto bytesRead = ::pread(ti.localFd, rawPage.data(), ti.pageSize, fileOffset);
            if (bytesRead < 0) {
                throw std::runtime_error(
                    "pread failed during sync at page " + std::to_string(absPageNum) +
                    ": " + std::string(std::strerror(errno)));
            }
            if (bytesRead != static_cast<ssize_t>(ti.pageSize)) {
                throw std::runtime_error(
                    "pread incomplete at page " + std::to_string(absPageNum) +
                    ": expected " + std::to_string(ti.pageSize) +
                    " bytes, got " + std::to_string(bytesRead));
            }
            rawPages[i] = std::move(rawPage);
            anyPresent = true;
        }

        if (!anyPresent) continue;

        // If some pages are missing, merge from existing S3 data.
        if (!allPresent) {
            std::string oldKey;
            bool oldIsSeekable = false;
            std::vector<FrameEntry> oldFrameTable;
            {
                std::lock_guard lock(ti.manifestMu);
                if (pgId < ti.manifest.pageGroupKeys.size()) {
                    oldKey = ti.manifest.pageGroupKeys[pgId];
                }
                oldIsSeekable = ti.manifest.subPagesPerFrame > 0 &&
                    pgId < ti.manifest.frameTables.size() &&
                    !ti.manifest.frameTables[pgId].empty();
                if (oldIsSeekable) {
                    oldFrameTable = ti.manifest.frameTables[pgId];
                }
            }
            if (!oldKey.empty()) {
                auto oldBlob = ti.s3->getObject(oldKey);
                if (oldBlob.has_value()) {
                    if (oldIsSeekable) {
                        // Decode old seekable blob frame by frame.
                        uint32_t oldSubPPF;
                        {
                            std::lock_guard lock(ti.manifestMu);
                            oldSubPPF = ti.manifest.subPagesPerFrame;
                        }
                        for (uint32_t f = 0; f < oldFrameTable.size(); f++) {
                            auto& entry = oldFrameTable[f];
                            if (entry.len == 0) continue;
                            if (entry.offset + entry.len > oldBlob->size()) break;
                            auto frameStartLocal = f * oldSubPPF;
                            auto pagesInFrame = entry.pageCount > 0
                                ? entry.pageCount
                                : std::min(oldSubPPF, ti.pagesPerGroup - frameStartLocal);
                            std::vector<uint8_t> frameData(
                                oldBlob->begin() + entry.offset,
                                oldBlob->begin() + entry.offset + entry.len);
                            auto rawFrame = decodeFrame(frameData, pagesInFrame, ti.pageSize);
                            for (uint32_t i = 0; i < pagesInFrame; i++) {
                                auto localIdx = frameStartLocal + i;
                                if (rawPages[localIdx].has_value()) continue;
                                auto off = static_cast<size_t>(i) * ti.pageSize;
                                if (off + ti.pageSize <= rawFrame.size()) {
                                    rawPages[localIdx] = std::vector<uint8_t>(
                                        rawFrame.data() + off,
                                        rawFrame.data() + off + ti.pageSize);
                                }
                            }
                        }
                    } else {
                        // Decode old legacy blob.
                        for (uint32_t i = 0; i < ti.pagesPerGroup; i++) {
                            if (rawPages[i].has_value()) continue;
                            auto compressedPage = extractPage(*oldBlob, i, ti.pagesPerGroup);
                            if (compressedPage.has_value() && !compressedPage->empty()) {
                                rawPages[i] = decompressPage(compressedPage->data(),
                                    compressedPage->size(), ti.pageSize);
                            }
                        }
                    }
                }
            }
        }

        std::vector<uint8_t> encoded;
        auto pgKey = ti.s3->pageGroupKey(pgId, nextVersion);

        if (useSeekable) {
            auto result = encodeSeekable(rawPages, ti.pageSize,
                config_.subPagesPerFrame, ti.compressionLevel);
            encoded = std::move(result.blob);
            newFrameTables[pgId] = std::move(result.frameTable);
        } else {
            // Legacy: compress each page individually.
            std::vector<std::optional<std::vector<uint8_t>>> compressedPages(ti.pagesPerGroup);
            for (uint32_t i = 0; i < ti.pagesPerGroup; i++) {
                if (rawPages[i].has_value()) {
                    compressedPages[i] = compressPage(rawPages[i]->data(),
                        ti.pageSize, ti.compressionLevel);
                }
            }
            encoded = encodeChunk(compressedPages, ti.pagesPerGroup);
        }

        if (ti.s3->putObject(pgKey, encoded.data(), encoded.size())) {
            newKeys[pgId] = pgKey;
            anyUploaded = true;
        }
    }

    if (!anyUploaded) {
        return;
    }

    Manifest newManifest;
    {
        std::lock_guard lock(ti.manifestMu);
        newManifest = ti.manifest;
    }
    newManifest.version = nextVersion;
    newManifest.pagesPerGroup = ti.pagesPerGroup;

    if (useSeekable) {
        newManifest.subPagesPerFrame = config_.subPagesPerFrame;
    }

    auto maxGroupId = uint64_t{0};
    for (auto& [pgId, _] : newKeys) {
        if (pgId + 1 > maxGroupId) maxGroupId = pgId + 1;
    }
    if (newManifest.pageGroupKeys.size() < maxGroupId) {
        newManifest.pageGroupKeys.resize(maxGroupId);
    }
    if (useSeekable && newManifest.frameTables.size() < maxGroupId) {
        newManifest.frameTables.resize(maxGroupId);
    }
    for (auto& [pgId, key] : newKeys) {
        newManifest.pageGroupKeys[pgId] = key;
        if (useSeekable && newFrameTables.count(pgId)) {
            newManifest.frameTables[pgId] = newFrameTables[pgId];
        }
    }

    {
        auto manifestPath = std::filesystem::path(config_.cacheDir) / "manifest.json";
        auto json = newManifest.toJSON();
        std::ofstream mf(manifestPath, std::ios::binary | std::ios::trunc);
        mf.write(json.data(), json.size());
    }

    ti.s3->putManifest(newManifest);

    {
        std::unique_lock lock(ti.manifestMu);
        ti.manifest = newManifest;
    }
}

uint64_t TieredFileSystem::evictStalePageGroups() {
    auto* afi = activeFileInfo_.load();
    if (!afi) return 0;

    Manifest currentManifest;
    {
        std::lock_guard lock(afi->manifestMu);
        currentManifest = afi->manifest;
    }

    return s3_->evictStalePageGroups(currentManifest);
}

} // namespace tiered
} // namespace lbug
