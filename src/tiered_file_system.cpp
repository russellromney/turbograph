#include "tiered_file_system.h"
#include "crypto.h"

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

#include "turbograph_zstd.h"

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

void TieredFileInfo::growGroupArrays(uint64_t newCount, bool trackAccess) {
    if (newCount <= totalGroups) return;

    // Lock accessMu to prevent evictToBudget from iterating stale array pointers
    // while we reallocate. touchGroup() is safe without this lock since it only
    // accesses individual atomic elements, not the array pointer itself.
    std::lock_guard<std::mutex> lock(accessMu);

    // Re-check under lock (another thread may have grown arrays already).
    if (newCount <= totalGroups) return;

    // Allocate new arrays, copy old data, zero-init new entries.
    auto newStates = std::make_unique<std::atomic<uint8_t>[]>(newCount);
    for (uint64_t i = 0; i < totalGroups; i++) {
        newStates[i].store(
            groupStates ? groupStates[i].load(std::memory_order_relaxed) :
            static_cast<uint8_t>(GroupState::NONE),
            std::memory_order_relaxed);
    }
    for (uint64_t i = totalGroups; i < newCount; i++) {
        newStates[i].store(static_cast<uint8_t>(GroupState::NONE),
            std::memory_order_relaxed);
    }
    groupStates = std::move(newStates);

    if (trackAccess) {
        auto newCounts = std::make_unique<std::atomic<uint32_t>[]>(newCount);
        auto newTimes = std::make_unique<std::atomic<uint64_t>[]>(newCount);
        for (uint64_t i = 0; i < totalGroups; i++) {
            newCounts[i].store(
                groupAccessCounts ? groupAccessCounts[i].load(std::memory_order_relaxed) : 0,
                std::memory_order_relaxed);
            newTimes[i].store(
                groupAccessTimes ? groupAccessTimes[i].load(std::memory_order_relaxed) : 0,
                std::memory_order_relaxed);
        }
        for (uint64_t i = totalGroups; i < newCount; i++) {
            newCounts[i].store(0, std::memory_order_relaxed);
            newTimes[i].store(0, std::memory_order_relaxed);
        }
        groupAccessCounts = std::move(newCounts);
        groupAccessTimes = std::move(newTimes);
    }

    totalGroups = newCount;
}

void TieredFileInfo::resetGroupStates() {
    if (!groupStates) return;
    for (uint64_t i = 0; i < totalGroups; i++) {
        groupStates[i].store(static_cast<uint8_t>(GroupState::NONE),
            std::memory_order_relaxed);
    }
}

static uint64_t nowMs() {
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now().time_since_epoch()).count());
}

void TieredFileInfo::touchGroup(uint64_t pgId) {
    if (!groupAccessTimes || pgId >= totalGroups) return;
    groupAccessTimes[pgId].store(nowMs(), std::memory_order_release);
    // CAS loop for atomic increment with saturation at 64.
    uint32_t old = groupAccessCounts[pgId].load(std::memory_order_relaxed);
    while (old < 64) {
        if (groupAccessCounts[pgId].compare_exchange_weak(
                old, old + 1, std::memory_order_release)) {
            break;
        }
        // old is updated by compare_exchange_weak on failure.
    }
}

// --- TieredFileSystem ---

TieredFileSystem::TieredFileSystem(TieredConfig config) : config_(std::move(config)) {
    s3_ = std::make_shared<S3Client>(config_.s3);

    // Resolve auto prefetch threads: num_cpus - 1, min 1.
    if (config_.prefetchThreads == 0) {
        auto cpus = std::thread::hardware_concurrency();
        config_.prefetchThreads = std::max(1u, cpus > 1 ? cpus - 1 : 1u);
    }

    // Start prefetch worker threads.
    prefetchStop_.store(false);
    for (uint32_t i = 0; i < config_.prefetchThreads; i++) {
        prefetchWorkers_.emplace_back([this] { prefetchWorkerLoop(); });
    }
}

// --- Schedule switching ---

void TieredFileSystem::setActiveSchedule(const std::string& name) {
    std::lock_guard lock(scheduleMu_);
    activeScheduleName_ = name;
    // Reset miss counter so the new schedule starts fresh.
    auto* afi = activeFileInfo_.load();
    if (afi) afi->consecutiveMisses = 0;
}

void TieredFileSystem::setSchedule(const std::string& name, const std::vector<float>& hops) {
    std::lock_guard lock(scheduleMu_);
    if (name == "scan") config_.schedules.scan = hops;
    else if (name == "lookup") config_.schedules.lookup = hops;
    else if (name == "default") config_.schedules.defaultSchedule = hops;
}

const std::vector<float>& TieredFileSystem::getActiveHops() const {
    // No lock needed for reads -- schedule changes are infrequent and
    // reading a stale schedule for one query is harmless.
    if (activeScheduleName_ == "scan") return config_.schedules.scan;
    if (activeScheduleName_ == "lookup") return config_.schedules.lookup;
    return config_.schedules.defaultSchedule;
}

const std::vector<float>& TieredFileSystem::getHopsForTable(bool isRelationship) const {
    // Relationship tables (CSR edge data) -> aggressive scan schedule.
    // Node tables (hash index access) -> conservative lookup schedule.
    if (isRelationship) return config_.schedules.scan;
    return config_.schedules.lookup;
}

void TieredFileSystem::setTablePageMap(std::unique_ptr<TablePageMap> map) {
    auto* afi = activeFileInfo_.load();
    if (!afi) return;
    if (!map) {
        afi->tablePageMap.reset();
        afi->tableMissCounters.reset();
        return;
    }
    if (map->size() > 0) {
        map->finalize();
        afi->tableMissCounters = std::make_unique<TableMissCounters>(map->maxTableId());
    }
    afi->tablePageMap = std::move(map);
}

bool TieredFileSystem::hasTablePageMap() const {
    auto* afi = activeFileInfo_.load();
    return afi && afi->tablePageMap;
}

void TieredFileSystem::setMetadataParser(MetadataParserFn fn) {
    metadataParser_ = std::move(fn);
}

uint64_t TieredFileSystem::prefetchTables(const std::vector<uint32_t>& tableIds) {
    auto* afi = activeFileInfo_.load();
    if (!afi || !afi->tablePageMap) return 0;

    // Collect all page groups that belong to any of the requested tables.
    std::unordered_set<uint32_t> tidSet(tableIds.begin(), tableIds.end());
    std::vector<uint64_t> groups;
    for (const auto& interval : afi->tablePageMap->intervals()) {
        if (!tidSet.count(interval.tableId)) continue;

        // Convert page range to page group range.
        uint64_t startGroup = interval.startPage / afi->pagesPerGroup;
        uint64_t endPage = interval.endPage; // exclusive
        uint64_t endGroup = (endPage + afi->pagesPerGroup - 1) / afi->pagesPerGroup;
        for (uint64_t g = startGroup; g < endGroup && g < afi->totalGroups; g++) {
            if (afi->getGroupState(g) == GroupState::NONE) {
                groups.push_back(g);
            }
        }
    }

    // Deduplicate (intervals from different columns in the same table may overlap in groups).
    std::sort(groups.begin(), groups.end());
    groups.erase(std::unique(groups.begin(), groups.end()), groups.end());

    if (!groups.empty()) {
        submitPrefetch(*afi, groups);
    }
    return groups.size();
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
        if (job.slingshot) {
            // Slingshot: group is already FETCHING (claimed by the sync reader).
            // Fetch the full group and mark PRESENT. Waiters on groupCv will wake.
            bool ok = fetchAndStoreGroup(ti, job.groupId);
            if (ok) {
                ti.markGroupPresent(job.groupId);
            } else {
                ti.markGroupNone(job.groupId);
            }
        } else if (ti.tryClaimGroup(job.groupId)) {
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
    //    Phase GraphZenith: if local manifest exists, compare against S3 to
    //    detect stale cache (S3 newer) or crash recovery (local "newer").
    Manifest m;
    bool manifestFound = false;
    bool needCacheInvalidation = false;
    Manifest localManifest;
    bool hasLocalManifest = false;

    if (std::filesystem::exists(manifestPath)) {
        std::ifstream f(manifestPath, std::ios::binary);
        std::string json((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
        auto parsed = Manifest::fromJSON(json);
        if (parsed.has_value()) {
            localManifest = *parsed;
            hasLocalManifest = true;
        }
    }

    // Always check S3 for the authoritative manifest.
    auto s3Manifest = s3_->getManifest();

    if (hasLocalManifest && s3Manifest.has_value()) {
        // Both exist: compare versions.
        if (localManifest.version == s3Manifest->version) {
            // Versions match: cache is warm.
            m = localManifest;
            manifestFound = true;
        } else if (s3Manifest->version > localManifest.version) {
            // S3 is newer: use S3 manifest, invalidate changed groups.
            m = *s3Manifest;
            manifestFound = true;
            needCacheInvalidation = true;
        } else {
            // Local is "newer" (crash during write, manifest never published).
            // Discard local, use S3, full cache invalidation.
            m = *s3Manifest;
            manifestFound = true;
            needCacheInvalidation = true;
        }
    } else if (hasLocalManifest && !s3Manifest.has_value()) {
        // Only local exists (e.g. fresh upload not yet committed, or offline).
        m = localManifest;
        manifestFound = true;
    } else if (!hasLocalManifest && s3Manifest.has_value()) {
        // Only S3 exists: cold open.
        m = *s3Manifest;
        manifestFound = true;
    } else {
        // Neither exists: new database.
        m.pageSize = config_.pageSize;
        m.pagesPerGroup = config_.pagesPerGroup;
    }

    // Persist the manifest locally if we used S3.
    if (manifestFound && (!hasLocalManifest || needCacheInvalidation)) {
        std::ofstream f(manifestPath, std::ios::binary | std::ios::trunc);
        auto json = m.toJSON();
        f.write(json.data(), json.size());
    }
    if (m.pageSize == 0) m.pageSize = config_.pageSize;
    if (m.pagesPerGroup == 0) m.pagesPerGroup = config_.pagesPerGroup;

    // Reject encrypted database opened without a key.
    if (m.encrypted && !config_.encryptionKey) {
        throw common::InternalException(
            "Database is encrypted but no encryption key was provided. "
            "Set TURBOGRAPH_ENCRYPTION_KEY or turbograph_encryption_key option.");
    }

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

        // Initialize access tracking arrays for cache eviction.
        if (config_.maxCacheBytes > 0) {
            info->groupAccessCounts = std::make_unique<std::atomic<uint32_t>[]>(totalGroups);
            info->groupAccessTimes = std::make_unique<std::atomic<uint64_t>[]>(totalGroups);
            for (uint64_t i = 0; i < totalGroups; i++) {
                info->groupAccessCounts[i].store(0, std::memory_order_relaxed);
                info->groupAccessTimes[i].store(0, std::memory_order_relaxed);
            }
        }
    }

    activeFileInfo_.store(info.get());

    // Phase GraphZenith: cache invalidation when S3 manifest is newer or local is stale.
    if (needCacheInvalidation && hasLocalManifest && m.pageCount > 0) {
        if (localManifest.version > m.version) {
            // Local "newer" (crash recovery): full invalidation.
            info->bitmap->clear();
            info->resetGroupStates();
        } else {
            // S3 is newer: diff manifests, invalidate changed groups.
            auto maxGroups = std::max(localManifest.pageGroupKeys.size(),
                                      m.pageGroupKeys.size());
            for (size_t gid = 0; gid < maxGroups; gid++) {
                bool changed = false;
                auto oldKey = gid < localManifest.pageGroupKeys.size()
                    ? localManifest.pageGroupKeys[gid] : std::string{};
                auto newKey = gid < m.pageGroupKeys.size()
                    ? m.pageGroupKeys[gid] : std::string{};
                if (oldKey != newKey) changed = true;

                if (!changed) {
                    bool oldHas = gid < localManifest.subframeOverrides.size() &&
                        !localManifest.subframeOverrides[gid].empty();
                    bool newHas = gid < m.subframeOverrides.size() &&
                        !m.subframeOverrides[gid].empty();
                    if (oldHas != newHas) {
                        changed = true;
                    } else if (oldHas && newHas) {
                        auto& ov1 = localManifest.subframeOverrides[gid];
                        auto& ov2 = m.subframeOverrides[gid];
                        if (ov1.size() != ov2.size()) {
                            changed = true;
                        } else {
                            for (auto& [idx, ov] : ov1) {
                                auto it = ov2.find(idx);
                                if (it == ov2.end() || it->second.key != ov.key ||
                                    it->second.entry.offset != ov.entry.offset ||
                                    it->second.entry.len != ov.entry.len) {
                                    changed = true;
                                    break;
                                }
                            }
                        }
                    }
                }

                if (changed) {
                    auto startPage = static_cast<uint64_t>(gid) * m.pagesPerGroup;
                    auto endPage = std::min(startPage + m.pagesPerGroup, m.pageCount);
                    if (endPage > startPage) {
                        info->bitmap->clearRange(startPage, endPage - startPage);
                    }
                    if (gid < info->totalGroups) {
                        info->markGroupNone(gid);
                    }
                }
            }
        }
    }

    // --- Phase Beacon: Eager-fetch structural pages ---
    // Fetch page 0 to check for Kuzu magic bytes. If valid, parse the database
    // header to discover catalog + metadata page ranges, then fetch all page
    // groups containing structural pages. This makes Database() construction
    // fast because structural pages are already in local cache.
    if (manifestFound && m.pageCount > 0) {
        // Fetch page 0 via a single readOnePage call (triggers frame/group fetch).
        auto page0 = readOnePage(*info, 0);

        // Parse header only if magic bytes match (skip for non-Kuzu data files).
        if (page0.size() >= 28 &&
            page0[0] == 'L' && page0[1] == 'B' && page0[2] == 'U' && page0[3] == 'G') {
            uint32_t catStart, catPages, metaStart, metaPages;
            std::memcpy(&catStart, page0.data() + 12, 4);
            std::memcpy(&catPages, page0.data() + 16, 4);
            std::memcpy(&metaStart, page0.data() + 20, 4);
            std::memcpy(&metaPages, page0.data() + 24, 4);

            // Collect page groups that contain structural pages.
            std::unordered_set<uint64_t> structGroups;
            structGroups.insert(0); // Page 0 group.

            auto addRange = [&](uint32_t start, uint32_t count) {
                if (start == UINT32_MAX || count == 0) return;
                for (uint32_t p = start; p < start + count; p++) {
                    structGroups.insert(p / m.pagesPerGroup);
                }
            };
            addRange(catStart, catPages);
            addRange(metaStart, metaPages);

            // Fetch remaining structural groups in parallel.
            std::vector<uint64_t> toFetch;
            for (auto gid : structGroups) {
                if (gid < info->totalGroups &&
                    info->getGroupState(gid) == GroupState::NONE) {
                    toFetch.push_back(gid);
                }
            }
            if (!toFetch.empty()) {
                submitPrefetch(*info, toFetch);
                for (auto gid : toFetch) {
                    info->waitForGroup(gid);
                }
            }

            // --- Phase Volley: Parse metadata pages for per-table prefetch ---
            // Metadata pages are now in local cache (Beacon fetched their groups).
            // If a parser callback is registered (extension layer), read metadata
            // pages into a contiguous buffer and parse to build a page-to-table map.
            if (metadataParser_ && metaStart != UINT32_MAX && metaPages > 0) {
                std::vector<uint8_t> metaBuf(
                    static_cast<size_t>(metaPages) * m.pageSize);
                for (uint32_t p = 0; p < metaPages; p++) {
                    auto page = readOnePage(*info, metaStart + p);
                    std::memcpy(metaBuf.data() + p * m.pageSize,
                        page.data(), m.pageSize);
                }
                setTablePageMap(metadataParser_(metaBuf.data(), metaBuf.size()));
            }
        }
    }

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

void TieredFileSystem::evictLocalGroup(uint64_t pageGroupId) const {
    auto* afi = activeFileInfo_.load();
    if (!afi) return;
    auto& ti = *afi;

    // 1. Reset group state to NONE.
    ti.markGroupNone(pageGroupId);

    // 2. Clear bitmap for all pages in this group.
    auto startPage = pageGroupId * ti.pagesPerGroup;
    ti.bitmap->clearRange(startPage, ti.pagesPerGroup);

    // 3. Punch hole in local cache file to reclaim NVMe blocks (Linux only).
#ifdef __linux__
    auto offset = static_cast<off_t>(startPage * ti.pageSize);
    auto len = static_cast<off_t>(ti.pagesPerGroup * ti.pageSize);
    ::fallocate(ti.localFd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, len);
#endif

    // 4. Reset access tracking for this group.
    if (ti.groupAccessCounts && pageGroupId < ti.totalGroups) {
        ti.groupAccessCounts[pageGroupId].store(0, std::memory_order_relaxed);
        ti.groupAccessTimes[pageGroupId].store(0, std::memory_order_relaxed);
    }
}

uint64_t TieredFileSystem::currentCacheBytes() const {
    auto* afi = activeFileInfo_.load();
    if (!afi || !afi->bitmap) return 0;
    return afi->bitmap->presentCount() * afi->pageSize;
}

uint64_t TieredFileSystem::evictToBudget() const {
    if (config_.maxCacheBytes == 0) return 0;

    auto* afi = activeFileInfo_.load();
    if (!afi || !afi->bitmap) return 0;
    auto& ti = *afi;

    uint64_t cacheBytes = ti.bitmap->presentCount() * ti.pageSize;
    if (cacheBytes <= config_.maxCacheBytes) return 0;

    // Target: evict to 80% of limit to avoid thrashing.
    // Reordered to avoid overflow for very large maxCacheBytes values.
    uint64_t target = config_.maxCacheBytes - config_.maxCacheBytes / 5;
    uint64_t now = nowMs();
    constexpr uint64_t RECENCY_WINDOW_MS = 3600 * 1000; // 1 hour.

    // Snapshot access array pointers under accessMu to prevent use-after-free
    // if growGroupArrays() reallocates while we iterate.
    std::atomic<uint32_t>* accessCountsSnap = nullptr;
    std::atomic<uint64_t>* accessTimesSnap = nullptr;
    uint64_t snapTotalGroups = 0;
    {
        std::lock_guard<std::mutex> lock(ti.accessMu);
        accessCountsSnap = ti.groupAccessCounts.get();
        accessTimesSnap = ti.groupAccessTimes.get();
        snapTotalGroups = ti.totalGroups;
    }

    // Eviction tier for a group. 0 = data (evict first), 1 = index, 2 = structural (never).
    auto groupTier = [&](uint64_t gid) -> int {
        auto startPage = gid * ti.pagesPerGroup;
        auto endPage = std::min(startPage + ti.pagesPerGroup,
            ti.bitmap->pageCount());
        bool hasStructural = false;
        bool hasIndex = false;
        for (auto p = startPage; p < endPage; p++) {
            if (ti.structuralPages && ti.structuralPages->isPresent(p)) {
                hasStructural = true;
                break;
            }
            if (ti.indexPages && ti.indexPages->isPresent(p)) {
                hasIndex = true;
            }
        }
        if (hasStructural) return 2;
        if (hasIndex) return 1;
        return 0;
    };

    // Score each PRESENT group. Lower score = evict first.
    // Score = tier_bonus (0/10/inf) + recency (0-1) + frequency (0-1).
    struct ScoredGroup {
        uint64_t gid;
        float score;
    };
    std::vector<ScoredGroup> candidates;

    // Derive effective group count from bitmap (source of truth for cached pages).
    // totalGroups is set at open time and may be 0 if the DB started empty.
    auto bitmapPages = ti.bitmap->pageCount();
    uint64_t effectiveGroups = (bitmapPages + ti.pagesPerGroup - 1) / ti.pagesPerGroup;
    if (snapTotalGroups > effectiveGroups) effectiveGroups = snapTotalGroups;

    for (uint64_t gid = 0; gid < effectiveGroups; gid++) {
        // A group is cached if any of its pages are in the bitmap.
        auto startPage = gid * ti.pagesPerGroup;
        bool hasCachedPages = false;
        for (uint32_t p = 0; p < ti.pagesPerGroup; p++) {
            if (ti.bitmap->isPresent(startPage + p)) {
                hasCachedPages = true;
                break;
            }
        }
        if (!hasCachedPages) continue;

        // Skip groups currently being fetched by background workers.
        if (gid < snapTotalGroups && ti.getGroupState(gid) == GroupState::FETCHING) continue;

        int tier = groupTier(gid);
        if (tier == 2) continue; // Never evict structural.

        float tierBonus = (tier == 1) ? 10.0f : 0.0f;

        float recency = 0.0f;
        float frequency = 0.0f;
        if (accessTimesSnap && accessCountsSnap && gid < snapTotalGroups) {
            uint64_t lastAccess = accessTimesSnap[gid].load(std::memory_order_acquire);
            if (lastAccess > 0 && now > lastAccess) {
                uint64_t ageMs = now - lastAccess;
                recency = 1.0f - std::min(1.0f,
                    static_cast<float>(ageMs) / RECENCY_WINDOW_MS);
            }
            uint32_t count = accessCountsSnap[gid].load(std::memory_order_acquire);
            frequency = std::min(1.0f, static_cast<float>(count) / 64.0f);
        }

        candidates.push_back({gid, tierBonus + recency + frequency});
    }

    // Sort by score ascending (coldest first).
    std::sort(candidates.begin(), candidates.end(),
        [](const ScoredGroup& a, const ScoredGroup& b) {
            return a.score < b.score;
        });

    // Evict until under target.
    uint64_t evicted = 0;
    uint64_t groupBytes = static_cast<uint64_t>(ti.pagesPerGroup) * ti.pageSize;
    for (auto& c : candidates) {
        if (cacheBytes <= target) break;
        evictLocalGroup(c.gid);
        cacheBytes = (cacheBytes > groupBytes) ? cacheBytes - groupBytes : 0;
        evicted++;
    }

    return evicted;
}

// --- fetchAndStoreFrame: fetch a single seekable frame via range GET ---

bool TieredFileSystem::fetchAndStoreFrame(TieredFileInfo& ti, uint64_t pageGroupId,
    uint32_t frameIdx, std::vector<uint8_t>* requestedPageOut,
    uint32_t requestedLocalIdx) const {

    std::string pgKey;
    std::vector<FrameEntry> frameTable;
    uint32_t subPPF = 0;
    std::string overrideKey;
    FrameEntry overrideEntry;
    bool hasOverride = false;
    {
        std::lock_guard lock(ti.manifestMu);
        if (pageGroupId >= ti.manifest.pageGroupKeys.size()) return false;
        pgKey = ti.manifest.pageGroupKeys[pageGroupId];
        if (pageGroupId < ti.manifest.frameTables.size()) {
            frameTable = ti.manifest.frameTables[pageGroupId];
        }
        subPPF = ti.manifest.subPagesPerFrame;

        // Phase GraphDrift: check for subframe override.
        if (pageGroupId < ti.manifest.subframeOverrides.size()) {
            auto& ovMap = ti.manifest.subframeOverrides[pageGroupId];
            auto it = ovMap.find(static_cast<size_t>(frameIdx));
            if (it != ovMap.end()) {
                overrideKey = it->second.key;
                overrideEntry = it->second.entry;
                hasOverride = true;
            }
        }
    }

    FrameEntry entry;
    std::optional<std::vector<uint8_t>> compressedFrame;

    if (hasOverride && !overrideKey.empty() && overrideEntry.len > 0) {
        // Phase GraphDrift: fetch override object (full GET, not range GET).
        compressedFrame = ti.s3->getObject(overrideKey);
        if (!compressedFrame.has_value()) return false;
        entry = overrideEntry;
        entry.offset = 0; // Override objects are standalone.
    } else {
        // Normal path: range GET from base group.
        if (pgKey.empty() || frameIdx >= frameTable.size() || subPPF == 0) return false;
        entry = frameTable[frameIdx];
        if (entry.len == 0) return false;
        compressedFrame = ti.s3->getObjectRange(pgKey, entry.offset, entry.len);
        if (!compressedFrame.has_value()) return false;
    }

    // Validate we got the expected number of bytes.
    if (compressedFrame->size() != entry.len) return false;

    // Use the page count stored at encode time (authoritative for partial frames).
    auto groupStartPage = pageGroupId * ti.pagesPerGroup;
    auto frameStartLocal = frameIdx * subPPF;
    uint32_t pagesInFrame = entry.pageCount > 0 ? entry.pageCount : subPPF;

    // GCM decrypt the frame if encrypted.
    std::vector<uint8_t> frameData;
    if (config_.encryptionKey) {
        frameData = aes256_gcm_decrypt(compressedFrame->data(),
            compressedFrame->size(), *config_.encryptionKey);
        if (frameData.empty()) return false; // Auth failure.
    } else {
        frameData = std::move(*compressedFrame);
    }

    auto rawFrame = decodeFrame(frameData, pagesInFrame, ti.pageSize);

    // Write each page in the frame to local file.
    for (uint32_t i = 0; i < pagesInFrame; i++) {
        auto localIdx = frameStartLocal + i;
        auto absPageNum = groupStartPage + localIdx;
        auto pageOffset = static_cast<size_t>(i) * ti.pageSize;
        if (pageOffset + ti.pageSize > rawFrame.size()) break;

        auto fileOffset = static_cast<off_t>(absPageNum * ti.pageSize);
        // CTR encrypt before writing to local cache.
        if (config_.encryptionKey) {
            std::vector<uint8_t> encPage(ti.pageSize);
            aes256_ctr(rawFrame.data() + pageOffset, encPage.data(),
                ti.pageSize, absPageNum, *config_.encryptionKey);
            ::pwrite(ti.localFd, encPage.data(), ti.pageSize, fileOffset);
        } else {
            ::pwrite(ti.localFd, rawFrame.data() + pageOffset, ti.pageSize, fileOffset);
        }
        ti.bitmap->markPresent(absPageNum);

        // Return plaintext (pre-encryption) to caller.
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

            // GCM decrypt the frame if encrypted.
            if (config_.encryptionKey) {
                auto decrypted = aes256_gcm_decrypt(frameData.data(),
                    frameData.size(), *config_.encryptionKey);
                if (decrypted.empty()) continue; // Auth failure, skip frame.
                frameData = std::move(decrypted);
            }

            auto rawFrame = decodeFrame(frameData, pagesInFrame, ti.pageSize);

            for (uint32_t i = 0; i < pagesInFrame; i++) {
                auto localIdx = frameStartLocal + i;
                auto absPageNum = groupStartPage + localIdx;
                auto pageOffset = static_cast<size_t>(i) * ti.pageSize;
                if (pageOffset + ti.pageSize > rawFrame.size()) break;

                auto fileOffset = static_cast<off_t>(absPageNum * ti.pageSize);
                // CTR encrypt before writing to local cache.
                if (config_.encryptionKey) {
                    std::vector<uint8_t> encPage(ti.pageSize);
                    aes256_ctr(rawFrame.data() + pageOffset, encPage.data(),
                        ti.pageSize, absPageNum, *config_.encryptionKey);
                    ::pwrite(ti.localFd, encPage.data(), ti.pageSize, fileOffset);
                } else {
                    ::pwrite(ti.localFd, rawFrame.data() + pageOffset, ti.pageSize, fileOffset);
                }
                ti.bitmap->markPresent(absPageNum);

                // Return plaintext to caller.
                if (requestedPageOut && localIdx == requestedLocalIdx) {
                    *requestedPageOut = std::vector<uint8_t>(
                        rawFrame.data() + pageOffset,
                        rawFrame.data() + pageOffset + ti.pageSize);
                }
            }
        }

        // Phase GraphDrift: apply subframe overrides on top of base group.
        // Override frames replace the corresponding base frames.
        std::unordered_map<size_t, SubframeOverride> overrides;
        {
            std::lock_guard lock(ti.manifestMu);
            if (pageGroupId < ti.manifest.subframeOverrides.size()) {
                overrides = ti.manifest.subframeOverrides[pageGroupId];
            }
        }
        for (auto& [ovFrameIdx, ov] : overrides) {
            if (ov.key.empty() || ov.entry.len == 0) continue;
            auto ovBlob = ti.s3->getObject(ov.key);
            if (!ovBlob.has_value()) continue;

            auto ovFrameStartLocal = static_cast<uint32_t>(ovFrameIdx) * subPPF;
            auto groupStartOv = pageGroupId * ti.pagesPerGroup;
            uint32_t ovPagesInFrame = ov.entry.pageCount > 0 ? ov.entry.pageCount : subPPF;

            std::vector<uint8_t> ovFrameData;
            if (config_.encryptionKey) {
                ovFrameData = aes256_gcm_decrypt(ovBlob->data(),
                    ovBlob->size(), *config_.encryptionKey);
                if (ovFrameData.empty()) continue;
            } else {
                ovFrameData = std::move(*ovBlob);
            }

            auto rawOvFrame = decodeFrame(ovFrameData, ovPagesInFrame, ti.pageSize);
            for (uint32_t i = 0; i < ovPagesInFrame; i++) {
                auto localIdx = ovFrameStartLocal + i;
                auto absPageNum = groupStartOv + localIdx;
                auto pageOffset = static_cast<size_t>(i) * ti.pageSize;
                if (pageOffset + ti.pageSize > rawOvFrame.size()) break;

                auto fileOffset = static_cast<off_t>(absPageNum * ti.pageSize);
                if (config_.encryptionKey) {
                    std::vector<uint8_t> encPage(ti.pageSize);
                    aes256_ctr(rawOvFrame.data() + pageOffset, encPage.data(),
                        ti.pageSize, absPageNum, *config_.encryptionKey);
                    ::pwrite(ti.localFd, encPage.data(), ti.pageSize, fileOffset);
                } else {
                    ::pwrite(ti.localFd, rawOvFrame.data() + pageOffset,
                        ti.pageSize, fileOffset);
                }
                ti.bitmap->markPresent(absPageNum);

                if (requestedPageOut && localIdx == requestedLocalIdx) {
                    *requestedPageOut = std::vector<uint8_t>(
                        rawOvFrame.data() + pageOffset,
                        rawOvFrame.data() + pageOffset + ti.pageSize);
                }
            }
        }

        return true;
    }

    // Legacy path: per-page compressed with offset header.
    // GCM decrypt the whole legacy blob if encrypted.
    if (config_.encryptionKey) {
        auto decrypted = aes256_gcm_decrypt(blob->data(), blob->size(),
            *config_.encryptionKey);
        if (decrypted.empty()) return false; // Auth failure.
        *blob = std::move(decrypted);
    }

    for (uint32_t i = 0; i < ti.pagesPerGroup; i++) {
        auto compressedPage = extractPage(*blob, i, ti.pagesPerGroup);
        if (!compressedPage.has_value() || compressedPage->empty()) continue;

        auto rawPage = decompressPage(compressedPage->data(),
            compressedPage->size(), ti.pageSize);
        if (rawPage.size() != ti.pageSize) continue;

        auto absPageNum = pageGroupId * ti.pagesPerGroup + i;
        auto fileOffset = static_cast<off_t>(absPageNum * ti.pageSize);
        // CTR encrypt before writing to local cache.
        if (config_.encryptionKey) {
            std::vector<uint8_t> encPage(ti.pageSize);
            aes256_ctr(rawPage.data(), encPage.data(),
                ti.pageSize, absPageNum, *config_.encryptionKey);
            ::pwrite(ti.localFd, encPage.data(), ti.pageSize, fileOffset);
        } else {
            ::pwrite(ti.localFd, rawPage.data(), ti.pageSize, fileOffset);
        }
        ti.bitmap->markPresent(absPageNum);

        // Return plaintext to caller.
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
        // Reset per-table miss counter on cache hit.
        if (ti.tablePageMap && ti.tableMissCounters) {
            auto result = ti.tablePageMap->lookup(static_cast<uint32_t>(pageNum));
            if (result.found) {
                ti.tableMissCounters->reset(result.tableId);
            }
        }
        // Touch group for eviction tracking.
        ti.touchGroup(pageNum / ti.pagesPerGroup);
        std::vector<uint8_t> page(ti.pageSize);
        auto offset = static_cast<off_t>(pageNum * ti.pageSize);
        auto bytesRead = ::pread(ti.localFd, page.data(), ti.pageSize, offset);
        if (bytesRead == static_cast<ssize_t>(ti.pageSize)) {
            if (config_.encryptionKey) {
                aes256_ctr(page.data(), page.data(), ti.pageSize,
                    pageNum, *config_.encryptionKey);
            }
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
            // Keep group as FETCHING (don't reset to NONE). Submit slingshot
            // to prefetch pool -- the worker will fetch the full S3 object,
            // mark PRESENT, and notify waiters. Other page reads in this group
            // see FETCHING, wait on groupCv, then read from local cache.
            // Pages from the already-fetched frame hit the bitmap and skip
            // the group state check entirely.
            {
                std::lock_guard lock(ti.slingshotMu);
                if (ti.slingshotSubmitted.insert(pageGroupId).second) {
                    // Submit as slingshot job (skip tryClaimGroup, group is already FETCHING).
                    std::lock_guard plock(prefetchMu_);
                    if (!prefetchStop_.load()) {
                        prefetchQueue_.push_back({&ti, pageGroupId, /*slingshot=*/true});
                        prefetchCv_.notify_one();
                    }
                } else {
                    // Slingshot already submitted. Leave group as FETCHING so
                    // other pages wait for the background fetch to complete.
                    // Don't reset to NONE (which would cause more frame fetches).
                }
            }
        }
    } else {
        if (!skipGroupTracking) ti.markGroupNone(pageGroupId);
        return std::vector<uint8_t>(ti.pageSize, 0);
    }

    // 7. Compute prefetch groups and submit asynchronously (only with group tracking).
    if (!skipGroupTracking) {
        // Select hops schedule and miss count based on which table this page belongs to.
        const std::vector<float>* hops;
        uint8_t missCount;

        if (ti.tablePageMap && ti.tableMissCounters) {
            auto tableLookup = ti.tablePageMap->lookup(static_cast<uint32_t>(pageNum));
            if (tableLookup.found) {
                ti.tableMissCounters->increment(tableLookup.tableId);
                hops = &getHopsForTable(tableLookup.isRelationship);
                missCount = ti.tableMissCounters->get(tableLookup.tableId);
            } else {
                // Page not in any known table (structural/metadata page).
                if (ti.consecutiveMisses < 255) ti.consecutiveMisses++;
                hops = &getActiveHops();
                missCount = ti.consecutiveMisses;
            }
        } else {
            // No table map yet (empty DB, tables created after open).
            if (ti.consecutiveMisses < 255) ti.consecutiveMisses++;
            hops = &getActiveHops();
            missCount = ti.consecutiveMisses;
        }

        if (missCount == 0) missCount = 1; // First miss is at least 1.

        uint64_t totalPageGroups = 0;
        {
            std::lock_guard lock(ti.manifestMu);
            totalPageGroups =
                (ti.manifest.pageCount + ti.pagesPerGroup - 1) / ti.pagesPerGroup;
        }

        float hopSum = 0;
        for (auto v : *hops) hopSum += v;
        bool fractionMode = (hopSum <= 1.01f);

        auto hopIdx = static_cast<size_t>(missCount - 1);
        uint64_t prefetchCount;
        if (hopIdx >= hops->size()) {
            prefetchCount = totalPageGroups;
        } else if (fractionMode) {
            prefetchCount = std::max(uint64_t{1},
                static_cast<uint64_t>((*hops)[hopIdx] * totalPageGroups));
        } else {
            prefetchCount = static_cast<uint64_t>((*hops)[hopIdx]);
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

    // Touch group for eviction tracking.
    ti.touchGroup(pageGroupId);

    // Trigger eviction if cache is over budget.
    if (config_.maxCacheBytes > 0) {
        evictToBudget();
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
    // Track page classification during tracking mode (same as readOnePage).
    {
        std::lock_guard lock(ti.trackMu);
        if (ti.trackMode == TieredFileInfo::TrackMode::STRUCTURAL && ti.structuralPages) {
            ti.structuralPages->markPresent(pageNum);
        } else if (ti.trackMode == TieredFileInfo::TrackMode::INDEX && ti.indexPages) {
            ti.indexPages->markPresent(pageNum);
        }
    }

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
        if (config_.encryptionKey) {
            std::vector<uint8_t> encrypted(ti.pageSize);
            aes256_ctr(rawPage.data(), encrypted.data(), ti.pageSize,
                pageNum, *config_.encryptionKey);
            ::pwrite(ti.localFd, encrypted.data(), ti.pageSize, fileOffset);
        } else {
            ::pwrite(ti.localFd, rawPage.data(), ti.pageSize, fileOffset);
        }
        ti.bitmap->markPresent(pageNum);
    }

    // Grow group arrays if writes extended beyond the initial group count.
    {
        uint64_t neededGroups = (maxPageNum + ti.pagesPerGroup) / ti.pagesPerGroup;
        if (neededGroups > ti.totalGroups) {
            ti.growGroupArrays(neededGroups, config_.maxCacheBytes > 0);
        }
    }

    // Mark synced groups as PRESENT in group state tracking.
    std::unordered_set<uint64_t> affectedPageGroups;
    for (auto& [pageNum, _] : dirtySnapshot) {
        affectedPageGroups.insert(pageNum / ti.pagesPerGroup);
    }
    for (auto pgId : affectedPageGroups) {
        ti.markGroupPresent(pgId);
    }

    {
        std::lock_guard lock(ti.pendingMu);
        for (auto pgId : affectedPageGroups) {
            ti.pendingPageGroups.insert(pgId);
        }
        // Phase GraphDrift: track which pages within each group are dirty.
        for (auto& [pageNum, _] : dirtySnapshot) {
            auto pgId = pageNum / ti.pagesPerGroup;
            auto localIdx = static_cast<uint32_t>(pageNum % ti.pagesPerGroup);
            ti.pendingDirtyPages[pgId].insert(localIdx);
        }
    }

    ti.bitmap->persist();

    // Phase GraphZenith: stamp journal_seq from config into manifest
    // so followers know where to replay graphstream journal from.
    {
        std::lock_guard lock(ti.manifestMu);
        ti.manifest.journalSeq = config_.journalSeq;
    }

    // Phase Laika: S3Primary ordering fix. Upload to S3 first, then persist
    // locally. flushPendingPageGroups() handles both the S3 upload and local
    // manifest persist in the correct order (S3 first, local second).
    // No separate local persist here; that was a bug that could leave the
    // local manifest ahead of S3 after a crash.
    flushPendingPageGroups();
}

void TieredFileSystem::flushPendingPageGroups() const {
    auto* afi = activeFileInfo_.load();
    if (!afi) {
        return;
    }

    auto& ti = *afi;

    std::unordered_set<uint64_t> pending;
    std::unordered_map<uint64_t, std::unordered_set<uint32_t>> dirtyPagesPerGroup;
    {
        std::lock_guard lock(ti.pendingMu);
        if (ti.pendingPageGroups.empty()) {
            return;
        }
        pending = std::move(ti.pendingPageGroups);
        ti.pendingPageGroups.clear();
        dirtyPagesPerGroup = std::move(ti.pendingDirtyPages);
        ti.pendingDirtyPages.clear();
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

    // Phase GraphDrift: override tracking.
    // Carry forward existing overrides, then add/replace as needed.
    std::vector<std::unordered_map<size_t, SubframeOverride>> newSubframeOverrides;
    std::vector<std::string> staleOverrideKeys; // Old override keys to GC.
    std::unordered_set<uint64_t> fullRewriteGroups;
    {
        std::lock_guard lock(ti.manifestMu);
        newSubframeOverrides = ti.manifest.subframeOverrides;
    }

    // Compute effective override threshold.
    uint32_t effectiveThreshold = config_.overrideThreshold;
    if (effectiveThreshold == 0 && useSeekable && config_.subPagesPerFrame > 0) {
        auto framesPerGroup = (ti.pagesPerGroup + config_.subPagesPerFrame - 1) /
            config_.subPagesPerFrame;
        effectiveThreshold = std::max(1u, framesPerGroup / 4);
    }

    for (auto pgId : pending) {
        // Phase GraphDrift: determine dirty frames for override decision.
        bool hasExistingFrameTable = false;
        {
            std::lock_guard lock(ti.manifestMu);
            hasExistingFrameTable = useSeekable &&
                pgId < ti.manifest.frameTables.size() &&
                !ti.manifest.frameTables[pgId].empty();
        }

        // Compute dirty frame indices from dirty pages.
        std::unordered_set<uint32_t> dirtyFrameSet;
        bool canUseOverride = hasExistingFrameTable && effectiveThreshold > 0;
        if (canUseOverride) {
            auto it = dirtyPagesPerGroup.find(pgId);
            if (it != dirtyPagesPerGroup.end()) {
                for (auto localIdx : it->second) {
                    dirtyFrameSet.insert(localIdx / config_.subPagesPerFrame);
                }
            }
            // If no dirty pages tracked (shouldn't happen), fall back to full rewrite.
            if (dirtyFrameSet.empty()) canUseOverride = false;
        }

        bool useOverride = canUseOverride &&
            dirtyFrameSet.size() < effectiveThreshold;

        if (useOverride) {
            // Phase GraphDrift: override path -- encode only dirty frames.
            while (newSubframeOverrides.size() <= pgId) {
                newSubframeOverrides.emplace_back();
            }

            for (auto frameIdx : dirtyFrameSet) {
                auto frameStartLocal = frameIdx * config_.subPagesPerFrame;
                auto frameEndLocal = std::min(
                    frameStartLocal + config_.subPagesPerFrame, ti.pagesPerGroup);
                auto pagesInFrame = frameEndLocal - frameStartLocal;

                // Gather raw pages for this frame from local cache.
                std::vector<std::optional<std::vector<uint8_t>>> framePages(pagesInFrame);
                for (uint32_t i = 0; i < pagesInFrame; i++) {
                    auto absPageNum = pgId * ti.pagesPerGroup + frameStartLocal + i;
                    if (!ti.bitmap->isPresent(absPageNum)) continue;
                    std::vector<uint8_t> rawPage(ti.pageSize);
                    auto fileOffset = static_cast<off_t>(absPageNum * ti.pageSize);
                    auto bytesRead = ::pread(ti.localFd, rawPage.data(),
                        ti.pageSize, fileOffset);
                    if (bytesRead != static_cast<ssize_t>(ti.pageSize)) continue;
                    if (config_.encryptionKey) {
                        aes256_ctr(rawPage.data(), rawPage.data(), ti.pageSize,
                            absPageNum, *config_.encryptionKey);
                    }
                    framePages[i] = std::move(rawPage);
                }

                // Encode just this frame using encodeSeekable with 1 frame.
                auto result = encodeSeekable(framePages, ti.pageSize,
                    pagesInFrame, ti.compressionLevel);
                if (result.blob.empty()) continue;

                std::vector<uint8_t> encoded;
                if (config_.encryptionKey) {
                    encoded = aes256_gcm_encrypt(result.blob.data(),
                        result.blob.size(), *config_.encryptionKey);
                } else {
                    encoded = std::move(result.blob);
                }

                auto overrideKey = ti.s3->overrideFrameKey(pgId, frameIdx, nextVersion);

                // Collect old override key for GC.
                auto ovIt = newSubframeOverrides[pgId].find(frameIdx);
                if (ovIt != newSubframeOverrides[pgId].end()) {
                    staleOverrideKeys.push_back(ovIt->second.key);
                }

                if (ti.s3->putObject(overrideKey, encoded.data(), encoded.size())) {
                    SubframeOverride ov;
                    ov.key = overrideKey;
                    ov.entry.offset = 0;
                    ov.entry.len = static_cast<uint32_t>(encoded.size());
                    ov.entry.pageCount = pagesInFrame;
                    newSubframeOverrides[pgId][frameIdx] = std::move(ov);
                    anyUploaded = true;
                }
            }
            // Base group key stays the same: no re-upload.
            continue;
        }

        // Full rewrite path (existing behavior).
        fullRewriteGroups.insert(pgId);

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
            // Local cache pages are CTR encrypted. Decrypt for encoding.
            if (config_.encryptionKey) {
                aes256_ctr(rawPage.data(), rawPage.data(), ti.pageSize,
                    absPageNum, *config_.encryptionKey);
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
                            // GCM decrypt old frame if encrypted.
                            if (config_.encryptionKey) {
                                auto decrypted = aes256_gcm_decrypt(frameData.data(),
                                    frameData.size(), *config_.encryptionKey);
                                if (decrypted.empty()) continue;
                                frameData = std::move(decrypted);
                            }
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
                        // Decode old legacy blob. GCM decrypt if encrypted.
                        if (config_.encryptionKey) {
                            auto dec = aes256_gcm_decrypt(oldBlob->data(),
                                oldBlob->size(), *config_.encryptionKey);
                            if (!dec.empty()) *oldBlob = std::move(dec);
                        }
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

            // GCM encrypt each frame individually.
            if (config_.encryptionKey) {
                std::vector<uint8_t> encBlob;
                std::vector<FrameEntry> encFrameTable;
                for (auto& fe : result.frameTable) {
                    if (fe.len == 0) {
                        encFrameTable.push_back(fe);
                        continue;
                    }
                    auto encFrame = aes256_gcm_encrypt(
                        result.blob.data() + fe.offset, fe.len,
                        *config_.encryptionKey);
                    FrameEntry encEntry;
                    encEntry.offset = encBlob.size();
                    encEntry.len = static_cast<uint32_t>(encFrame.size());
                    encEntry.pageCount = fe.pageCount;
                    encFrameTable.push_back(encEntry);
                    encBlob.insert(encBlob.end(), encFrame.begin(), encFrame.end());
                }
                encoded = std::move(encBlob);
                newFrameTables[pgId] = std::move(encFrameTable);
            } else {
                encoded = std::move(result.blob);
                newFrameTables[pgId] = std::move(result.frameTable);
            }
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
            // GCM encrypt the whole legacy blob.
            if (config_.encryptionKey) {
                encoded = aes256_gcm_encrypt(encoded.data(), encoded.size(),
                    *config_.encryptionKey);
            }
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
    newManifest.encrypted = config_.encryptionKey.has_value();

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

    // Phase GraphDrift: clear overrides for groups that got full rewrites.
    for (auto pgId : fullRewriteGroups) {
        if (pgId < newSubframeOverrides.size()) {
            for (auto& [_, ov] : newSubframeOverrides[pgId]) {
                staleOverrideKeys.push_back(ov.key);
            }
            newSubframeOverrides[pgId].clear();
        }
    }

    newManifest.subframeOverrides = std::move(newSubframeOverrides);
    newManifest.normalizeOverrides();

    // Phase GraphDrift: auto-compaction.
    // Groups that accumulated too many overrides get merged back into the base.
    if (config_.compactionThreshold > 0 && useSeekable) {
        std::vector<uint64_t> groupsToCompact;
        for (size_t gid = 0; gid < newManifest.subframeOverrides.size(); gid++) {
            if (newManifest.subframeOverrides[gid].size() >= config_.compactionThreshold) {
                groupsToCompact.push_back(gid);
            }
        }

        for (auto gid : groupsToCompact) {
            // Compact: read base group + all override frames, merge, upload new base.
            auto compactVersion = nextVersion; // Reuse same version for compaction.

            std::string baseKey;
            std::vector<FrameEntry> baseFrameTable;
            uint32_t baseSubPPF = 0;
            {
                if (gid < newManifest.pageGroupKeys.size()) {
                    baseKey = newManifest.pageGroupKeys[gid];
                }
                if (gid < newManifest.frameTables.size()) {
                    baseFrameTable = newManifest.frameTables[gid];
                }
                baseSubPPF = newManifest.subPagesPerFrame;
            }
            if (baseKey.empty() || baseSubPPF == 0) continue;

            // Read base group.
            auto baseBlob = ti.s3->getObject(baseKey);
            if (!baseBlob.has_value()) continue;

            // Decode all frames from base group.
            std::vector<std::optional<std::vector<uint8_t>>> mergedPages(ti.pagesPerGroup);
            for (uint32_t f = 0; f < baseFrameTable.size(); f++) {
                auto& entry = baseFrameTable[f];
                if (entry.len == 0) continue;
                if (entry.offset + entry.len > baseBlob->size()) break;
                auto frameStartLocal = f * baseSubPPF;
                uint32_t pagesInFrame = entry.pageCount > 0
                    ? entry.pageCount
                    : std::min(baseSubPPF, ti.pagesPerGroup - frameStartLocal);
                std::vector<uint8_t> frameData(
                    baseBlob->begin() + entry.offset,
                    baseBlob->begin() + entry.offset + entry.len);
                if (config_.encryptionKey) {
                    auto dec = aes256_gcm_decrypt(frameData.data(),
                        frameData.size(), *config_.encryptionKey);
                    if (dec.empty()) continue;
                    frameData = std::move(dec);
                }
                auto rawFrame = decodeFrame(frameData, pagesInFrame, ti.pageSize);
                for (uint32_t i = 0; i < pagesInFrame; i++) {
                    auto localIdx = frameStartLocal + i;
                    auto off = static_cast<size_t>(i) * ti.pageSize;
                    if (off + ti.pageSize <= rawFrame.size()) {
                        mergedPages[localIdx] = std::vector<uint8_t>(
                            rawFrame.data() + off,
                            rawFrame.data() + off + ti.pageSize);
                    }
                }
            }

            // Apply overrides on top.
            for (auto& [frameIdx, ov] : newManifest.subframeOverrides[gid]) {
                if (ov.key.empty() || ov.entry.len == 0) continue;
                auto ovBlob = ti.s3->getObject(ov.key);
                if (!ovBlob.has_value()) continue;
                auto frameStartLocal = static_cast<uint32_t>(frameIdx) * baseSubPPF;
                uint32_t pagesInFrame = ov.entry.pageCount > 0
                    ? ov.entry.pageCount
                    : std::min(baseSubPPF, ti.pagesPerGroup - frameStartLocal);
                std::vector<uint8_t> ovData;
                if (config_.encryptionKey) {
                    ovData = aes256_gcm_decrypt(ovBlob->data(),
                        ovBlob->size(), *config_.encryptionKey);
                    if (ovData.empty()) continue;
                } else {
                    ovData = std::move(*ovBlob);
                }
                auto rawFrame = decodeFrame(ovData, pagesInFrame, ti.pageSize);
                for (uint32_t i = 0; i < pagesInFrame; i++) {
                    auto localIdx = frameStartLocal + i;
                    auto off = static_cast<size_t>(i) * ti.pageSize;
                    if (off + ti.pageSize <= rawFrame.size()) {
                        mergedPages[localIdx] = std::vector<uint8_t>(
                            rawFrame.data() + off,
                            rawFrame.data() + off + ti.pageSize);
                    }
                }
                staleOverrideKeys.push_back(ov.key);
            }

            // Re-encode full group.
            auto result = encodeSeekable(mergedPages, ti.pageSize,
                config_.subPagesPerFrame, ti.compressionLevel);
            if (result.blob.empty()) continue;

            std::vector<uint8_t> encoded;
            std::vector<FrameEntry> compactFrameTable;
            if (config_.encryptionKey) {
                std::vector<uint8_t> encBlob;
                for (auto& fe : result.frameTable) {
                    if (fe.len == 0) {
                        compactFrameTable.push_back(fe);
                        continue;
                    }
                    auto encFrame = aes256_gcm_encrypt(
                        result.blob.data() + fe.offset, fe.len,
                        *config_.encryptionKey);
                    FrameEntry encEntry;
                    encEntry.offset = encBlob.size();
                    encEntry.len = static_cast<uint32_t>(encFrame.size());
                    encEntry.pageCount = fe.pageCount;
                    compactFrameTable.push_back(encEntry);
                    encBlob.insert(encBlob.end(), encFrame.begin(), encFrame.end());
                }
                encoded = std::move(encBlob);
            } else {
                encoded = std::move(result.blob);
                compactFrameTable = std::move(result.frameTable);
            }

            auto compactKey = ti.s3->pageGroupKey(gid, compactVersion);
            if (ti.s3->putObject(compactKey, encoded.data(), encoded.size())) {
                // Mark old base key for GC (if different from compacted key).
                if (baseKey != compactKey) {
                    staleOverrideKeys.push_back(baseKey);
                }
                newManifest.pageGroupKeys[gid] = compactKey;
                newManifest.frameTables[gid] = compactFrameTable;
                newManifest.subframeOverrides[gid].clear();
            }
        }
    }

    // Phase Laika: S3Primary ordering fix. Publish manifest to S3 FIRST,
    // then persist locally. If we crash after S3 publish but before local
    // persist, the local manifest is behind S3, which is safe (we re-fetch
    // from S3 on reopen). The old order (local first, then S3) was a data
    // loss bug: crash after local persist but before S3 upload left the
    // local manifest claiming a version that S3 never received.
    ti.s3->putManifest(newManifest);

    {
        auto manifestPath = std::filesystem::path(config_.cacheDir) / "manifest.json";
        auto json = newManifest.toJSON();
        std::ofstream mf(manifestPath, std::ios::binary | std::ios::trunc);
        mf.write(json.data(), json.size());
    }

    {
        std::unique_lock lock(ti.manifestMu);
        ti.manifest = newManifest;
    }

    // Phase GraphDrift: async GC of stale override keys.
    for (auto& key : staleOverrideKeys) {
        ti.s3->deleteObject(key);
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

// ============================================================================
// Phase GraphZenith: hakuzu integration methods
// ============================================================================

uint64_t TieredFileSystem::syncAndGetVersion() {
    auto* afi = activeFileInfo_.load();
    if (!afi) return 0;

    doSyncFile(*afi);

    std::lock_guard lock(afi->manifestMu);
    return afi->manifest.version;
}

uint64_t TieredFileSystem::getManifestVersion() const {
    auto* afi = activeFileInfo_.load();
    if (!afi) return 0;

    std::lock_guard lock(afi->manifestMu);
    return afi->manifest.version;
}

uint64_t TieredFileSystem::applyRemoteManifest(const std::string& jsonStr) {
    auto parsed = Manifest::fromJSON(jsonStr);
    if (!parsed.has_value()) {
        throw std::runtime_error("applyRemoteManifest: invalid manifest JSON");
    }

    auto* afi = activeFileInfo_.load();
    if (!afi) {
        throw std::runtime_error("applyRemoteManifest: no active file");
    }

    auto& ti = *afi;
    auto& newManifest = *parsed;

    // Validate pageSize consistency.
    if (newManifest.pageSize != 0 && newManifest.pageSize != config_.pageSize) {
        throw std::runtime_error("Remote manifest pageSize mismatch: got " +
            std::to_string(newManifest.pageSize) + ", expected " +
            std::to_string(config_.pageSize));
    }

    // Hold manifestMu across the entire operation to prevent concurrent readers
    // from seeing stale state between cache invalidation and manifest update.
    std::lock_guard lock(ti.manifestMu);
    Manifest oldManifest = ti.manifest;

    // If S3 version is older or equal, nothing to do (except crash recovery case).
    if (newManifest.version < oldManifest.version) {
        // Local is "newer" (crash during write, manifest never published).
        // Discard local, use S3, invalidate entire cache.
        ti.bitmap->clear();
        ti.resetGroupStates();
        ti.manifest = newManifest;

        // Persist atomically: write tmp then rename.
        auto manifestPath = std::filesystem::path(config_.cacheDir) / "manifest.json";
        auto tmpPath = manifestPath.string() + ".tmp";
        auto json = newManifest.toJSON();
        {
            std::ofstream mf(tmpPath, std::ios::binary | std::ios::trunc);
            mf.write(json.data(), json.size());
            mf.flush();
        }
        std::filesystem::rename(tmpPath, manifestPath);

        return newManifest.version;
    }

    if (newManifest.version == oldManifest.version) {
        // Same version, cache is warm.
        return newManifest.version;
    }

    // S3 is newer: diff manifests, invalidate changed groups.
    auto maxGroups = std::max(oldManifest.pageGroupKeys.size(),
                              newManifest.pageGroupKeys.size());
    for (size_t gid = 0; gid < maxGroups; gid++) {
        bool changed = false;

        // Key changed or new group appeared.
        auto oldKey = gid < oldManifest.pageGroupKeys.size()
            ? oldManifest.pageGroupKeys[gid] : std::string{};
        auto newKey = gid < newManifest.pageGroupKeys.size()
            ? newManifest.pageGroupKeys[gid] : std::string{};
        if (oldKey != newKey) changed = true;

        // Subframe overrides changed.
        if (!changed) {
            bool oldHas = gid < oldManifest.subframeOverrides.size() &&
                !oldManifest.subframeOverrides[gid].empty();
            bool newHas = gid < newManifest.subframeOverrides.size() &&
                !newManifest.subframeOverrides[gid].empty();

            if (oldHas != newHas) {
                changed = true;
            } else if (oldHas && newHas) {
                auto& ov1 = oldManifest.subframeOverrides[gid];
                auto& ov2 = newManifest.subframeOverrides[gid];
                if (ov1.size() != ov2.size()) {
                    changed = true;
                } else {
                    for (auto& [idx, ov] : ov1) {
                        auto it = ov2.find(idx);
                        if (it == ov2.end() || it->second.key != ov.key ||
                            it->second.entry.offset != ov.entry.offset ||
                            it->second.entry.len != ov.entry.len) {
                            changed = true;
                            break;
                        }
                    }
                }
            }
        }

        if (changed) {
            // Invalidate this group's cached pages.
            auto startPage = static_cast<uint64_t>(gid) * ti.pagesPerGroup;
            auto endPage = startPage + ti.pagesPerGroup;
            if (endPage > newManifest.pageCount) endPage = newManifest.pageCount;
            if (endPage > startPage) {
                ti.bitmap->clearRange(startPage, endPage - startPage);
            }
            if (gid < ti.totalGroups) {
                ti.markGroupNone(gid);
            }
        }
    }

    // Grow group arrays if needed.
    if (newManifest.pageCount > 0) {
        auto neededGroups = (newManifest.pageCount + ti.pagesPerGroup - 1) / ti.pagesPerGroup;
        if (neededGroups > ti.totalGroups) {
            ti.growGroupArrays(neededGroups, config_.maxCacheBytes > 0);
        }
    }

    // Resize local cache file if needed.
    if (newManifest.pageCount > oldManifest.pageCount) {
        auto targetSize = static_cast<off_t>(newManifest.pageCount * newManifest.pageSize);
        ::ftruncate(ti.localFd, targetSize);
    }

    // Resize bitmap if needed.
    if (newManifest.pageCount > 0) {
        ti.bitmap->resize(newManifest.pageCount);
    }

    ti.manifest = newManifest;

    ti.bitmap->persist();

    // After updating in-memory state, persist manifest atomically.
    // Write to tmp file then rename to avoid corrupt manifest on crash.
    auto manifestPath = std::filesystem::path(config_.cacheDir) / "manifest.json";
    auto tmpPath = manifestPath.string() + ".tmp";
    auto json = newManifest.toJSON();
    {
        std::ofstream mf(tmpPath, std::ios::binary | std::ios::trunc);
        mf.write(json.data(), json.size());
        mf.flush();
    }
    std::filesystem::rename(tmpPath, manifestPath);

    return newManifest.version;
}

} // namespace tiered
} // namespace lbug
