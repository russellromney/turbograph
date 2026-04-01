#pragma once

#include "chunk_codec.h"
#include "manifest.h"
#include "page_bitmap.h"
#include "s3_client.h"

#include "common/file_system/file_system.h"

#include <atomic>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <thread>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace lbug {
namespace tiered {

struct TieredConfig {
    S3Config s3;
    std::string cacheDir;
    std::string dataFilePath;                    // Exact path of the .kz data file to intercept.
    uint32_t pageSize = 4096;                    // Must match LBUG_PAGE_SIZE.
    int compressionLevel = 3;                    // zstd compression level.
    uint32_t pagesPerGroup = 2048;               // Pages per page group (2048 * 4KB = 8MB).
    uint32_t subPagesPerFrame = 4;               // Pages per seekable sub-frame (4 * 4KB = 16KB raw).

    // Named prefetch schedules. Each entry controls how many page groups to
    // fetch on that consecutive cache miss. An implicit final hop always
    // fetches everything remaining. Two modes (auto-detected by sum):
    //   Fraction mode (sum <= 1.0): each value is a fraction of totalPageGroups.
    //   Absolute mode (sum > 1.0):  each value is a page group count.
    //
    // "scan":   aggressive, for edge traversals and sequential column scans.
    // "lookup": conservative, for point queries and hash index lookups.
    //           Three free hops before any prefetch.
    // "default": used when no schedule is explicitly selected.
    struct PrefetchSchedules {
        std::vector<float> scan = {0.3f, 0.3f, 0.4f};
        std::vector<float> lookup = {0.0f, 0.0f, 0.0f};
        std::vector<float> defaultSchedule = {0.33f, 0.33f};
    } schedules;

    // Number of background threads for async prefetch.
    uint32_t prefetchThreads = 0; // 0 = auto (num_cpus - 1, min 1).

};

// Per-page-group fetch state for async prefetch coordination.
enum class GroupState : uint8_t {
    NONE = 0,     // Not fetched, not being fetched.
    FETCHING = 1, // Background thread is fetching this group.
    PRESENT = 2,  // Data is in local cache file, ready to pread.
};

// FileInfo subclass that holds tiered state for a single open file.
struct TieredFileInfo : public common::FileInfo {
    TieredFileInfo(std::string path, common::FileSystem* fs, std::shared_ptr<S3Client> s3,
        std::unique_ptr<PageBitmap> bitmap, Manifest manifest,
        uint32_t pageSize, int compressionLevel,
        uint32_t pagesPerGroup, int localFd);

    ~TieredFileInfo() override;

    std::shared_ptr<S3Client> s3;
    std::unique_ptr<PageBitmap> bitmap;
    int localFd;                                 // Raw fd for pread/pwrite on local cache file.

    mutable std::mutex manifestMu;
    Manifest manifest;

    mutable std::mutex dirtyMu;
    std::unordered_map<uint64_t, std::vector<uint8_t>> dirtyPages; // pageIdx -> raw page data.

    uint32_t pageSize;
    int compressionLevel;
    uint32_t pagesPerGroup;

    mutable uint8_t consecutiveMisses = 0; // For hop-based adaptive prefetch.

    // Page classification bitmaps for selective cache eviction.
    // Structural: page 0, catalog, metadata -- read during Database() construction.
    // Index: PIP pages, hash index data -- read during first query execution.
    // Pages not in either bitmap are data pages (column chunks, CSR edges, overflow).
    mutable std::mutex trackMu;
    std::unique_ptr<PageBitmap> structuralPages;
    std::unique_ptr<PageBitmap> indexPages;
    enum class TrackMode : uint8_t { NONE, STRUCTURAL, INDEX } trackMode = TrackMode::NONE;

    // Pending page groups that need background upload.
    mutable std::mutex pendingMu;
    std::unordered_set<uint64_t> pendingPageGroups;

    // Groups already submitted to prefetch pool (dedup for slingshot).
    mutable std::mutex slingshotMu;
    std::unordered_set<uint64_t> slingshotSubmitted;

    // Per-page-group fetch state: NONE → FETCHING → PRESENT.
    // Used to coordinate sync fetch with async prefetch.
    uint64_t totalGroups = 0;
    std::unique_ptr<std::atomic<uint8_t>[]> groupStates;
    mutable std::mutex groupCvMu;
    mutable std::condition_variable groupCv;

    void initGroupStates(uint64_t count);
    GroupState getGroupState(uint64_t pgId) const;
    bool tryClaimGroup(uint64_t pgId);      // CAS NONE→FETCHING, returns true on success.
    void markGroupPresent(uint64_t pgId);   // Set PRESENT + notify waiters.
    void markGroupNone(uint64_t pgId);      // Reset to NONE (fetch failed).
    void waitForGroup(uint64_t pgId) const; // Block until PRESENT.
    void resetGroupStates();                // Reset all to NONE.
};

class TieredFileSystem : public common::FileSystem {
public:
    explicit TieredFileSystem(TieredConfig config);
    ~TieredFileSystem() override;

    // FileSystem interface.
    std::unique_ptr<common::FileInfo> openFile(const std::string& path,
        common::FileOpenFlags flags, main::ClientContext* context = nullptr) override;

    std::vector<std::string> glob(main::ClientContext* context,
        const std::string& path) const override;

    bool canHandleFile(const std::string_view path) const override;

    // --- Schedule switching (for UDF / benchmark) ---
    // Set the active prefetch schedule by name: "scan", "lookup", "default".
    // Also resets the consecutive miss counter.
    void setActiveSchedule(const std::string& name);
    const std::string& getActiveSchedule() const { return activeScheduleName_; }

    // Set a custom schedule by name. Overwrites the named slot.
    void setSchedule(const std::string& name, const std::vector<float>& hops);

    // Expose S3 client for I/O counters (benchmarking).
    S3Client& s3() { return *s3_; }

    // Called by TieredFileInfo destructor to prevent dangling pointer.
    void clearActiveFileInfo(TieredFileInfo* expected) {
        activeFileInfo_.compare_exchange_strong(expected, nullptr);
    }

    // Drain prefetch queue and wait for in-flight workers to finish.
    // Called by TieredFileInfo destructor before freeing the file info.
    void drainPrefetchAndWait() const;

    // --- Page classification tracking ---
    // Call beginTrackStructural before Database() construction, endTrack after.
    // Call beginTrackIndex before the first warmup query, endTrack after.
    void beginTrackStructural();
    void beginTrackIndex();
    void endTrack();

    // --- Selective cache eviction ---
    // Each level preserves pages from higher-priority tiers:
    //   clearCacheAll:            nuke everything (cold benchmark)
    //   clearCacheKeepStructural: keep structural, evict index+data (interior benchmark)
    //   clearCacheKeepIndex:      keep structural+index, evict data (index benchmark)
    // Warm benchmark: don't clear cache, just close/reopen connection.
    void clearCacheAll();
    void clearCacheKeepStructural();
    void clearCacheKeepIndex();

    // Evict a single page group from the local NVMe cache.
    // Clears bitmap, resets group state, punches hole in cache file (Linux).
    void evictLocalGroup(uint64_t pageGroupId);

    // Delete stale page group objects from S3 (background GC).
    uint64_t evictStalePageGroups();

    void syncFile(const common::FileInfo& fileInfo) const override;

    bool fileOrPathExists(const std::string& path,
        main::ClientContext* context = nullptr) override;

protected:
    void readFromFile(common::FileInfo& fileInfo, void* buffer, uint64_t numBytes,
        uint64_t position) const override;

    int64_t readFile(common::FileInfo& fileInfo, void* buf, size_t numBytes) const override;

    void writeFile(common::FileInfo& fileInfo, const uint8_t* buffer, uint64_t numBytes,
        uint64_t offset) const override;

    int64_t seek(common::FileInfo& fileInfo, uint64_t offset, int whence) const override;

    uint64_t getFileSize(const common::FileInfo& fileInfo) const override;

    void truncate(common::FileInfo& fileInfo, uint64_t size) const override;

private:
    // Read a single full page by page number. Returns raw page data.
    // Priority: dirty -> bitmap/local file -> S3 (targeted range request) -> zeros.
    std::vector<uint8_t> readOnePage(TieredFileInfo& ti, uint64_t pageNum) const;

    // Write a single full page by page number. Stores raw (uncompressed) in dirty map.
    void writeOnePage(TieredFileInfo& ti, uint64_t pageNum, const uint8_t* data) const;

    // Sync: write dirty pages to local file + bitmap, encode to S3.
    void doSyncFile(TieredFileInfo& ti) const;

    // Upload pending page groups to S3. Called synchronously from doSyncFile().
    void flushPendingPageGroups() const;

    // Fetch a single page group from S3, decompress, write to local file,
    // mark pages in bitmap. Does NOT update groupState (caller's responsibility).
    // Returns true if the requested page (localIdx within the group) was found.
    bool fetchAndStoreGroup(TieredFileInfo& ti, uint64_t pageGroupId,
        std::vector<uint8_t>* requestedPageOut = nullptr,
        uint32_t requestedLocalIdx = 0) const;

    // Fetch a single seekable frame via S3 range GET, decompress, write pages
    // to local file + bitmap. Returns the requested page if it was in this frame.
    bool fetchAndStoreFrame(TieredFileInfo& ti, uint64_t pageGroupId,
        uint32_t frameIdx, std::vector<uint8_t>* requestedPageOut = nullptr,
        uint32_t requestedLocalIdx = 0) const;

    // Background prefetch pool.
    struct PrefetchJob {
        TieredFileInfo* ti;
        uint64_t groupId;
        bool slingshot = false; // If true, group is already FETCHING; skip tryClaimGroup.
    };
    void prefetchWorkerLoop() const;
    void submitPrefetch(TieredFileInfo& ti, const std::vector<uint64_t>& groupIds) const;
    void drainPrefetch() const;

    mutable std::vector<std::thread> prefetchWorkers_;
    mutable std::deque<PrefetchJob> prefetchQueue_;
    mutable std::mutex prefetchMu_;
    mutable std::condition_variable prefetchCv_;
    mutable std::atomic<bool> prefetchStop_{false};
    mutable std::atomic<uint32_t> prefetchInFlight_{0};

    TieredConfig config_;
    std::shared_ptr<S3Client> s3_;
    mutable bool existsCached_ = false;
    mutable bool existsResult_ = false;
    mutable std::mutex existsMu_;

    // Active prefetch schedule.
    std::string activeScheduleName_ = "scan"; // Graph queries are almost all traversals.
    mutable std::mutex scheduleMu_;
    const std::vector<float>& getActiveHops() const;

    // Raw pointer to the active TieredFileInfo, set in openFile().
    // Used by flushPendingPageGroups() and clearCache() to access file state.
    std::atomic<TieredFileInfo*> activeFileInfo_{nullptr};
};

} // namespace tiered
} // namespace lbug
