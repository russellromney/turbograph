#pragma once

#include "chunk_codec.h"
#include "manifest.h"
#include "page_bitmap.h"
#include "s3_client.h"
#include "table_page_map.h"

#include "common/file_system/file_system.h"

#include <array>
#include <atomic>
#include <condition_variable>
#include <deque>
#include <functional>
#include <memory>
#include <optional>
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
    uint32_t pagesPerGroup = 4096;               // Pages per page group (4096 * 4KB = 16MB).
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

    // Cache eviction. 0 = unlimited (default).
    uint64_t maxCacheBytes = 0;

    // Phase GraphDrift: subframe override threshold.
    // If dirty frame count in a group < threshold, upload only dirty frames.
    // 0 = auto (framesPerGroup / 4, i.e. 25% of frames).
    uint32_t overrideThreshold = 0;

    // Phase GraphDrift: compaction threshold.
    // When a group accumulates >= this many overrides, compact back into base.
    // 0 = disabled.
    uint32_t compactionThreshold = 8;

    // Phase GraphZenith: journal sequence set by hakuzu before sync.
    // Written into the manifest so followers know where to replay from.
    uint64_t journalSeq = 0;

    // Encryption key for pages at rest (local cache + S3).
    // Empty = no encryption. 32 bytes = AES-256.
    // CTR mode for local cache (deterministic, page_num as IV).
    // GCM mode for S3 frames (random nonce, authenticated).
    std::optional<std::array<uint8_t, 32>> encryptionKey;
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

    mutable uint8_t consecutiveMisses = 0; // For structural/metadata pages not in any table.

    // Per-table prefetch state. Built from metadata pages during openFile().
    // readOnePage() uses per-table miss counters to select schedule:
    // relationship tables -> scan, node tables -> lookup.
    // Pages not in the map (structural/metadata) use consecutiveMisses + active schedule.
    std::unique_ptr<TablePageMap> tablePageMap;
    std::unique_ptr<TableMissCounters> tableMissCounters;

    // Page classification bitmaps for selective cache eviction.
    // Structural: page 0, catalog, metadata -- read during Database() construction.
    // Index: PIP pages, hash index data -- read during first query execution.
    // Pages not in either bitmap are data pages (column chunks, CSR edges, overflow).
    mutable std::mutex trackMu;
    std::unique_ptr<PageBitmap> structuralPages;
    std::unique_ptr<PageBitmap> indexPages;
    enum class TrackMode : uint8_t { NONE, STRUCTURAL, INDEX } trackMode = TrackMode::NONE;

    // Pending page groups that need background upload.
    // Phase GraphDrift: maps group ID to set of dirty local page indices within group.
    // This allows override detection: if few frames are dirty, upload only those.
    mutable std::mutex pendingMu;
    std::unordered_set<uint64_t> pendingPageGroups;
    std::unordered_map<uint64_t, std::unordered_set<uint32_t>> pendingDirtyPages;

    // Groups already submitted to prefetch pool (dedup for slingshot).
    mutable std::mutex slingshotMu;
    std::unordered_set<uint64_t> slingshotSubmitted;

    // Per-page-group fetch state: NONE → FETCHING → PRESENT.
    // Used to coordinate sync fetch with async prefetch.
    uint64_t totalGroups = 0;
    std::unique_ptr<std::atomic<uint8_t>[]> groupStates;
    mutable std::mutex groupCvMu;
    mutable std::condition_variable groupCv;

    // Per-group access tracking for cache eviction.
    // Parallel arrays indexed by group ID. Only allocated when maxCacheBytes > 0.
    mutable std::mutex accessMu;
    std::unique_ptr<std::atomic<uint32_t>[]> groupAccessCounts;  // Capped at 64.
    std::unique_ptr<std::atomic<uint64_t>[]> groupAccessTimes;   // Monotonic timestamp (ms).

    // Touch a group: update access time and increment count.
    void touchGroup(uint64_t pgId);

    void initGroupStates(uint64_t count);
    // Grow group arrays (states + access tracking) to cover at least newCount groups.
    // Called from doSyncFile when writes extend the DB beyond the initial group count.
    void growGroupArrays(uint64_t newCount, bool trackAccess);
    GroupState getGroupState(uint64_t pgId) const;
    bool tryClaimGroup(uint64_t pgId);      // CAS NONE->FETCHING, returns true on success.
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

    // Install a page-to-table mapping on the active file info.
    // After this call, readOnePage() uses per-table miss counters and
    // auto-selects prefetch schedules (rel -> scan, node -> lookup).
    void setTablePageMap(std::unique_ptr<TablePageMap> map);

    // Check whether per-table prefetch is active.
    bool hasTablePageMap() const;

    // Proactively prefetch all page groups belonging to the given table IDs.
    // Used by Phase Cypher to warm tables before query execution.
    // Returns the number of page groups submitted to the prefetch pool.
    uint64_t prefetchTables(const std::vector<uint32_t>& tableIds);

    // Parses metadata pages into a TablePageMap during openFile().
    // Set by the extension during load(). Takes raw metadata bytes + length.
    using MetadataParserFn = std::function<std::unique_ptr<TablePageMap>(
        const uint8_t* data, size_t len)>;

    // Register the metadata parser. Runs in openFile() after Beacon.
    void setMetadataParser(MetadataParserFn fn);

    // --- Phase GraphZenith: hakuzu integration ---

    // Trigger doSyncFile and return the new manifest version.
    // Returns 0 if no active file or no dirty pages.
    uint64_t syncAndGetVersion();

    // Return the current manifest version without syncing.
    uint64_t getManifestVersion() const;

    // Phase GraphBridge: return the current manifest as a JSON string.
    // Does not sync. Returns the last-synced manifest state.
    std::string getManifestJSON() const;

    // Follower: apply a remote manifest received from the leader.
    // Parses JSON, compares versions, invalidates cache for changed groups,
    // sets the new manifest as active. Returns the new version.
    uint64_t applyRemoteManifest(const std::string& jsonStr);

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
    void evictLocalGroup(uint64_t pageGroupId) const;

    // Evict groups until cache is under budget. Returns number of groups evicted.
    // Eviction priority: data first, index under pressure, structural never.
    // Within a tier, coldest groups (lowest score) evicted first.
    uint64_t evictToBudget() const;

    // Current cache size in bytes (present pages * page size).
    uint64_t currentCacheBytes() const;

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
    const std::vector<float>& getHopsForTable(bool isRelationship) const;

    // Metadata parser callback, set by extension layer.
    MetadataParserFn metadataParser_;

    // Raw pointer to the active TieredFileInfo, set in openFile().
    // Used by flushPendingPageGroups() and clearCache() to access file state.
    std::atomic<TieredFileInfo*> activeFileInfo_{nullptr};
};

} // namespace tiered
} // namespace lbug
