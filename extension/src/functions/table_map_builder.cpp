// Phase Volley: raw metadata parser for per-table prefetch schedules.
//
// Parses LadybugDB's serialized StorageManager metadata pages during openFile()
// to build a page-to-table mapping. Uses Kuzu's own Deserializer and type-level
// deserialize methods (LogicalType, ColumnChunkMetadata) but walks the tree
// structure manually to avoid needing a MemoryManager or Catalog.
//
// The binary format matches StorageManager::serialize() in storage_manager.cpp.
// Format is stable across Kuzu versions. If it ever changes, users can vacuum
// into the new version.

#include "main/turbograph_extension.h"
#include "main/turbograph_functions.h"
#include "table_page_map.h"
#include "tiered_file_system.h"

#include "catalog/catalog.h"
#include "catalog/catalog_entry/node_table_catalog_entry.h"
#include "catalog/catalog_entry/rel_group_catalog_entry.h"
#include "common/enums/rel_direction.h"
#include "common/enums/table_type.h"
#include "common/serializer/deserializer.h"
#include "common/serializer/reader.h"
#include "common/types/types.h"
#include "main/database.h"
#include "storage/compression/compression.h"
#include "storage/enums/residency_state.h"
#include "storage/stats/hyperloglog.h"
#include "storage/storage_manager.h"
#include "storage/table/chunked_node_group.h"
#include "storage/table/column_chunk.h"
#include "storage/table/column_chunk_data.h"
#include "storage/table/column_chunk_metadata.h"
#include "storage/table/node_group.h"
#include "storage/table/node_table.h"
#include "storage/table/rel_table.h"
#include "storage/table/rel_table_data.h"
#include "transaction/transaction.h"

#include <cstring>

namespace lbug {
namespace turbograph_extension {

using namespace common;
using namespace storage;

// --- MemoryReader: reads sequentially from a contiguous byte buffer ---

class MemoryReader : public Reader {
public:
    MemoryReader(const uint8_t* data, size_t size) : data_(data), size_(size) {}
    void read(uint8_t* dst, uint64_t size) override {
        if (offset_ + size > size_) {
            throw std::runtime_error("MemoryReader: read past end");
        }
        std::memcpy(dst, data_ + offset_, size);
        offset_ += size;
    }
    bool finished() override { return offset_ >= size_; }
private:
    const uint8_t* data_;
    size_t size_;
    size_t offset_ = 0;
};

// --- Lightweight skip/parse functions for the deserialization tree ---
// These read through the binary stream using Kuzu's Deserializer, extracting
// PageRanges from ColumnChunkMetadata entries without creating Table objects.

// Forward declarations.
static void parseColumnChunkData(Deserializer& deSer, uint32_t tableId,
    bool isRel, tiered::TablePageMap& map);
static void parseColumnChunk(Deserializer& deSer, uint32_t tableId,
    bool isRel, tiered::TablePageMap& map);

// Skip a VersionInfo entry. Format:
//   uint64 numVectors, then per-vector: bool hasInfo, if true: VectorVersionInfo.
static void skipVersionInfo(Deserializer& deSer) {
    uint64_t numVectors = 0;
    deSer.deserializeValue(numVectors);
    for (uint64_t i = 0; i < numVectors; i++) {
        bool hasInfo = false;
        deSer.deserializeValue(hasInfo);
        if (hasInfo) {
            // VectorVersionInfo: insertionStatus(1), deletionStatus(1),
            // conditional transaction data.
            uint8_t insertionStatus = 0, deletionStatus = 0;
            deSer.deserializeValue(insertionStatus);
            deSer.deserializeValue(deletionStatus);
            // DeletionStatus: 0=NO_DELETED, 1=CHECK_VERSION
            if (deletionStatus == 1) {
                uint64_t sameDeletionVersion = 0;
                deSer.deserializeValue(sameDeletionVersion);
                // INVALID_TRANSACTION = UINT64_MAX
                if (sameDeletionVersion == UINT64_MAX) {
                    // Array of DEFAULT_VECTOR_CAPACITY (2048) transaction IDs.
                    constexpr uint64_t DEFAULT_VECTOR_CAPACITY = 2048;
                    std::vector<uint8_t> skipBuf(DEFAULT_VECTOR_CAPACITY * sizeof(uint64_t));
                    deSer.read(skipBuf.data(), skipBuf.size());
                }
            }
        }
    }
}

// Skip a TableStats entry. Format:
//   cardinality(8), vector of ColumnStats (each: bool hasHll, if true: 64 bytes).
static void skipTableStats(Deserializer& deSer) {
    uint64_t cardinality = 0;
    deSer.deserializeValue(cardinality);
    uint64_t numStats = 0;
    deSer.deserializeValue(numStats);
    for (uint64_t i = 0; i < numStats; i++) {
        bool hasHll = false;
        deSer.deserializeValue(hasHll);
        if (hasHll) {
            // HyperLogLog: M=64 bytes.
            uint8_t hllBuf[64];
            deSer.read(hllBuf, 64);
        }
    }
}

// Parse a NullChunkData. Format: ColumnChunkMetadata only (no recursive children).
static void parseNullChunkData(Deserializer& deSer, uint32_t tableId,
    bool isRel, tiered::TablePageMap& map) {
    auto meta = ColumnChunkMetadata::deserialize(deSer);
    if (meta.getStartPageIdx() != INVALID_PAGE_IDX && meta.getNumPages() > 0) {
        map.addInterval(meta.getStartPageIdx(), meta.getNumPages(), tableId, isRel);
    }
}

// Parse a DictionaryChunk. Format: offsetChunk(ColumnChunkData) + stringDataChunk(ColumnChunkData).
static void parseDictionaryChunk(Deserializer& deSer, uint32_t tableId,
    bool isRel, tiered::TablePageMap& map) {
    parseColumnChunkData(deSer, tableId, isRel, map); // offset chunk
    parseColumnChunkData(deSer, tableId, isRel, map); // string data chunk
}

// Parse a single ColumnChunkData entry. Extracts its PageRange and recurses
// into type-specific children (STRUCT, STRING, LIST/ARRAY).
static void parseColumnChunkData(Deserializer& deSer, uint32_t tableId,
    bool isRel, tiered::TablePageMap& map) {
    // 1. LogicalType (variable length, type-dependent extra info).
    auto type = LogicalType::deserialize(deSer);

    // 2. ColumnChunkMetadata -- this is what we want!
    auto meta = ColumnChunkMetadata::deserialize(deSer);
    if (meta.getStartPageIdx() != INVALID_PAGE_IDX && meta.getNumPages() > 0) {
        map.addInterval(meta.getStartPageIdx(), meta.getNumPages(), tableId, isRel);
    }

    // 3. enableCompression (bool).
    bool enableCompression = false;
    deSer.deserializeValue(enableCompression);

    // 4. hasNull (bool).
    bool hasNull = false;
    deSer.deserializeValue(hasNull);
    if (hasNull) {
        parseNullChunkData(deSer, tableId, isRel, map);
    }

    // 5. Type-specific children.
    auto physType = type.getPhysicalType();
    if (physType == PhysicalTypeID::STRUCT) {
        // Struct: vector of child ColumnChunkData.
        uint64_t numChildren = 0;
        deSer.deserializeValue(numChildren);
        for (uint64_t c = 0; c < numChildren; c++) {
            parseColumnChunkData(deSer, tableId, isRel, map);
        }
    } else if (physType == PhysicalTypeID::STRING) {
        // String: index column chunk + dictionary chunk (2 more ColumnChunkData).
        parseColumnChunkData(deSer, tableId, isRel, map); // index
        parseDictionaryChunk(deSer, tableId, isRel, map); // dict = offset + data
    } else if (physType == PhysicalTypeID::LIST || physType == PhysicalTypeID::ARRAY) {
        // List/Array: size + data + offset column chunks.
        parseColumnChunkData(deSer, tableId, isRel, map); // size
        parseColumnChunkData(deSer, tableId, isRel, map); // data
        parseColumnChunkData(deSer, tableId, isRel, map); // offset
    }
}

// Parse a ColumnChunk. Format: bool enableCompression, uint64 numSegments, then segments.
static void parseColumnChunk(Deserializer& deSer, uint32_t tableId,
    bool isRel, tiered::TablePageMap& map) {
    bool enableCompression = false;
    deSer.deserializeValue(enableCompression);
    uint64_t numSegments = 0;
    deSer.deserializeValue(numSegments);
    for (uint64_t s = 0; s < numSegments; s++) {
        parseColumnChunkData(deSer, tableId, isRel, map);
    }
}

// Parse a ChunkedNodeGroup. Format: vector of ColumnChunks, startRowIdx, versionInfo.
static void parseChunkedNodeGroup(Deserializer& deSer, uint32_t tableId,
    bool isRel, tiered::TablePageMap& map) {
    // Column chunks vector.
    uint64_t numChunks = 0;
    deSer.deserializeValue(numChunks);
    for (uint64_t c = 0; c < numChunks; c++) {
        parseColumnChunk(deSer, tableId, isRel, map);
    }
    // startRowIdx.
    uint64_t startRowIdx = 0;
    deSer.deserializeValue(startRowIdx);
    // hasVersionInfo.
    bool hasVersionInfo = false;
    deSer.deserializeValue(hasVersionInfo);
    if (hasVersionInfo) {
        skipVersionInfo(deSer);
    }
}

// Parse a ChunkedCSRNodeGroup. Same as ChunkedNodeGroup but with 2 extra header chunks.
static void parseChunkedCSRNodeGroup(Deserializer& deSer, uint32_t tableId,
    bool isRel, tiered::TablePageMap& map) {
    // CSR header: offset column chunk + length column chunk.
    parseColumnChunk(deSer, tableId, isRel, map); // offset
    parseColumnChunk(deSer, tableId, isRel, map); // length
    // Then same as ChunkedNodeGroup.
    parseChunkedNodeGroup(deSer, tableId, isRel, map);
}

// Parse a NodeGroup. Format: idx, enableCompression, format, hasCheckpointedData, then data.
static void parseNodeGroup(Deserializer& deSer, uint32_t tableId,
    bool isRel, tiered::TablePageMap& map) {
    uint64_t nodeGroupIdx = 0;
    deSer.deserializeValue(nodeGroupIdx);
    bool enableCompression = false;
    deSer.deserializeValue(enableCompression);
    uint8_t format = 0; // NodeGroupDataFormat: 0=REGULAR, 1=CSR.
    deSer.deserializeValue(format);
    bool hasCheckpointedData = false;
    deSer.deserializeValue(hasCheckpointedData);

    if (hasCheckpointedData) {
        if (format == 0) { // REGULAR
            parseChunkedNodeGroup(deSer, tableId, isRel, map);
        } else { // CSR
            parseChunkedCSRNodeGroup(deSer, tableId, isRel, map);
        }
    }
}

// Parse a NodeGroupCollection. Format: vector of NodeGroups + TableStats.
static void parseNodeGroupCollection(Deserializer& deSer, uint32_t tableId,
    bool isRel, tiered::TablePageMap& map) {
    // NodeGroups vector.
    uint64_t numGroups = 0;
    deSer.deserializeValue(numGroups);
    for (uint64_t g = 0; g < numGroups; g++) {
        parseNodeGroup(deSer, tableId, isRel, map);
    }
    // TableStats.
    skipTableStats(deSer);
}

// Skip an IndexInfo entry. Format: name(str), type(str), tableID(8),
// columns(vector of uint32), keyTypes(vector of uint8), isPrimary(1), isBuiltin(1).
static void skipIndexInfo(Deserializer& deSer) {
    std::string name, indexType;
    deSer.deserializeValue(name);
    deSer.deserializeValue(indexType);
    uint64_t tableID = 0;
    deSer.deserializeValue(tableID);
    uint64_t numCols = 0;
    deSer.deserializeValue(numCols);
    for (uint64_t i = 0; i < numCols; i++) {
        uint32_t colID = 0;
        deSer.deserializeValue(colID);
    }
    uint64_t numKeyTypes = 0;
    deSer.deserializeValue(numKeyTypes);
    for (uint64_t i = 0; i < numKeyTypes; i++) {
        uint8_t keyType = 0;
        deSer.deserializeValue(keyType);
    }
    bool isPrimary = false, isBuiltin = false;
    deSer.deserializeValue(isPrimary);
    deSer.deserializeValue(isBuiltin);
}

// Parse a NodeTable. Format: NodeGroupCollection + indexes.
static void parseNodeTable(Deserializer& deSer, uint32_t tableId,
    tiered::TablePageMap& map) {
    parseNodeGroupCollection(deSer, tableId, /*isRel=*/false, map);
    // Indexes.
    uint64_t numIndexes = 0;
    deSer.deserializeValue(numIndexes);
    for (uint64_t i = 0; i < numIndexes; i++) {
        skipIndexInfo(deSer);
        // Storage info buffer (opaque blob).
        uint64_t storageInfoSize = 0;
        deSer.deserializeValue(storageInfoSize);
        if (storageInfoSize > 0) {
            std::vector<uint8_t> buf(storageInfoSize);
            deSer.read(buf.data(), storageInfoSize);
        }
    }
}

// Skip a RelTableCatalogInfo. Format: srcTableID(8), dstTableID(8), oid(8).
static void skipRelTableCatalogInfo(Deserializer& deSer, uint64_t& oidOut) {
    uint64_t srcTableID = 0, dstTableID = 0;
    deSer.deserializeValue(srcTableID);
    deSer.deserializeValue(dstTableID);
    deSer.deserializeValue(oidOut);
}

// Parse a RelTable. Format: nextRelOffset(8), then 2 directions of RelTableData.
static void parseRelTable(Deserializer& deSer, uint32_t tableId,
    tiered::TablePageMap& map) {
    uint64_t nextRelOffset = 0;
    deSer.deserializeValue(nextRelOffset);
    // Always 2 directions: FWD + BWD.
    for (int dir = 0; dir < 2; dir++) {
        // RelTableData::deserialize just calls NodeGroupCollection::deserialize.
        parseNodeGroupCollection(deSer, tableId, /*isRel=*/true, map);
    }
}

// --- Public API ---

// Parse raw metadata pages into a TablePageMap.
// Called from openFile() via the MetadataParserFn callback.
std::unique_ptr<tiered::TablePageMap> parseMetadataPages(
    const uint8_t* data, size_t len) {
    try {
        auto reader = std::make_unique<MemoryReader>(data, len);
        Deserializer deSer(std::move(reader));

        auto map = std::make_unique<tiered::TablePageMap>();

        // StorageManager format:
        //   num_node_tables(8), then per-table: table_id(8) + NodeTable data
        //   num_rel_groups(8), then per-group: rel_group_id(8) +
        //     num_inner_rel_tables(8) + per-inner: RelTableCatalogInfo + RelTable data
        uint64_t numNodeTables = 0;
        deSer.deserializeValue(numNodeTables);
        for (uint64_t i = 0; i < numNodeTables; i++) {
            uint64_t tableId = 0;
            deSer.deserializeValue(tableId);
            parseNodeTable(deSer, static_cast<uint32_t>(tableId), *map);
        }

        uint64_t numRelGroups = 0;
        deSer.deserializeValue(numRelGroups);
        for (uint64_t i = 0; i < numRelGroups; i++) {
            uint64_t relGroupId = 0;
            deSer.deserializeValue(relGroupId);
            uint64_t numInner = 0;
            deSer.deserializeValue(numInner);
            for (uint64_t k = 0; k < numInner; k++) {
                uint64_t oid = 0;
                skipRelTableCatalogInfo(deSer, oid);
                parseRelTable(deSer, static_cast<uint32_t>(oid), *map);
            }
        }

        return map;
    } catch (const std::exception&) {
        // Parse failure (corrupt metadata or format mismatch).
        // Return null to fall back to global scheduling.
        return nullptr;
    }
}

// Post-construction builder (used by UDF for manual rebuild).
// Walks StorageManager's in-memory tables via Kuzu APIs.
std::unique_ptr<tiered::TablePageMap> buildTablePageMap(main::Database* db) {
    if (!db) return nullptr;
    auto* sm = db->getStorageManager();
    auto* catalog = db->getCatalog();
    if (!sm || !catalog) return nullptr;

    auto map = std::make_unique<tiered::TablePageMap>();

    // Helper: walk a ChunkedNodeGroup's columns and add ON_DISK PageRanges.
    auto collectChunkedGroup = [&](const ChunkedNodeGroup* chunked,
        uint32_t tableId, bool isRel) {
        auto numCols = chunked->getNumColumns();
        for (idx_t col = 0; col < numCols; col++) {
            const auto& chunk = chunked->getColumnChunk(static_cast<column_id_t>(col));
            auto segments = chunk.getSegments();
            for (const auto* seg : segments) {
                if (seg->getResidencyState() != ResidencyState::ON_DISK) continue;
                const auto& meta = seg->getMetadata();
                if (meta.getStartPageIdx() != INVALID_PAGE_IDX && meta.getNumPages() > 0) {
                    map->addInterval(meta.getStartPageIdx(), meta.getNumPages(), tableId, isRel);
                }
            }
        }
    };

    // Helper: walk node groups.
    auto collectNodeGroups = [&](
        const std::function<node_group_idx_t()>& getNum,
        const std::function<NodeGroup*(node_group_idx_t)>& getGroup,
        uint32_t tableId, bool isRel) {
        for (node_group_idx_t g = 0; g < getNum(); g++) {
            auto* ng = getGroup(g);
            if (!ng) continue;
            for (node_group_idx_t c = 0; c < ng->getNumChunkedGroups(); c++) {
                auto* chunked = ng->getChunkedNodeGroup(c);
                if (chunked) collectChunkedGroup(chunked, tableId, isRel);
            }
        }
    };

    // Node tables.
    auto nodeEntries = catalog->getNodeTableEntries(&transaction::DUMMY_TRANSACTION);
    for (auto* entry : nodeEntries) {
        auto tableId = entry->getTableID();
        auto* table = sm->getTable(tableId);
        if (!table) continue;
        auto* nt = dynamic_cast<NodeTable*>(table);
        if (!nt) continue;
        collectNodeGroups(
            [nt]() { return nt->getNumNodeGroups(); },
            [nt](node_group_idx_t i) { return nt->getNodeGroup(i); },
            tableId, false);
    }

    // Rel tables.
    auto relEntries = catalog->getRelGroupEntries(&transaction::DUMMY_TRANSACTION);
    for (auto* rge : relEntries) {
        for (auto& info : rge->getRelEntryInfos()) {
            auto* table = sm->getTable(info.oid);
            if (!table) continue;
            auto* rt = dynamic_cast<RelTable*>(table);
            if (!rt) continue;
            for (auto dir : {RelDataDirection::FWD, RelDataDirection::BWD}) {
                auto* rtd = rt->getDirectedTableData(dir);
                if (!rtd) continue;
                collectNodeGroups(
                    [rtd]() { return rtd->getNumNodeGroups(); },
                    [rtd](node_group_idx_t i) { return rtd->getNodeGroup(i); },
                    info.oid, true);
            }
        }
    }

    return map;
}

} // namespace turbograph_extension
} // namespace lbug
