// Phase Cypher: Query plan frontrunning.
//
// Before executing a Cypher query, prepare it to get the logical plan,
// walk the plan tree to find which tables will be accessed, and
// proactively prefetch all their page groups from S3.

#include "main/turbograph_extension.h"
#include "main/turbograph_functions.h"
#include "table_page_map.h"
#include "tiered_file_system.h"

#include "main/client_context.h"
#include "main/connection.h"
#include "main/prepared_statement.h"
#include "main/prepared_statement_manager.h"
#include "planner/operator/logical_operator.h"
#include "planner/operator/logical_plan.h"
#include "planner/operator/scan/logical_scan_node_table.h"
#include "planner/operator/extend/base_logical_extend.h"
#include "planner/operator/extend/logical_extend.h"

#include <unordered_set>

namespace lbug {
namespace turbograph_extension {

using namespace planner;
using namespace common;

// Walk a logical operator tree and collect all table IDs referenced
// by SCAN_NODE_TABLE and EXTEND operators.
static void collectTableIds(const LogicalOperator* op,
    std::unordered_set<table_id_t>& nodeTableIds,
    std::unordered_set<table_id_t>& relTableIds) {
    if (!op) return;

    auto type = op->getOperatorType();

    if (type == LogicalOperatorType::SCAN_NODE_TABLE) {
        auto& scan = op->constCast<LogicalScanNodeTable>();
        for (auto id : scan.getTableIDs()) {
            nodeTableIds.insert(id);
        }
    } else if (type == LogicalOperatorType::EXTEND ||
               type == LogicalOperatorType::RECURSIVE_EXTEND) {
        auto& extend = op->constCast<BaseLogicalExtend>();
        auto rel = extend.getRel();
        if (rel) {
            for (auto id : rel->getTableIDs()) {
                relTableIds.insert(id);
            }
        }
        // Also collect the neighbor node table.
        auto nbrNode = extend.getNbrNode();
        if (nbrNode) {
            for (auto id : nbrNode->getTableIDs()) {
                nodeTableIds.insert(id);
            }
        }
    }

    for (uint32_t i = 0; i < op->getNumChildren(); i++) {
        collectTableIds(op->getChild(i).get(), nodeTableIds, relTableIds);
    }
}

std::pair<std::unordered_set<table_id_t>, std::unordered_set<table_id_t>>
extractTablesFromPlan(main::Connection& conn, const std::string& cypher) {
    std::unordered_set<table_id_t> nodeTableIds, relTableIds;

    auto stmt = conn.prepare(cypher);
    if (!stmt || !stmt->isSuccess()) {
        return {nodeTableIds, relTableIds};
    }

    auto* ctx = conn.getClientContext();
    auto& mgr = ctx->getCachedPreparedStatementManager();
    auto* cached = mgr.getCachedStatement(stmt->getName());
    if (!cached || !cached->logicalPlan) {
        return {nodeTableIds, relTableIds};
    }

    auto* root = cached->logicalPlan->getLastOperator().get();
    collectTableIds(root, nodeTableIds, relTableIds);

    return {nodeTableIds, relTableIds};
}

} // namespace turbograph_extension
} // namespace lbug
