/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "base/ddl_parser.h"

#include <algorithm>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "codec/schema_codec.h"
#include "common/timer.h"
#include "node/node_manager.h"
#include "plan/plan_api.h"
#include "proto/common.pb.h"
#include "proto/fe_type.pb.h"
#include "sdk/base_impl.h"
#include "sdk/sql_insert_row.h"
#include "vm/engine.h"
#include "vm/physical_op.h"
#include "vm/simple_catalog.h"

namespace openmldb::base {

using hybridse::vm::Catalog;
using hybridse::vm::DataProviderType;
using hybridse::vm::Filter;
using hybridse::vm::Join;
using hybridse::vm::Key;
using hybridse::vm::PhysicalOpNode;
using hybridse::vm::PhysicalOpType;
using hybridse::vm::SchemasContext;
using hybridse::vm::Sort;

constexpr const char* DB_NAME = "ddl_parser_single_db";

// Ref hybridse/src/passes/physical/group_and_sort_optimized.cc:651
// TODO(hw): hybridse should open this method
bool ResolveColumnToSourceColumnName(const hybridse::node::ColumnRefNode* col, const SchemasContext* schemas_ctx,
                                     std::string* source_name);

class IndexMapBuilder {
 public:
    IndexMapBuilder() = default;
    // Create the index with unset TTLSt, return false if the index(same table, same keys, same ts) existed
    bool CreateIndex(const std::string& table, const hybridse::node::ExprListNode* keys,
                     const hybridse::node::OrderByNode* ts, const SchemasContext* ctx);
    bool UpdateIndex(const hybridse::vm::Range& range);
    // After ToMap, inner data will be cleared
    IndexMap ToMap();

 private:
    static std::vector<std::string> NormalizeColumns(const std::string& table,
                                                     const std::vector<hybridse::node::ExprNode*>& nodes,
                                                     const SchemasContext* ctx);
    // table, keys and ts -> table:key1,key2,...;ts
    static std::string Encode(const std::string& table, const hybridse::node::ExprListNode* keys,
                              const hybridse::node::OrderByNode* ts, const SchemasContext* ctx);

    static std::pair<std::string, common::ColumnKey> Decode(const std::string& index_str);

    static std::string GetTsCol(const std::string& index_str) {
        std::size_t ts_mark_pos = index_str.find(TS_MARK);
        if (ts_mark_pos == std::string::npos) {
            return {};
        }
        auto ts_begin = ts_mark_pos + 1;
        return index_str.substr(ts_begin);
    }

    static std::string GetTable(const std::string& index_str) {
        auto key_sep = index_str.find(KEY_MARK);
        if (key_sep == std::string::npos) {
            return {};
        }
        return index_str.substr(0, key_sep);
    }

 private:
    static constexpr char KEY_MARK = ':';
    static constexpr char KEY_SEP = ',';
    static constexpr char TS_MARK = ';';

    std::string latest_record_;
    // map<table_keys_and_order_str, ttl_st>
    std::map<std::string, common::TTLSt*> index_map_;
};

// no plan_ctx_, node_manager_: we assume that creating new op won't affect the upper level structure.
class GroupAndSortOptimizedParser {
 public:
    GroupAndSortOptimizedParser() = default;

    // LRD
    void Parse(PhysicalOpNode* cur_op) {
        if (!cur_op) {
            LOG(DFATAL) << "parse nullptr";
            return;
        }

        // just parse, won't modify, but need to cast ptr, so we use non-const producers.
        auto& producers = cur_op->producers();
        for (auto& producer : producers) {
            Parse(producer);
        }

        DLOG(INFO) << "parse " << hybridse::vm::PhysicalOpTypeName(cur_op->GetOpType());
        TransformParse(cur_op);
    }

    IndexMap GetIndexes() { return index_map_builder_.ToMap(); }

 private:
    // recursive parse, return true iff kProviderTypeTable optimized
    // new_in is useless, but we keep it, GroupAndSortOptimizedParser will be more similar to GroupAndSortOptimized.
    bool KeysOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* left_key, Key* index_key,
                            Key* right_key, Sort* sort, PhysicalOpNode** new_in);

    bool KeysAndOrderFilterOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
                                          Key* hash, Sort* sort, PhysicalOpNode** new_in) {
        return KeysOptimizedParse(root_schemas_ctx, in, group, hash, nullptr, sort, new_in);
    }

    bool JoinKeysOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Join* join,
                                PhysicalOpNode** new_in) {
        if (nullptr == join) {
            return false;
        }
        return FilterAndOrderOptimizedParse(root_schemas_ctx, in, join, &join->right_sort_, new_in);
    }
    bool FilterAndOrderOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Filter* filter,
                                      Sort* sort, PhysicalOpNode** new_in) {
        return KeysOptimizedParse(root_schemas_ctx, in, &filter->left_key_, &filter->index_key_, &filter->right_key_,
                                  sort, new_in);
    }
    bool FilterOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Filter* filter,
                              PhysicalOpNode** new_in) {
        return FilterAndOrderOptimizedParse(root_schemas_ctx, in, filter, nullptr, new_in);
    }
    bool KeyAndOrderOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group, Sort* sort,
                                   PhysicalOpNode** new_in) {
        Key mock_key;
        return KeysAndOrderFilterOptimizedParse(root_schemas_ctx, in, group, &mock_key, sort, new_in);
    }
    bool GroupOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in, Key* group,
                             PhysicalOpNode** new_in) {
        return KeyAndOrderOptimizedParse(root_schemas_ctx, in, group, nullptr, new_in);
    }

    static std::vector<PhysicalOpNode*> InitJoinList(hybridse::vm::PhysicalWindowAggrerationNode* op);

    void TransformParse(PhysicalOpNode* in);

 private:
    IndexMapBuilder index_map_builder_;
};

IndexMap DDLParser::ExtractIndexes(const std::string& sql, const ::hybridse::type::Database& db) {
    hybridse::vm::MockRequestRunSession session;
    return ExtractIndexes(sql, db, &session);
}

IndexMap DDLParser::ExtractIndexes(
    const std::string& sql,
    const std::map<std::string, ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>>& schemas) {
    ::hybridse::type::Database db;
    db.set_name(DB_NAME);
    AddTables(schemas, &db);
    return ExtractIndexes(sql, db);
}

IndexMap DDLParser::ExtractIndexes(const std::string& sql,
                                   const std::map<std::string, std::vector<::openmldb::common::ColumnDesc>>& schemas) {
    ::hybridse::type::Database db;
    db.set_name(DB_NAME);
    AddTables(schemas, &db);
    return ExtractIndexes(sql, db);
}

IndexMap DDLParser::ExtractIndexesForBatch(const std::string& sql, const ::hybridse::type::Database& db) {
    hybridse::vm::BatchRunSession session;
    return ExtractIndexes(sql, db, &session);
}

std::string DDLParser::Explain(const std::string& sql, const ::hybridse::type::Database& db) {
    hybridse::vm::MockRequestRunSession session;
    if (!GetPlan(sql, db, &session)) {
        LOG(ERROR) << "sql get plan failed";
        return {};
    }
    std::ostringstream plan_oss;
    session.GetCompileInfo()->DumpPhysicalPlan(plan_oss, "\t");
    return plan_oss.str();
}

hybridse::sdk::Status DDLParser::ExtractLongWindowInfos(const std::string& sql,
                                                        const std::unordered_map<std::string, std::string>& window_map,
                                                        LongWindowInfos* infos) {
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);

    if (0 != sql_status.code) {
        DLOG(ERROR) << sql_status.msg;
        return hybridse::sdk::Status(::hybridse::common::StatusCode::kSyntaxError, sql_status.msg,
                                     sql_status.GetTraces());
    }

    hybridse::node::PlanNode* node = plan_trees[0];
    switch (node->GetType()) {
        case hybridse::node::kPlanTypeQuery: {
            // TODO(ace): Traverse Node return Status
            if (!TraverseNode(node, window_map, infos)) {
                return hybridse::sdk::Status(::hybridse::common::StatusCode::kUnsupportPlan, "TraverseNode failed");
            }
            break;
        }
        default: {
            DLOG(ERROR) << "only support extract long window infos from query";
            return hybridse::sdk::Status(::hybridse::common::StatusCode::kUnsupportPlan,
                                         "only support extract long window infos from query");
        }
    }

    return {};
}

bool DDLParser::TraverseNode(hybridse::node::PlanNode* node,
                             const std::unordered_map<std::string, std::string>& window_map,
                             LongWindowInfos* long_window_infos) {
    switch (node->GetType()) {
        case hybridse::node::kPlanTypeProject: {
            hybridse::node::ProjectPlanNode* project_plan_node = dynamic_cast<hybridse::node::ProjectPlanNode*>(node);
            if (!ExtractInfosFromProjectPlan(project_plan_node, window_map, long_window_infos)) {
                return false;
            }
            break;
        }
        default: {
            for (int i = 0; i < node->GetChildrenSize(); ++i) {
                if (!TraverseNode(node->GetChildren()[i], window_map, long_window_infos)) {
                    return false;
                }
            }
        }
    }
    return true;
}
bool DDLParser::ExtractInfosFromProjectPlan(hybridse::node::ProjectPlanNode* project_plan_node,
                                            const std::unordered_map<std::string, std::string>& window_map,
                                            LongWindowInfos* long_window_infos) {
    for (const auto& project_list : project_plan_node->project_list_vec_) {
        if (project_list->GetType() != hybridse::node::kProjectList) {
            DLOG(ERROR) << "extract long window infos from project list failed";
            return false;
        }
        hybridse::node::ProjectListNode* project_list_node =
            dynamic_cast<hybridse::node::ProjectListNode*>(project_list);
        auto window = project_list_node->GetW();
        if (window == nullptr) {
            continue;
        }

        int partition_num = window->GetKeys()->GetChildNum();
        std::string partition_col;
        for (int i = 0; i < partition_num; i++) {
            auto partition_key = window->GetKeys()->GetChild(i);
            if (partition_key->GetExprType() != hybridse::node::kExprColumnRef) {
                DLOG(ERROR) << "extract long window infos from window partition key failed";
                return false;
            }
            hybridse::node::ColumnRefNode* column_node = dynamic_cast<hybridse::node::ColumnRefNode*>(partition_key);
            partition_col += column_node->GetColumnName() + ",";
        }
        if (!partition_col.empty()) {
            partition_col.pop_back();
        }

        std::string order_by_col;
        auto order_exprs = window->GetOrders()->order_expressions();
        for (uint32_t i = 0; i < order_exprs->GetChildNum(); i++) {
            auto order_expr = order_exprs->GetChild(i);
            if (order_expr->GetExprType() != hybridse::node::kExprOrderExpression) {
                DLOG(ERROR) << "extract long window infos from window order by failed";
                return false;
            }
            auto order_node = dynamic_cast<hybridse::node::OrderExpression*>(order_expr);
            auto order_col_node = order_node->expr();
            if (order_col_node->GetExprType() != hybridse::node::kExprColumnRef) {
                DLOG(ERROR) << "extract long window infos from window order by failed";
                return false;
            }
            const hybridse::node::ColumnRefNode* column_node =
                reinterpret_cast<const hybridse::node::ColumnRefNode*>(order_col_node);
            order_by_col += column_node->GetColumnName() + ",";
        }
        if (!order_by_col.empty()) {
            order_by_col.pop_back();
        }

        for (const auto& project : project_list_node->GetProjects()) {
            if (project->GetType() != hybridse::node::kProjectNode) {
                DLOG(ERROR) << "extract long window infos from project failed";
                return false;
            }
            hybridse::node::ProjectNode* project_node = dynamic_cast<hybridse::node::ProjectNode*>(project);
            if (!project_node->IsAgg()) {
                continue;
            }
            auto project_expr = project_node->GetExpression();
            if (project_expr->GetExprType() != hybridse::node::kExprCall) {
                DLOG(ERROR) << "extract long window infos from agg func failed";
                return false;
            }
            hybridse::node::CallExprNode* agg_expr = dynamic_cast<hybridse::node::CallExprNode*>(project_expr);
            auto window_name = agg_expr->GetOver() ? agg_expr->GetOver()->GetName() : "";
            // skip if window isn't long window
            if (window_map.find(window_name) == window_map.end()) {
                continue;
            }
            std::string aggr_name = agg_expr->GetFnDef()->GetName();
            std::string aggr_col;
            if (agg_expr->GetChildNum() > 2 || agg_expr->GetChildNum() <= 0) {
                DLOG(ERROR) << "only support single aggr column and an optional filter condition";
                return false;
            }
            aggr_col += agg_expr->GetChild(0)->GetExprString();

            // extract filter column from condition expr
            std::string filter_col;
            if (agg_expr->GetChildNum() == 2) {
                auto cond_expr = agg_expr->GetChild(1);
                if (cond_expr->GetExprType() != hybridse::node::kExprBinary) {
                    DLOG(ERROR) << "long window only support binary expr on single column";
                    return false;
                }
                auto left = cond_expr->GetChild(0);
                auto right = cond_expr->GetChild(1);
                if (left->GetExprType() == hybridse::node::kExprColumnRef) {
                    filter_col = dynamic_cast<const hybridse::node::ColumnRefNode*>(left)->GetColumnName();
                    if (right->GetExprType() != hybridse::node::kExprPrimary) {
                        DLOG(ERROR) << "the other node should be ConstNode";
                        return false;
                    }
                } else if (right->GetExprType() == hybridse::node::kExprColumnRef) {
                    filter_col = dynamic_cast<const hybridse::node::ColumnRefNode*>(right)->GetColumnName();
                    if (left->GetExprType() != hybridse::node::kExprPrimary) {
                        DLOG(ERROR) << "the other node should be ConstNode";
                        return false;
                    }
                } else {
                    DLOG(ERROR) << "get filter_col failed";
                    return false;
                }
            }

            (*long_window_infos)
                .emplace_back(window_name, aggr_name, aggr_col, partition_col, order_by_col,
                              window_map.at(window_name));
            if (!filter_col.empty()) {
                (*long_window_infos).back().filter_col_ = filter_col;
            }
        }
    }
    return true;
}

IndexMap DDLParser::ExtractIndexes(const std::string& sql, const hybridse::type::Database& db,
                                   hybridse::vm::RunSession* session) {
    // To show index-based-optimization -> IndexSupport() == true -> whether to do LeftJoinOptimized
    // tablet catalog supports index, so we should add index support too
    auto cp = db;
    cp.set_name(DB_NAME);
    if (!GetPlan(sql, cp, session)) {
        LOG(ERROR) << "sql get plan failed";
        return {};
    }
    auto compile_info = session->GetCompileInfo();
    auto plan = session->GetCompileInfo()->GetPhysicalPlan();
    return ParseIndexes(const_cast<hybridse::vm::PhysicalOpNode*>(plan));
}

std::shared_ptr<hybridse::sdk::Schema> DDLParser::GetOutputSchema(const std::string& sql,
                                                                  const hybridse::type::Database& db) {
    hybridse::vm::MockRequestRunSession session;
    if (!GetPlan(sql, db, &session)) {
        LOG(ERROR) << "sql get plan failed";
        return {};
    }
    auto output_schema_ptr = session.GetCompileInfo()->GetPhysicalPlan()->GetOutputSchema();
    return std::make_shared<hybridse::sdk::SchemaImpl>(*output_schema_ptr);
}

std::shared_ptr<hybridse::sdk::Schema> DDLParser::GetOutputSchema(
    const std::string& sql, const std::map<std::string, std::vector<::openmldb::common::ColumnDesc>>& schemas) {
    ::hybridse::type::Database db;
    db.set_name(DB_NAME);
    AddTables(schemas, &db);
    return GetOutputSchema(sql, db);
}

IndexMap DDLParser::ParseIndexes(hybridse::vm::PhysicalOpNode* node) {
    // This physical plan is optimized, but no real optimization about index(cuz no index in fake catalog).
    // So we can run GroupAndSortOptimizedParser on the plan(very like transformer's pass-ApplyPasses)
    GroupAndSortOptimizedParser parser;
    parser.Parse(node);
    return parser.GetIndexes();
}

bool DDLParser::GetPlan(const std::string& sql, const hybridse::type::Database& db, hybridse::vm::RunSession* session) {
    auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
    catalog->AddDatabase(db);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::hybridse::vm::EngineOptions options;
    options.SetKeepIr(true);
    options.SetCompileOnly(true);
    auto engine = std::make_shared<hybridse::vm::Engine>(catalog, options);

    // TODO(hw): ok and status may not be consistent? why engine always use '!ok || 0 != status.code'?
    ::hybridse::base::Status status;
    auto ok = engine->Get(sql, db.name(), *session, status);
    if (!(ok && status.isOK())) {
        LOG(WARNING) << "hybrid engine compile sql failed, " << status.str();
        return false;
    }
    return true;
}

bool DDLParser::GetPlan(const std::string& sql, const hybridse::type::Database& db, hybridse::vm::RunSession* session,
                        hybridse::base::Status* status) {
    auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
    catalog->AddDatabase(db);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::hybridse::vm::EngineOptions options;
    options.SetKeepIr(true);
    options.SetCompileOnly(true);
    auto engine = std::make_shared<hybridse::vm::Engine>(catalog, options);
    auto ok = engine->Get(sql, db.name(), *session, *status);
    if (!(ok && status->isOK())) {
        LOG(WARNING) << "hybrid engine compile sql failed, " << status->str();
        return false;
    }
    return true;
}

template <typename T>
void DDLParser::AddTables(const T& schema, hybridse::type::Database* db) {
    for (auto& table : schema) {
        // add to database
        auto def = db->add_tables();
        def->set_name(table.first);
        auto& cols = table.second;
        for (auto& col : cols) {
            auto add = def->add_columns();
            add->set_name(col.name());
            add->set_type(codec::SchemaCodec::ConvertType(col.data_type()));
        }
    }
}

std::vector<std::string> DDLParser::ValidateSQLInBatch(const std::string& sql, const hybridse::type::Database& db) {
    hybridse::vm::BatchRunSession session;
    hybridse::base::Status status;
    auto ok = GetPlan(sql, db, &session, &status);
    if (!ok || !status.isOK()) {
        return {status.GetMsg(), status.GetTraces()};
    }
    return {};
}

std::vector<std::string> DDLParser::ValidateSQLInBatch(
    const std::string& sql, const std::map<std::string, std::vector<::openmldb::common::ColumnDesc>>& schemas) {
    ::hybridse::type::Database db;
    db.set_name(DB_NAME);
    AddTables(schemas, &db);
    return ValidateSQLInBatch(sql, db);
}

std::vector<std::string> DDLParser::ValidateSQLInRequest(const std::string& sql, const hybridse::type::Database& db) {
    hybridse::vm::MockRequestRunSession session;
    hybridse::base::Status status;
    auto ok = GetPlan(sql, db, &session, &status);
    if (!ok || !status.isOK()) {
        return {status.GetMsg(), status.GetTraces()};
    }
    return {};
}

std::vector<std::string> DDLParser::ValidateSQLInRequest(
    const std::string& sql, const std::map<std::string, std::vector<::openmldb::common::ColumnDesc>>& schemas) {
    ::hybridse::type::Database db;
    db.set_name(DB_NAME);
    AddTables(schemas, &db);
    return ValidateSQLInRequest(sql, db);
}

bool IndexMapBuilder::CreateIndex(const std::string& table, const hybridse::node::ExprListNode* keys,
                                  const hybridse::node::OrderByNode* ts, const SchemasContext* ctx) {
    // we encode table, keys and ts to one string
    auto index = Encode(table, keys, ts, ctx);
    if (index.empty()) {
        LOG(WARNING) << "index encode failed for table " << table;
        return false;
    }

    if (index_map_.find(index) != index_map_.end()) {
        // TODO(hw): ttl merge
        LOG(DFATAL) << "index existed in cache, can't handle it now";
        return false;
    }
    DLOG(INFO) << "create index with unset ttl: " << index;

    // default TTLSt is abs and ttl=0, rows will never expire.
    // default TTLSt debug string is {}, but if we get, they will be the default values.
    index_map_[index] = new common::TTLSt;
    latest_record_ = index;
    return true;
}

int64_t AbsTTLConvert(int64_t time_ms, bool zero_eq_unbounded) {
    if (zero_eq_unbounded && time_ms == 0) {
        return 0;
    }
    return time_ms == 0 ? 1 : (time_ms / 60000 + (time_ms % 60000 ? 1 : 0));
}

int64_t LatTTLConvert(int64_t lat, bool zero_eq_unbounded) {
    if (zero_eq_unbounded && lat == 0) {
        return 0;
    }

    return lat == 0 ? 1 : lat;
}

// history_range_start == INT64_MIN: unbounded
// history_range_start == 0: not unbounded
// NOTICE: do not convert invalid range/rows start. It'll get 0 from `GetHistoryRangeStart`.
int64_t AbsTTLConvert(int64_t history_range_start) {
    return history_range_start == INT64_MIN ? 0 : AbsTTLConvert(-1 * history_range_start, false);
}
int64_t LatTTLConvert(int64_t history_rows_start) {
    return history_rows_start == INT64_MIN ? 0 : LatTTLConvert(-1 * history_rows_start, false);
}

bool IndexMapBuilder::UpdateIndex(const hybridse::vm::Range& range) {
    if (latest_record_.empty() || index_map_.find(latest_record_) == index_map_.end()) {
        LOG(DFATAL) << "want to update ttl status, but index is not created before";
        return false;
    }
    // TODO(hw): it's better to check the ts col name
    //  but range's column names may be renamed, needs schema context

    if (!range.Valid()) {
        DLOG(INFO) << "range is invalid, can't update ttl, still use the default ttl";
        return true;
    }

    std::stringstream ss;
    range.frame()->Print(ss, "");
    DLOG(INFO) << "frame info: " << ss.str();

    auto ttl_st_ptr = index_map_[latest_record_];
    auto frame = range.frame();
    auto type = frame->frame_type();
    switch (type) {
        case hybridse::node::kFrameRows:
            ttl_st_ptr->set_ttl_type(type::TTLType::kLatestTime);
            ttl_st_ptr->set_lat_ttl(LatTTLConvert(frame->GetHistoryRowsStart()));
            break;
        case hybridse::node::kFrameRange:
        case hybridse::node::kFrameRowsRange:
            ttl_st_ptr->set_ttl_type(type::TTLType::kAbsoluteTime);
            ttl_st_ptr->set_abs_ttl(AbsTTLConvert(frame->GetHistoryRangeStart()));
            break;
        case hybridse::node::kFrameRowsMergeRowsRange:
            // use abs and ttl, only >abs_ttl and > lat_ttl will be expired
            ttl_st_ptr->set_ttl_type(type::TTLType::kAbsAndLat);
            ttl_st_ptr->set_abs_ttl(AbsTTLConvert(frame->GetHistoryRangeStart()));
            ttl_st_ptr->set_lat_ttl(LatTTLConvert(frame->GetHistoryRowsStart()));
            break;
        default:
            LOG(WARNING) << "invalid type";
            return false;
    }

    DLOG(INFO) << latest_record_ << " update ttl " << index_map_[latest_record_]->DebugString();
    // to avoid double update
    latest_record_.clear();
    return true;
}

IndexMap IndexMapBuilder::ToMap() {
    IndexMap result;
    for (auto& pair : index_map_) {
        if (!pair.second->has_ttl_type()) {
            pair.second->set_ttl_type(::openmldb::type::TTLType::kLatestTime);
            pair.second->set_lat_ttl(1);
        }
        auto dec = Decode(pair.first);
        // message owns the TTLSt
        dec.second.set_allocated_ttl(pair.second);
        result[dec.first].emplace_back(dec.second);
    }
    // TTLSt is owned by result now, index_map_ can't be reused
    index_map_.clear();
    return result;
}

std::string IndexMapBuilder::Encode(const std::string& table, const hybridse::node::ExprListNode* keys,
                                    const hybridse::node::OrderByNode* ts, const SchemasContext* ctx) {
    // children are ColumnRefNode
    auto cols = NormalizeColumns(table, keys->children_, ctx);
    if (cols.empty()) {
        return {};
    }

    std::stringstream ss;
    ss << table << ":";
    auto iter = cols.begin();
    ss << (*iter);
    iter++;
    for (; iter != cols.end(); iter++) {
        ss << "," << (*iter);
    }
    ss << ";";

    if (ts != nullptr && ts->order_expressions_ != nullptr) {
        for (auto order : ts->order_expressions_->children_) {
            auto cast = dynamic_cast<hybridse::node::OrderExpression*>(order);
            if (cast && cast->expr() != nullptr) {
                auto res = NormalizeColumns(table, {const_cast<hybridse::node::ExprNode*>(cast->expr())}, ctx);
                if (res.size() != 1 || res[0].empty()) {
                    LOG(DFATAL) << "parse ts col from order node failed, skip it. " << cast->GetExprString();
                } else {
                    ss << res[0];
                }
            }
        }
    }
    return ss.str();
}

std::vector<std::string> IndexMapBuilder::NormalizeColumns(const std::string& table,
                                                           const std::vector<hybridse::node::ExprNode*>& nodes,
                                                           const SchemasContext* ctx) {
    if (table.empty() || nodes.empty()) {
        return {};
    }
    std::vector<std::string> result;
    for (auto& node : nodes) {
        if (nullptr != node && node->GetExprType() == hybridse::node::kExprColumnRef) {
            auto cast = hybridse::node::ColumnRefNode::CastFrom(node);
            std::string name;
            if (!ResolveColumnToSourceColumnName(cast, ctx, &name)) {
                return {};
            }
            result.emplace_back(name);
        }
    }
    // sort to avoid dup index
    std::sort(result.begin(), result.end());
    return result;
}

// ColumnKey in result doesn't set ttl
std::pair<std::string, common::ColumnKey> IndexMapBuilder::Decode(const std::string& index_str) {
    if (index_str.empty()) {
        return {};
    }

    auto key_sep = index_str.find(KEY_MARK);
    auto table_name = index_str.substr(0, key_sep);

    common::ColumnKey column_key;
    auto ts_sep = index_str.find(TS_MARK);
    auto keys_str = index_str.substr(key_sep + 1, ts_sep - key_sep - 1);
    // split keys
    std::vector<std::string> keys;
    boost::split(keys, keys_str, boost::is_any_of(std::string(1, KEY_SEP)));
    for (auto& key : keys) {
        DCHECK(!key.empty());
        column_key.add_col_name(key);
    }
    // if no ts hint, do not set. No ts in index is OK
    auto ts_col = GetTsCol(index_str);
    if (!ts_col.empty()) {
        column_key.set_ts_name(ts_col);
    }
    return std::make_pair(table_name, column_key);
}

bool GroupAndSortOptimizedParser::KeysOptimizedParse(const SchemasContext* root_schemas_ctx, PhysicalOpNode* in,
                                                     Key* left_key, Key* index_key, Key* right_key, Sort* sort,
                                                     PhysicalOpNode**) {
    if (nullptr == left_key || nullptr == index_key || !left_key->ValidKey()) {
        return false;
    }

    if (right_key != nullptr && !right_key->ValidKey()) {
        return false;
    }

    if (PhysicalOpType::kPhysicalOpDataProvider == in->GetOpType()) {
        auto scan_op = dynamic_cast<hybridse::vm::PhysicalDataProviderNode*>(in);
        // Do not optimize with Request DataProvider (no index has been provided)
        if (DataProviderType::kProviderTypeRequest == scan_op->provider_type_) {
            return false;
        }

        if (DataProviderType::kProviderTypeTable == scan_op->provider_type_ ||
            DataProviderType::kProviderTypePartition == scan_op->provider_type_) {
            const hybridse::node::ExprListNode* right_partition =
                right_key == nullptr ? left_key->keys() : right_key->keys();

            size_t key_num = right_partition->GetChildNum();
            std::vector<bool> bitmap(key_num, false);
            hybridse::node::ExprListNode order_values;

            if (DataProviderType::kProviderTypeTable == scan_op->provider_type_) {
                // Apply key columns and order column optimization with all indexes binding to
                // scan_op->table_handler_ Return false if fail to find an appropriate index
                auto groups = right_partition;
                auto order = (nullptr == sort ? nullptr : sort->orders_);
                DLOG(INFO) << "keys and order optimized: keys=" << hybridse::node::ExprString(groups)
                           << ", order=" << (order == nullptr ? "null" : hybridse::node::ExprString(order))
                           << " for table " << scan_op->table_handler_->GetName();

                // columns in groups or order, may be renamed
                index_map_builder_.CreateIndex(scan_op->table_handler_->GetName(), groups, order, root_schemas_ctx);
                // parser won't create partition_op
                return true;
            } else {
                auto partition_op = dynamic_cast<hybridse::vm::PhysicalPartitionProviderNode*>(scan_op);
                DCHECK(partition_op != nullptr);
                auto index_name = partition_op->index_name_;
                // -- return false won't change index_name
                LOG(WARNING) << "What if the index is not best index? Do we need to adjust index?";
                return false;
            }
        }
    } else if (PhysicalOpType::kPhysicalOpSimpleProject == in->GetOpType()) {
        auto simple_project = dynamic_cast<hybridse::vm::PhysicalSimpleProjectNode*>(in);
        PhysicalOpNode* new_depend;
        return KeysOptimizedParse(root_schemas_ctx, simple_project->producers()[0], left_key, index_key, right_key,
                                  sort, &new_depend);

    } else if (PhysicalOpType::kPhysicalOpRename == in->GetOpType()) {
        PhysicalOpNode* new_depend;
        return KeysOptimizedParse(root_schemas_ctx, in->producers()[0], left_key, index_key, right_key, sort,
                                  &new_depend);
    }
    return false;
}

std::vector<PhysicalOpNode*> GroupAndSortOptimizedParser::InitJoinList(
    hybridse::vm::PhysicalWindowAggrerationNode* op) {
    std::vector<PhysicalOpNode*> joined_op_list;
    auto& window_joins = op->window_joins_.window_joins();
    PhysicalOpNode* cur = op->GetProducer(0);
    for (auto& pair : window_joins) {
        auto joined = new hybridse::vm::PhysicalJoinNode(cur, pair.first, pair.second);
        joined_op_list.push_back(joined);
        cur = joined;
    }
    return joined_op_list;
}

void GroupAndSortOptimizedParser::TransformParse(PhysicalOpNode* in) {
    switch (in->GetOpType()) {
        case PhysicalOpType::kPhysicalOpGroupBy: {
            auto group_op = dynamic_cast<hybridse::vm::PhysicalGroupNode*>(in);
            DCHECK(group_op);
            PhysicalOpNode* new_producer;
            if (GroupOptimizedParse(group_op->schemas_ctx(), group_op->GetProducer(0), &group_op->group_,
                                    &new_producer)) {
                // no orders->no sort->no ttl info
                DLOG(INFO) << "ttl won't update by node:\n" << group_op->GetTreeString();
            }
            break;
        }
        case PhysicalOpType::kPhysicalOpProject: {
            auto project_op = dynamic_cast<hybridse::vm::PhysicalProjectNode*>(in);
            DCHECK(project_op);
            if (hybridse::vm::ProjectType::kWindowAggregation == project_op->project_type_) {
                auto window_agg_op = dynamic_cast<hybridse::vm::PhysicalWindowAggrerationNode*>(project_op);
                CHECK_NOTNULL(window_agg_op);
                PhysicalOpNode* input = window_agg_op->GetProducer(0);

                PhysicalOpNode* new_producer;
                if (!window_agg_op->instance_not_in_window()) {
                    if (KeyAndOrderOptimizedParse(input->schemas_ctx(), input, &window_agg_op->window_.partition_,
                                                  &window_agg_op->window_.sort_, &new_producer)) {
                        index_map_builder_.UpdateIndex(window_agg_op->window_.range());
                    }
                }
                // must prepare for window join column infer
                auto& window_joins = window_agg_op->window_joins();
                auto& window_unions = window_agg_op->window_unions();
                auto joined_op_list = InitJoinList(window_agg_op);
                if (!window_joins.Empty()) {
                    size_t join_idx = 0;
                    for (auto& window_join : window_joins.window_joins()) {
                        PhysicalOpNode* cur_joined = joined_op_list[join_idx];

                        PhysicalOpNode* new_join_right;
                        if (JoinKeysOptimizedParse(cur_joined->schemas_ctx(), window_join.first, &window_join.second,
                                                   &new_join_right)) {
                            // no range info
                            DLOG(INFO) << "ttl won't update by node:\n" << window_agg_op->GetTreeString();
                        }
                        join_idx += 1;
                    }
                }
                // joined_op_list need to be deleted
                for (auto& op : joined_op_list) {
                    delete op;
                }

                if (!window_unions.Empty()) {
                    for (auto& window_union : window_unions.window_unions_) {
                        PhysicalOpNode* new_producer1;
                        if (KeyAndOrderOptimizedParse(window_union.first->schemas_ctx(), window_union.first,
                                                      &window_union.second.partition_, &window_union.second.sort_,
                                                      &new_producer1)) {
                            index_map_builder_.UpdateIndex(window_union.second.range());
                        }
                    }
                }
            }
            break;
        }
        case PhysicalOpType::kPhysicalOpRequestUnion: {
            auto union_op = dynamic_cast<hybridse::vm::PhysicalRequestUnionNode*>(in);
            DCHECK(union_op);
            PhysicalOpNode* new_producer;
            if (!union_op->instance_not_in_window()) {
                if (KeysAndOrderFilterOptimizedParse(union_op->schemas_ctx(), union_op->GetProducer(1),
                                                     &union_op->window_.partition_, &union_op->window_.index_key_,
                                                     &union_op->window_.sort_, &new_producer)) {
                    index_map_builder_.UpdateIndex(union_op->window().range());
                }
            }

            if (!union_op->window_unions().Empty()) {
                for (auto& window_union : union_op->window_unions_.window_unions_) {
                    PhysicalOpNode* new_producer1;
                    auto& window = window_union.second;
                    if (KeysAndOrderFilterOptimizedParse(window_union.first->schemas_ctx(), window_union.first,
                                                         &window.partition_, &window.index_key_, &window.sort_,
                                                         &new_producer1)) {
                        index_map_builder_.UpdateIndex(window.range());
                    }
                }
            }
            break;
        }
        case PhysicalOpType::kPhysicalOpRequestJoin: {
            auto* join_op = dynamic_cast<hybridse::vm::PhysicalRequestJoinNode*>(in);
            DCHECK(join_op);
            PhysicalOpNode* new_producer;
            // Optimized Right Table Partition
            if (JoinKeysOptimizedParse(join_op->schemas_ctx(), join_op->GetProducer(1), &join_op->join_,
                                       &new_producer)) {
                // no range info
                DLOG(INFO) << "ttl won't update by node:\n" << join_op->GetTreeString();
            }

            break;
        }
        case PhysicalOpType::kPhysicalOpJoin: {
            auto* join_op = dynamic_cast<hybridse::vm::PhysicalJoinNode*>(in);
            DCHECK(join_op);
            PhysicalOpNode* new_producer;
            // Optimized Right Table Partition
            if (JoinKeysOptimizedParse(join_op->schemas_ctx(), join_op->GetProducer(1), &join_op->join_,
                                       &new_producer)) {
                // no range info
                DLOG(INFO) << "ttl won't update by node:\n" << join_op->GetTreeString();
            }

            break;
        }
        case PhysicalOpType::kPhysicalOpFilter: {
            auto* filter_op = dynamic_cast<hybridse::vm::PhysicalFilterNode*>(in);
            DCHECK(filter_op);
            PhysicalOpNode* new_producer;
            if (FilterOptimizedParse(filter_op->schemas_ctx(), filter_op->GetProducer(0), &filter_op->filter_,
                                     &new_producer)) {
                // no range info
                DLOG(INFO) << "ttl won't update by node:\n" << filter_op->GetTreeString();
            }
        }
        default: { break; }
    }
}

bool ResolveColumnToSourceColumnName(const hybridse::node::ColumnRefNode* col, const SchemasContext* schemas_ctx,
                                     std::string* source_name) {
    // use detailed column resolve utility
    size_t column_id;
    int path_idx;
    size_t child_column_id;
    size_t source_column_id;
    const PhysicalOpNode* source;
    hybridse::base::Status status =
        schemas_ctx->ResolveColumnID(col->GetDBName(), col->GetRelationName(), col->GetColumnName(), &column_id,
                                     &path_idx, &child_column_id, &source_column_id, &source);

    // try loose the relation
    if (!status.isOK() && !col->GetRelationName().empty()) {
        status = schemas_ctx->ResolveColumnID("", "", col->GetColumnName(), &column_id, &path_idx, &child_column_id,
                                              &source_column_id, &source);
    }

    if (!status.isOK()) {
        LOG(WARNING) << "Illegal index column: " << col->GetExprString();
        return false;
    }
    if (source == nullptr || source->GetOpType() != PhysicalOpType::kPhysicalOpDataProvider) {
        LOG(WARNING) << "Index column is not from any source table: " << col->GetExprString();
        return false;
    }
    status = source->schemas_ctx()->ResolveColumnNameByID(source_column_id, source_name);
    if (!status.isOK()) {
        LOG(WARNING) << "Illegal source column id #" << source_column_id << " for index column "
                     << col->GetExprString();
        return false;
    }
    return true;
}
}  // namespace openmldb::base
