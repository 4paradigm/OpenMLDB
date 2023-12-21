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
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "codec/schema_codec.h"
#include "google/protobuf/util/message_differencer.h"
#include "node/node_manager.h"
#include "plan/plan_api.h"
#include "proto/common.pb.h"
#include "proto/fe_type.pb.h"
#include "proto/type.pb.h"
#include "sdk/base_impl.h"
#include "vm/physical_op.h"

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

class IndexMapBuilder final : public ::hybridse::vm::IndexHintHandler {
 public:
    IndexMapBuilder() {}
    ~IndexMapBuilder() override {}

    void Report(absl::string_view db, absl::string_view table, absl::Span<std::string const> keys, absl::string_view ts,
                const PhysicalOpNode* expr_node) override;

    MultiDBIndexMap ToMap();

 private:
    void UpdateTTLByWindow(const hybridse::vm::WindowOp&, common::TTLSt*);
    // db, table, keys and ts -> db$table:key1,key2,...;ts
    std::string Encode(absl::string_view db, absl::string_view table, absl::Span<std::string const> keys,
                       absl::string_view ts);

    // return db, table, index_str(key1,key2,...;ts), column_key
    static std::tuple<std::string, std::string, std::string, common::ColumnKey> Decode(const std::string& index_str);

    static std::string GetTsCol(const std::string& index_str) {
        std::size_t ts_mark_pos = index_str.find(TS_MARK);
        if (ts_mark_pos == std::string::npos) {
            return {};
        }
        auto ts_begin = ts_mark_pos + 1;
        return index_str.substr(ts_begin);
    }

    static std::pair<std::string, std::string> GetTable(const std::string& index_str) {
        auto db_table_start = index_str.find(UNIQ_MARK);
        auto table_start = index_str.find(TABLE_MARK);
        auto key_sep = index_str.find(KEY_MARK);
        if (db_table_start == std::string::npos || table_start == std::string::npos || key_sep == std::string::npos) {
            LOG(DFATAL) << "invalid index str " << index_str;
            return {};
        }
        // i|db$table:key1,key2,...
        return std::make_pair(index_str.substr(db_table_start + 1, table_start - db_table_start - 1),
                              index_str.substr(table_start + 1, key_sep - 1 - table_start));
    }

 private:
    static constexpr char UNIQ_MARK = '|';
    static constexpr char TABLE_MARK = '$';
    static constexpr char KEY_MARK = ':';
    static constexpr char KEY_SEP = ',';
    static constexpr char TS_MARK = ';';

    uint64_t index_id_ = 0;
    // map<db_table_keys_and_order_str, ttl_st>
    std::map<std::string, common::TTLSt*> index_map_;
};

// multi database
MultiDBIndexMap DDLParser::ExtractIndexes(const std::string& sql, const std::string& used_db,
                                          const MultiDBTableDescMap& schemas) {
    auto catalog = buildCatalog(schemas);
    return ExtractIndexes(sql, used_db, catalog);
}

MultiDBIndexMap DDLParser::ExtractIndexes(const std::string& sql, const std::string& used_db,
                                          const std::shared_ptr<hybridse::vm::SimpleCatalog>& catalog) {
    hybridse::vm::MockRequestRunSession session;
    auto index_hints = std::make_shared<IndexMapBuilder>();
    session.SetIndexHintsHandler(index_hints);

    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::hybridse::vm::EngineOptions options;
    options.SetKeepIr(true);
    options.SetCompileOnly(true);
    auto engine = std::make_shared<hybridse::vm::Engine>(catalog, options);

    hybridse::base::Status status;
    engine->Get(sql, used_db, session, status);

    return index_hints->ToMap();
}

std::string DDLParser::PhysicalPlan(const std::string& sql, const ::hybridse::type::Database& db) {
    hybridse::vm::MockRequestRunSession session;
    auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
    catalog->AddDatabase(db);
    if (!GetPlan(sql, db.name(), catalog, &session)) {
        LOG(ERROR) << "sql get plan failed";
        return {};
    }
    std::ostringstream plan_oss;
    session.GetCompileInfo()->DumpPhysicalPlan(plan_oss, "\t");
    return plan_oss.str();
}

bool DDLParser::Explain(const std::string& sql, const std::string& db,
                        const std::shared_ptr<hybridse::vm::SimpleCatalog>& catalog,
                        hybridse::vm::ExplainOutput* output) {
    ::hybridse::base::Status vm_status;
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::hybridse::vm::EngineOptions options;
    options.SetKeepIr(true);
    options.SetCompileOnly(true);
    auto engine = std::make_shared<hybridse::vm::Engine>(catalog, options);
    // use mock, to disable enable_request_performance_sensitive, avoid no matched index, it may get error `Isn't
    // partition provider:DATA_PROVIDER(table=xxx)`
    auto ok = engine->Explain(sql, db, ::hybridse::vm::kMockRequestMode, output, &vm_status);
    if (!ok) {
        LOG(WARNING) << "hybrid engine compile sql failed, " << vm_status.str();
        return false;
    }
    return true;
}

bool DDLParser::Explain(const std::string& sql, const std::string& db, const MultiDBTableDescMap& schemas,
                        ::hybridse::vm::ExplainOutput* output) {
    auto catalog = buildCatalog(schemas);
    return Explain(sql, db, catalog, output);
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

// schemas: <db, <table, columns>>
std::shared_ptr<hybridse::sdk::Schema> DDLParser::GetOutputSchema(const std::string& sql, const std::string& db,
                                                                  const MultiDBTableDescMap& schemas) {
    // multi database
    auto catalog = buildCatalog(schemas);
    return GetOutputSchema(sql, db, catalog);
}

std::shared_ptr<hybridse::sdk::Schema> DDLParser::GetOutputSchema(
    const std::string& sql, const std::string& db, const std::shared_ptr<hybridse::vm::SimpleCatalog>& catalog) {
    hybridse::vm::MockRequestRunSession session;
    if (!GetPlan(sql, db, catalog, &session)) {
        LOG(ERROR) << "sql get plan failed";
        return {};
    }
    auto output_schema_ptr = session.GetCompileInfo()->GetPhysicalPlan()->GetOutputSchema();
    return std::make_shared<hybridse::sdk::SchemaImpl>(*output_schema_ptr);
}

bool DDLParser::GetPlan(const std::string& sql, const std::string& db,
                        const std::shared_ptr<hybridse::vm::SimpleCatalog>& catalog,
                        hybridse::vm::RunSession* session) {
    hybridse::base::Status status;
    return GetPlan(sql, db, catalog, session, &status);
}

bool DDLParser::GetPlan(const std::string& sql, const std::string& db,
                        const std::shared_ptr<hybridse::vm::SimpleCatalog>& catalog, hybridse::vm::RunSession* session,
                        hybridse::base::Status* status) {
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    ::hybridse::vm::EngineOptions options;
    options.SetKeepIr(true);
    options.SetCompileOnly(true);
    auto engine = std::make_shared<hybridse::vm::Engine>(catalog, options);
    auto ok = engine->Get(sql, db, *session, *status);
    if (!(ok && status->isOK())) {
        LOG(WARNING) << "hybrid engine compile sql failed, " << status->str();
        return false;
    }
    return true;
}

template <typename T>
void DDLParser::AddTables(const T& table_defs, hybridse::type::Database* db) {
    for (auto& table : table_defs) {
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

std::shared_ptr<hybridse::vm::SimpleCatalog> DDLParser::buildCatalog(const MultiDBTableDescMap& schemas) {
    auto catalog = std::make_shared<hybridse::vm::SimpleCatalog>(true);
    for (auto& db_item : schemas) {
        auto& db_name = db_item.first;
        auto& table_map = db_item.second;
        ::hybridse::type::Database db;
        db.set_name(db_name);
        AddTables(table_map, &db);
        catalog->AddDatabase(db);
    }
    return catalog;
}

std::vector<std::string> DDLParser::ValidateSQLInBatch(const std::string& sql, const std::string& db,
                                                       const std::shared_ptr<hybridse::vm::SimpleCatalog>& catalog) {
    hybridse::vm::BatchRunSession session;
    hybridse::base::Status status;
    auto ok = GetPlan(sql, db, catalog, &session, &status);
    if (!ok || !status.isOK()) {
        return {status.GetMsg(), status.GetTraces()};
    }
    return {};
}

std::vector<std::string> DDLParser::ValidateSQLInBatch(const std::string& sql, const std::string& db,
                                                       const MultiDBTableDescMap& schemas) {
    auto catalog = buildCatalog(schemas);
    return ValidateSQLInBatch(sql, db, catalog);
}

std::vector<std::string> DDLParser::ValidateSQLInRequest(const std::string& sql, const std::string& db,
                                                         const std::shared_ptr<hybridse::vm::SimpleCatalog>& catalog) {
    hybridse::vm::MockRequestRunSession session;
    hybridse::base::Status status;
    auto ok = GetPlan(sql, db, catalog, &session, &status);
    if (!ok || !status.isOK()) {
        return {status.GetMsg(), status.GetTraces()};
    }
    return {};
}

std::vector<std::string> DDLParser::ValidateSQLInRequest(const std::string& sql, const std::string& db,
                                                         const MultiDBTableDescMap& schemas) {
    auto catalog = buildCatalog(schemas);
    return ValidateSQLInRequest(sql, db, catalog);
}

void IndexMapBuilder::Report(absl::string_view db, absl::string_view table, absl::Span<std::string const> keys,
                             absl::string_view ts, const PhysicalOpNode* expr_node) {
    // we encode table, keys and ts to one string
    auto index = Encode(db, table, keys, ts);
    if (index.empty()) {
        LOG(WARNING) << "index encode failed for table " << table;
        return;
    }

    if (index_map_.find(index) != index_map_.end()) {
        // index id has unique idx, can't be dup. It's a weird case
        LOG(DFATAL) << "index " << index << " existed in cache";
        return;
    }

    // default TTLSt is abs and ttl=0, rows will never expire.
    // default TTLSt debug string is {}, but if we get, they will be the default values.
    auto* ttl = new common::TTLSt;

    if (expr_node != nullptr) {
        switch (expr_node->GetOpType()) {
            case hybridse::vm::kPhysicalOpRequestUnion: {
                auto ru = expr_node->GetAsOrNull<hybridse::vm::PhysicalRequestUnionNode>();
                UpdateTTLByWindow(ru->window(), ttl);
                break;
            }
            case hybridse::vm::kPhysicalOpProject: {
                auto ru = expr_node->GetAsOrNull<hybridse::vm::PhysicalProjectNode>();
                if (ru != nullptr && ru->project_type_ == hybridse::vm::ProjectType::kWindowAggregation) {
                    auto win_project = ru->GetAsOrNull<hybridse::vm::PhysicalWindowAggrerationNode>();
                    UpdateTTLByWindow(win_project->window_, ttl);
                }
                break;
            }
            case hybridse::vm::kPhysicalOpRequestJoin: {
                auto join_node = expr_node->GetAsOrNull<hybridse::vm::PhysicalRequestJoinNode>();
                if (join_node->join().join_type() == hybridse::node::JoinType::kJoinTypeLeft) {
                    ttl->set_ttl_type(type::TTLType::kAbsoluteTime);
                    ttl->set_abs_ttl(0);
                }
                break;
            }
            case hybridse::vm::kPhysicalOpJoin: {
                auto join_node = expr_node->GetAsOrNull<hybridse::vm::PhysicalJoinNode>();
                if (join_node->join().join_type() == hybridse::node::JoinType::kJoinTypeLeft) {
                    ttl->set_ttl_type(type::TTLType::kAbsoluteTime);
                    ttl->set_abs_ttl(0);
                }
                break;
            }
            default:
                break;
        }
    }

    index_map_[index] = ttl;

    LOG(INFO) << "suggest creating index for " << db << "." << table << ": " << index << ", " << ttl->DebugString();
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
// And after convert, 0 means unbounded, history_range_start 0 will be converted to 1
// NOTICE: do not convert invalid range/rows start, it'll return 0 by `GetHistoryRangeStart`.
int64_t AbsTTLConvert(int64_t history_range_start) {
    return history_range_start == INT64_MIN ? 0 : AbsTTLConvert(-1 * history_range_start, false);
}
int64_t LatTTLConvert(int64_t history_rows_start) {
    return history_rows_start == INT64_MIN ? 0 : LatTTLConvert(-1 * history_rows_start, false);
}

void IndexMapBuilder::UpdateTTLByWindow(const hybridse::vm::WindowOp& window, common::TTLSt* ttl_st_ptr) {
    auto& range = window.range();

    std::stringstream ss;
    range.frame()->Print(ss, "");
    DLOG(INFO) << "frame info: " << ss.str();

    auto frame = range.frame();
    auto type = frame->frame_type();
    switch (type) {
        case hybridse::node::kFrameRows: {
            ttl_st_ptr->set_ttl_type(type::TTLType::kLatestTime);
            ttl_st_ptr->set_lat_ttl(LatTTLConvert(frame->GetHistoryRowsStart()));
            break;
        }
        case hybridse::node::kFrameRange:
        case hybridse::node::kFrameRowsRange: {
            ttl_st_ptr->set_ttl_type(type::TTLType::kAbsoluteTime);
            ttl_st_ptr->set_abs_ttl(AbsTTLConvert(frame->GetHistoryRangeStart()));
            break;
        }
        case hybridse::node::kFrameRowsMergeRowsRange: {
            // use abs and ttl, only >abs_ttl and > lat_ttl will be expired
            ttl_st_ptr->set_ttl_type(type::TTLType::kAbsAndLat);
            ttl_st_ptr->set_abs_ttl(AbsTTLConvert(frame->GetHistoryRangeStart()));
            ttl_st_ptr->set_lat_ttl(LatTTLConvert(frame->GetHistoryRowsStart()));
            break;
        }
        default:
            LOG(WARNING) << "invalid type";
            return;
    }
}

MultiDBIndexMap IndexMapBuilder::ToMap() {
    // index_map_ may have duplicated index, we need to merge them here(don't merge in CreateIndex for debug)
    // <db, <table, <index_str, ColumnKey>>>
    std::map<std::string, std::map<std::string, std::map<std::string, common::ColumnKey>>> tmp_map;
    for (auto& pair : index_map_) {
        if (!pair.second->has_ttl_type()) {
            pair.second->set_ttl_type(::openmldb::type::TTLType::kLatestTime);
            pair.second->set_lat_ttl(1);
        }
        auto[db, table, idx_str, column_key] = Decode(pair.first);
        DLOG(INFO) << "decode index '" << pair.first << "': " << db << " " << table << " " << idx_str << " "
                   << column_key.ShortDebugString();
        auto& idx_map_of_table = tmp_map[db][table];
        auto iter = idx_map_of_table.find(idx_str);
        if (iter != idx_map_of_table.end()) {
            // dup index, ttl merge
            TTLMerge(iter->second.ttl(), *pair.second, iter->second.mutable_ttl());
        } else {
            // message owns the TTLSt
            column_key.set_allocated_ttl(pair.second);
            idx_map_of_table.emplace(idx_str, column_key);
        }
    }

    MultiDBIndexMap result;
    for (auto& db_map : tmp_map) {
        auto& db = db_map.first;
        for (auto& pair : db_map.second) {
            auto& table = pair.first;
            auto& idx_map_of_table = pair.second;
            for (auto& idx_pair : idx_map_of_table) {
                auto& column_key = idx_pair.second;
                result[db][table].emplace_back(column_key);
            }
        }
    }

    // TTLSt is owned by result now, index_map_ can't be reused
    index_map_.clear();
    index_id_ = 0;
    return result;
}

std::string IndexMapBuilder::Encode(absl::string_view db, absl::string_view table, absl::Span<std::string const> keys,
                                    absl::string_view ts) {
    // children are ColumnRefNode
    std::vector<std::string> cols(keys.begin(), keys.end());
    std::sort(cols.begin(), cols.end());
    if (cols.empty()) {
        return {};
    }

    std::stringstream ss;
    // we add a unique mark to avoid conflict with index name, leave the indexes with same name(ttl may be different)
    // you should do merge later
    ss << index_id_++ << UNIQ_MARK << db << TABLE_MARK << table << KEY_MARK;
    auto iter = cols.begin();
    ss << (*iter);
    iter++;
    for (; iter != cols.end(); iter++) {
        ss << KEY_SEP << (*iter);
    }
    ss << TS_MARK;
    if (!ts.empty()) {
        ss << ts;
    }

    return ss.str();
}

// ColumnKey in result doesn't set ttl
std::tuple<std::string, std::string, std::string, common::ColumnKey> IndexMapBuilder::Decode(
    const std::string& index_str) {
    if (index_str.empty()) {
        return {};
    }

    const auto[db_name, table_name] = GetTable(index_str);

    common::ColumnKey column_key;
    auto key_sep = index_str.find(KEY_MARK);
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
    return std::make_tuple(db_name, table_name, index_str.substr(key_sep + 1), column_key);
}

// return merged result: return new if new is bigger, else return old
google::protobuf::uint64 TTLValueMerge(google::protobuf::uint64 old_value, google::protobuf::uint64 new_value) {
    google::protobuf::uint64 result = old_value;
    // 0 is the max, means no ttl, don't update
    // if old value != 0
    if (old_value != 0 && (new_value == 0 || old_value < new_value)) {
        result = new_value;
    }
    return result;
}

void TTLValueMerge(const common::TTLSt& old_ttl, const common::TTLSt& new_ttl, common::TTLSt* result) {
    google::protobuf::uint64 tmp_result;
    tmp_result = TTLValueMerge(old_ttl.abs_ttl(), new_ttl.abs_ttl());
    result->set_abs_ttl(tmp_result);
    tmp_result = TTLValueMerge(old_ttl.lat_ttl(), new_ttl.lat_ttl());
    result->set_lat_ttl(tmp_result);
}

// 4(same type merge): same type and max ttls
// 12(A(4,2)): see in code
bool TTLMerge(const common::TTLSt& old_ttl, const common::TTLSt& new_ttl, common::TTLSt* result) {
    // TTLSt has type and two values, updated is complex, so we just check result==old_ttl in the end
    result->CopyFrom(old_ttl);

    // merge case 1. same type, just merge values(max ttls)
    // it's ok to merge both abs and lat ttl value even type is only abs or lat(just unused and default is 0)
    // merge case 2. different type
    if (old_ttl.ttl_type() == new_ttl.ttl_type()) {
        TTLValueMerge(old_ttl, new_ttl, result);
    } else {
        // type is different
        if (old_ttl.ttl_type() == type::TTLType::kAbsAndLat) {
            // 3 cases, abs&lat + ?. ?: abs / lat / abs||lat(new_ttl type != abs&lat). Use new ttl, merge values.
            // abs&lat + abs -> abs, abs&lat + lat -> lat, abs&lat + abs||lat -> abs||lat. Use the max range, it's ok to
            // merge two ttl(e.g. abs 10s&lat 1 + abs 10s(lat 0) -> abs 10s), we won't use the another ttl if result is
            // lat or abs.
            result->set_ttl_type(new_ttl.ttl_type());
            TTLValueMerge(old_ttl, new_ttl, result);
        } else if (new_ttl.ttl_type() == type::TTLType::kAbsAndLat) {
            // 3 cases, ? + abs&lat. ?: abs / lat / abs||lat
            result->set_ttl_type(old_ttl.ttl_type());
            TTLValueMerge(old_ttl, new_ttl, result);
        } else if (old_ttl.ttl_type() == type::TTLType::kAbsOrLat) {
            // 2 cases, abs||lat + ? -> abs||lat. Stay old, merge values
            // ?: abs / lat 2 cases(abs||lat + abs&lat is in 2)
            TTLValueMerge(old_ttl, new_ttl, result);
        } else if (new_ttl.ttl_type() == type::TTLType::kAbsOrLat) {
            // 2 cases, abs + abs||lat -> abs||lat, lat + abs||lat -> abs||lat. Use new, merge can't use lat ttl(0) if
            // type is abs abs&lat + abs||lat is in 1
            result->set_ttl_type(type::TTLType::kAbsOrLat);
            if (old_ttl.ttl_type() == type::TTLType::kAbsoluteTime) {
                result->set_abs_ttl(TTLValueMerge(old_ttl.abs_ttl(), new_ttl.abs_ttl()));
                result->set_lat_ttl(new_ttl.lat_ttl());
            } else {
                result->set_abs_ttl(new_ttl.abs_ttl());
                result->set_lat_ttl(TTLValueMerge(old_ttl.lat_ttl(), new_ttl.lat_ttl()));
            }
        } else {
            // 2 cases, abs + lat -> abs||lat, lat + abs -> abs||lat. Set type, merge can't use lat ttl(0) if type is
            // abs
            result->set_ttl_type(type::TTLType::kAbsOrLat);
            if (old_ttl.ttl_type() == type::TTLType::kAbsoluteTime) {
                DCHECK(new_ttl.ttl_type() == type::TTLType::kLatestTime);
                result->set_abs_ttl(old_ttl.abs_ttl());
                result->set_lat_ttl(new_ttl.lat_ttl());
            } else {
                DCHECK(old_ttl.ttl_type() == type::TTLType::kLatestTime);
                result->set_abs_ttl(new_ttl.abs_ttl());
                result->set_lat_ttl(old_ttl.lat_ttl());
            }
        }
    }

    // old ttl may not have one ttl value, but the result must have, so fix the cmp
    common::TTLSt old_ttl_fixed(old_ttl);
    if (!old_ttl_fixed.has_abs_ttl()) {
        old_ttl_fixed.set_abs_ttl(0);
    }
    if (!old_ttl_fixed.has_lat_ttl()) {
        old_ttl_fixed.set_lat_ttl(0);
    }
    return !google::protobuf::util::MessageDifferencer::Equals(old_ttl_fixed, *result);
}
}  // namespace openmldb::base
