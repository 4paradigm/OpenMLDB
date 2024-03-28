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

#include "sdk/sql_cluster_router.h"

#include <algorithm>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>

#include "absl/strings/ascii.h"
#include "absl/strings/numbers.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_split.h"
#include "absl/strings/strip.h"
#include "absl/strings/substitute.h"
#include "absl/types/span.h"
#include "base/ddl_parser.h"
#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "base/status_util.h"
#include "boost/none.hpp"
#include "boost/property_tree/ini_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "brpc/channel.h"
#include "cmd/display.h"
#include "codec/encrypt.h"
#include "codegen/insert_row_builder.h"
#include "common/timer.h"
#include "glog/logging.h"
#include "nameserver/system_table.h"
#include "plan/plan_api.h"
#include "proto/fe_common.pb.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "schema/schema_adapter.h"
#include "sdk/base.h"
#include "sdk/base_impl.h"
#include "sdk/batch_request_result_set_sql.h"
#include "sdk/job_table_helper.h"
#include "sdk/node_adapter.h"
#include "sdk/query_future_impl.h"
#include "sdk/result_set_sql.h"
#include "sdk/sdk_util.h"
#include "sdk/split.h"
#include "udf/udf.h"
#include "vm/catalog.h"

DECLARE_string(bucket_size);
DECLARE_uint32(replica_num);
DECLARE_int32(sync_job_timeout);
DECLARE_int32(deploy_job_max_wait_time_ms);

namespace openmldb {
namespace sdk {

using hybridse::common::StatusCode;
using hybridse::plan::PlanAPI;

constexpr const char* SKIP_INDEX_CHECK_OPTION = "skip_index_check";
constexpr const char* SYNC_OPTION = "sync";
constexpr const char* RANGE_BIAS_OPTION = "range_bias";
constexpr const char* ROWS_BIAS_OPTION = "rows_bias";

class ExplainInfoImpl : public ExplainInfo {
 public:
    ExplainInfoImpl(const ::hybridse::sdk::SchemaImpl& input_schema, const ::hybridse::sdk::SchemaImpl& output_schema,
                    const std::string& logical_plan, const std::string& physical_plan, const std::string& ir,
                    const std::string& request_db_name, const std::string& request_name)
        : input_schema_(input_schema),
          output_schema_(output_schema),
          logical_plan_(logical_plan),
          physical_plan_(physical_plan),
          ir_(ir),
          request_db_name_(request_db_name),
          request_name_(request_name) {}
    ~ExplainInfoImpl() {}

    const ::hybridse::sdk::Schema& GetInputSchema() override { return input_schema_; }

    const ::hybridse::sdk::Schema& GetOutputSchema() override { return output_schema_; }

    const std::string& GetLogicalPlan() override { return logical_plan_; }

    const std::string& GetPhysicalPlan() override { return physical_plan_; }

    const std::string& GetIR() override { return ir_; }

    const std::string& GetRequestName() override { return request_name_; }
    const std::string& GetRequestDbName() override { return request_db_name_; }

 private:
    ::hybridse::sdk::SchemaImpl input_schema_;
    ::hybridse::sdk::SchemaImpl output_schema_;
    std::string logical_plan_;
    std::string physical_plan_;
    std::string ir_;
    std::string request_db_name_;
    std::string request_name_;
};

SQLClusterRouter::SQLClusterRouter(const SQLRouterOptions& options)
    : options_(std::make_shared<SQLRouterOptions>(options)),
      is_cluster_mode_(true),
      interactive_validator_(),
      cluster_sdk_(nullptr),
      mu_(),
      rand_(::baidu::common::timer::now_time()) {}

SQLClusterRouter::SQLClusterRouter(const StandaloneOptions& options)
    : options_(std::make_shared<StandaloneOptions>(options)),
      is_cluster_mode_(false),
      interactive_validator_(),
      cluster_sdk_(nullptr),
      mu_(),
      rand_(::baidu::common::timer::now_time()) {}

SQLClusterRouter::SQLClusterRouter(DBSDK* sdk)
    : options_(),
      is_cluster_mode_(sdk->IsClusterMode()),
      interactive_validator_(),
      cluster_sdk_(sdk),
      mu_(),
      rand_(::baidu::common::timer::now_time()) {
    options_ = sdk->GetOptions();
}

SQLClusterRouter::~SQLClusterRouter() { delete cluster_sdk_; }

bool SQLClusterRouter::Init() {
    // set log first(If setup before, setup below won't work, e.g. router in tablet server, router in CLI)
    if (cluster_sdk_ == nullptr) {
        // glog setting for SDK
        FLAGS_glog_level = options_->glog_level;
        FLAGS_glog_dir = options_->glog_dir;
    }
    base::SetupGlog();

    if (cluster_sdk_ == nullptr) {
        // init cluster_sdk_, require options_ or standalone_options_ is set
        if (is_cluster_mode_) {
            auto ops = std::dynamic_pointer_cast<SQLRouterOptions>(options_);
            cluster_sdk_ = new ClusterSDK(ops);
            // TODO(hw): no detail error info
            bool ok = cluster_sdk_->Init();
            if (!ok) {
                LOG(WARNING) << "fail to init cluster sdk";
                return false;
            }
        } else {
            auto ops = std::dynamic_pointer_cast<StandaloneOptions>(options_);
            cluster_sdk_ = new ::openmldb::sdk::StandAloneSDK(ops);
            bool ok = cluster_sdk_->Init();
            if (!ok) {
                LOG(WARNING) << "fail to init standalone sdk";
                return false;
            }
        }
    }

    std::string db = openmldb::nameserver::INFORMATION_SCHEMA_DB;
    std::string table = openmldb::nameserver::GLOBAL_VARIABLES;
    std::string sql = "select * from " + table;
    hybridse::sdk::Status status;
    auto rs = ExecuteSQLParameterized(db, sql, std::shared_ptr<openmldb::sdk::SQLRequestRow>(), &status);
    if (rs != nullptr) {
        std::string key;
        std::string value;
        while (rs->Next()) {
            key = rs->GetStringUnsafe(0);
            value = rs->GetStringUnsafe(1);
            session_variables_[key] = value;
        }
    } else {
        // if not allowed to get system table or system table is empty, init session here
        session_variables_.emplace("execute_mode", "offline");
        session_variables_.emplace("enable_trace", "false");
        session_variables_.emplace("sync_job", "false");
        session_variables_.emplace("job_timeout", "60000");  // rpc request timeout for taskmanager
        session_variables_.emplace("insert_memory_usage_limit", "0");
        session_variables_.emplace("spark_config", "");
    }
    return Auth();
}

bool SQLClusterRouter::Auth() {
    auto ns_client = cluster_sdk_->GetNsClient();
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    auto ok = ns_client->ShowTable(nameserver::USER_INFO_NAME, nameserver::INTERNAL_DB, false, tables, msg);
    if (!ok) {
        LOG(WARNING) << "fail to get table from nameserver. error msg: " << msg;
        return false;
    }
    if (tables.empty()) {
        return true;
    }
    UserInfo info;
    auto result = GetUser(options_->user, &info);
    if (result.ok()) {
        if (!(*result)) {
            if (options_->user == "root") {
                return true;
            }
            LOG(WARNING) << "user " << options_->user << " does not exist";
            return false;
        }
        auto password = options_->password.empty() ? options_->password : codec::Encrypt(options_->password);
        if (info.password != password) {
            LOG(WARNING) << "wrong password!";
            return false;
        }
    } else {
        LOG(WARNING) << result.status();
        return false;
    }
    return true;
}

std::shared_ptr<SQLRequestRow> SQLClusterRouter::GetRequestRow(const std::string& db, const std::string& sql,
                                                               ::hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");

    auto cache = GetCache(db, sql, hybridse::vm::kRequestMode);
    std::set<std::string> col_set;
    std::shared_ptr<RouterSQLCache> router_cache;
    if (cache) {
        router_cache = std::dynamic_pointer_cast<RouterSQLCache>(cache);
    }
    if (router_cache) {
        status->code = 0;
        const std::string& router_col = router_cache->GetRouter().GetRouterCol();
        if (!router_col.empty()) {
            col_set.insert(router_col);
        }
        return std::make_shared<SQLRequestRow>(router_cache->GetSchema(), col_set);
    }
    ::hybridse::vm::ExplainOutput explain;
    ::hybridse::base::Status vm_status;

    bool ok = cluster_sdk_->GetEngine()->Explain(sql, db, ::hybridse::vm::kRequestMode, &explain, &vm_status);
    if (!ok) {
        COPY_PREPEND_AND_WARN(status, vm_status, "fail to explain sql " + sql);
        return {};
    }
    auto schema = std::make_shared<::hybridse::sdk::SchemaImpl>(explain.input_schema);
    const std::string& main_db = explain.router.GetMainDb().empty() ? db : explain.router.GetMainDb();
    const std::string& main_table = explain.router.GetMainTable();
    uint32_t tid = 0;
    if (!main_table.empty()) {
        auto table_info = cluster_sdk_->GetTableInfo(main_db, main_table);
        tid = table_info->tid();
    }
    std::shared_ptr<::hybridse::sdk::Schema> parameter_schema;
    router_cache = std::make_shared<RouterSQLCache>(main_db, tid, main_table, schema, parameter_schema, explain.router);
    SetCache(db, sql, hybridse::vm::kRequestMode, router_cache);
    const std::string& router_col = explain.router.GetRouterCol();
    if (!router_col.empty()) {
        col_set.insert(router_col);
    }
    return std::make_shared<SQLRequestRow>(schema, col_set);
}

std::shared_ptr<SQLRequestRow> SQLClusterRouter::GetRequestRowByProcedure(const std::string& db,
                                                                          const std::string& sp_name,
                                                                          ::hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info = cluster_sdk_->GetProcedureInfo(db, sp_name, &status->msg);
    if (!sp_info) {
        CODE_PREPEND_AND_WARN(status, StatusCode::kProcedureNotFound, db + "-" + sp_name);
        return nullptr;
    }
    const std::string& sql = sp_info->GetSql();
    return GetRequestRow(db, sql, status);
}

std::shared_ptr<openmldb::sdk::SQLDeleteRow> SQLClusterRouter::GetDeleteRow(const std::string& db,
                                                                            const std::string& sql,
                                                                            ::hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    if (cache) {
        auto delete_cache = std::dynamic_pointer_cast<DeleteSQLCache>(cache);
        if (delete_cache) {
            status->code = 0;
            return std::make_shared<openmldb::sdk::SQLDeleteRow>(
                delete_cache->GetDatabase(), delete_cache->GetTableName(), delete_cache->GetDefaultCondition(),
                delete_cache->GetCondition());
        }
    }
    ::hybridse::node::NodeManager nm;
    ::hybridse::plan::PlanNodeList plans;
    bool ok = GetSQLPlan(sql, &nm, &plans);
    if (!ok || plans.empty()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to get sql plan " + sql);
        return {};
    }
    ::hybridse::node::PlanNode* plan = plans[0];
    if (plan->GetType() != hybridse::node::kPlanTypeDelete) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "invalid sql node expect delete");
        return {};
    }
    auto delete_plan = dynamic_cast<::hybridse::node::DeletePlanNode*>(plan);
    auto condition = delete_plan->GetCondition();
    if (!condition) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "no condition in delete sql");
        return {};
    }
    std::string database = delete_plan->GetDatabase().empty() ? db : delete_plan->GetDatabase();
    if (database.empty()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "no db in sql and no default db");
        return {};
    }
    const auto& table_name = delete_plan->GetTableName();
    auto table_info = cluster_sdk_->GetTableInfo(database, table_name);
    if (!table_info) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, absl::StrCat(database, "-", table_name));
        return {};
    }
    auto col_map = schema::SchemaAdapter::GetColMap(*table_info);
    std::vector<Condition> condition_vec;
    std::vector<Condition> parameter_vec;
    auto binary_node = dynamic_cast<const hybridse::node::BinaryExpr*>(condition);
    *status =
        NodeAdapter::ExtractCondition(binary_node, col_map, table_info->column_key(), &condition_vec, &parameter_vec);
    if (!status->IsOK()) {
        LOG(WARNING) << status->ToString();
        return {};
    }
    auto delete_cache =
        std::make_shared<DeleteSQLCache>(db, table_info->tid(), table_name, condition_vec, parameter_vec);
    SetCache(db, sql, hybridse::vm::kBatchMode, delete_cache);
    *status = {};
    return std::make_shared<openmldb::sdk::SQLDeleteRow>(delete_cache->GetDatabase(), delete_cache->GetTableName(),
                                                         delete_cache->GetDefaultCondition(),
                                                         delete_cache->GetCondition());
}

std::shared_ptr<SQLInsertRow> SQLClusterRouter::GetInsertRow(const std::string& db, const std::string& sql,
                                                             ::hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    if (cache) {
        auto insert_cache = std::dynamic_pointer_cast<InsertSQLCache>(cache);
        if (insert_cache) {
            *status = {};
            return std::make_shared<SQLInsertRow>(insert_cache->GetTableInfo(), insert_cache->GetSchema(),
                                                  insert_cache->GetDefaultValue(), insert_cache->GetStrLength(),
                                                  insert_cache->GetHoleIdxArr(), insert_cache->IsPutIfAbsent());
        }
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    std::vector<uint32_t> stmt_column_idx_arr;
    bool put_if_absent = false;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map, &str_length, &stmt_column_idx_arr, &put_if_absent)) {
        PREPEND_AND_WARN(status, "fail to get insert info");
        return {};
    }
    auto schema = openmldb::schema::SchemaAdapter::ConvertSchema(table_info->column_desc());
    auto insert_cache = std::make_shared<InsertSQLCache>(
        table_info, schema, default_map, str_length,
        SQLInsertRow::GetHoleIdxArr(default_map, stmt_column_idx_arr, schema), put_if_absent);
    SetCache(db, sql, hybridse::vm::kBatchMode, insert_cache);
    *status = {};
    return std::make_shared<SQLInsertRow>(insert_cache->GetTableInfo(), insert_cache->GetSchema(),
                                          insert_cache->GetDefaultValue(), insert_cache->GetStrLength(),
                                          insert_cache->GetHoleIdxArr(), insert_cache->IsPutIfAbsent());
}

bool SQLClusterRouter::GetMultiRowInsertInfo(const std::string& db, const std::string& sql,
                                             ::hybridse::sdk::Status* status,
                                             std::shared_ptr<::openmldb::nameserver::TableInfo>* table_info,
                                             std::vector<DefaultValueMap>* default_maps,
                                             std::vector<uint32_t>* str_lengths, bool* put_if_absent,
                                             std::vector<std::shared_ptr<int8_t>>* codegen_rows) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    // TODO(hw): return status?
    RET_FALSE_IF_NULL_AND_WARN(table_info, "output table_info is nullptr");
    RET_FALSE_IF_NULL_AND_WARN(default_maps, "output default_maps is nullptr");
    RET_FALSE_IF_NULL_AND_WARN(str_lengths, "output str_lengths is nullptr");
    RET_FALSE_IF_NULL_AND_WARN(put_if_absent, "output put_if_absent is nullptr");
    ::hybridse::node::NodeManager nm;
    ::hybridse::plan::PlanNodeList plans;
    bool ok = GetSQLPlan(sql, &nm, &plans);
    if (!ok || plans.empty()) {
        SET_STATUS_AND_WARN(status, StatusCode::kPlanError, "fail to get sql plan with sql " + sql);
        return false;
    }
    ::hybridse::node::PlanNode* plan = plans[0];
    if (plan->GetType() != hybridse::node::kPlanTypeInsert) {
        SET_STATUS_AND_WARN(status, StatusCode::kPlanError, "invalid sql node expect insert");
        return false;
    }
    auto* iplan = dynamic_cast<::hybridse::node::InsertPlanNode*>(plan);
    const ::hybridse::node::InsertStmt* insert_stmt = iplan->GetInsertNode();
    if (insert_stmt == nullptr) {
        SET_STATUS_AND_WARN(status, StatusCode::kPlanError, "insert stmt is null");
        return false;
    }
    *put_if_absent = insert_stmt->insert_mode_ == ::hybridse::node::InsertStmt::IGNORE;
    std::string db_name;
    if (!insert_stmt->db_name_.empty()) {
        db_name = insert_stmt->db_name_;
    } else {
        db_name = db;
    }
    if (db_name.empty()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "Please enter database first");
        return false;
    }
    *table_info = cluster_sdk_->GetTableInfo(db_name, insert_stmt->table_name_);
    if (!(*table_info)) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, db_name + "-" + insert_stmt->table_name_ + " not exist");
        return false;
    }
    std::map<uint32_t, uint32_t> column_map;

    // codegen is the new approach encoding insert rows
    // TODO(someone): initiate a session variable to control row encode implementation
    bool insert_codegen = false;
    for (int i = 0; i < (*table_info)->column_desc_size(); ++i) {
        auto& col_desc = (*table_info)->column_desc(i);
        if (!codec::ColumnSupportLegacyCodec(col_desc)) {
            insert_codegen = true;
            break;
        }
    }
    for (size_t j = 0; j < insert_stmt->columns_.size(); ++j) {
        const std::string& col_name = insert_stmt->columns_[j];
        bool find_flag = false;
        for (int i = 0; i < (*table_info)->column_desc_size(); ++i) {
            if (col_name == (*table_info)->column_desc(i).name()) {
                if (column_map.count(i) > 0) {
                    SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "duplicate column of " + col_name);
                    return false;
                }
                column_map.insert(std::make_pair(i, j));
                find_flag = true;

                break;
            }
        }
        if (!find_flag) {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                                "can't find column " + col_name + " in table " + (*table_info)->name());
            return false;
        }
    }

    ::hybridse::codec::Schema sc;
    if (!schema::SchemaAdapter::ConvertSchema((*table_info)->column_desc(), &sc)) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "failed to convert table schema");
        return false;
    }
    // TODO(someone):
    // 1. default value from table definition
    // 2. parameters
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    auto jit = std::shared_ptr<hybridse::vm::HybridSeJitWrapper>(hybridse::vm::HybridSeJitWrapper::Create());
    if (!jit->Init()) {
        return false;
    }
    ::hybridse::codegen::InsertRowBuilder insert_builder(jit.get(), &sc);

    size_t total_rows_size = insert_stmt->values_.size();
    for (size_t i = 0; i < total_rows_size; i++) {
        hybridse::node::ExprNode* value = insert_stmt->values_[i];
        if (value->GetExprType() != ::hybridse::node::kExprList) {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                                "fail to parse row [" + std::to_string(i) +
                                    "]: invalid row expression, expect kExprList but " +
                                    hybridse::node::ExprTypeName(value->GetExprType()));
            return false;
        }
        if (insert_codegen) {
            auto s = insert_builder.ComputeRow(dynamic_cast<::hybridse::node::ExprListNode*>(value));
            if (!s.ok()) {
                SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                                    absl::Substitute("$2. Fail to encode row[$0]: $1.", i, s.status().ToString(),
                                                     value->GetExprString()));
                return false;
            }

            codegen_rows->push_back(s.value());
            continue;
        } else {
            uint32_t str_length = 0;
            default_maps->push_back(GetDefaultMap(*table_info, column_map,
                                                  dynamic_cast<::hybridse::node::ExprListNode*>(value), &str_length));
            if (!default_maps->back()) {
                SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                                    "fail to parse row[" + std::to_string(i) + "]: " + value->GetExprString());
                return false;
            }
            str_lengths->push_back(str_length);
        }
    }

    if (!insert_codegen) {
        if (default_maps->empty() || str_lengths->empty()) {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "default_maps or str_lengths are empty");
            return false;
        }
        if (default_maps->size() != str_lengths->size()) {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "default maps isn't match with str_lengths");
            return false;
        }
    }
    return true;
}

bool SQLClusterRouter::GetInsertInfo(const std::string& db, const std::string& sql, ::hybridse::sdk::Status* status,
                                     std::shared_ptr<::openmldb::nameserver::TableInfo>* table_info,
                                     DefaultValueMap* default_map, uint32_t* str_length,
                                     std::vector<uint32_t>* stmt_column_idx_in_table, bool* put_if_absent) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    RET_FALSE_IF_NULL_AND_WARN(table_info, "output table_info is nullptr");
    RET_FALSE_IF_NULL_AND_WARN(default_map, "output default_map is nullptr");
    RET_FALSE_IF_NULL_AND_WARN(str_length, "output str_length is nullptr");
    RET_FALSE_IF_NULL_AND_WARN(stmt_column_idx_in_table, "output stmt_column_idx_in_table is nullptr");

    ::hybridse::node::NodeManager nm;
    ::hybridse::plan::PlanNodeList plans;
    bool ok = GetSQLPlan(sql, &nm, &plans);
    if (!ok || plans.empty()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to get sql plan with sql " + sql);
        return false;
    }
    ::hybridse::node::PlanNode* plan = plans[0];
    if (plan->GetType() != hybridse::node::kPlanTypeInsert) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "invalid sql node expect insert");
        return false;
    }
    auto* iplan = dynamic_cast<::hybridse::node::InsertPlanNode*>(plan);
    const ::hybridse::node::InsertStmt* insert_stmt = iplan->GetInsertNode();
    if (insert_stmt == nullptr) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "insert stmt is null");
        return false;
    }
    *table_info = cluster_sdk_->GetTableInfo(db, insert_stmt->table_name_);
    if (!(*table_info)) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                            "table with name " + insert_stmt->table_name_ + " in db " + db + " does not exist");
        return false;
    }
    // <table schema idx, insert stmt column idx>
    std::map<uint32_t, uint32_t> column_map;
    for (size_t j = 0; j < insert_stmt->columns_.size(); ++j) {
        const std::string& col_name = insert_stmt->columns_[j];
        bool find_flag = false;
        for (int i = 0; i < (*table_info)->column_desc_size(); ++i) {
            if (col_name == (*table_info)->column_desc(i).name()) {
                if (column_map.count(i) > 0) {
                    SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "duplicate column of " + col_name);
                    return false;
                }
                column_map.insert(std::make_pair(i, j));
                stmt_column_idx_in_table->emplace_back(i);
                find_flag = true;
                break;
            }
        }
        if (!find_flag) {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                                "can't find column " + col_name + " in table " + (*table_info)->name());
            return false;
        }
    }
    *default_map = GetDefaultMap(*table_info, column_map,
                                 dynamic_cast<::hybridse::node::ExprListNode*>(insert_stmt->values_[0]), str_length);
    if (!(*default_map)) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "get default value map of " + sql + " failed");
        return false;
    }
    *put_if_absent = insert_stmt->insert_mode_ == ::hybridse::node::InsertStmt::IGNORE;
    return true;
}

DefaultValueMap SQLClusterRouter::GetDefaultMap(const std::shared_ptr<::openmldb::nameserver::TableInfo>& table_info,
                                                const std::map<uint32_t, uint32_t>& column_map,
                                                ::hybridse::node::ExprListNode* row, uint32_t* str_length) {
    if (row == nullptr || str_length == nullptr) {
        LOG(WARNING) << "row or str length is NULL";
        return {};
    }
    auto default_map = std::make_shared<std::map<uint32_t, std::shared_ptr<::hybridse::node::ConstNode>>>();
    if ((column_map.empty() && static_cast<int32_t>(row->children_.size()) < table_info->column_desc_size()) ||
        (!column_map.empty() && row->children_.size() < column_map.size())) {
        LOG(WARNING) << "insert value number less than column number";
        return {};
    }
    for (int32_t idx = 0; idx < table_info->column_desc_size(); idx++) {
        auto column = table_info->column_desc(idx);
        if (!column_map.empty() && (column_map.count(idx) == 0)) {
            if (column.has_default_value()) {
                auto val = NodeAdapter::StringToData(column.default_value(), column.data_type());
                default_map->insert(std::make_pair(idx, val));
                if (column.data_type() == ::openmldb::type::kVarchar ||
                    column.data_type() == ::openmldb::type::kString) {
                    *str_length += strlen(val->GetStr());
                }
                continue;
            }
            if (!column.not_null()) {
                default_map->emplace(idx, std::make_shared<::hybridse::node::ConstNode>());
                continue;
            }
            LOG(WARNING) << "column " << column.name() << " can't be null";
            return {};
        }

        uint32_t i = idx;
        if (!column_map.empty()) {
            i = column_map.at(idx);
        }
        if (hybridse::node::kExprPrimary != row->children_.at(i)->GetExprType() &&
            hybridse::node::kExprParameter != row->children_.at(i)->GetExprType()) {
            LOG(WARNING) << "insert value isn't const value or placeholder";
            return {};
        }

        if (hybridse::node::kExprPrimary == row->children_.at(i)->GetExprType()) {
            auto* primary = dynamic_cast<::hybridse::node::ConstNode*>(row->children_.at(i));
            std::shared_ptr<::hybridse::node::ConstNode> val;
            if (primary->IsNull()) {
                if (column.not_null()) {
                    LOG(WARNING) << "column " << column.name() << " can't be null";
                    return {};
                }
                val = std::make_shared<::hybridse::node::ConstNode>(*primary);
            } else {
                val = NodeAdapter::TransformDataType(*primary, column.data_type());
                if (!val) {
                    LOG(WARNING) << "default value type mismatch, column " << column.name();
                    return {};
                }
            }
            default_map->insert(std::make_pair(idx, val));
            if (!primary->IsNull() &&
                (column.data_type() == ::openmldb::type::kVarchar || column.data_type() == ::openmldb::type::kString)) {
                *str_length += strlen(primary->GetStr());
            }
        }
    }
    return default_map;
}
// Get Cache with given db, sql and engine mode
std::shared_ptr<SQLCache> SQLClusterRouter::GetCache(const std::string& db, const std::string& sql,
                                                     const hybridse::vm::EngineMode engine_mode) {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    auto mode_cache_it = input_lru_cache_.find(db);
    if (mode_cache_it == input_lru_cache_.end()) {
        return {};
    }

    auto it = mode_cache_it->second.find(engine_mode);
    if (it != mode_cache_it->second.end()) {
        auto value = it->second.get(sql);
        if (value != boost::none) {
            // Check cache validation, the name is the same, but the tid may be different.
            // Notice that we won't check it when table_info is disabled and router is enabled.
            //  invalid router info doesn't have tid, so it won't get confused.
            auto cached_info = value.value();
            if (cached_info) {
                if (!cached_info->GetTableName().empty()) {
                    auto current_info =
                        cluster_sdk_->GetTableInfo(cached_info->GetDatabase(), cached_info->GetTableName());
                    if (!current_info || cached_info->GetTableId() != current_info->tid()) {
                        // just leave, this invalid value will be updated by SetCache()
                        return {};
                    }
                }
                return cached_info;
            }
        }
    }
    return {};
}

void SQLClusterRouter::SetCache(const std::string& db, const std::string& sql,
                                const hybridse::vm::EngineMode engine_mode,
                                const std::shared_ptr<SQLCache>& router_cache) {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    auto it = input_lru_cache_.find(db);
    if (it == input_lru_cache_.end()) {
        decltype(input_lru_cache_)::mapped_type db_value;
        input_lru_cache_.insert(std::make_pair(db, db_value));
        it = input_lru_cache_.find(db);
    }

    auto cache_it = it->second.find(engine_mode);
    if (cache_it == it->second.end()) {
        decltype(it->second)::mapped_type value(options_->max_sql_cache_size);
        auto pair = it->second.emplace(engine_mode, value);
        cache_it = pair.first;
    }
    cache_it->second.upsert(sql, router_cache);
}

std::shared_ptr<SQLInsertRows> SQLClusterRouter::GetInsertRows(const std::string& db, const std::string& sql,
                                                               ::hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    std::shared_ptr<InsertSQLCache> insert_cache;
    if (cache) {
        insert_cache = std::dynamic_pointer_cast<InsertSQLCache>(cache);
        if (insert_cache) {
            status->SetOK();
            return std::make_shared<SQLInsertRows>(insert_cache->GetTableInfo(), insert_cache->GetSchema(),
                                                   insert_cache->GetDefaultValue(), insert_cache->GetStrLength(),
                                                   insert_cache->GetHoleIdxArr(), insert_cache->IsPutIfAbsent());
        }
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    std::vector<uint32_t> stmt_column_idx_arr;
    bool put_if_absent = false;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map, &str_length, &stmt_column_idx_arr, &put_if_absent)) {
        return {};
    }
    auto col_schema = openmldb::schema::SchemaAdapter::ConvertSchema(table_info->column_desc());
    insert_cache = std::make_shared<InsertSQLCache>(
        table_info, col_schema, default_map, str_length,
        SQLInsertRow::GetHoleIdxArr(default_map, stmt_column_idx_arr, col_schema), put_if_absent);
    SetCache(db, sql, hybridse::vm::kBatchMode, insert_cache);
    return std::make_shared<SQLInsertRows>(table_info, insert_cache->GetSchema(), default_map, str_length,
                                           insert_cache->GetHoleIdxArr(), insert_cache->IsPutIfAbsent());
}

bool SQLClusterRouter::ExecuteDDL(const std::string& db, const std::string& sql, hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "no nameserver exist");
        return false;
    }
    // TODO(wangtaize) update ns client to thread safe

    // parse sql to judge whether is create procedure case
    hybridse::node::NodeManager node_manager;
    DLOG(INFO) << "start to execute script from dbms:\n" << sql;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);
    if (plan_trees.empty() || sql_status.code != 0) {
        COPY_PREPEND_AND_WARN(status, sql_status, "create logic plan failed");
        return false;
    }
    hybridse::node::PlanNode* node = plan_trees[0];
    base::Status ret;
    if (node->GetType() == hybridse::node::kPlanTypeCreateSp) {
        ret = HandleSQLCreateProcedure(dynamic_cast<hybridse::node::CreateProcedurePlanNode*>(node), db, sql, ns_ptr);
    } else if (node->GetType() == hybridse::node::kPlanTypeCreate) {
        ret = HandleSQLCreateTable(dynamic_cast<hybridse::node::CreatePlanNode*>(node), db, ns_ptr);
    } else {
        HandleSQLCmd(dynamic_cast<hybridse::node::CmdPlanNode*>(node), db, status);
        ret = {status->code, status->msg};
    }
    if (!ret.OK()) {
        APPEND_FROM_BASE_AND_WARN(status, ret, "fail to execute " + sql);
        return false;
    }
    return true;
}

bool SQLClusterRouter::ShowDB(std::vector<std::string>* dbs, hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "no nameserver exist");
        return false;
    }
    std::string err;
    bool ok = ns_ptr->ShowDatabase(dbs, err);
    if (!ok) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to show databases: " + err);
        return false;
    }
    return true;
}

// get all table names in all DB
std::vector<std::string> SQLClusterRouter::GetAllTables() { return cluster_sdk_->GetAllTables(); }

bool SQLClusterRouter::CreateDB(const std::string& db, hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    // We use hybridse parser to check db name, to ensure syntactic consistency.
    if (db.empty() || !CheckSQLSyntax("CREATE DATABASE `" + db + "`;")) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "db name(" + db + ") is invalid");
        return false;
    }

    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "no nameserver exist");
        return false;
    }

    std::string err;
    bool ok = ns_ptr->CreateDatabase(db, err);
    if (!ok) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to create db " + db + " -- " + err);
        return false;
    }
    status->code = 0;
    return true;
}

bool SQLClusterRouter::DropDB(const std::string& db, hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (db.empty() || !CheckSQLSyntax("DROP DATABASE `" + db + "`;")) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "db name(" + db + ") is invalid");
        return false;
    }

    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "no nameserver exist");
        return false;
    }
    std::string err;
    bool ok = ns_ptr->DropDatabase(db, err);
    if (!ok) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to drop db " + db + " -- " + err);
        return false;
    }
    return true;
}

bool SQLClusterRouter::DropTable(const std::string& db, const std::string& table, const bool if_exists,
                                 hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (db.empty() || table.empty()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                            "db name(" + db + ") or table name(" + table + ") is invalid");
        return false;
    }

    // RefreshCatalog to avoid getting out-of-date table info
    if (!RefreshCatalog()) {
        SET_STATUS_AND_WARN(status, StatusCode::kRuntimeError, "Fail to refresh catalog");
        return false;
    }

    auto table_info = cluster_sdk_->GetTableInfo(db, table);

    if (table_info == nullptr) {
        if (if_exists) {
            *status = {};
            return true;
        } else {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to drop, table does not exist!");
            return false;
        }
    }

    // delete pre-aggr meta info if need
    if (table_info->base_table_tid() > 0) {
        std::string meta_db = openmldb::nameserver::INTERNAL_DB;
        std::string meta_table = openmldb::nameserver::PRE_AGG_META_NAME;
        std::string select_aggr_info =
            absl::StrCat("select base_db,base_table,aggr_func,aggr_col,partition_cols,order_by_col,filter_col from ",
                         meta_db, ".", meta_table, " where aggr_table = '", table_info->name(), "';");
        auto rs = ExecuteSQL("", select_aggr_info, true, true, 0, status);
        WARN_NOT_OK_AND_RET(status, "get aggr info failed", false);
        if (rs->Size() != 1) {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                                "duplicate records generate with aggr table name: " + table_info->name());
            return false;
        }
        std::string idx_key;
        if (rs->Next()) {
            for (int i = 0; i < rs->GetSchema()->GetColumnCnt(); i++) {
                if (!idx_key.empty()) {
                    idx_key += "|";
                }
                auto k = rs->GetAsStringUnsafe(i);
                if (k.empty()) {
                    idx_key += hybridse::codec::EMPTY_STRING;
                } else {
                    idx_key += k;
                }
            }
        } else {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "access ResultSet failed");
            return false;
        }
        auto tablet_accessor = cluster_sdk_->GetTablet(meta_db, meta_table, (uint32_t)0);
        if (!tablet_accessor) {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "get tablet accessor failed");
            return false;
        }
        auto tablet_client = tablet_accessor->GetClient();
        if (!tablet_client) {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "get tablet client failed");
            return false;
        }
        auto tid = cluster_sdk_->GetTableId(meta_db, meta_table);
        std::string msg;
        if (!tablet_client->Delete(tid, 0, table_info->name(), "aggr_table", msg) ||
            !tablet_client->Delete(tid, 0, idx_key, "unique_key", msg)) {
            SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "delete aggr meta failed");
            return false;
        }
    }

    // Check offline table info first
    if (table_info->has_offline_table_info()) {
        auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
        if (!taskmanager_client_ptr) {
            SET_STATUS_AND_WARN(status, StatusCode::kRuntimeError, "no taskmanager client");
            return false;
        }

        ::openmldb::base::Status rpcStatus = taskmanager_client_ptr->DropOfflineTable(db, table, GetJobTimeout());
        if (rpcStatus.code != 0) {
            APPEND_FROM_BASE_AND_WARN(status, rpcStatus, "drop offline table failed");
            return false;
        }
    }

    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        SET_STATUS_AND_WARN(status, StatusCode::kRuntimeError, "no ns client");
        return false;
    }
    std::string err;
    bool ok = ns_ptr->DropTable(db, table, err);
    if (!ok) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to drop, " + err);
        return false;
    }
    return true;
}

/**
 * Get SQL cache
 * @param db
 * @param sql
 * @param engine_mode
 * @param row
 * @param parameter
 * @return
 */
std::shared_ptr<SQLCache> SQLClusterRouter::GetSQLCache(const std::string& db, const std::string& sql,
                                                        const ::hybridse::vm::EngineMode engine_mode,
                                                        const std::shared_ptr<SQLRequestRow>& parameter,
                                                        hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    ::hybridse::codec::Schema parameter_schema_raw;
    if (parameter) {
        for (int i = 0; i < parameter->GetSchema()->GetColumnCnt(); i++) {
            auto column = parameter_schema_raw.Add();
            hybridse::type::Type hybridse_type;
            if (!openmldb::schema::SchemaAdapter::ConvertType(parameter->GetSchema()->GetColumnType(i),
                                                              &hybridse_type)) {
                SET_STATUS_AND_WARN(
                    status, StatusCode::kCmdError,
                    "convert failed -- invalid parameter type " + parameter->GetSchema()->GetColumnType(i));
                return {};
            }
            column->set_type(hybridse_type);
        }
    }
    auto router_cache = std::dynamic_pointer_cast<RouterSQLCache>(GetCache(db, sql, engine_mode));
    auto parameter_schema = std::make_shared<::hybridse::sdk::SchemaImpl>(parameter_schema_raw);
    if (router_cache && router_cache->IsCompatibleCache(parameter_schema)) {
        router_cache.reset();
    }
    if (!router_cache) {
        ::hybridse::vm::ExplainOutput explain;
        ::hybridse::base::Status base_status;
        if (cluster_sdk_->GetEngine()->Explain(sql, db, engine_mode, parameter_schema_raw, &explain, &base_status)) {
            std::shared_ptr<::hybridse::sdk::SchemaImpl> schema;
            const std::string& main_db = explain.router.GetMainDb().empty() ? db : explain.router.GetMainDb();
            uint32_t tid = 0;
            std::string table_name;
            if (!explain.input_schema.empty()) {
                schema = std::make_shared<::hybridse::sdk::SchemaImpl>(explain.input_schema);
            } else {
                const std::string& main_table = explain.router.GetMainTable();
                auto table_info = cluster_sdk_->GetTableInfo(main_db, main_table);
                ::hybridse::codec::Schema raw_schema;
                if (table_info) {
                    if (::openmldb::schema::SchemaAdapter::ConvertSchema(table_info->column_desc(), &raw_schema)) {
                        schema = std::make_shared<::hybridse::sdk::SchemaImpl>(raw_schema);
                    }
                    tid = table_info->tid();
                }
            }
            router_cache =
                std::make_shared<RouterSQLCache>(main_db, tid, table_name, schema, parameter_schema, explain.router);
            SetCache(db, sql, engine_mode, router_cache);
        } else {
            COPY_PREPEND_AND_WARN(status, base_status, "fail to explain " + sql);
            return {};
        }
    }
    return router_cache;
}
std::shared_ptr<::openmldb::client::TabletClient> SQLClusterRouter::GetTabletClient(
    const std::string& db, const std::string& sql, const ::hybridse::vm::EngineMode engine_mode,
    const std::shared_ptr<SQLRequestRow>& row, hybridse::sdk::Status* status) {
    return GetTabletClient(db, sql, engine_mode, row, std::shared_ptr<openmldb::sdk::SQLRequestRow>(), status);
}
std::shared_ptr<::openmldb::client::TabletClient> SQLClusterRouter::GetTabletClient(
    const std::string& db, const std::string& sql, const ::hybridse::vm::EngineMode engine_mode,
    const std::shared_ptr<SQLRequestRow>& row, const std::shared_ptr<openmldb::sdk::SQLRequestRow>& parameter,
    hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    auto cache = GetSQLCache(db, sql, engine_mode, parameter, status);
    WARN_NOT_OK_AND_RET(status, "sql plan failed(get/create cache failed)", nullptr);
    std::shared_ptr<::openmldb::catalog::TabletAccessor> tablet;
    if (cache) {
        auto router_cache = std::dynamic_pointer_cast<RouterSQLCache>(cache);
        if (router_cache) {
            const auto& router = router_cache->GetRouter();
            const std::string& col = router.GetRouterCol();
            const std::string& main_table = router.GetMainTable();
            const std::string main_db = router.GetMainDb().empty() ? db : router.GetMainDb();
            if (!main_table.empty()) {
                DLOG(INFO) << "get main table" << main_table;
                std::string val;
                if (!col.empty() && row && row->GetRecordVal(col, &val)) {
                    tablet = cluster_sdk_->GetTablet(main_db, main_table, val);
                }
                if (!tablet) {
                    tablet = cluster_sdk_->GetTablet(main_db, main_table);
                }
            }
        }
    }
    if (!tablet) {
        tablet = cluster_sdk_->GetTablet();
    }
    if (!tablet) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to get tablet");
        return {};
    }
    return tablet->GetClient();
}

// Get clients when online batch query in Cluster OpenMLDB
std::shared_ptr<::openmldb::client::TabletClient> SQLClusterRouter::GetTabletClientForBatchQuery(
    const std::string& db, const std::string& sql, const std::shared_ptr<SQLRequestRow>& parameter,
    hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    auto cache = GetSQLCache(db, sql, hybridse::vm::kBatchMode, parameter, status);
    WARN_NOT_OK_AND_RET(status, "sql plan failed(get/create cache failed)", nullptr);
    if (!cache) {
        SET_STATUS_AND_WARN(status, StatusCode::kRuntimeError, "get sql cache but it's nullptr");
        return {};
    }

    auto router_cache = std::dynamic_pointer_cast<RouterSQLCache>(cache);
    if (!router_cache) {
        SET_STATUS_AND_WARN(status, StatusCode::kRuntimeError, "cast to RouterSQLCache failed");
        return {};
    }

    const auto& router = router_cache->GetRouter();
    const std::string& main_table = router.GetMainTable();
    const std::string main_db = router.GetMainDb().empty() ? db : router.GetMainDb();
    if (!main_table.empty()) {
        DLOG(INFO) << "get main table " << main_table;
        auto tablet_accessor = cluster_sdk_->GetTablet(main_db, main_table);
        if (tablet_accessor) {
            *status = {};
            return tablet_accessor->GetClient();
        } else {
            SET_STATUS_AND_WARN(status, StatusCode::kRuntimeError,
                                absl::StrCat("main table ", main_db, ".", main_table, " tablet accessor is null"));
        }
    } else {
        auto tablet_accessor = cluster_sdk_->GetTablet();
        if (tablet_accessor) {
            *status = {};
            return tablet_accessor->GetClient();
        } else {
            SET_STATUS_AND_WARN(status, StatusCode::kRuntimeError, "random tablet accessor is null");
        }
    }
    return {};
}

std::shared_ptr<TableReader> SQLClusterRouter::GetTableReader() {
    return std::make_shared<TableReaderImpl>(cluster_sdk_);
}

std::shared_ptr<openmldb::client::TabletClient> SQLClusterRouter::GetTablet(const std::string& db,
                                                                            const std::string& sp_name,
                                                                            const std::string& router_col,
                                                                            hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    auto sp_info = cluster_sdk_->GetProcedureInfo(db, sp_name, &status->msg);
    if (!sp_info) {
        CODE_PREPEND_AND_WARN(status, StatusCode::kCmdError, "procedure not found");
        return nullptr;
    }
    const std::string& table = sp_info->GetMainTable();
    const std::string& db_name = sp_info->GetMainDb().empty() ? db : sp_info->GetMainDb();
    std::shared_ptr<::openmldb::catalog::TabletAccessor> tablet;
    if (router_col.empty()) {
        tablet = cluster_sdk_->GetTablet(db_name, table);
    } else {
        tablet = cluster_sdk_->GetTablet(db_name, table, router_col);
    }
    if (!tablet) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to get tablet, table " + db_name + "." + table);
        return nullptr;
    }
    return tablet->GetClient();
}

bool SQLClusterRouter::IsConstQuery(::hybridse::vm::PhysicalOpNode* node) {
    if (node->GetOpType() == ::hybridse::vm::kPhysicalOpConstProject) {
        return true;
    }

    if (node->GetProducerCnt() <= 0) {
        return false;
    }

    for (size_t i = 0; i < node->GetProducerCnt(); i++) {
        if (!IsConstQuery(node->GetProducer(i))) {
            return false;
        }
    }
    return true;
}
void SQLClusterRouter::GetTables(::hybridse::vm::PhysicalOpNode* node, std::set<std::string>* tables) {
    if (node == NULL || tables == NULL) return;
    if (node->GetOpType() == ::hybridse::vm::kPhysicalOpDataProvider) {
        ::hybridse::vm::PhysicalDataProviderNode* data_node =
            reinterpret_cast<::hybridse::vm::PhysicalDataProviderNode*>(node);
        if (data_node->provider_type_ == ::hybridse::vm::kProviderTypeTable ||
            data_node->provider_type_ == ::hybridse::vm::kProviderTypePartition) {
            tables->insert(data_node->table_handler_->GetName());
        }
    }
    if (node->GetProducerCnt() <= 0) return;
    for (size_t i = 0; i < node->GetProducerCnt(); i++) {
        GetTables(node->GetProducer(i), tables);
    }
}
std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQLRequest(const std::string& db,
                                                                              const std::string& sql,
                                                                              std::shared_ptr<SQLRequestRow> row,
                                                                              hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (!row || !row->OK()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "make sure the request row is built before execute sql");
        return {};
    }
    auto cntl = std::make_shared<::brpc::Controller>();
    cntl->set_timeout_ms(options_->request_timeout);
    auto response = std::make_shared<::openmldb::api::QueryResponse>();
    auto client = GetTabletClient(db, sql, hybridse::vm::kRequestMode, row, status);
    if (0 != status->code) {
        return {};
    }
    if (!client) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "tablet client not found");
        return {};
    }
    if (!client->Query(db, sql, row->GetRow(), cntl.get(), response.get(), options_->enable_debug) ||
        response->code() != ::openmldb::base::kOk) {
        RPC_STATUS_AND_WARN(status, cntl, response, "Query request rpc failed");
        return {};
    }

    auto rs = ResultSetSQL::MakeResultSet(response, cntl, status);
    return rs;
}

std::shared_ptr<::hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQLParameterized(
    const std::string& db, const std::string& sql, std::shared_ptr<openmldb::sdk::SQLRequestRow> parameter,
    ::hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    std::vector<openmldb::type::DataType> parameter_types;
    if (parameter && !ExtractDBTypes(parameter->GetSchema(), &parameter_types)) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "convert parameter types error");
        return {};
    }
    auto client = GetTabletClientForBatchQuery(db, sql, parameter, status);
    if (!status->IsOK() || !client) {
        status->Prepend("get tablet client failed");
        return {};
    }
    auto cntl = std::make_shared<::brpc::Controller>();
    cntl->set_timeout_ms(options_->request_timeout);
    DLOG(INFO) << "send query to tablet " << client->GetEndpoint();
    auto response = std::make_shared<::openmldb::api::QueryResponse>();
    if (!client->Query(db, sql, parameter_types, parameter ? parameter->GetRow() : "", cntl.get(), response.get(),
                       options_->enable_debug)) {
        // rpc error is in cntl or response
        RPC_STATUS_AND_WARN(status, cntl, response, "Query rpc failed");
        return {};
    }
    return ResultSetSQL::MakeResultSet(response, cntl, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQLBatchRequest(
    const std::string& db, const std::string& sql, std::shared_ptr<SQLRequestRowBatch> row_batch,
    hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (!row_batch) {
        LOG(WARNING) << "input row_batch is nullptr";
        return nullptr;
    }
    auto cntl = std::make_shared<::brpc::Controller>();
    cntl->set_timeout_ms(options_->request_timeout);
    auto response = std::make_shared<::openmldb::api::SQLBatchRequestQueryResponse>();
    auto client = GetTabletClient(db, sql, hybridse::vm::kBatchRequestMode, std::shared_ptr<SQLRequestRow>(),
                                  std::shared_ptr<SQLRequestRow>(), status);
    if (0 != status->code) {
        return nullptr;
    }
    if (!client) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "GetTabletClient ok but tablet client is null");
        return nullptr;
    }
    if (!client->SQLBatchRequestQuery(db, sql, row_batch, cntl.get(), response.get(), options_->enable_debug) ||
        response->code() != ::openmldb::base::kOk) {
        RPC_STATUS_AND_WARN(status, cntl, response, "SQLBatchRequestQuery rpc failed");
        return nullptr;
    }

    auto rs = std::make_shared<openmldb::sdk::SQLBatchRequestResultSet>(response, cntl);
    if (!rs->Init()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "Batch request result set init fail");
        return nullptr;
    }
    return rs;
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db, const std::string& sql, ::hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "status is nullptr");

    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    std::vector<DefaultValueMap> default_maps;
    std::vector<uint32_t> str_lengths;
    bool put_if_absent = false;
    std::vector<std::shared_ptr<int8_t>> codegen_rows;
    if (!GetMultiRowInsertInfo(db, sql, status, &table_info, &default_maps, &str_lengths, &put_if_absent,
                               &codegen_rows)) {
        CODE_PREPEND_AND_WARN(status, StatusCode::kCmdError, "Fail to get insert info");
        return false;
    }

    auto schema = ::openmldb::schema::SchemaAdapter::ConvertSchema(table_info->column_desc());
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
    bool ret = cluster_sdk_->GetTablet(table_info->db(), table_info->name(), &tablets);
    if (!ret || tablets.empty()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                            "Fail to execute insert statement: fail to get " + table_info->name() + " tablets");
        return false;
    }

    std::vector<size_t> fails;
    if (!codegen_rows.empty()) {
        for (size_t i = 0 ; i < codegen_rows.size(); ++i) {
            auto r = codegen_rows[i];
            auto row = std::make_shared<SQLInsertRow>(table_info, schema, r, put_if_absent);
            if (!PutRow(table_info->tid(), row, tablets, status)) {
                LOG(WARNING) << "fail to put row["
                             << "] due to: " << status->msg;
                fails.push_back(i);
                continue;
            }
        }
    } else {
        for (size_t i = 0; i < default_maps.size(); i++) {
            auto row =
                std::make_shared<SQLInsertRow>(table_info, schema, default_maps[i], str_lengths[i], put_if_absent);
            if (!row || !row->Init(0) || !row->IsComplete()) {
                // TODO(hw): SQLInsertRow or DefaultValueMap needs print helper function
                LOG(WARNING) << "fail to build row[" << i << "]";
                fails.push_back(i);
                continue;
            }
            if (!PutRow(table_info->tid(), row, tablets, status)) {
                LOG(WARNING) << "fail to put row[" << i << "] due to: " << status->msg;
                fails.push_back(i);
                continue;
            }
        }
    }
    if (!fails.empty()) {
        auto ori_size = fails.size();
        // for peek
        absl::Span<const size_t> slice(fails.data(), ori_size > 10 ? 10 : ori_size);
        SET_STATUS_AND_WARN(
            status, StatusCode::kCmdError,
            absl::StrCat("insert values ", ori_size, " failed, failed rows peek: ", absl::StrJoin(slice, ",")));
        return false;
    }
    return true;
}

bool SQLClusterRouter::PutRow(uint32_t tid, const std::shared_ptr<SQLInsertRow>& row,
                              const std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>>& tablets,
                              ::hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    const auto& dimensions = row->GetDimensions();
    uint64_t cur_ts = ::baidu::common::timer::get_micros() / 1000;
    for (const auto& kv : dimensions) {
        uint32_t pid = kv.first;
        if (pid < tablets.size()) {
            auto tablet = tablets[pid];
            if (tablet) {
                auto client = tablet->GetClient();
                if (client) {
                    DLOG(INFO) << "put data to endpoint " << client->GetEndpoint() << " with dimensions size "
                               << kv.second.size();
                    auto ret =
                        client->Put(tid, pid, cur_ts, row->GetRow(), kv.second,
                                    insert_memory_usage_limit_.load(std::memory_order_relaxed), row->IsPutIfAbsent());
                    if (!ret.OK()) {
                        if (RevertPut(row->GetTableInfo(), pid, dimensions, cur_ts, base::Slice(row->GetRow()), tablets)
                                .IsOK()) {
                            SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                                                absl::StrCat("INSERT failed, tid ", tid));
                        } else {
                            SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                                                "INSERT failed, tid " + std::to_string(tid) +
                                                    ". Note that data might have been partially inserted. "
                                                    "You are encouraged to perform DELETE to remove any partially "
                                                    "inserted data before trying INSERT again.");
                        }
                        return false;
                    }
                    continue;
                }
            }
        }

        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to get tablet client. pid " + std::to_string(pid));
        return false;
    }
    return true;
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db, const std::string& sql, std::shared_ptr<SQLInsertRows> rows,
                                     hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (!rows) {
        LOG(WARNING) << "input rows is nullptr";
        return false;
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    if (cache) {
        std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
        bool ret = cluster_sdk_->GetTablet(db, cache->GetTableName(), &tablets);
        if (!ret || tablets.empty()) {
            status->msg = "fail to get table " + cache->GetTableName() + " tablet";
            return false;
        }
        for (uint32_t i = 0; i < rows->GetCnt(); ++i) {
            std::shared_ptr<SQLInsertRow> row = rows->GetRow(i);
            if (!PutRow(cache->GetTableId(), row, tablets, status)) {
                return false;
            }
        }
        return true;
    } else {
        status->msg = "please use getInsertRow with " + sql + " first";
        return false;
    }
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db, const std::string& sql, std::shared_ptr<SQLInsertRow> row,
                                     hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (!row) {
        LOG(WARNING) << "input row is nullptr";
        return false;
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    if (cache) {
        std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
        bool ret = cluster_sdk_->GetTablet(db, cache->GetTableName(), &tablets);
        if (!ret || tablets.empty()) {
            status->msg = "fail to get table " + cache->GetTableName() + " tablet";
            return false;
        }
        if (!PutRow(cache->GetTableId(), row, tablets, status)) {
            return false;
        }
        return true;
    } else {
        status->msg = "please use getInsertRow with " + sql + " first";
        return false;
    }
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db, const std::string& name, int tid, int partition_num,
                                     hybridse::sdk::ByteArrayPtr dimension, int dimension_len,
                                     hybridse::sdk::ByteArrayPtr value, int len, bool put_if_absent,
                                     hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (dimension == nullptr || dimension_len <= 0 || value == nullptr || len <= 0 || partition_num <= 0) {
        *status = {StatusCode::kCmdError, "invalid parameter"};
        return false;
    }
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
    bool ret = cluster_sdk_->GetTablet(db, name, &tablets);
    if (!ret || tablets.empty()) {
        status->msg = "fail to get table " + name + " tablet";
        return false;
    }
    std::map<uint32_t, ::google::protobuf::RepeatedPtrField<::openmldb::api::Dimension>> dimensions_map;
    int pos = 0;
    while (pos < dimension_len) {
        int idx = *(reinterpret_cast<int*>(dimension + pos));
        pos += sizeof(int);
        int key_len = *(reinterpret_cast<int*>(dimension + pos));
        pos += sizeof(int);
        base::Slice key(dimension + pos, key_len);
        uint32_t pid = static_cast<uint32_t>(::openmldb::base::hash64(key.data(), key.size()) % partition_num);
        auto it = dimensions_map.find(pid);
        if (it == dimensions_map.end()) {
            it = dimensions_map.emplace(pid, ::google::protobuf::RepeatedPtrField<::openmldb::api::Dimension>()).first;
        }
        auto dim = it->second.Add();
        dim->set_idx(idx);
        dim->set_key(key.data(), key.size());
        pos += key_len;
    }
    base::Slice row_value(value, len);
    uint64_t cur_ts = ::baidu::common::timer::get_micros() / 1000;
    for (auto& kv : dimensions_map) {
        uint32_t pid = kv.first;
        if (pid < tablets.size()) {
            auto tablet = tablets[pid];
            if (tablet) {
                auto client = tablet->GetClient();
                if (client) {
                    DLOG(INFO) << "put data to endpoint " << client->GetEndpoint() << " with dimensions size "
                               << kv.second.size();
                    auto ret = client->Put(tid, pid, cur_ts, row_value, &kv.second,
                                           insert_memory_usage_limit_.load(std::memory_order_relaxed), put_if_absent);
                    if (!ret.OK()) {
                        // TODO(hw): show put failed row(readable)? ::hybridse::codec::RowView::GetRowString?
                        SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                                            "INSERT failed, tid " + std::to_string(tid) +
                                                ". Note that data might have been partially inserted. "
                                                "You are encouraged to perform DELETE to remove any partially "
                                                "inserted data before trying INSERT again.");
                        std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>> dimensions;
                        for (const auto& val : dimensions_map) {
                            std::vector<std::pair<std::string, uint32_t>> vec;
                            for (const auto& data : val.second) {
                                vec.emplace_back(data.key(), data.idx());
                            }
                            dimensions.emplace(val.first, std::move(vec));
                        }
                        auto table_info = cluster_sdk_->GetTableInfo(db, name);
                        if (!table_info) {
                            return false;
                        }
                        // TODO(hw): better to return absl::Status
                        if (RevertPut(*table_info, pid, dimensions, cur_ts, row_value, tablets).IsOK()) {
                            SET_STATUS_AND_WARN(status, StatusCode::kCmdError,
                                                absl::StrCat("INSERT failed, tid ", tid));
                        }
                        return false;
                    }
                    continue;
                }
            }
        }
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "fail to get tablet client. pid " + std::to_string(pid));
        return false;
    }
    return true;
}

bool SQLClusterRouter::GetSQLPlan(const std::string& sql, ::hybridse::node::NodeManager* nm,
                                  ::hybridse::node::PlanNodeList* plan) {
    if (nm == NULL || plan == NULL) return false;
    ::hybridse::base::Status sql_status;
    PlanAPI::CreatePlanTreeFromScript(sql, *plan, nm, sql_status);
    if (0 != sql_status.code) {
        LOG(WARNING) << "create logic plan failed, [" << sql_status.code << "] " << sql_status.msg;
        return false;
    }
    return true;
}

bool SQLClusterRouter::RefreshCatalog() { return cluster_sdk_->Refresh(); }

std::shared_ptr<ExplainInfo> SQLClusterRouter::Explain(const std::string& db, const std::string& sql,
                                                       ::hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    ::hybridse::vm::ExplainOutput explain_output;
    ::hybridse::base::Status vm_status;
    ::hybridse::codec::Schema parameter_schema;
    bool ok = cluster_sdk_->GetEngine()->Explain(sql, db, ::hybridse::vm::kRequestMode, parameter_schema,
                                                 &explain_output, &vm_status);
    if (!ok) {
        COPY_PREPEND_AND_WARN(status, vm_status, "fail to explain " + sql);
        return std::shared_ptr<ExplainInfo>();
    }
    ::hybridse::sdk::SchemaImpl input_schema(explain_output.input_schema);
    ::hybridse::sdk::SchemaImpl output_schema(explain_output.output_schema);

    return std::make_shared<ExplainInfoImpl>(input_schema, output_schema, explain_output.logical_plan,
                                             explain_output.physical_plan, explain_output.ir,
                                             explain_output.request_db_name, explain_output.request_name);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::CallProcedure(const std::string& db,
                                                                          const std::string& sp_name,
                                                                          std::shared_ptr<SQLRequestRow> row,
                                                                          hybridse::sdk::Status* status) {
    if (!row || !row->OK()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "make sure the request row is built before execute sql");
        return nullptr;
    }
    return CallProcedure(db, sp_name, base::Slice(row->GetRow()), "", status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::CallProcedure(const std::string& db,
                                                                          const std::string& sp_name,
                                                                          hybridse::sdk::ByteArrayPtr buf, int len,
                                                                          const std::string& router_col,
                                                                          hybridse::sdk::Status* status) {
    if (buf == nullptr || len == 0) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "invalid request row data");
        return nullptr;
    }
    return CallProcedure(db, sp_name, base::Slice(buf, len), router_col, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::CallProcedure(const std::string& db,
                                                                          const std::string& sp_name,
                                                                          const base::Slice& row,
                                                                          const std::string& router_col,
                                                                          hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    auto tablet = GetTablet(db, sp_name, router_col, status);
    if (!tablet) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "cannot get tablet");
        return nullptr;
    }

    auto cntl = std::make_shared<::brpc::Controller>();
    auto response = std::make_shared<::openmldb::api::QueryResponse>();
    bool ok = tablet->CallProcedure(db, sp_name, row, cntl.get(), response.get(), options_->enable_debug,
                                    options_->request_timeout);
    if (!ok || response->code() != ::openmldb::base::kOk) {
        RPC_STATUS_AND_WARN(status, cntl, response, "CallProcedure failed");
        return nullptr;
    }
    return ResultSetSQL::MakeResultSet(response, cntl, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::CallSQLBatchRequestProcedure(
    const std::string& db, const std::string& sp_name, std::shared_ptr<SQLRequestRowBatch> row_batch,
    hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (!row_batch) {
        SET_STATUS_AND_WARN(status, StatusCode::kNullInputPointer, "row_batch is nullptr");
        return nullptr;
    }
    auto tablet = GetTablet(db, sp_name, "", status);
    if (!tablet) {
        return nullptr;
    }

    auto cntl = std::make_shared<::brpc::Controller>();
    auto response = std::make_shared<::openmldb::api::SQLBatchRequestQueryResponse>();
    bool ok = tablet->CallSQLBatchRequestProcedure(db, sp_name, row_batch, cntl.get(), response.get(),
                                                   options_->enable_debug, options_->request_timeout);
    if (!ok || response->code() != ::openmldb::base::kOk) {
        RPC_STATUS_AND_WARN(status, cntl, response, "CallSQLBatchRequestProcedure failed");
        return nullptr;
    }
    auto rs = std::make_shared<::openmldb::sdk::SQLBatchRequestResultSet>(response, cntl);
    if (!rs->Init()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "SQLBatchRequestResultSet init failed");
        return nullptr;
    }
    return rs;
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::CallSQLBatchRequestProcedure(
    const std::string& db, const std::string& sp_name, hybridse::sdk::ByteArrayPtr meta, int meta_len,
    hybridse::sdk::ByteArrayPtr buf, int len, hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (meta == nullptr || meta_len == 0 || buf == nullptr || len == 0) {
        SET_STATUS_AND_WARN(status, StatusCode::kNullInputPointer, "input data is null");
        return nullptr;
    }
    auto tablet = GetTablet(db, sp_name, "", status);
    if (!tablet) {
        return nullptr;
    }

    auto cntl = std::make_shared<::brpc::Controller>();
    auto response = std::make_shared<::openmldb::api::SQLBatchRequestQueryResponse>();
    auto ret = tablet->CallSQLBatchRequestProcedure(db, sp_name, base::Slice(meta, meta_len), base::Slice(buf, len),
                                                    options_->enable_debug, options_->request_timeout, cntl.get(),
                                                    response.get());
    if (!ret.OK()) {
        RPC_STATUS_AND_WARN(status, cntl, response, "CallSQLBatchRequestProcedure failed" + ret.GetMsg());
        return nullptr;
    }
    auto rs = std::make_shared<::openmldb::sdk::SQLBatchRequestResultSet>(response, cntl);
    if (!rs->Init()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "SQLBatchRequestResultSet init failed");
        return nullptr;
    }
    return rs;
}

std::shared_ptr<hybridse::sdk::ProcedureInfo> SQLClusterRouter::ShowProcedure(const std::string& db,
                                                                              const std::string& sp_name,
                                                                              hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info = cluster_sdk_->GetProcedureInfo(db, sp_name, &status->msg);
    if (!sp_info) {
        SET_STATUS_AND_WARN(status, StatusCode::kProcedureNotFound, status->msg);
        return nullptr;
    }
    return sp_info;
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::HandleSQLCmd(const hybridse::node::CmdPlanNode* cmd_node,
                                                                         const std::string& db,
                                                                         ::hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (cmd_node == nullptr) {
        SET_STATUS_AND_WARN(status, StatusCode::kNullInputPointer, "node is nullptr");
        return {};
    }
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "no nameserver exist");
        return {};
    }
    bool ret = true;
    std::string msg;
    switch (cmd_node->GetCmdType()) {
        case hybridse::node::kCmdShowDatabases: {
            std::vector<std::string> dbs;
            auto ok = ns_ptr->ShowDatabase(&dbs, msg);
            if (ok) {
                std::vector<std::vector<std::string>> values;
                for (const auto& val : dbs) {
                    std::vector<std::string> vec = {val};
                    values.emplace_back(std::move(vec));
                }
                return ResultSetSQL::MakeResultSet({"Databases"}, values, status);
            } else {
                *status = {StatusCode::kCmdError, msg};
            }
            return {};
        }

        case hybridse::node::kCmdShowTables: {
            if (db.empty()) {
                *status = {StatusCode::kCmdError, "please enter database first"};
                return {};
            }
            auto tables = cluster_sdk_->GetTables(db);
            std::vector<std::vector<std::string>> values;
            for (auto it = tables.begin(); it != tables.end(); ++it) {
                std::vector<std::string> vec = {(*it)->name()};
                values.emplace_back(std::move(vec));
            }
            return ResultSetSQL::MakeResultSet({"Tables"}, values, status);
        }

        case hybridse::node::kCmdShowUser: {
            std::vector<std::string> value = { options_->user };
            return ResultSetSQL::MakeResultSet({"User"}, {value}, status);
        }

        case hybridse::node::kCmdShowCreateTable: {
            auto& args = cmd_node->GetArgs();
            std::string cur_db = db;
            std::string table_name;
            if (!ParseNamesFromArgs(db, args, &cur_db, &table_name).IsOK()) {
                *status = {StatusCode::kCmdError, msg};
                return {};
            }
            if (cur_db.empty()) {
                *status = {::hybridse::common::StatusCode::kCmdError, "please enter database first"};
                return {};
            }
            auto table = cluster_sdk_->GetTableInfo(cur_db, table_name);
            if (table == nullptr) {
                *status = {StatusCode::kCmdError, "table " + table_name + " does not exist"};
                return {};
            }
            std::string sql = SDKUtil::GenCreateTableSQL(*table);
            std::vector<std::vector<std::string>> values;
            std::vector<std::string> vec = {table_name, sql};
            values.push_back(std::move(vec));
            return ResultSetSQL::MakeResultSet({"Table", "Create Table"}, values, status);
        }

        case hybridse::node::kCmdDescTable: {
            std::string cur_db = db;
            std::string table_name;
            const auto& args = cmd_node->GetArgs();
            if (!ParseNamesFromArgs(db, args, &cur_db, &table_name).IsOK()) {
                *status = {StatusCode::kCmdError, msg};
                return {};
            }
            if (cur_db.empty()) {
                *status = {::hybridse::common::StatusCode::kCmdError, "please enter database first"};
                return {};
            }
            auto table = cluster_sdk_->GetTableInfo(cur_db, table_name);
            if (table == nullptr) {
                *status = {StatusCode::kCmdError, "table " + table_name + " does not exist"};
                return {};
            }
            std::vector<std::vector<std::string>> result;
            std::stringstream ss;
            ::openmldb::cmd::PrintSchema(table->column_desc(), table->added_column_desc(), ss);
            std::vector<std::string> vec = {ss.str()};
            result.emplace_back(std::move(vec));
            ss.str("");
            ::openmldb::cmd::PrintColumnKey(table->column_key(), ss);
            std::vector<std::string> vec1 = {ss.str()};
            result.emplace_back(std::move(vec1));
            if (table->has_offline_table_info()) {
                ss.str("");
                ::openmldb::cmd::PrintOfflineTableInfo(table->offline_table_info(), ss);
                std::vector<std::string> vec2 = {ss.str()};
                result.emplace_back(std::move(vec2));
            }
            ss.str("");
            std::unordered_map<std::string, std::string> options;
            std::string storage_mode = StorageMode_Name(table->storage_mode());
            // remove the prefix 'k', i.e., change kMemory to Memory
            options["storage_mode"] = storage_mode.substr(1, storage_mode.size() - 1);
            std::string compress_type = CompressType_Name(table->compress_type());
            options["compress_type"] = compress_type.substr(1, compress_type.size() - 1);
            ::openmldb::cmd::PrintTableOptions(options, ss);
            result.emplace_back(std::vector{ss.str()});
            return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, result, status);
        }

        case hybridse::node::kCmdCreateDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            if (ns_ptr->CreateDatabase(name, msg, cmd_node->IsIfNotExists())) {
                *status = {};
            } else {
                *status = {StatusCode::kCmdError, "Create database failed for " + msg};
            }
            return {};
        }
        case hybridse::node::kCmdUseDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            if (ns_ptr->Use(name, msg)) {
                db_ = name;
                *status = {::hybridse::common::kOk, "Database changed"};
            } else {
                *status = {StatusCode::kCmdError, "Use database failed for " + msg};
            }
            return {};
        }
        case hybridse::node::kCmdDropDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            if (ns_ptr->DropDatabase(name, msg, cmd_node->IsIfExists())) {
                *status = {};
            } else {
                *status = {StatusCode::kCmdError, msg};
            }
            return {};
        }
        case hybridse::node::kCmdDropUser: {
            std::string name = cmd_node->GetArgs()[0];
            if (cmd_node->IsIfExists()) {
                *status = DeleteUser(name);
            } else {
                UserInfo user_info;
                auto result = GetUser(name, &user_info);
                if (!result.ok()) {
                    *status = {StatusCode::kCmdError, result.status().message()};
                } else if (!(*result)) {
                    *status = {StatusCode::kCmdError, absl::StrCat("user ", name, " does not exist")};
                } else {
                    *status = DeleteUser(name);
                }
            }
            return {};
        }
        case hybridse::node::kCmdShowFunctions: {
            std::vector<::openmldb::common::ExternalFun> funs;
            base::Status st = ns_ptr->ShowFunction("", &funs);
            if (!st.OK()) {
                APPEND_FROM_BASE_AND_WARN(status, st, "show udf function failed");
                return {};
            }
            std::vector<std::vector<std::string>> lines;
            for (auto& fun_info : funs) {
                std::string is_aggregate = fun_info.is_aggregate() ? "true" : "false";
                std::string return_nullable = fun_info.return_nullable() ? "true" : "false";
                std::string arg_nullable = fun_info.arg_nullable() ? "true" : "false";
                std::string arg_type = "";
                for (int i = 0; i < fun_info.arg_type_size(); i++) {
                    arg_type = absl::StrCat(arg_type, openmldb::type::DataType_Name(fun_info.arg_type(i)).substr(1));
                    if (i != fun_info.arg_type_size() - 1) {
                        arg_type = absl::StrCat(arg_type, "|");
                    }
                }
                std::vector<std::string> vec = {
                    fun_info.name(), openmldb::type::DataType_Name(fun_info.return_type()).substr(1),
                    arg_type,        is_aggregate,
                    fun_info.file(), return_nullable,
                    arg_nullable};
                lines.push_back(vec);
            }
            return ResultSetSQL::MakeResultSet(
                {"Name", "Return_type", "Arg_type", "Is_aggregate", "File", "Return_nullable", "Arg_nullable"}, lines,
                status);
        }
        case hybridse::node::kCmdDropFunction: {
            std::string name = cmd_node->GetArgs()[0];
            if (!interactive_validator_.Check(CmdType::kDrop, TargetType::kFunction, name)) {
                return {};
            }
            auto base_status = ns_ptr->DropFunction(name, cmd_node->IsIfExists());
            if (base_status.OK()) {
                *status = {};
                // zk deleted already, remove from cluster_sdk, only failed when func not exist in sdk, ignore error
                cluster_sdk_->RemoveExternalFun(name);
                // drop function from taskmanager, ignore error, taskmanager can recreate the function
                auto taskmanager_client = cluster_sdk_->GetTaskManagerClient();
                if (taskmanager_client) {
                    base_status = taskmanager_client->DropFunction(name, GetJobTimeout());
                    if (!base_status.OK()) {
                        LOG(WARNING) << "drop function " << name << " failed: [" << base_status.GetCode() << "] "
                                     << base_status.GetMsg();
                        APPEND_FROM_BASE_AND_WARN(status, base_status, "drop function failed from taskmanager");
                        return {};
                    }
                }
            } else {
                // not exists or nameserver delete failed on zk
                APPEND_FROM_BASE_AND_WARN(status, base_status, "drop function failed");
            }
            return {};
        }
        case hybridse::node::kCmdShowCreateSp: {
            auto& args = cmd_node->GetArgs();
            std::string db_name, sp_name;
            if (!ParseNamesFromArgs(db, args, &db_name, &sp_name).IsOK()) {
                *status = {StatusCode::kCmdError, msg};
                return {};
            }
            auto sp_info = cluster_sdk_->GetProcedureInfo(db_name, sp_name, &msg);
            if (!sp_info) {
                *status = {StatusCode::kCmdError, "Failed to show procedure, " + msg};
                return {};
            }
            // PrintProcedureInfo(*sp_info);
            *status = {};
            return {};
        }
        case hybridse::node::kCmdShowProcedures: {
            std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> sp_infos = cluster_sdk_->GetProcedureInfo(&msg);
            std::vector<std::vector<std::string>> lines;
            lines.reserve(sp_infos.size());
            for (auto& sp_info : sp_infos) {
                lines.push_back({sp_info->GetDbName(), sp_info->GetSpName()});
            }
            return ResultSetSQL::MakeResultSet({"DB", "SP"}, lines, status);
        }
        case hybridse::node::kCmdDropSp: {
            if (db.empty()) {
                *status = {StatusCode::kCmdError, "please enter database first"};
                return {};
            }
            std::string sp_name = cmd_node->GetArgs()[0];
            if (!interactive_validator_.Check(CmdType::kDrop, TargetType::kProcedure, sp_name)) {
                return {};
            }
            if (ns_ptr->DropProcedure(db, sp_name, msg)) {
                *status = {};
                RefreshCatalog();
            } else {
                *status = {StatusCode::kCmdError, "Failed to drop, " + msg};
            }
            return {};
        }
        case hybridse::node::kCmdShowDeployment: {
            std::string db_name, deploy_name;
            auto& args = cmd_node->GetArgs();
            *status = ParseNamesFromArgs(db, args, &db_name, &deploy_name);
            if (!status->IsOK()) {
                return {};
            }

            std::vector<api::ProcedureInfo> sps;
            if (!ns_ptr->ShowProcedure(db_name, deploy_name, &sps, &msg)) {
                *status = {StatusCode::kCmdError, msg};
                return {};
            }
            // check if deployment
            if (sps.empty() || sps[0].type() != type::kReqDeployment) {
                *status = {StatusCode::kCmdError, sps.empty() ? "not found" : "not a deployment"};
                return {};
            }
            std::stringstream ss;
            auto sp = std::make_shared<catalog::ProcedureInfoImpl>(sps[0]);
            ::openmldb::cmd::PrintProcedureInfo(*sp, ss);
            std::vector<std::vector<std::string>> result;
            std::vector<std::string> vec = {ss.str()};
            result.emplace_back(std::move(vec));
            return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, result, status);
        }
        case hybridse::node::kCmdShowDeployments: {
            if (db.empty()) {
                *status = {StatusCode::kCmdError, "please enter database first"};
                return {};
            }
            // ns client get all procedures of one db
            std::vector<api::ProcedureInfo> sps;
            if (!ns_ptr->ShowProcedure(db, "", &sps, &msg)) {
                *status = {StatusCode::kCmdError, msg};
                return {};
            }
            std::vector<std::vector<std::string>> lines;
            for (auto& sp_info : sps) {
                if (sp_info.type() == type::kReqDeployment) {
                    lines.push_back({sp_info.db_name(), sp_info.sp_name()});
                }
            }
            return ResultSetSQL::MakeResultSet({"DB", "Deployment"}, lines, status);
        }
        case hybridse::node::kCmdDropDeployment: {
            std::string db_name, deploy_name;
            auto& args = cmd_node->GetArgs();
            *status = ParseNamesFromArgs(db, args, &db_name, &deploy_name);
            if (!status->IsOK()) {
                return {};
            }

            // check if deployment, avoid deleting the normal procedure
            auto sp = cluster_sdk_->GetProcedureInfo(db_name, deploy_name, &msg);
            if (!sp || sp->GetType() != hybridse::sdk::kReqDeployment) {
                *status = {StatusCode::kCmdError, sp ? "not a deployment" : "deployment not found"};
                return {};
            }
            if (!interactive_validator_.Check(CmdType::kDrop, TargetType::kDeployment, deploy_name)) {
                return {};
            }
            if (ns_ptr->DropProcedure(db_name, deploy_name, msg)) {
                RefreshCatalog();
                *status = {};
            } else {
                *status = {StatusCode::kCmdError, "Failed to drop. error: " + msg};
            }
            return {};
        }
        case hybridse::node::kCmdShowSessionVariables: {
            std::vector<std::vector<std::string>> items;
            for (auto& pair : session_variables_) {
                std::vector<std::string> vec = {pair.first, pair.second};
                items.emplace_back(std::move(vec));
            }
            return ResultSetSQL::MakeResultSet({"Variable_name", "Value"}, items, status);
        }
        case hybridse::node::kCmdShowGlobalVariables: {
            std::string db = openmldb::nameserver::INFORMATION_SCHEMA_DB;
            std::string table = openmldb::nameserver::GLOBAL_VARIABLES;
            std::string sql = "select * from " + table;
            ::hybridse::sdk::Status status;
            auto rs = ExecuteSQLParameterized(db, sql, std::shared_ptr<openmldb::sdk::SQLRequestRow>(), &status);
            if (status.code != 0) {
                return {};
            }
            return rs;
        }
        case hybridse::node::kCmdExit: {
            std::cout << "Bye" << std::endl;
            exit(0);
        }
        case hybridse::node::kCmdShowJobs: {
            return GetJobResultSet(status);
        }
        case hybridse::node::kCmdShowJob: {
            int job_id;
            if (!absl::SimpleAtoi(cmd_node->GetArgs()[0], &job_id)) {
                *status = {StatusCode::kCmdError, "Failed to parse job id: " + cmd_node->GetArgs()[0]};
                return {};
            }
            return this->GetJobResultSet(job_id, status);
        }
        case hybridse::node::kCmdShowJobLog: {
            int job_id;
            if (!absl::SimpleAtoi(cmd_node->GetArgs()[0], &job_id)) {
                *status = {StatusCode::kCmdError, "Failed to parse job id: " + cmd_node->GetArgs()[0]};
                return {};
            }
            auto log = GetJobLog(job_id, status);

            if (!status->IsOK()) {
                *status = {::hybridse::common::StatusCode::kCmdError,
                           "Failed to get job log for job id: " + cmd_node->GetArgs()[0] +
                               ", code: " + std::to_string(status->code) + ", message: " + status->msg};
                return {};
            } else {
                std::vector<std::string> value = {log};
                return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, {value}, status);
            }
        }
        case hybridse::node::kCmdStopJob: {
            int job_id;
            if (!absl::SimpleAtoi(cmd_node->GetArgs()[0], &job_id)) {
                *status = {StatusCode::kCmdError, "Failed to parse job id: " + cmd_node->GetArgs()[0]};
                return {};
            }
            ::openmldb::taskmanager::JobInfo job_info;
            StopJob(job_id, &job_info);
            return this->GetJobResultSet(job_id, status);
        }
        case hybridse::node::kCmdDropTable: {
            *status = {};
            std::string db_name = db;
            std::string table_name;
            if (!ParseNamesFromArgs(db, cmd_node->GetArgs(), &db_name, &table_name).IsOK()) {
                *status = {StatusCode::kCmdError, msg};
                return {};
            }
            if (!interactive_validator_.Check(CmdType::kDrop, TargetType::kTable, table_name)) {
                return {};
            }
            if (DropTable(db_name, table_name, cmd_node->IsIfExists(), status)) {
                RefreshCatalog();
            }
            return {};
        }
        case hybridse::node::kCmdTruncate: {
            *status = {};
            std::string db_name;
            std::string table_name;
            if (!ParseNamesFromArgs(db, cmd_node->GetArgs(), &db_name, &table_name).IsOK()) {
                *status = {StatusCode::kCmdError, msg};
                return {};
            }
            if (!interactive_validator_.Check(CmdType::kTruncate, TargetType::kTable, table_name)) {
                return {};
            }
            auto base_status = ns_ptr->TruncateTable(db_name, table_name);
            if (!base_status.OK()) {
                *status = {StatusCode::kCmdError, base_status.GetMsg()};
            }
            return {};
        }
        case hybridse::node::kCmdDropIndex: {
            std::string db_name = db;
            std::string table_name;
            std::string index_name;
            if (cmd_node->GetArgs().size() == 3) {
                db_name = cmd_node->GetArgs()[0];
                table_name = cmd_node->GetArgs()[1];
                index_name = cmd_node->GetArgs()[2];
            } else if (cmd_node->GetArgs().size() == 2) {
                table_name = cmd_node->GetArgs()[0];
                index_name = cmd_node->GetArgs()[1];
            } else {
                *status = {StatusCode::kCmdError, "Invalid Cmd Args size"};
                return {};
            }
            if (!interactive_validator_.Check(CmdType::kDrop, TargetType::kIndex, index_name + " on " + table_name)) {
                return {};
            }
            ret = ns_ptr->DeleteIndex(db_name, table_name, index_name, msg);
            ret == true ? * status = {} : * status = {::hybridse::common::StatusCode::kCmdError, msg};
            return {};
        }
        case hybridse::node::kCmdShowComponents: {
            auto rs = ExecuteShowComponents(status);
            if (FLAGS_role == "sql_client" && status->IsOK() && rs) {
                return std::make_shared<ReadableResultSetSQL>(rs);
            }
            return rs;
        }
        case hybridse::node::kCmdShowTableStatus: {
            const auto& args = cmd_node->GetArgs();
            return ExecuteShowTableStatus(db, args.size() > 0 ? args[0] : "", status);
        }
        default: {
            *status = {StatusCode::kCmdError, "fail to execute script with unsupported type"};
        }
    }
    return {};
}

base::Status SQLClusterRouter::HandleSQLCreateTable(hybridse::node::CreatePlanNode* create_node, const std::string& db,
                                                    std::shared_ptr<::openmldb::client::NsClient> ns_ptr) {
    return HandleSQLCreateTable(create_node, db, ns_ptr, "");
}

base::Status SQLClusterRouter::HandleSQLCreateTable(hybridse::node::CreatePlanNode* create_node, const std::string& db,
                                                    std::shared_ptr<::openmldb::client::NsClient> ns_ptr,
                                                    const std::string& sql) {
    if (create_node == nullptr || ns_ptr == nullptr) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "fail to execute plan : null pointer");
    }

    std::string db_name = create_node->GetDatabase().empty() ? db : create_node->GetDatabase();
    if (db_name.empty()) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "ERROR: Please use database first");
    }

    if (create_node->like_clause_ == nullptr) {
        ::openmldb::nameserver::TableInfo table_info;
        table_info.set_db(db_name);

        std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> all_tablet;
        all_tablet = cluster_sdk_->GetAllTablet();
        // set dafault value
        uint32_t default_replica_num = std::min(static_cast<uint32_t>(all_tablet.size()), FLAGS_replica_num);

        hybridse::base::Status sql_status;
        bool is_cluster_mode = cluster_sdk_->IsClusterMode();
        ::openmldb::sdk::NodeAdapter::TransformToTableDef(create_node, &table_info, default_replica_num,
                                                          is_cluster_mode, &sql_status);
        if (sql_status.code != 0) {
            return base::Status(sql_status.code, sql_status.msg);
        }
        std::string msg;
        if (!ns_ptr->CreateTable(table_info, create_node->GetIfNotExist(), msg)) {
            return base::Status(base::ReturnCode::kSQLCmdRunError, msg);
        }
        if (interactive_validator_.Interactive() && table_info.column_key_size() == 0) {
            return base::Status{base::ReturnCode::kOk,
                                "As there is no index specified, a default index type `absolute 0` will be created. "
                                "The data attached to the index will never expire to be deleted. "
                                "Please refer to this link for more details: " +
                                    base::NOTICE_URL};
        }
    } else {
        auto dbs = cluster_sdk_->GetAllDbs();
        auto it = std::find(dbs.begin(), dbs.end(), db_name);
        if (it == dbs.end()) {
            return base::Status(base::ReturnCode::kSQLCmdRunError, "fail to create, database does not exist!");
        }

        LOG(WARNING) << "CREATE TABLE LIKE will run in offline job, please wait.";
        std::string output;
        // need to load spark config
        auto config = ParseSparkConfigString(GetSparkConfig());
        ReadSparkConfFromFile(std::dynamic_pointer_cast<SQLRouterOptions>(options_)->spark_conf_path, &config);
        AddUserToConfig(&config);
        // create table like hive is a long op, use large timeout
        ::openmldb::base::Status status =
            ExecuteOfflineQueryGetOutput(sql, config, db, FLAGS_sync_job_timeout, &output);
        if (!status.OK()) {
            LOG(ERROR) << "Fail to create table, error message: " + status.msg;
            return base::Status(base::ReturnCode::kSQLCmdRunError, status.msg);
        }
    }

    return {};
}

base::Status SQLClusterRouter::HandleSQLCreateProcedure(hybridse::node::CreateProcedurePlanNode* create_sp,
                                                        const std::string& db, const std::string& sql,
                                                        std::shared_ptr<::openmldb::client::NsClient> ns_ptr) {
    if (create_sp == nullptr) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "CreateProcedurePlanNode null");
    }
    // construct sp_info
    hybridse::base::Status sql_status;
    openmldb::api::ProcedureInfo sp_info;
    sp_info.set_db_name(db);
    sp_info.set_sp_name(create_sp->GetSpName());
    sp_info.set_sql(sql);
    PBSchema* schema = sp_info.mutable_input_schema();
    for (auto input : create_sp->GetInputParameterList()) {
        if (input == nullptr) {
            return base::Status(base::ReturnCode::kSQLCmdRunError, "fail to execute plan : InputParameterNode null");
        }
        if (input->GetType() == hybridse::node::kInputParameter) {
            hybridse::node::InputParameterNode* input_ptr = dynamic_cast<hybridse::node::InputParameterNode*>(input);
            if (input_ptr == nullptr) {
                return base::Status(base::ReturnCode::kSQLCmdRunError, "cast InputParameterNode failed");
            }
            openmldb::common::ColumnDesc* col_desc = schema->Add();
            col_desc->set_name(input_ptr->GetColumnName());
            openmldb::type::DataType rtidb_type;
            bool ok = ::openmldb::schema::SchemaAdapter::ConvertType(input_ptr->GetColumnType(), &rtidb_type);
            if (!ok) {
                return base::Status(base::ReturnCode::kSQLCmdRunError, "convert type failed");
            }
            col_desc->set_data_type(rtidb_type);
            col_desc->set_is_constant(input_ptr->GetIsConstant());
        } else {
            return base::Status(
                base::ReturnCode::kSQLCmdRunError,
                "fail to execute script with unsupported type " + hybridse::node::NameOfSqlNodeType(input->GetType()));
        }
    }
    // get input schema, check input parameter, and fill sp_info
    std::set<size_t> input_common_column_indices;
    for (int i = 0; i < schema->size(); ++i) {
        if (schema->Get(i).is_constant()) {
            input_common_column_indices.insert(i);
        }
    }
    bool ok;
    hybridse::vm::ExplainOutput explain_output;
    if (input_common_column_indices.empty()) {
        ok = cluster_sdk_->GetEngine()->Explain(sql, db, hybridse::vm::kRequestMode, &explain_output, &sql_status);
    } else {
        ok = cluster_sdk_->GetEngine()->Explain(sql, db, hybridse::vm::kBatchRequestMode, input_common_column_indices,
                                                &explain_output, &sql_status);
    }
    if (!ok) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "fail to explain sql" + sql_status.msg);
    }
    PBSchema rtidb_input_schema;
    if (!openmldb::schema::SchemaAdapter::ConvertSchema(explain_output.input_schema, &rtidb_input_schema)) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "convert input schema failed");
    }
    if (!CheckParameter(*schema, rtidb_input_schema)) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "check input parameter failed");
    }
    sp_info.mutable_input_schema()->CopyFrom(*schema);
    // get output schema, and fill sp_info
    PBSchema rtidb_output_schema;
    if (!openmldb::schema::SchemaAdapter::ConvertSchema(explain_output.output_schema, &rtidb_output_schema)) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "convert output schema failed");
    }
    sp_info.mutable_output_schema()->CopyFrom(rtidb_output_schema);
    sp_info.set_main_db(explain_output.request_db_name);
    sp_info.set_main_table(explain_output.request_name);

    auto& tables = explain_output.dependent_tables;
    DLOG(INFO) << "dependent tables: [" << absl::StrJoin(tables, ",", absl::PairFormatter("=")) << "]";
    for (auto& table : tables) {
        auto pair = sp_info.add_tables();
        pair->set_db_name(table.first);
        pair->set_table_name(table.second);
    }
    // send request to ns client
    return ns_ptr->CreateProcedure(sp_info, options_->request_timeout);
}

bool SQLClusterRouter::CheckParameter(const PBSchema& parameter, const PBSchema& input_schema) {
    if (parameter.size() != input_schema.size()) {
        return false;
    }
    for (int32_t i = 0; i < parameter.size(); i++) {
        if (parameter.Get(i).name() != input_schema.Get(i).name()) {
            LOG(WARNING) << "check column name failed, expect " << input_schema.Get(i).name() << ", but "
                         << parameter.Get(i).name();
            return false;
        }
        if (parameter.Get(i).data_type() != input_schema.Get(i).data_type()) {
            LOG(WARNING) << "check column type failed, expect "
                         << openmldb::type::DataType_Name(input_schema.Get(i).data_type()) << ", but "
                         << openmldb::type::DataType_Name(parameter.Get(i).data_type());
            return false;
        }
    }
    return true;
}

bool SQLClusterRouter::CheckSQLSyntax(const std::string& sql) {
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);
    if (0 != sql_status.code) {
        LOG(WARNING) << "[" << sql_status.GetCode() << "]" << sql_status.str();
        return false;
    }
    return true;
}

bool SQLClusterRouter::ExtractDBTypes(const std::shared_ptr<hybridse::sdk::Schema>& schema,
                                      std::vector<openmldb::type::DataType>* db_types) {
    if (schema) {
        for (int i = 0; i < schema->GetColumnCnt(); i++) {
            openmldb::type::DataType casted_type;
            if (!openmldb::schema::SchemaAdapter::ConvertType(schema->GetColumnType(i), &casted_type)) {
                LOG(WARNING) << "Invalid parameter type " << schema->GetColumnType(i);
                return false;
            }
            db_types->push_back(casted_type);
        }
    }
    return true;
}
std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> SQLClusterRouter::ShowProcedure(std::string* msg) {
    std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> vec;
    if (msg == nullptr) {
        *msg = "null ptr";
        return vec;
    }
    return cluster_sdk_->GetProcedureInfo(msg);
}

std::shared_ptr<hybridse::sdk::ProcedureInfo> SQLClusterRouter::ShowProcedure(const std::string& db,
                                                                              const std::string& sp_name,
                                                                              std::string* msg) {
    if (msg == nullptr) {
        *msg = "null ptr";
        return nullptr;
    }
    return cluster_sdk_->GetProcedureInfo(db, sp_name, msg);
}

std::shared_ptr<openmldb::sdk::QueryFuture> SQLClusterRouter::CallProcedure(const std::string& db,
                                                                            const std::string& sp_name,
                                                                            int64_t timeout_ms,
                                                                            std::shared_ptr<SQLRequestRow> row,
                                                                            hybridse::sdk::Status* status) {
    if (!row || !row->OK()) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "make sure the request row is built before execute sql");
        return {};
    }
    return CallProcedure(db, sp_name, timeout_ms, base::Slice(row->GetRow()), "", status);
}

std::shared_ptr<openmldb::sdk::QueryFuture> SQLClusterRouter::CallProcedure(
    const std::string& db, const std::string& sp_name, int64_t timeout_ms, hybridse::sdk::ByteArrayPtr buf, int len,
    const std::string& router_col, hybridse::sdk::Status* status) {
    if (buf == nullptr || len == 0) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "invalid request row data");
        return nullptr;
    }
    return CallProcedure(db, sp_name, timeout_ms, base::Slice(buf, len), router_col, status);
}

std::shared_ptr<openmldb::sdk::QueryFuture> SQLClusterRouter::CallProcedure(const std::string& db,
                                                                            const std::string& sp_name,
                                                                            int64_t timeout_ms, const base::Slice& row,
                                                                            const std::string& router_col,
                                                                            hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    auto tablet = GetTablet(db, sp_name, router_col, status);
    if (!tablet) {
        return {};
    }
    std::shared_ptr<openmldb::api::QueryResponse> response = std::make_shared<openmldb::api::QueryResponse>();
    std::shared_ptr<brpc::Controller> cntl = std::make_shared<brpc::Controller>();
    auto* callback = new openmldb::RpcCallback<openmldb::api::QueryResponse>(response, cntl);

    std::shared_ptr<openmldb::sdk::QueryFutureImpl> future = std::make_shared<openmldb::sdk::QueryFutureImpl>(callback);
    bool ok = tablet->CallProcedure(db, sp_name, row, timeout_ms, options_->enable_debug, callback);
    if (!ok) {
        // async rpc
        SET_STATUS_AND_WARN(status, StatusCode::kConnError, "CallProcedure failed(stub is null)");
        return {};
    }
    return future;
}

std::shared_ptr<openmldb::sdk::QueryFuture> SQLClusterRouter::CallSQLBatchRequestProcedure(
    const std::string& db, const std::string& sp_name, int64_t timeout_ms,
    std::shared_ptr<SQLRequestRowBatch> row_batch, hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (!row_batch) {
        // todo
        return nullptr;
    }
    auto tablet = GetTablet(db, sp_name, "", status);
    if (!tablet) {
        return nullptr;
    }

    std::shared_ptr<brpc::Controller> cntl = std::make_shared<brpc::Controller>();
    auto response = std::make_shared<openmldb::api::SQLBatchRequestQueryResponse>();
    openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback =
        new openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>(response, cntl);

    std::shared_ptr<openmldb::sdk::BatchQueryFutureImpl> future =
        std::make_shared<openmldb::sdk::BatchQueryFutureImpl>(callback);
    bool ok =
        tablet->CallSQLBatchRequestProcedure(db, sp_name, row_batch, options_->enable_debug, timeout_ms, callback);
    if (!ok) {
        // async rpc only check ok
        SET_STATUS_AND_WARN(status, StatusCode::kConnError, "CallSQLBatchRequestProcedure failed(stub is null)");
        return nullptr;
    }
    return future;
}

std::shared_ptr<openmldb::sdk::QueryFuture> SQLClusterRouter::CallSQLBatchRequestProcedure(
    const std::string& db, const std::string& sp_name, int64_t timeout_ms, hybridse::sdk::ByteArrayPtr meta,
    int meta_len, hybridse::sdk::ByteArrayPtr buf, int len, hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (meta == nullptr || meta_len == 0 || buf == nullptr || len == 0) {
        SET_STATUS_AND_WARN(status, StatusCode::kNullInputPointer, "input data is null");
        return nullptr;
    }
    auto tablet = GetTablet(db, sp_name, "", status);
    if (!tablet) {
        return nullptr;
    }
    std::shared_ptr<brpc::Controller> cntl = std::make_shared<brpc::Controller>();
    auto response = std::make_shared<openmldb::api::SQLBatchRequestQueryResponse>();
    auto callback = new openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>(response, cntl);
    auto future = std::make_shared<openmldb::sdk::BatchQueryFutureImpl>(callback);
    auto ret = tablet->CallSQLBatchRequestProcedure(db, sp_name, base::Slice(meta, meta_len), base::Slice(buf, len),
                                                    options_->enable_debug, timeout_ms, callback);
    if (!ret.OK()) {
        SET_STATUS_AND_WARN(status, StatusCode::kConnError, "CallSQLBatchRequestProcedure failed " + ret.GetMsg());
        return nullptr;
    }
    return future;
}

std::shared_ptr<hybridse::sdk::Schema> SQLClusterRouter::GetTableSchema(const std::string& db,
                                                                        const std::string& table_name) {
    auto table_info = cluster_sdk_->GetTableInfo(db, table_name);
    if (!table_info) {
        LOG(ERROR) << "table with name " + table_name + " in db " + db + " does not exist";
        return {};
    }

    ::hybridse::vm::Schema output_schema;
    if (::openmldb::schema::SchemaAdapter::ConvertSchema(table_info->column_desc(), &output_schema)) {
        return std::make_shared<::hybridse::sdk::SchemaImpl>(output_schema);
    } else {
        LOG(ERROR) << "Failed to convert schema for " + table_name + "in db " + db;
    }
    return {};
}

std::vector<std::string> SQLClusterRouter::GetTableNames(const std::string& db) {
    auto table_names = cluster_sdk_->GetTableNames(db);
    return table_names;
}

::openmldb::nameserver::TableInfo SQLClusterRouter::GetTableInfo(const std::string& db, const std::string& table) {
    auto table_infos = cluster_sdk_->GetTableInfo(db, table);
    if (!table_infos) {
        return {};
    }
    return *table_infos;
}

bool SQLClusterRouter::UpdateOfflineTableInfo(const ::openmldb::nameserver::TableInfo& info) {
    auto ret = cluster_sdk_->GetNsClient()->UpdateOfflineTableInfo(info);
    if (!ret.OK()) {
        LOG(WARNING) << "update offline table info failed: " << ret.msg;
        return false;
    }
    return true;
}

::openmldb::base::Status SQLClusterRouter::ShowJobs(const bool only_unfinished,
                                                    std::vector<::openmldb::taskmanager::JobInfo>* job_infos) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {base::ReturnCode::kServerConnError, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ShowJobs(only_unfinished, GetJobTimeout(), job_infos);
}

::openmldb::base::Status SQLClusterRouter::ShowJob(const int id, ::openmldb::taskmanager::JobInfo* job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {base::ReturnCode::kServerConnError, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ShowJob(id, GetJobTimeout(), job_info);
}

::openmldb::base::Status SQLClusterRouter::StopJob(const int id, ::openmldb::taskmanager::JobInfo* job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {base::ReturnCode::kServerConnError, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->StopJob(id, GetJobTimeout(), job_info);
}

::openmldb::base::Status SQLClusterRouter::ExecuteOfflineQueryAsync(const std::string& sql,
                                                                    const std::map<std::string, std::string>& config,
                                                                    const std::string& default_db, int job_timeout,
                                                                    ::openmldb::taskmanager::JobInfo* job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {base::ReturnCode::kServerConnError, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->RunBatchAndShow(sql, config, default_db, false, job_timeout, job_info);
}

::openmldb::base::Status SQLClusterRouter::ExecuteOfflineQueryGetOutput(
    const std::string& sql, const std::map<std::string, std::string>& config, const std::string& default_db,
    int job_timeout, std::string* output) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {base::ReturnCode::kServerConnError, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->RunBatchSql(sql, config, default_db, job_timeout, output);
}

::openmldb::base::Status SQLClusterRouter::ImportOnlineData(const std::string& sql,
                                                            const std::map<std::string, std::string>& config,
                                                            const std::string& default_db, bool sync_job,
                                                            int job_timeout,
                                                            ::openmldb::taskmanager::JobInfo* job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {base::ReturnCode::kServerConnError, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ImportOnlineData(sql, config, default_db, sync_job, job_timeout, job_info);
}

::openmldb::base::Status SQLClusterRouter::ImportOfflineData(const std::string& sql,
                                                             const std::map<std::string, std::string>& config,
                                                             const std::string& default_db, bool sync_job,
                                                             int job_timeout,
                                                             ::openmldb::taskmanager::JobInfo* job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {base::ReturnCode::kServerConnError, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ImportOfflineData(sql, config, default_db, sync_job, job_timeout, job_info);
}

::openmldb::base::Status SQLClusterRouter::ExportOfflineData(const std::string& sql,
                                                             const std::map<std::string, std::string>& config,
                                                             const std::string& default_db, bool sync_job,
                                                             int job_timeout,
                                                             ::openmldb::taskmanager::JobInfo* job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {base::ReturnCode::kServerConnError, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ExportOfflineData(sql, config, default_db, sync_job, job_timeout, job_info);
}

::openmldb::base::Status SQLClusterRouter::CreatePreAggrTable(const std::string& aggr_db, const std::string& aggr_table,
                                                              const ::openmldb::base::LongWindowInfo& window_info,
                                                              const ::openmldb::nameserver::TableInfo& base_table_info,
                                                              std::shared_ptr<::openmldb::client::NsClient> ns_ptr) {
    ::openmldb::nameserver::TableInfo table_info;
    table_info.set_db(aggr_db);
    table_info.set_name(aggr_table);
    table_info.set_replica_num(base_table_info.replica_num());
    table_info.set_partition_num(base_table_info.partition_num());
    auto table_partition = table_info.mutable_table_partition();
    table_partition->CopyFrom(base_table_info.table_partition());
    table_info.set_storage_mode(base_table_info.storage_mode());
    table_info.set_base_table_tid(base_table_info.tid());
    auto SetColumnDesc = [](const std::string& name, openmldb::type::DataType type,
                            openmldb::common::ColumnDesc* field) {
        if (field != nullptr) {
            field->set_name(name);
            field->set_data_type(type);
        }
    };
    SetColumnDesc("key", openmldb::type::DataType::kString, table_info.add_column_desc());
    SetColumnDesc("ts_start", openmldb::type::DataType::kTimestamp, table_info.add_column_desc());
    SetColumnDesc("ts_end", openmldb::type::DataType::kTimestamp, table_info.add_column_desc());
    SetColumnDesc("num_rows", openmldb::type::DataType::kInt, table_info.add_column_desc());
    SetColumnDesc("agg_val", openmldb::type::DataType::kString, table_info.add_column_desc());
    SetColumnDesc("binlog_offset", openmldb::type::DataType::kBigInt, table_info.add_column_desc());
    SetColumnDesc("filter_key", openmldb::type::DataType::kString, table_info.add_column_desc());
    auto index = table_info.add_column_key();
    index->set_index_name("key_index");
    index->add_col_name("key");
    index->set_ts_name("ts_start");

    // keep ttl in pre-aggr table the same as base table
    auto ttl = index->mutable_ttl();
    for (int i = 0; i < base_table_info.column_key_size(); i++) {
        const auto& column_key = base_table_info.column_key(i);
        std::string keys = "";
        for (int j = 0; j < column_key.col_name_size(); j++) {
            keys += column_key.col_name(j) + ",";
        }
        keys.pop_back();
        std::string ts_name = column_key.ts_name();
        if (keys == window_info.partition_col_ && ts_name == window_info.order_col_) {
            ttl->CopyFrom(column_key.ttl());
            break;
        }
    }

    std::string msg;
    if (!ns_ptr->CreateTable(table_info, true, msg)) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, msg);
    }
    RefreshCatalog();
    return {};
}

std::string SQLClusterRouter::GetJobLog(const int id, hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");

    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();

    if (!taskmanager_client_ptr) {
        SET_STATUS_AND_WARN(status, hybridse::common::kConnError, "Fail to get TaskManager client");
        return "";
    }

    base::Status base_s;
    auto log = taskmanager_client_ptr->GetJobLog(id, GetJobTimeout(), &base_s);

    if (base_s.OK()) {
        status->SetCode(base_s.code);
        status->SetMsg(base_s.msg);
    } else {
        APPEND_FROM_BASE(status, base_s, "get joblog");
    }

    return log;
}

bool SQLClusterRouter::NotifyTableChange() { return cluster_sdk_->TriggerNotify(::openmldb::type::NotifyType::kTable); }

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQL(const std::string& sql,
                                                                       hybridse::sdk::Status* status) {
    std::string db = GetDatabase();
    return ExecuteSQL(db, sql, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQL(const std::string& db, const std::string& sql,
                                                                       hybridse::sdk::Status* status) {
    // To avoid small sync job timeout, we set offline_job_timeout to the biggest value, user can set >
    // FLAGS_sync_job_timeout
    auto sync_job = IsSyncJob();
    auto timeout = GetJobTimeout();
    if (sync_job && FLAGS_sync_job_timeout > timeout) {
        timeout = FLAGS_sync_job_timeout;
    }
    return ExecuteSQL(db, sql, IsOnlineMode(), sync_job, timeout, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQL(const std::string& db, const std::string& sql,
                                                                       bool is_online_mode, bool is_sync_job,
                                                                       int offline_job_timeout,
                                                                       hybridse::sdk::Status* status) {
    return ExecuteSQL(db, sql, {}, is_online_mode, is_sync_job, offline_job_timeout, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQL(
    const std::string& db, const std::string& sql, std::shared_ptr<openmldb::sdk::SQLRequestRow> parameter,
    bool is_online_mode, bool is_sync_job, int offline_job_timeout, hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    // functions we called later may not change the status if it's succeed. So if we pass error status here, we'll get a
    // fake error
    status->SetOK();
    hybridse::node::NodeManager node_manager;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::base::Status sql_status;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);
    if (!sql_status.isOK()) {
        COPY_PREPEND_AND_WARN(status, sql_status, "create logic plan tree failed");
        return {};
    }
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        SET_STATUS_AND_WARN(status, StatusCode::kRuntimeError, "no ns client, retry or check ns process");
        return {};
    }
    hybridse::node::PlanNode* node = plan_trees[0];
    std::string msg;
    switch (node->GetType()) {
        case hybridse::node::kPlanTypeCmd: {
            return HandleSQLCmd(dynamic_cast<hybridse::node::CmdPlanNode*>(node), db, status);
        }
        case hybridse::node::kPlanTypeExplain: {
            std::string empty;
            std::string mu_script = sql;
            mu_script.replace(0u, 7u, empty);
            auto info = Explain(db, mu_script, status);
            if (!info) {
                return {};
            }
            *status = {};
            std::vector<std::string> value = {info->GetPhysicalPlan() + "\n"};
            return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, {value}, status);
        }
        case hybridse::node::kPlanTypeCreate: {
            auto create_node = dynamic_cast<hybridse::node::CreatePlanNode*>(node);
            auto base_status = HandleSQLCreateTable(create_node, db, ns_ptr, sql);
            if (base_status.OK()) {
                RefreshCatalog();
                *status = {StatusCode::kOk, base_status.msg};
            } else {
                *status = {StatusCode::kCmdError, base_status.msg};
            }
            return {};
        }
        case hybridse::node::kPlanTypeCreateSp: {
            if (db.empty()) {
                *status = {StatusCode::kCmdError, "Please use database first"};
                return {};
            }
            auto create_node = dynamic_cast<hybridse::node::CreateProcedurePlanNode*>(node);
            auto base_status = HandleSQLCreateProcedure(create_node, db, sql, ns_ptr);
            if (base_status.OK()) {
                RefreshCatalog();
                *status = {};
            } else {
                *status = {StatusCode::kCmdError, base_status.msg};
            }
            return {};
        }
        case hybridse::node::kPlanTypeCreateUser: {
            auto create_node = dynamic_cast<hybridse::node::CreateUserPlanNode*>(node);
            UserInfo user_info;;
            auto result = GetUser(create_node->Name(), &user_info);
            if (!result.ok()) {
                *status = {StatusCode::kCmdError, result.status().message()};
            } else if (*result) {
                if (!create_node->IfNotExists()) {
                    *status = {StatusCode::kCmdError, absl::StrCat("user ", create_node->Name(), " already exists")};
                }
            } else {
                std::string password;
                if (create_node->Options()) {
                    auto ret = NodeAdapter::ExtractUserOption(*create_node->Options());
                    if (!ret.ok()) {
                        *status = {StatusCode::kCmdError, ret.status().message()};
                        return {};
                    }
                    password = *ret;
                }
                *status = AddUser(create_node->Name(), password);
            }
            return {};
        }
        case hybridse::node::kPlanTypeAlterUser: {
            auto alter_node = dynamic_cast<hybridse::node::AlterUserPlanNode*>(node);
            UserInfo user_info;
            auto result = GetUser(alter_node->Name(), &user_info);
            if (!result.ok()) {
                *status = {StatusCode::kCmdError, result.status().message()};
                return {};
            } else if (!(*result)) {
                if (!alter_node->IfExists() && alter_node->Name() != "root") {
                    *status = {StatusCode::kCmdError, absl::StrCat("user ", alter_node->Name(), " does not exists")};
                    return {};
                }
                user_info.name = "root";
                user_info.create_time = ::baidu::common::timer::get_micros() / 1000;
                user_info.privileges = "ALL";
            }
            if (alter_node->Options() && !alter_node->Options()->empty()) {
                auto ret = NodeAdapter::ExtractUserOption(*alter_node->Options());
                if (!ret.ok()) {
                    *status = {StatusCode::kCmdError, ret.status().message()};
                    return {};
                }
                *status = UpdateUser(user_info, *ret);
            }
            return {};
        }
        case hybridse::node::kPlanTypeCreateIndex: {
            auto create_index_plan_node = dynamic_cast<hybridse::node::CreateIndexPlanNode*>(node);
            auto create_index_node = create_index_plan_node->create_index_node_;
            std::string db_name = create_index_node->db_name_.empty() ? db : create_index_node->db_name_;
            if (db_name.empty()) {
                *status = {::hybridse::common::StatusCode::kCmdError, "Please use database first"};
                return {};
            }
            ::openmldb::common::ColumnKey column_key;
            hybridse::base::Status base_status;
            if (!::openmldb::sdk::NodeAdapter::TransformToColumnKey(create_index_node->index_, {}, &column_key,
                                                                    &base_status)) {
                COPY_PREPEND_AND_WARN(status, base_status, "TransformToColumnKey failed");
                return {};
            }
            column_key.set_index_name(create_index_node->index_name_);
            // no skip load data, so it's always be a async op
            if (ns_ptr->AddIndex(db_name, create_index_node->table_name_, column_key, nullptr, msg)) {
                *status = {::hybridse::common::StatusCode::kOk,
                           "AddIndex is an asynchronous job. Run 'SHOW JOBS FROM NAMESERVER' to see the job status"};
            } else {
                SET_STATUS_AND_WARN(status, StatusCode::kCmdError, absl::StrCat("ns add index failed. msg: ", msg));
            }
            return {};
        }
        case hybridse::node::kPlanTypeInsert: {
            if (cluster_sdk_->IsClusterMode() && !is_online_mode) {
                // Not support for inserting into offline storage
                *status = {StatusCode::kCmdError,
                           "Can not insert in offline mode, please set @@SESSION.execute_mode='online'"};
                return {};
            }
            // if db name has been specified in sql, db parameter will be ignored
            ExecuteInsert(db, sql, status);
            return {};
        }
        case hybridse::node::kPlanTypeDeploy: {
            if (cluster_sdk_->IsClusterMode() && !is_online_mode) {
                // avoid run deploy in offline mode
                *status = {::hybridse::common::StatusCode::kCmdError,
                           "Can not deploy in offline mode, please set @@SESSION.execute_mode='online'"};
                return {};
            }
            auto deploy_node = dynamic_cast<hybridse::node::DeployPlanNode*>(node);
            std::optional<uint64_t> job_id;
            *status = HandleDeploy(db, deploy_node, &job_id);
            if (status->IsOK()) {
                if (job_id.has_value()) {
                    schema::PBSchema job_schema;
                    auto col = job_schema.Add();
                    col->set_name("job_id");
                    col->set_data_type(::openmldb::type::DataType::kBigInt);
                    std::vector<std::string> value = {std::to_string(job_id.value())};
                    return ResultSetSQL::MakeResultSet(job_schema, {value}, status);
                }
                RefreshCatalog();
                *status = {
                    StatusCode::kOk,
                    "\n- DEPLOY may modify table TTL. Data imported before DEPLOY may expire before the new TTL.\n"
                    "- DEPLOY will fail if the disk table requires creating an index, "
                    "the partial index may have been created."};
            }
            return {};
        }
        case hybridse::node::kPlanTypeFuncDef:
        case hybridse::node::kPlanTypeQuery: {
            if (!cluster_sdk_->IsClusterMode() || is_online_mode) {
                // Run online query
                return ExecuteSQLParameterized(db, sql, parameter, status);
            } else {
                // Run offline query
                return ExecuteOfflineQuery(db, sql, is_sync_job, offline_job_timeout, status);
            }
        }
        case hybridse::node::kPlanTypeSelectInto: {
            if (!cluster_sdk_->IsClusterMode() || is_online_mode) {
                auto* select_into_plan_node = dynamic_cast<hybridse::node::SelectIntoPlanNode*>(node);
                const std::string& query_sql = select_into_plan_node->QueryStr();
                auto rs = ExecuteSQLParameterized(db, query_sql, parameter, status);
                if (!rs) {
                    return {};
                }
                const std::string& file_path = select_into_plan_node->OutFile();
                const std::shared_ptr<hybridse::node::OptionsMap> options_map = select_into_plan_node->Options();
                auto base_status = SaveResultSet(file_path, options_map, rs.get());
                if (!base_status.OK()) {
                    *status = {StatusCode::kCmdError, base_status.msg};
                } else {
                    *status = {};
                }
            } else {
                ::openmldb::taskmanager::JobInfo job_info;
                std::map<std::string, std::string> config = ParseSparkConfigString(GetSparkConfig());
                ReadSparkConfFromFile(std::dynamic_pointer_cast<SQLRouterOptions>(options_)->spark_conf_path, &config);
                AddUserToConfig(&config);

                auto base_status = ExportOfflineData(sql, config, db, is_sync_job, offline_job_timeout, &job_info);
                if (base_status.OK()) {
                    return this->GetJobResultSet(job_info.id(), status);
                } else {
                    *status = {StatusCode::kCmdError, base_status.msg};
                }
            }
            return {};
        }
        case hybridse::node::kPlanTypeSet: {
            *status = SetVariable(dynamic_cast<hybridse::node::SetPlanNode*>(node));
            return {};
        }
        case hybridse::node::kPlanTypeLoadData: {
            auto plan = dynamic_cast<hybridse::node::LoadDataPlanNode*>(node);
            std::string database = plan->Db().empty() ? db : plan->Db();
            if (database.empty()) {
                *status = {StatusCode::kCmdError, "no db in sql and no default db"};
                return {};
            }

            openmldb::sdk::LoadOptionsMapParser options_parser(plan->Options());
            auto is_local = options_parser.IsLocalMode();
            if (!is_local.ok()) {
                *status = {StatusCode::kCmdError, is_local.status().ToString()};
                return {};
            }
            if (!cluster_sdk_->IsClusterMode() || is_local.value()) {
                if (cluster_sdk_->IsClusterMode() && !IsOnlineMode()) {
                    *status = {::hybridse::common::StatusCode::kCmdError,
                               "local load only supports loading data to online storage"};
                    return {};
                }

                auto thread = options_parser.GetAs<int64_t>("thread");
                if (!thread.ok()) {
                    *status = {::hybridse::common::StatusCode::kCmdError,
                               "thread is not set" + options_parser.ToString()};
                    return {};
                }
                if (thread.value() > static_cast<int64_t>(options_->max_sql_cache_size)) {
                    LOG(INFO) << "Load Data thread exceeds the max allowed number. Change to the max: "
                              << options_->max_sql_cache_size;
                    options_parser.Set("thread", static_cast<int64_t>(options_->max_sql_cache_size));
                }
                auto st = options_parser.Validate();
                if (!st.ok()) {
                    *status = {::hybridse::common::StatusCode::kCmdError, st.ToString()};
                    return {};
                }
                // Load data locally, report error in status
                *status = HandleLoadDataInfile(database, plan->Table(), plan->File(), options_parser);
            } else {
                // Load data using Spark
                ::openmldb::taskmanager::JobInfo job_info;
                std::map<std::string, std::string> config = ParseSparkConfigString(GetSparkConfig());
                ReadSparkConfFromFile(std::dynamic_pointer_cast<SQLRouterOptions>(options_)->spark_conf_path, &config);
                AddUserToConfig(&config);

                ::openmldb::base::Status base_status;
                if (is_online_mode) {
                    // Handle in online mode
                    config.emplace("spark.insert_memory_usage_limit",
                            std::to_string(insert_memory_usage_limit_.load(std::memory_order_relaxed)));
                    base_status = ImportOnlineData(sql, config, database, is_sync_job, offline_job_timeout, &job_info);
                } else {
                    // Handle in offline mode
                    base_status = ImportOfflineData(sql, config, database, is_sync_job, offline_job_timeout, &job_info);
                }
                if (base_status.OK() && job_info.id() > 0) {
                    return this->GetJobResultSet(job_info.id(), status);
                } else {
                    APPEND_FROM_BASE_AND_WARN(status, base_status, "taskmanager load data failed");
                }
            }
            return {};
        }
        case hybridse::node::kPlanTypeCreateFunction: {
            *status = HandleCreateFunction(dynamic_cast<hybridse::node::CreateFunctionPlanNode*>(node));
            return {};
        }
        case hybridse::node::kPlanTypeDelete: {
            auto plan = dynamic_cast<hybridse::node::DeletePlanNode*>(node);
            std::string database = plan->GetDatabase().empty() ? db : plan->GetDatabase();
            if (database.empty()) {
                *status = {StatusCode::kCmdError, " no db in sql and no default db"};
                return {};
            }
            *status = HandleDelete(database, plan->GetTableName(), plan->GetCondition());
            return {};
        }
        case hybridse::node::kPlanTypeAlterTable: {
            auto plan = dynamic_cast<hybridse::node::AlterTableStmtPlanNode*>(node);

            std::string db_name;
            if (!plan->db_.empty()) {
                db_name = {plan->db_.begin(), plan->db_.end()};
            } else {
                db_name = db;
            }

            std::string table(plan->table_.begin(), plan->table_.end());
            // Refresh before getting actual table info
            cluster_sdk_->Refresh();
            auto tableInfo = cluster_sdk_->GetTableInfo(db_name, table);

            if (tableInfo == nullptr) {
                *status = {StatusCode::kCmdError, "table does not exist"};
                return {};
            }

            // Create offline table info if not exists
            ::openmldb::nameserver::OfflineTableInfo offline_table_info;

            if (tableInfo->has_offline_table_info()) {
                // Get the existed offline table info
                offline_table_info.CopyFrom(tableInfo->offline_table_info());
            } else {
                // Create empty offline table info
                offline_table_info.set_path("");
                offline_table_info.set_format("parquet");
            }

            auto current_symbolic_paths = offline_table_info.symbolic_paths();

            std::vector<const hybridse::node::AlterActionBase*> actions = plan->actions_;

            // Handle multiple add and delete actions
            for (const auto& action : actions) {
                auto kind = action->kind();

                if (kind == hybridse::node::AlterActionBase::ActionKind::ADD_PATH) {  // Handle add path
                    auto addAction = dynamic_cast<const hybridse::node::AddPathAction*>(action);
                    auto target = addAction->target_;

                    bool isDuplicated = false;
                    for (const auto& item : current_symbolic_paths) {
                        if (item == target) {
                            isDuplicated = true;
                            break;
                        }
                    }
                    if (isDuplicated) {
                        *status = {StatusCode::kCmdError, "The symbolic path exists: " + target};
                        return {};
                    } else {
                        offline_table_info.add_symbolic_paths(target);
                    }

                } else {  // Handle delete path
                    auto dropAction = dynamic_cast<const hybridse::node::DropPathAction*>(action);
                    auto target = dropAction->target_;

                    bool isExist = false;
                    int index = 0;
                    for (const auto& item : current_symbolic_paths) {
                        if (item == target) {
                            isExist = true;
                            break;
                        }
                        index += 1;
                    }
                    if (isExist) {
                        offline_table_info.mutable_symbolic_paths()->erase(
                            offline_table_info.mutable_symbolic_paths()->begin() + index);
                    } else {
                        *status = {StatusCode::kCmdError, "The symbolic path does not exist: " + target};
                        return {};
                    }
                }
            }

            // Update offline table info
            tableInfo->mutable_offline_table_info()->CopyFrom(offline_table_info);

            // Send reqeust to persistent table info
            auto ns = cluster_sdk_->GetNsClient();
            ns->UpdateOfflineTableInfo(*tableInfo);
            return {};
        }
        case hybridse::node::kPlanTypeShow: {
            auto plan = dynamic_cast<hybridse::node::ShowPlanNode*>(node);
            auto target = absl::AsciiStrToUpper(plan->GetTarget());
            if (target.empty() || target == "TASKMANAGER") {
                return GetTaskManagerJobResult(plan->GetLikeStr(), status);
            } else if (target == "NAMESERVER") {
                return GetNameServerJobResult(plan->GetLikeStr(), status);
            } else {
                *status = {StatusCode::kCmdError, absl::StrCat("invalid component ", target)};
            }
            return {};
        }
        default: {
            *status = {StatusCode::kCmdError, "Unsupported command"};
            return {};
        }
    }
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteOfflineQuery(const std::string& db,
                                                                                const std::string& sql,
                                                                                bool is_sync_job, int job_timeout,
                                                                                ::hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    std::map<std::string, std::string> config = ParseSparkConfigString(GetSparkConfig());
    ReadSparkConfFromFile(std::dynamic_pointer_cast<SQLRouterOptions>(options_)->spark_conf_path, &config);
    AddUserToConfig(&config);

    if (is_sync_job) {
        // Run offline sql and wait to get output
        std::string output;
        LOG(WARNING) << "offline sync SELECT will show output without the data integrity promise. And it will use "
                        "local filesystem of TaskManager, it's dangerous to select a large result. You'd better use "
                        "SELECT INTO to get the correct result.";
        auto base_status = ExecuteOfflineQueryGetOutput(sql, config, db, job_timeout, &output);
        if (!base_status.OK()) {
            APPEND_FROM_BASE_AND_WARN(status, base_status, "sync offline query failed");
            return {};
        }
        // Print the output from job output
        std::vector<std::string> value = {output};
        return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, {value}, status);
    } else {
        // Run offline sql and return job info immediately
        ::openmldb::taskmanager::JobInfo job_info;
        auto base_status = ExecuteOfflineQueryAsync(sql, config, db, job_timeout, &job_info);
        if (!base_status.OK()) {
            APPEND_FROM_BASE_AND_WARN(status, base_status, "async offline query failed");
            return {};
        }

        return this->GetJobResultSet(job_info.id(), status);
    }
}

bool SQLClusterRouter::IsOnlineMode() {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    auto it = session_variables_.find("execute_mode");
    if (it != session_variables_.end() && it->second == "online") {
        return true;
    }
    return false;
}
bool SQLClusterRouter::IsEnableTrace() {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    auto it = session_variables_.find("enable_trace");
    if (it != session_variables_.end() && it->second == "true") {
        return true;
    }
    return false;
}

bool SQLClusterRouter::IsSyncJob() {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    auto it = session_variables_.find("sync_job");
    if (it != session_variables_.end() && it->second == "true") {
        return true;
    }
    return false;
}

int SQLClusterRouter::GetJobTimeout() {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    auto it = session_variables_.find("job_timeout");
    if (it != session_variables_.end()) {
        int new_timeout = 0;
        if (absl::SimpleAtoi(it->second, &new_timeout)) {
            return new_timeout;
        }
    }
    // default timeout use 60000
    return 60000;
}

std::string SQLClusterRouter::GetSparkConfig() {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    auto it = session_variables_.find("spark_config");
    if (it != session_variables_.end()) {
        return it->second;
    }

    return "";
}

::hybridse::sdk::Status SQLClusterRouter::SetVariable(hybridse::node::SetPlanNode* node) {
    std::string key = node->Key();
    std::transform(key.begin(), key.end(), key.begin(), ::tolower);
    // update session if set global
    if (node->Scope() == hybridse::node::VariableScope::kGlobalSystemVariable) {
        std::string value = node->Value()->GetExprString();
        std::transform(value.begin(), value.end(), value.begin(), ::tolower);
        hybridse::sdk::Status status;
        std::string sql = "INSERT INTO GLOBAL_VARIABLES values('" + key + "', '" + value + "');";
        if (!ExecuteInsert("INFORMATION_SCHEMA", sql, &status)) {
            RETURN_NOT_OK_PREPEND(status, "set global variable failed(insert into info table)");
        }
        if (!cluster_sdk_->TriggerNotify(::openmldb::type::NotifyType::kGlobalVar)) {
            return {StatusCode::kRunError, "zk globalvar node update failed"};
        }
    }
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    std::string value = node->Value()->GetExprString();
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    // TODO(hw): validation can be simpler
    if (key == "execute_mode") {
        if (value != "online" && value != "offline") {
            return {StatusCode::kCmdError, "the value of execute_mode must be online|offline"};
        }
    } else if (key == "enable_trace" || key == "sync_job") {
        if (value != "true" && value != "false") {
            return {StatusCode::kCmdError, "the value of " + key + " must be true|false"};
        }
    } else if (key == "job_timeout") {
        // we only validate the value here, job timeout will be used in every offline call
        int new_timeout = 0;
        if (!absl::SimpleAtoi(value, &new_timeout)) {
            return {StatusCode::kCmdError, "Fail to parse value, can't set the request timeout"};
        }
    } else if (key == "insert_memory_usage_limit") {
        int limit = 0;
        if (!absl::SimpleAtoi(value, &limit)) {
            return {StatusCode::kCmdError, "Fail to parse value, can't set the insert_memory_usage_limit"};
        }
        if (limit < 0 || limit > 100) {
            return {StatusCode::kCmdError, "Invalid value! The value must be between 0 and 100"};
        }
        insert_memory_usage_limit_.store(limit, std::memory_order_relaxed);
    } else if (key == "spark_config") {
        if (!CheckSparkConfigString(value)) {
            return {StatusCode::kCmdError,
                    "Fail to parse spark config, set like 'spark.executor.memory=2g;spark.executor.cores=2'"};
        }
    } else {
        return {};
    }
    session_variables_[key] = value;
    return {};
}

bool SQLClusterRouter::CheckSparkConfigString(const std::string& input) {
    std::istringstream iss(input);
    std::string keyValue;

    while (std::getline(iss, keyValue, ';')) {
        // Check if the substring starts with "spark."
        if (keyValue.find("spark.") != 0) {
            return false;
        }
    }

    return true;
}

::hybridse::sdk::Status SQLClusterRouter::ParseNamesFromArgs(const std::string& db,
                                                             const std::vector<std::string>& args, std::string* db_name,
                                                             std::string* name) {
    if (args.size() == 1) {
        if (db.empty()) {
            return {StatusCode::kCmdError, "Please enter database first"};
        }
        *db_name = db;
        *name = args[0];
    } else if (args.size() == 2) {
        *db_name = args[0];
        *name = args[1];
    } else {
        return {StatusCode::kCmdError, "Invalid args"};
    }
    return {};
}

std::string SQLClusterRouter::GetDatabase() {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    return db_;
}

void SQLClusterRouter::SetDatabase(const std::string& db) {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    db_ = db;
}

void SQLClusterRouter::SetInteractive(bool value) { interactive_validator_.SetInteractive(value); }

::openmldb::base::Status SQLClusterRouter::SaveResultSet(const std::string& file_path,
                                                         const std::shared_ptr<hybridse::node::OptionsMap>& options_map,
                                                         ::hybridse::sdk::ResultSet* result_set) {
    if (!result_set) {
        return {base::kInvalidParameter, "output result_set is nullptr"};
    }
    openmldb::sdk::WriteOptionsMapParser options_parser(options_map);
    auto st = options_parser.Validate();
    if (!st.ok()) {
        return {base::kInvalidParameter, st.ToString()};
    }
    auto mode = options_parser.GetAs<std::string>("mode");
    auto delimiter = options_parser.GetAs<std::string>("delimiter");
    auto quote_or = options_parser.GetAs<std::string>("quote");
    auto null_value = options_parser.GetAs<std::string>("null_value");
    auto header = options_parser.GetAs<bool>("header");
    if (!mode.ok() || !delimiter.ok() || !quote_or.ok() || !null_value.ok() || !header.ok()) {
        return {base::kInvalidParameter, "impossible: get options from map failed" + options_parser.ToString()};
    }
    auto quote = quote_or.value().empty() ? '\0' : quote_or.value()[0];
    // Check file
    std::ofstream fstream;
    if (mode.value() == "error_if_exists") {
        if (access(file_path.c_str(), 0) == 0) {
            return {base::kSQLCmdRunError, "File already exists"};
        } else {
            fstream.open(file_path);
        }
    } else if (mode.value() == "overwrite") {
        fstream.open(file_path, std::ios::out);
    } else if (mode.value() == "append") {
        fstream.open(file_path, std::ios::app);
        fstream << std::endl;
        if (header.value()) {
            LOG(WARNING) << "In the middle of output file will have header";
        }
    }
    if (!fstream.is_open()) {
        return {openmldb::base::kSQLCmdRunError, "Failed to open file, please check file path"};
    }
    // Write data
    auto* schema = result_set->GetSchema();
    // Add Header
    if (header.value()) {
        std::string schemaString;
        for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
            schemaString.append(schema->GetColumnName(i));
            if (i != schema->GetColumnCnt() - 1) {
                schemaString += delimiter.value();
            }
        }
        fstream << schemaString << std::endl;
    }
    if (result_set->Size() != 0) {
        bool first = true;
        while (result_set->Next()) {
            std::string rowString;
            for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
                if (result_set->IsNULL(i)) {
                    rowString.append(null_value.value());
                } else {
                    std::string val;
                    bool ok = result_set->GetAsString(i, val);
                    if (!ok) {
                        return {openmldb::base::kSQLCmdRunError, "Failed to get result set value"};
                    }
                    if (quote != '\0' && schema->GetColumnType(i) == hybridse::sdk::kTypeString) {
                        rowString.append(quote + val + quote);
                    } else {
                        rowString.append(val);
                    }
                }
                if (i != schema->GetColumnCnt() - 1) {
                    rowString += delimiter.value();
                } else {
                    if (!first) {
                        fstream << std::endl;
                    } else {
                        first = false;
                    }
                    fstream << rowString;
                }
            }
        }
    }
    return {};
}

// Only csv format
hybridse::sdk::Status SQLClusterRouter::HandleLoadDataInfile(
    const std::string& database, const std::string& table, const std::string& file_path,
    const openmldb::sdk::LoadOptionsMapParser& options_parser) {
    if (database.empty()) {
        return {StatusCode::kCmdError, "database is empty"};
    }

    DLOG(INFO) << options_parser.ToString();

    std::vector<std::string> file_list = base::FindFiles(file_path);
    if (file_list.empty()) {
        return {StatusCode::kCmdError, "file not exist"};
    }

    auto thread = options_parser.GetAs<int64_t>("thread");
    if (!thread.ok()) {
        return {StatusCode::kCmdError, "thread option get failed " + options_parser.ToString()};
    }
    auto thread_num = thread.value();
    std::vector<uint64_t> counts(thread_num);
    std::vector<std::future<hybridse::sdk::Status>> future_statuses;
    for (int i = 0; i < thread_num; i++) {
        future_statuses.emplace_back(std::async(&SQLClusterRouter::LoadDataMultipleFile, this, i, thread_num, database,
                                                table, file_list, options_parser, &(counts[i])));
    }
    uint64_t total_count = 0;
    hybridse::sdk::Status status;
    for (int i = 0; i < thread_num; i++) {
        auto s = future_statuses[i].get();
        if (s.IsOK()) {
            total_count += counts[i];
        } else {
            // keep the last error code
            status.code = s.code;
            status.msg = s.msg;
        }
    }

    // all ok TODO(hw): move load result to resultset
    if (status.IsOK()) {
        status.msg = absl::StrCat("Load ", total_count, " rows");
        DLOG(INFO) << status.msg;
    } else {
        // the last error
        absl::StrAppend(&status.msg, "\n", "Load ", total_count, " rows");
        LOG(WARNING) << status.ToString();
    }
    return status;
}

hybridse::sdk::Status SQLClusterRouter::LoadDataMultipleFile(int id, int step, const std::string& database,
                                                             const std::string& table,
                                                             const std::vector<std::string>& file_list,
                                                             const openmldb::sdk::LoadOptionsMapParser& options_parser,
                                                             uint64_t* count) {
    for (const auto& file : file_list) {
        uint64_t cur_count = 0;
        auto status = LoadDataSingleFile(id, step, database, table, file, options_parser, &cur_count);
        DLOG(INFO) << "[thread " << id << "] Loaded " << count << " rows in " << file;
        if (!status.IsOK()) {
            return status;
        }
        (*count) += cur_count;
    }
    return {0, absl::StrCat("Load ", std::to_string(*count), " rows")};
}

hybridse::sdk::Status SQLClusterRouter::LoadDataSingleFile(int id, int step, const std::string& database,
                                                           const std::string& table, const std::string& file_path,
                                                           const openmldb::sdk::LoadOptionsMapParser& options_parser,
                                                           uint64_t* count) {
    *count = 0;
    // read csv
    if (!base::IsExists(file_path)) {
        return {StatusCode::kCmdError, "file not exist"};
    }
    std::ifstream file(file_path);
    if (!file.is_open()) {
        return {StatusCode::kCmdError, "open file failed"};
    }

    std::string line;
    if (!std::getline(file, line)) {
        return {StatusCode::kCmdError, "read from file failed"};
    }
    std::vector<std::string> cols;
    auto deli = options_parser.GetAs<std::string>("delimiter");
    auto quote = options_parser.GetAs<std::string>("quote");
    auto null_value = options_parser.GetAs<std::string>("null_value");
    if (!deli.ok() || !quote.ok() || !null_value.ok()) {
        return {StatusCode::kCmdError, "delimiter/quote/null_value option get failed " + options_parser.ToString()};
    }
    // peek the first line, check the column size and if it's a header
    ::openmldb::sdk::SplitLineWithDelimiterForStrings(line, deli.value(), &cols,
                                                      quote.value().empty() ? '\0' : quote.value()[0]);
    auto schema = GetTableSchema(database, table);
    if (!schema) {
        return {StatusCode::kTableNotFound, "table does not exist"};
    }
    if (static_cast<int>(cols.size()) != schema->GetColumnCnt()) {
        return {StatusCode::kCmdError, "mismatch column size"};
    }
    auto header = options_parser.GetAs<bool>("header");
    if (!header.ok()) {
        return {StatusCode::kCmdError, "header option get failed " + options_parser.ToString()};
    }
    if (header.value()) {
        // the first line is the column names, check if equal with table schema
        for (int i = 0; i < schema->GetColumnCnt(); ++i) {
            if (cols[i] != schema->GetColumnName(i)) {
                return {StatusCode::kCmdError, absl::StrCat("mismatch column name ", cols[i], " ",
                                                            schema->GetColumnName(i), " in file ", file_path)};
            }
        }
        // then read the first row of data
        std::getline(file, line);
    }

    // build placeholder
    std::string holders;
    for (auto i = 0; i < schema->GetColumnCnt(); ++i) {
        holders += ((i == 0) ? "?" : ",?");
    }
    hybridse::sdk::Status status;
    std::string insert_placeholder = "insert into " + table + " values(" + holders + ");";
    std::vector<int> str_cols_idx;
    for (int i = 0; i < schema->GetColumnCnt(); ++i) {
        if (schema->GetColumnType(i) == hybridse::sdk::kTypeString) {
            str_cols_idx.emplace_back(i);
        }
    }

    int64_t i = 0;
    do {
        // only process the line assigned to its own id
        if (i % step == id) {
            cols.clear();
            ::openmldb::sdk::SplitLineWithDelimiterForStrings(line, deli.value(), &cols,
                                                              quote.value().empty() ? '\0' : quote.value()[0]);
            auto ret = InsertOneRow(database, insert_placeholder, str_cols_idx, null_value.value(), cols);
            if (!ret.IsOK()) {
                return {StatusCode::kCmdError, absl::StrCat("file [", file_path, "] line [lineno=", i, ": ", line,
                                                            "] insert failed, ", ret.msg)};
            } else {
                (*count)++;
            }
        }
        ++i;
    } while (std::getline(file, line));
    return {StatusCode::kOk, "Load " + std::to_string(i) + " rows"};
}

hybridse::sdk::Status SQLClusterRouter::InsertOneRow(const std::string& database, const std::string& insert_placeholder,
                                                     const std::vector<int>& str_col_idx, const std::string& null_value,
                                                     const std::vector<std::string>& cols) {
    if (cols.empty()) {
        return {StatusCode::kCmdError, "cols is empty"};
    }
    if (database.empty()) {
        return {StatusCode::kCmdError, "database is empty"};
    }
    hybridse::sdk::Status status;
    auto row = GetInsertRow(database, insert_placeholder, &status);
    if (!row) {
        return status;
    }
    // build row from cols
    auto& schema = row->GetSchema();
    auto cnt = schema->GetColumnCnt();
    if (cnt != static_cast<int>(cols.size())) {
        return {StatusCode::kCmdError, "col size mismatch"};
    }
    // scan all strings , calc the sum, to init SQLInsertRow's string length
    std::string::size_type str_len_sum = 0;
    for (auto idx : str_col_idx) {
        if (cols[idx] != null_value) {
            str_len_sum += cols[idx].length();
        }
    }
    row->Init(static_cast<int>(str_len_sum));

    for (int i = 0; i < cnt; ++i) {
        if (!::openmldb::codec::AppendColumnValue(cols[i], schema->GetColumnType(i), schema->IsColumnNotNull(i),
                                                  null_value, row)) {
            return {StatusCode::kCmdError, absl::StrCat("translate failed on column ", schema->GetColumnName(i), "(", i,
                                                        ") with value ", cols[i])};
        }
    }
    if (!row->IsComplete()) {
        return {StatusCode::kCmdError, "row is not complete: " + absl::StrJoin(cols, ",")};
    }

    if (!ExecuteInsert(database, insert_placeholder, row, &status)) {
        RETURN_NOT_OK_PREPEND(status, "insert row failed");
    }
    return {};
}

hybridse::sdk::Status SQLClusterRouter::HandleDelete(const std::string& db, const std::string& table_name,
                                                     const hybridse::node::ExprNode* condition) {
    if (db.empty() || table_name.empty()) {
        return {StatusCode::kCmdError, "database or table is empty"};
    }
    if (condition == nullptr) {
        return {StatusCode::kCmdError, "has not where condition"};
    }
    auto table_info = cluster_sdk_->GetTableInfo(db, table_name);
    if (!table_info) {
        return {StatusCode::kCmdError, "table " + table_name + " in db " + db + " does not exist"};
    }
    std::vector<Condition> condition_vec;
    std::vector<Condition> parameter_vec;
    auto binary_node = dynamic_cast<const hybridse::node::BinaryExpr*>(condition);
    auto col_map = schema::SchemaAdapter::GetColMap(*table_info);
    auto status =
        NodeAdapter::ExtractCondition(binary_node, col_map, table_info->column_key(), &condition_vec, &parameter_vec);
    if (!status.IsOK()) {
        return status;
    }
    if (!parameter_vec.empty()) {
        return {StatusCode::kCmdError, "unsupport placeholder in sql"};
    }
    DeleteOption option;
    status = NodeAdapter::ExtractDeleteOption(table_info->column_key(), condition_vec, &option);
    if (!status.IsOK()) {
        return status;
    }
    status = SendDeleteRequst(table_info, option);
    if (status.IsOK() && db != nameserver::INTERNAL_DB) {
        status = {
            StatusCode::kOk,
            "DELETE is a dangerous operation. Once deleted, it is very difficult to recover. You may also note that:\n"
            "- The deleted data will not be released immediately from the main memory; "
            "it remains until after a garbage collection interval (gc_interval)\n"
            "Please refer to this link for more details: " +
                base::NOTICE_URL};
    }
    return status;
}

hybridse::sdk::Status SQLClusterRouter::SendDeleteRequst(
    const std::shared_ptr<::openmldb::nameserver::TableInfo>& table_info, const DeleteOption& option) {
    if (!option.idx.has_value()) {
        std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
        if (!cluster_sdk_->GetTablet(table_info->db(), table_info->name(), &tablets)) {
            return {StatusCode::kCmdError, "get tablet failed"};
        }
        for (auto& tablet : tablets) {
            if (!tablet || !tablet->GetClient()) {
                return {StatusCode::kCmdError, "cannot connect tablet"};
            }
        }
        for (size_t pid = 0; pid < tablets.size(); pid++) {
            auto tablet_client = tablets.at(pid)->GetClient();
            if (!tablet_client) {
                return {StatusCode::kCmdError, "tablet client is null"};
            }
            auto ret = tablet_client->Delete(table_info->tid(), pid, option, options_->request_timeout);
            if (!ret.OK()) {
                return {StatusCode::kCmdError, ret.GetMsg()};
            }
        }
    } else {
        uint32_t pid = ::openmldb::base::hash64(option.key) % table_info->table_partition_size();
        auto tablet = cluster_sdk_->GetTablet(table_info->db(), table_info->name(), pid);
        if (!tablet) {
            return {StatusCode::kCmdError, "cannot connect tablet"};
        }
        auto tablet_client = tablet->GetClient();
        if (!tablet_client) {
            return {StatusCode::kCmdError, "tablet client is null"};
        }
        if (auto ret = tablet_client->Delete(table_info->tid(), pid, option, options_->request_timeout); !ret.OK()) {
            return {StatusCode::kCmdError, ret.GetMsg()};
        }
    }
    return {};
}

bool SQLClusterRouter::ExecuteDelete(std::shared_ptr<SQLDeleteRow> row, hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    if (!row) {
        SET_STATUS_AND_WARN(status, StatusCode::kNullInputPointer, "delete row is nullptr");
        return false;
    }
    const auto& db = row->GetDatabase();
    const auto& table_name = row->GetTableName();
    auto table_info = cluster_sdk_->GetTableInfo(db, table_name);
    if (!table_info) {
        SET_STATUS_AND_WARN(status, StatusCode::kCmdError, "table " + db + "." + table_name + " does not exist");
        return false;
    }
    const auto& condition_vec = row->GetValue();
    DeleteOption option;
    *status = NodeAdapter::ExtractDeleteOption(table_info->column_key(), condition_vec, &option);
    if (!status->IsOK()) {
        return false;
    }
    *status = SendDeleteRequst(table_info, option);
    return status->IsOK();
}

hybridse::sdk::Status SQLClusterRouter::HandleCreateFunction(const hybridse::node::CreateFunctionPlanNode* node) {
    if (node == nullptr) {
        return {StatusCode::kCmdError, "illegal create function statement"};
    }
    auto fun = std::make_shared<openmldb::common::ExternalFun>();
    fun->set_name(node->Name());
    auto type_node = dynamic_cast<const ::hybridse::node::TypeNode*>(node->GetReturnType());
    if (type_node == nullptr) {
        return {StatusCode::kCmdError, "illegal create function statement"};
    }
    openmldb::type::DataType data_type;
    if (!::openmldb::schema::SchemaAdapter::ConvertType(type_node->base(), &data_type)) {
        return {StatusCode::kCmdError, "illegal return type"};
    }
    fun->set_return_type(data_type);
    for (const auto arg_node : node->GetArgsType()) {
        type_node = dynamic_cast<const ::hybridse::node::TypeNode*>(arg_node);
        if (type_node == nullptr) {
            return {StatusCode::kCmdError, "illegal create function statement"};
        }
        if (!::openmldb::schema::SchemaAdapter::ConvertType(type_node->base(), &data_type)) {
            return {StatusCode::kCmdError, "illegal argument type"};
        }
        fun->add_arg_type(data_type);
    }
    fun->set_is_aggregate(node->IsAggregate());
    auto option = node->Options();
    if (!option || option->find("FILE") == option->end()) {
        return {StatusCode::kCmdError, "missing FILE option"};
    }
    fun->set_file((*option)["FILE"]->GetExprString());
    if (auto iter = option->find("RETURN_NULLABLE"); iter != option->end()) {
        if (iter->second->GetDataType() != hybridse::node::kBool) {
            return {StatusCode::kCmdError, "return_nullable should be bool"};
        }
        fun->set_return_nullable(iter->second->GetBool());
    }
    if (auto iter = option->find("ARG_NULLABLE"); iter != option->end()) {
        if (iter->second->GetDataType() != hybridse::node::kBool) {
            return {StatusCode::kCmdError, "arg_nullable should be bool"};
        }
        fun->set_arg_nullable(iter->second->GetBool());
    }
    hybridse::sdk::Status st;
    if (cluster_sdk_->IsClusterMode()) {
        auto taskmanager_client = cluster_sdk_->GetTaskManagerClient();
        if (taskmanager_client) {
            auto ret = taskmanager_client->CreateFunction(fun, GetJobTimeout());
            if (!ret.OK()) {
                APPEND_FROM_BASE_AND_WARN(&st, ret, "create function failed on taskmanager");
                return st;
            }
        }
    }
    auto ns = cluster_sdk_->GetNsClient();
    auto ret = ns->CreateFunction(*fun);
    if (!ret.OK()) {
        APPEND_FROM_BASE_AND_WARN(&st, ret, "create function failed on nameserver");
        return st;
    }
    cluster_sdk_->RegisterExternalFun(fun);
    return st;
}

hybridse::sdk::Status SQLClusterRouter::HandleDeploy(const std::string& db,
                                                     const hybridse::node::DeployPlanNode* deploy_node,
                                                     std::optional<uint64_t>* job_id) {
    hybridse::sdk::Status status;
    if (db.empty()) {
        return {StatusCode::kCmdError, "database is empty"};
    }
    if (deploy_node == nullptr) {
        return {StatusCode::kCmdError, "illegal deploy statement"};
    }

    std::string select_sql = deploy_node->StmtStr() + ";";
    hybridse::vm::ExplainOutput explain_output;
    hybridse::base::Status sql_status;
    if (!cluster_sdk_->GetEngine()->Explain(select_sql, db, hybridse::vm::kMockRequestMode, &explain_output,
                                            &sql_status)) {
        COPY_PREPEND_AND_WARN(&status, sql_status, "explain failed");
        return status;
    }
    // pack ProcedureInfo
    ::openmldb::api::ProcedureInfo sp_info;
    sp_info.set_db_name(db);
    sp_info.set_sp_name(deploy_node->Name());
    if (!explain_output.request_db_name.empty()) {
        sp_info.set_main_db(explain_output.request_db_name);
    } else {
        sp_info.set_main_db(db);
    }
    sp_info.set_main_table(explain_output.request_name);
    auto input_schema = sp_info.mutable_input_schema();
    auto output_schema = sp_info.mutable_output_schema();
    if (!openmldb::schema::SchemaAdapter::ConvertSchema(explain_output.input_schema, input_schema) ||
        !openmldb::schema::SchemaAdapter::ConvertSchema(explain_output.output_schema, output_schema)) {
        return {StatusCode::kCmdError, "convert schema failed"};
    }

    auto& table_pair = explain_output.dependent_tables;
    for (auto& table : table_pair) {
        auto db_table = sp_info.add_tables();
        db_table->set_db_name(table.first);
        db_table->set_table_name(table.second);
    }
    const auto& router_col = explain_output.router.GetRouterCol();
    std::stringstream str_stream;
    str_stream << "CREATE PROCEDURE " << deploy_node->Name() << " (";
    for (int idx = 0; idx < input_schema->size(); idx++) {
        const auto& col = input_schema->Get(idx);
        auto it = codec::DATA_TYPE_STR_MAP.find(col.data_type());
        if (it == codec::DATA_TYPE_STR_MAP.end()) {
            return {StatusCode::kCmdError, "illegal data type"};
        }
        str_stream << "`" << col.name() << "` " << it->second;
        if (idx != input_schema->size() - 1) {
            str_stream << ", ";
        }
        if (col.name() == router_col) {
            sp_info.add_router_col(idx);
        }
    }
    str_stream << ") BEGIN " << select_sql << " END;";

    sp_info.set_sql(str_stream.str());
    sp_info.set_type(::openmldb::type::ProcedureType::kReqDeployment);
    for (const auto& o : *deploy_node->Options()) {
        auto option = sp_info.add_options();
        option->set_name(o.first);
        option->mutable_value()->set_value(o.second->GetExprString());
    }

    TableInfoMap table_map;
    uint64_t record_cnt = 0;
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    auto ns = cluster_sdk_->GetNsClient();
    for (const auto& pair : table_pair) {
        tables.clear();
        // args: table name, db name, ...
        auto ok = ns->ShowTable(pair.second, pair.first, false, tables, msg);
        if (!ok) {
            return {StatusCode::kCmdError, msg};
        }
        if (tables.empty() || tables.size() > 1) {
            return {StatusCode::kCmdError, "table not found or more than one table found"};
        }
        table_map[pair.first][pair.second] = tables[0];
        for (int idx = 0; idx < tables[0].table_partition_size(); idx++) {
            record_cnt += tables[0].table_partition(idx).record_cnt();
        }
    }

    bool skip_index_check = false;
    auto iter = deploy_node->Options()->find(SKIP_INDEX_CHECK_OPTION);
    if (iter != deploy_node->Options()->end()) {
        std::string skip_index_value = iter->second->GetExprString();
        if (absl::EqualsIgnoreCase(skip_index_value, "true")) {
            skip_index_check = true;
        }
    }

    // bias parse
    // range bias: int means xx ms, int & interval will covert to minutes, if == 0, no bias, if has part < 1min, add
    // 1min
    Bias bias;
    iter = deploy_node->Options()->find(RANGE_BIAS_OPTION);
    if (iter != deploy_node->Options()->end()) {
        if (!bias.SetRange(iter->second)) {
            return {StatusCode::kCmdError, "range bias '" + iter->second->GetExprString() + "' is illegal"};
        }
    }
    iter = deploy_node->Options()->find(ROWS_BIAS_OPTION);
    if (iter != deploy_node->Options()->end()) {
        if (!bias.SetRows(iter->second)) {
            return {StatusCode::kCmdError, "rows bias '" + iter->second->GetExprString() + "' is illegal"};
        }
    }

    base::MultiDBIndexMap new_index_map;
    // merge index, update exists index ttl(add bias) in table, get new index(add bias) to create
    auto get_index_status = GetNewIndex(table_map, select_sql, db, skip_index_check, bias, &new_index_map);
    if (!get_index_status.IsOK()) {
        return get_index_status;
    }
    std::stringstream index_stream;
    for (auto [db, db_map] : new_index_map) {
        for (auto [table, index_list] : db_map) {
            for (auto index : index_list) {
                index_stream << db << "-" << table << "-";
                for (auto col : index.col_name()) {
                    index_stream << col << ",";
                }
                index_stream << "|" << index.ts_name() << ";";
            }
        }
    }
    LOG(INFO) << "should create new indexs: " << index_stream.str();

    if (!new_index_map.empty()) {
        if (cluster_sdk_->IsClusterMode() && record_cnt > 0) {
            // long windows only support one table(no join/union), no secondary table, so no multi db support
            auto lw_status = HandleLongWindows(deploy_node, table_pair, select_sql);
            if (!lw_status.IsOK()) {
                return lw_status;
            }
            uint64_t id = 0;
            auto deploy_status = ns->DeploySQL(sp_info, new_index_map, &id);
            if (!deploy_status.OK()) {
                return {deploy_status.GetCode(), deploy_status.GetMsg()};
            }
            bool sync = true;
            auto iter = deploy_node->Options()->find(SYNC_OPTION);
            if (iter != deploy_node->Options()->end()) {
                std::string skip_index_value = iter->second->GetExprString();
                if (absl::EqualsIgnoreCase(skip_index_value, "false")) {
                    sync = false;
                }
            }
            if (sync) {
                if (record_cnt > 1000000) {
                    LOG(INFO) << "There is data in the table, it may take a few minutes to load the data";
                } else if (record_cnt > 100000) {
                    LOG(INFO) << "There is data in the table, it may take a few seconds to load the data";
                }
                uint64_t start_ts = ::baidu::common::timer::get_micros() / 1000;
                while (true) {
                    nameserver::ShowOPStatusResponse response;
                    auto status = ns->ShowOPStatus(id, &response);
                    if (!status.OK()) {
                        return {status.GetCode(), status.GetMsg()};
                    }
                    if (response.op_status_size() < 1) {
                        return {-1, absl::StrCat("op does not exist. id ", id)};
                    }
                    if (response.op_status(0).status() == "kDone") {
                        return {};
                    } else if (response.op_status(0).status() == "kFailed" ||
                               response.op_status(0).status() == "kCanceled") {
                        return {-1, absl::StrCat("op status is ", response.op_status(0).status())};
                    }
                    uint64_t cur_ts = ::baidu::common::timer::get_micros() / 1000;
                    if (cur_ts - start_ts > static_cast<uint64_t>(FLAGS_deploy_job_max_wait_time_ms)) {
                        return {-1, "exceed max wait time"};
                    }
                    sleep(1);
                }
            } else {
                job_id->emplace(id);
            }
            return {};
        } else {
            auto add_index_status = AddNewIndex(new_index_map);
            if (!add_index_status.IsOK()) {
                return add_index_status;
            }
        }
    }
    auto lw_status = HandleLongWindows(deploy_node, table_pair, select_sql);
    if (!lw_status.IsOK()) {
        return lw_status;
    }

    auto ob_status = ns->CreateProcedure(sp_info, options_->request_timeout);
    if (!ob_status.OK()) {
        APPEND_FROM_BASE_AND_WARN(&status, ob_status, "ns create procedure failed");
        return status;
    }
    return {};
}

hybridse::sdk::Status SQLClusterRouter::GetNewIndex(const TableInfoMap& table_map, const std::string& select_sql,
                                                    const std::string& db, bool skip_index_check, const Bias& bias,
                                                    base::MultiDBIndexMap* new_index_map) {
    // convert info map to desc map
    base::MultiDBTableDescMap table_desc_map;
    for (auto& db_table : table_map) {
        for (auto& kv : db_table.second) {
            table_desc_map[db_table.first].emplace(
                kv.first,
                std::vector<common::ColumnDesc>{kv.second.column_desc().begin(), kv.second.column_desc().end()});
        }
    }
    auto index_map = base::DDLParser::ExtractIndexes(select_sql, db, table_desc_map);
    auto ns = cluster_sdk_->GetNsClient();
    // add bias in the loop
    for (auto& db_index : index_map) {
        auto& db_name = db_index.first;
        for (auto& kv : db_index.second) {
            // for each table, the indexs to be added may be dup(key&ts may be the same, ttl may be different, it's
            // still called dup) so we need to merge them, and after merge, if the new index is the same as the existed
            // one(in table), we do update, not create
            std::string table_name = kv.first;
            std::vector<::openmldb::common::ColumnKey> extract_column_keys = kv.second;

            // gen existed index map
            auto it = table_map.find(db_name);
            if (it == table_map.end()) {
                return {StatusCode::kCmdError, "db " + db_name + "does not exist"};
            }
            auto table_it = it->second.find(table_name);
            if (table_it == it->second.end()) {
                return {StatusCode::kCmdError, "table " + table_name + " does not exist"};
            }
            auto& table = table_it->second;
            std::set<std::string> col_set;
            for (const auto& column_desc : table.column_desc()) {
                col_set.insert(column_desc.name());
            }
            std::map<std::string, ::openmldb::common::ColumnKey> exists_index_map;
            for (const auto& column_key : table.column_key()) {
                if (exists_index_map.find(openmldb::schema::IndexUtil::GetIDStr(column_key)) !=
                    exists_index_map.end()) {
                    LOG(WARNING) << "exist two indexes which are the same id "
                                 << openmldb::schema::IndexUtil::GetIDStr(column_key) << " in table " << table_name;
                }
                exists_index_map.emplace(openmldb::schema::IndexUtil::GetIDStr(column_key), column_key);
            }

            int cur_index_num = table.column_key_size();
            int add_index_num = 0;
            // extract_column_keys & exists_index_map won't have dup indexs, just check if the indexs are the same as
            // the existed ones
            std::vector<::openmldb::common::ColumnKey> new_indexs;
            for (auto& column_key : extract_column_keys) {
                // add bias on extracted index, 0 means unbounded
                auto new_column_key = bias.AddBias(column_key);
                LOG(INFO) << "add bias on extracted index " << column_key.ShortDebugString() << " with bias " << bias
                          << ", result " << new_column_key.ShortDebugString();

                auto index_id = openmldb::schema::IndexUtil::GetIDStr(new_column_key);
                auto it = exists_index_map.find(index_id);
                if (it != exists_index_map.end()) {
                    auto& old_column_key = it->second;
                    common::TTLSt result;
                    // if skip index check, we don't do update ttl, for backward compatibility(server <=0.8.0)
                    if (base::TTLMerge(old_column_key.ttl(), new_column_key.ttl(), &result) && !skip_index_check) {
                        // update ttl
                        auto ns_ptr = cluster_sdk_->GetNsClient();
                        std::string err;
                        if (!ns_ptr->UpdateTTL(db_name, table_name, result.ttl_type(), result.abs_ttl(),
                                               result.lat_ttl(), old_column_key.index_name(), err)) {
                            return {StatusCode::kCmdError, "update ttl failed"};
                        }
                    }
                } else {
                    new_column_key.set_index_name(
                        absl::StrCat("INDEX_", cur_index_num + add_index_num, "_", ::baidu::common::timer::now_time()));
                    add_index_num++;
                    new_indexs.emplace_back(new_column_key);
                }
            }

            if (!new_indexs.empty()) {
                (*new_index_map)[db_name].emplace(table_name, std::move(new_indexs));
            }
        }
    }
    return {};
}

hybridse::sdk::Status SQLClusterRouter::AddNewIndex(const base::MultiDBIndexMap& new_index_map) {
    auto ns = cluster_sdk_->GetNsClient();
    for (auto& db_map : new_index_map) {
        auto& db = db_map.first;
        for (auto& kv : db_map.second) {
            bool skip_load_data = cluster_sdk_->IsClusterMode();
            auto status = ns->AddMultiIndex(db, kv.first, kv.second, skip_load_data);
            if (!status.OK()) {
                return {StatusCode::kCmdError,
                        absl::StrCat("table [", db, ".", kv.first, "] add index failed. ", status.msg)};
            }
        }
    }
    return {};
}

hybridse::sdk::Status SQLClusterRouter::HandleLongWindows(
    const hybridse::node::DeployPlanNode* deploy_node, const std::set<std::pair<std::string, std::string>>& table_pair,
    const std::string& select_sql) {
    auto iter = deploy_node->Options()->find(hybridse::vm::LONG_WINDOWS);
    std::string long_window_param = "";
    if (iter != deploy_node->Options()->end()) {
        long_window_param = iter->second->GetExprString();
    } else {
        return {};
    }
    std::unordered_map<std::string, std::string> long_window_map;
    if (!long_window_param.empty()) {
        if (table_pair.size() != 1) {
            return {StatusCode::kUnsupportSql, "unsupport multi tables with long window options"};
        }
        std::string base_db = table_pair.begin()->first;
        std::string base_table = table_pair.begin()->second;
        std::vector<std::string> windows;
        windows = absl::StrSplit(long_window_param, ",");
        for (auto& window : windows) {
            std::vector<std::string> window_info;
            window_info = absl::StrSplit(window, ":");

            if (window_info.size() == 2) {
                long_window_map[window_info[0]] = window_info[1];
            } else if (window_info.size() == 1) {
                long_window_map[window_info[0]] = FLAGS_bucket_size;
            } else {
                return {StatusCode::kSyntaxError, "illegal long window format"};
            }
        }
        // extract long windows info from select_sql
        openmldb::base::LongWindowInfos long_window_infos;
        auto extract_status = base::DDLParser::ExtractLongWindowInfos(select_sql, long_window_map, &long_window_infos);
        if (!extract_status.IsOK()) {
            return extract_status;
        }
        std::set<std::string> distinct_long_window;
        for (const auto& info : long_window_infos) {
            distinct_long_window.insert(info.window_name_);
        }
        if (distinct_long_window.size() != long_window_map.size()) {
            return {StatusCode::kSyntaxError, "long_windows option doesn't match window in sql"};
        }
        auto ns_client = cluster_sdk_->GetNsClient();
        std::vector<::openmldb::nameserver::TableInfo> tables;
        std::string msg;
        ns_client->ShowTable(base_table, base_db, false, tables, msg);
        if (tables.size() != 1) {
            return {StatusCode::kTableNotFound, absl::StrCat("base table", base_db, ".", base_table, "not found")};
        }
        std::string meta_db = openmldb::nameserver::INTERNAL_DB;
        std::string meta_table = openmldb::nameserver::PRE_AGG_META_NAME;
        std::string aggr_db = openmldb::nameserver::PRE_AGG_DB;

        uint64_t record_cnt = 0;
        auto& table = tables[0];
        for (int idx = 0; idx < table.table_partition_size(); idx++) {
            record_cnt += table.table_partition(idx).record_cnt();
        }
        // TODO(zhanghaohit): record_cnt is updated by ns periodically, causing a delay to get the latest value
        if (record_cnt > 0) {
            return {StatusCode::kUnSupport,
                    "table " + table.name() +
                        " has online data, cannot deploy with long_windows option. please drop this table and create a "
                        "new one"};
        }

        for (const auto& lw : long_window_infos) {
            if (absl::EndsWithIgnoreCase(lw.aggr_func_, "_where")) {
                // TOOD(ace): *_where op only support for memory base table
                if (tables[0].storage_mode() != common::StorageMode::kMemory) {
                    return {StatusCode::kUnSupport,
                            absl::StrCat(lw.aggr_func_, " only support over memory base table")};
                }

                // TODO(#2313): *_where for rows bucket should support later
                if (openmldb::base::IsNumber(long_window_map.at(lw.window_name_))) {
                    return {StatusCode::kUnSupport, absl::StrCat("unsupport *_where op (", lw.aggr_func_,
                                                                 ") for rows bucket type long window")};
                }

                // unsupport filter col of date/timestamp
                for (int i = 0; i < tables[0].column_desc_size(); ++i) {
                    if (lw.filter_col_ == tables[0].column_desc(i).name()) {
                        auto type = tables[0].column_desc(i).data_type();
                        if (type == type::DataType::kDate || type == type::DataType::kTimestamp) {
                            return {
                                StatusCode::kUnSupport,
                                absl::Substitute("unsupport date or timestamp as filter column ($0)", lw.filter_col_)};
                        }
                    }
                }
            }
            // check if pre-aggr table exists
            ::hybridse::sdk::Status status;
            bool is_exist = CheckPreAggrTableExist(base_table, base_db, lw, &status);
            if (!status.IsOK()) {
                return status;
            }
            if (is_exist) {
                continue;
            }
            // insert pre-aggr meta info to meta table
            std::string aggr_col = lw.aggr_col_ == "*" ? "" : lw.aggr_col_;
            auto aggr_table =
                absl::StrCat("pre_", base_db, "_", deploy_node->Name(), "_", lw.window_name_, "_", lw.aggr_func_, "_",
                             aggr_col, lw.filter_col_.empty() ? "" : "_" + lw.filter_col_);
            std::string insert_sql = absl::StrCat(
                "insert into ", meta_db, ".", meta_table, " values('" + aggr_table, "', '", aggr_db, "', '", base_db,
                "', '", base_table, "', '", lw.aggr_func_, "', '", lw.aggr_col_, "', '", lw.partition_col_, "', '",
                lw.order_col_, "', '", lw.bucket_size_, "', '", lw.filter_col_, "');");
            bool ok = ExecuteInsert("", insert_sql, &status);
            if (!ok) {
                RETURN_NOT_OK_PREPEND(status, "insert pre-aggr meta failed");
            }

            // create pre-aggr table
            auto create_status = CreatePreAggrTable(aggr_db, aggr_table, lw, tables[0], ns_client);
            if (!create_status.OK()) {
                return {StatusCode::kRunError, "create pre-aggr table failed"};
            }

            // create aggregator
            std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
            bool ret = cluster_sdk_->GetTablet(base_db, base_table, &tablets);
            if (!ret || tablets.empty()) {
                return {StatusCode::kRunError, "get tablets failed"};
            }
            auto base_table_info = cluster_sdk_->GetTableInfo(base_db, base_table);
            if (!base_table_info) {
                return {StatusCode::kTableNotFound, "get table info failed"};
            }
            auto aggr_id = cluster_sdk_->GetTableId(aggr_db, aggr_table);
            ::openmldb::api::TableMeta base_table_meta;
            base_table_meta.set_db(base_table_info->db());
            base_table_meta.set_name(base_table_info->name());
            base_table_meta.set_tid(static_cast<::google::protobuf::int32>(base_table_info->tid()));
            for (int idx = 0; idx < base_table_info->column_desc_size(); idx++) {
                ::openmldb::common::ColumnDesc* column_desc = base_table_meta.add_column_desc();
                column_desc->CopyFrom(base_table_info->column_desc(idx));
            }

            uint32_t index_pos;
            bool found_idx = false;
            for (int idx = 0; idx < base_table_info->column_key_size(); idx++) {
                ::openmldb::common::ColumnKey* column_key = base_table_meta.add_column_key();
                column_key->CopyFrom(base_table_info->column_key(idx));
                std::string partition_keys = "";
                for (int j = 0; j < column_key->col_name_size(); j++) {
                    partition_keys += column_key->col_name(j) + ",";
                }
                partition_keys.pop_back();
                if (partition_keys == lw.partition_col_ && column_key->ts_name() == lw.order_col_) {
                    index_pos = idx;
                    found_idx = true;
                }
            }
            if (!found_idx) {
                return {StatusCode::kIndexNotFound, "index associated with aggregator not found"};
            }
            for (uint32_t pid = 0; pid < tablets.size(); ++pid) {
                auto tablet_client = tablets[pid]->GetClient();
                if (tablet_client == nullptr) {
                    return {StatusCode::kRunError, "get tablet client failed"};
                }
                base_table_meta.set_pid(pid);
                if (!tablet_client->CreateAggregator(base_table_meta, aggr_id, pid, index_pos, lw)) {
                    return {StatusCode::kRunError, "create aggregator failed"};
                }
            }
        }
    }
    return {};
}

bool SQLClusterRouter::CheckPreAggrTableExist(const std::string& base_table, const std::string& base_db,
                                              const openmldb::base::LongWindowInfo& lw,
                                              ::hybridse::sdk::Status* status) {
    RET_FALSE_IF_NULL_AND_WARN(status, "output status is nullptr");
    std::string meta_db = openmldb::nameserver::INTERNAL_DB;
    std::string meta_table = openmldb::nameserver::PRE_AGG_META_NAME;
    std::string filter_cond = lw.filter_col_.empty() ? "" : " and filter_col = '" + lw.filter_col_ + "'";
    std::string meta_info =
        absl::StrCat("base_db = '", base_db, "' and base_table = '", base_table, "' and aggr_func = '", lw.aggr_func_,
                     "' and aggr_col = '", lw.aggr_col_, "' and partition_cols = '", lw.partition_col_,
                     "' and order_by_col = '", lw.order_col_, "'", filter_cond);
    std::string select_sql =
        absl::StrCat("select bucket_size from ", meta_db, ".", meta_table, " where ", meta_info, ";");
    auto rs = ExecuteSQL("", select_sql, status);
    if (!status->IsOK()) {
        LOG(ERROR) << "Select from " << meta_db << "." << meta_table << "  failed: " << status->msg;
        return false;
    }

    if (rs->Size() > 0) {
        LOG(INFO) << "Pre-aggregated table with same meta info already exist: " << meta_info;
        return true;
    } else {
        return false;
    }
}

static const ::openmldb::schema::PBSchema& GetComponetSchema() {
    auto add_field = [](const std::string& name, openmldb::type::DataType type, openmldb::common::ColumnDesc* field) {
        if (field != nullptr) {
            field->set_name(name);
            field->set_data_type(type);
        }
    };
    auto build_schema = [&add_field]() {
        ::openmldb::schema::PBSchema schema;
        add_field("Endpoint", openmldb::type::DataType::kString, schema.Add());
        add_field("Role", openmldb::type::DataType::kString, schema.Add());
        add_field("Connect_time", openmldb::type::DataType::kTimestamp, schema.Add());
        add_field("Status", openmldb::type::DataType::kString, schema.Add());
        add_field("Ns_role", openmldb::type::DataType::kString, schema.Add());
        return schema;
    };
    static ::openmldb::schema::PBSchema schema = build_schema();
    return schema;
}

// Implementation for SHOW COMPONENTS
//
// Error handling:
// it do not set status to fail even e.g. some zk query internally failed,
// which as a consequence, produce partial or empty result on internal error.
//
// in detail
// 1. tablet and nameserver are required, error status or empty result considered error (warning log printed)
// 2. task manager and api server are optional, only error status produce warning log
// 3. task manager only query and show in cluster mode
//
//
// output schema: (Endpoint: string, Role: string,
//                 Connect_time: int64, Status: string, Ns_role: string)
// where
// - Endpoint IP:PORT or DOMAIN:PORT
// - Role can be 'tablet', 'nameserver', 'taskmanager'
// - Connect_time last conncted timestamp from epoch
// - Status can be 'online', 'offline' or 'NULL' (otherwise)
// - Ns_role can be 'master', 'standby', or 'NULL' (for non-namespace component)
std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteShowComponents(hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    std::vector<std::shared_ptr<ResultSetSQL>> data;

    {
        hybridse::sdk::Status s;
        auto tablets = std::dynamic_pointer_cast<ResultSetSQL>(ExecuteShowTablets(&s));
        if (tablets != nullptr && s.IsOK()) {
            data.push_back(std::move(tablets));
        } else {
            LOG(WARNING) << "[WARN]: show tablets, code: " << s.code << ", msg: " << s.msg;
        }
    }

    {
        hybridse::sdk::Status s;
        auto nameservers = std::dynamic_pointer_cast<ResultSetSQL>(ExecuteShowNameServers(&s));
        if (nameservers != nullptr && s.IsOK()) {
            data.push_back(std::move(nameservers));
        } else {
            LOG(WARNING) << "[WARN]: show nameservers, code: " << s.code << ", msg: " << s.msg;
        }
    }

    if (cluster_sdk_->IsClusterMode()) {
        // only get task managers in cluster mode
        hybridse::sdk::Status s;
        auto task_managers = std::dynamic_pointer_cast<ResultSetSQL>(ExecuteShowTaskManagers(&s));
        if (s.IsOK()) {
            if (task_managers != nullptr) {
                data.push_back(std::move(task_managers));
            }
        } else {
            LOG(WARNING) << "[WARN]: show taskmanagers, code: " << s.code << ", msg: " << s.msg;
        }
    }

    {
        hybridse::sdk::Status s;
        auto api_servres = std::dynamic_pointer_cast<ResultSetSQL>(ExecuteShowApiServers(&s));
        if (s.IsOK()) {
            if (api_servres != nullptr) {
                data.push_back(std::move(api_servres));
            }
        } else {
            LOG(WARNING) << "[WARN]: show api servers, code: " << s.code << ", msg: " << s.msg;
        }
    }

    status->code = hybridse::common::kOk;
    return MultipleResultSetSQL::MakeResultSet(data, 0, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteShowNameServers(hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");

    const auto& schema = GetComponetSchema();

    auto zk_client = cluster_sdk_->GetZkClient();
    if (!cluster_sdk_->IsClusterMode() || zk_client == nullptr) {
        // standalone mode
        std::string endpoint, real_endpoint;
        if (!cluster_sdk_->GetNsAddress(&endpoint, &real_endpoint)) {
            status->code = hybridse::common::kRunError;
            status->msg = "fail to get ns address";
            return {};
        }

        // TODO(aceforeverd): support connect time for ns in standalone mode
        std::vector<std::vector<std::string>> data = {{endpoint, "nameserver", "0", "online", "master"}};
        return ResultSetSQL::MakeResultSet(schema, data, status);
    }

    auto ns_client = cluster_sdk_->GetNsClient();
    if (!ns_client) {
        *status = {hybridse::common::kRunError, "ns client is nullptr"};
        return {};
    }
    const auto& leader = ns_client->GetEndpoint();

    std::string node_path = absl::StrCat(std::dynamic_pointer_cast<SQLRouterOptions>(options_)->zk_path, "/leader");
    std::vector<std::string> children;
    if (!zk_client->GetChildren(node_path, children) || children.empty()) {
        status->code = hybridse::common::kRunError;
        status->msg = "get nameserver children failed";
        return {};
    }

    // endponit => create time (time in milliseconds from epoch)
    std::map<std::string, int64_t> endpoint_map;
    for (const auto& path : children) {
        std::string real_path = absl::StrCat(node_path, "/", path);
        std::string endpoint;
        Stat stat;
        if (!zk_client->GetNodeValueAndStat(real_path.c_str(), &endpoint, &stat)) {
            status->code = hybridse::common::kRunError;
            status->msg = absl::StrCat("get endpoint failed for path: ", real_path);
            return {};
        }
        if (endpoint_map.find(endpoint) == endpoint_map.end()) {
            endpoint_map[endpoint] = stat.ctime;
        } else {
            // pickup the latest register time
            endpoint_map[endpoint] = std::max(stat.ctime, endpoint_map[endpoint]);
        }
    }

    std::vector<std::vector<std::string>> data;
    for (auto it = endpoint_map.cbegin(); it != endpoint_map.cend(); it++) {
        std::vector<std::string> val = {
            it->first,                                  // endpoint
            "nameserver",                               // role
            std::to_string(it->second),                 // connect time
            "online",                                   // status
            it->first == leader ? "master" : "standby"  // ns_role
        };
        data.push_back(std::move(val));
    }
    return ResultSetSQL::MakeResultSet(schema, data, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteShowTablets(hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    const auto& schema = GetComponetSchema();
    auto ns_client = cluster_sdk_->GetNsClient();

    std::vector<::openmldb::client::TabletInfo> tablets;
    std::string msg;
    bool ok = ns_client->ShowTablet(tablets, msg);
    if (!ok) {
        status->code = hybridse::common::StatusCode::kRunError;
        status->msg = absl::StrCat("Fail to show tablets. error msg: ", msg);
        return {};
    }

    std::vector<std::vector<std::string>> data(tablets.size(), std::vector<std::string>(schema.size(), ""));

    for (size_t i = 0; i < tablets.size(); i++) {
        data[i][0] = tablets[i].endpoint;  // endpoint
        data[i][1] = "tablet";             // role

        // connecct time
        data[i][2] = std::to_string(::baidu::common::timer::get_micros() / 1000 - tablets[i].age);

        // state
        if (tablets[i].state == "kHealthy") {
            data[i][3] = "online";
        } else if (tablets[i].state == "kOffline") {
            data[i][3] = "offline";
        } else {
            data[i][3] = "NULL";
        }
        data[i][4] = "NULL";  // ns_role
    }

    return ResultSetSQL::MakeResultSet(schema, data, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteShowTaskManagers(hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");

    auto zk_client = cluster_sdk_->GetZkClient();
    if (!cluster_sdk_->IsClusterMode() || zk_client == nullptr) {
        // standalone mode
        SET_STATUS_AND_WARN(status, hybridse::common::kUnSupport, "show taskmanagers not supported in standalone mode");
        return {};
    }

    std::string node_path =
        absl::StrCat(std::dynamic_pointer_cast<SQLRouterOptions>(options_)->zk_path, "/taskmanager/leader");
    std::string endpoint;
    Stat stat;
    if (!zk_client->GetNodeValueAndStat(node_path.c_str(), &endpoint, &stat)) {
        // as task manager is optional, the zk query will fail if taskmanager not exist
        // here will ignore all the zk errors since `GetNodeValueAndStat` lacks errcode
        LOG(INFO) << "query taskmanager from zk failed";
        return {};
    }

    // taskmanager only registered leader on zk, return one row only currently
    // TODO(#1417): return multiple rows
    const auto& schema = GetComponetSchema();
    std::vector<std::vector<std::string>> data = {
        {endpoint, "taskmanager", std::to_string(stat.ctime), "online", "NULL"}};

    return ResultSetSQL::MakeResultSet(schema, data, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteShowApiServers(hybridse::sdk::Status* status) {
    // TODO(#1416): support show api servers
    return {};
}

static const std::initializer_list<std::string> GetTableStatusSchema() {
    static const std::initializer_list<std::string> schema = {"Table_id",
                                                              "Table_name",
                                                              "Database_name",
                                                              "Storage_type",
                                                              "Rows",
                                                              "Memory_data_size",
                                                              "Disk_data_size",
                                                              "Partition",
                                                              "Partition_unalive",
                                                              "Replica",
                                                              "Offline_path",
                                                              "Offline_format",
                                                              "Offline_symbolic_paths",
                                                              "Warnings"};
    return schema;
}

// output schema:
// - Table_id: tid
// - Table_name
// - Database_name
// - Storage_type: memory/disk
// - Rows: number of rows
// - Memory_data_size: Memory space in bytes taken or related to a table.
//    1. memory: table data + index
//    2. SSD/HDD: rocksdb’s table memory
// - Disk_data_size: disk space in bytes taken by a table in bytes
//    1. memory:  binlog + snapshot
//    2. SSD/HDD: binlog + rocksdb data (sst files), wal files and checkpoints are not included
// - Partition: partition number
// - Partition_unalive: partition number that is unalive
// - Replica: replica number
// - Offline_path: data path for offline data
// - Offline_format: format for offline data
// - Offline_symbolic_paths: symbolic paths for offline data
// - Warnings: any warnings raised during the checking
//
// if db is empty:
//   show table status in all databases except hidden databases
// else: show table status in current database, include hidden database
std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteShowTableStatus(const std::string& db,
                                                                                   const std::string& pattern,
                                                                                   hybridse::sdk::Status* status) {
    RET_IF_NULL_AND_WARN(status, "output status is nullptr");
    base::StringRef pattern_ref(pattern);
    // NOTE: cluster_sdk_->GetTables(db) seems not accurate, query directly
    std::vector<nameserver::TableInfo> tables;
    std::string msg;
    cluster_sdk_->GetNsClient()->ShowTable("", "", true, tables, msg);

    std::vector<std::vector<std::string>> data;
    data.reserve(tables.size());

    auto all_tablets = cluster_sdk_->GetAllTablet();
    TableStatusMap table_statuses;
    for (auto& tablet_accessor : all_tablets) {
        std::shared_ptr<client::TabletClient> tablet_client;
        if (tablet_accessor && (tablet_client = tablet_accessor->GetClient())) {
            ::openmldb::api::GetTableStatusResponse response;
            if (auto st = tablet_client->GetTableStatus(response); st.OK()) {
                for (const auto& table_status : response.all_table_status()) {
                    table_statuses[table_status.tid()][table_status.pid()][tablet_client->GetEndpoint()] = table_status;
                }
            } else {
                LOG(WARNING) << "get table status from tablet failed: " << st.GetMsg();
            }
        }
    }

    bool matched = false, is_null = true;
    for (auto it = tables.rbegin(); it != tables.rend(); it++) {
        auto& tinfo = *it;
        if (!pattern.empty()) {
            // rule 1: if pattern is provided, show all dbs matching the pattern
            base::StringRef db_ref(tinfo.db());
            hybridse::udf::v1::like(&db_ref, &pattern_ref, &matched, &is_null);
            if (is_null || !matched) {
                continue;
            }
        } else if (!db.empty()) {
            // rule 2: selected a db, show tables only inside the db if no pattern is provided
            if (db != tinfo.db()) {
                continue;
            }
        } else if (nameserver::IsHiddenDb(tinfo.db())) {
            // rule 3: if no db selected, show all tables except those in hidden db if no pattern is provided
            continue;
        }

        auto tid = tinfo.tid();
        auto table_name = tinfo.name();
        auto db = tinfo.db();
        auto& inner_storage_mode = StorageMode_Name(tinfo.storage_mode());
        std::string storage_type = absl::AsciiStrToLower(absl::StripPrefix(inner_storage_mode, "k"));
        std::string error_msg;

        auto partition_num = tinfo.partition_num();
        auto replica_num = tinfo.replica_num();
        uint64_t rows = 0, mem_bytes = 0, disk_bytes = 0;
        uint32_t partition_unalive = 0;
        for (auto& partition_info : tinfo.table_partition()) {
            rows += partition_info.record_cnt();
            mem_bytes += partition_info.record_byte_size();
            disk_bytes += partition_info.diskused();
            for (auto& meta : partition_info.partition_meta()) {
                if (!meta.is_alive()) {
                    partition_unalive++;
                }
            }
            CheckTableStatus(db, table_name, tid, partition_info, replica_num, table_statuses, &error_msg);
        }
        // TODO(hw): miss options, it can be got from desc table
        std::string offline_path = "NULL", offline_format = "NULL", symbolic_paths = "NULL";
        if (tinfo.has_offline_table_info()) {
            offline_path = tinfo.offline_table_info().path();
            offline_format = tinfo.offline_table_info().format();
            symbolic_paths = "";
            for (const auto& ele : tinfo.offline_table_info().symbolic_paths()) {
                symbolic_paths += ele + ",";
            }
        }

        data.push_back({std::to_string(tid), table_name, db, storage_type, std::to_string(rows),
                        std::to_string(mem_bytes), std::to_string(disk_bytes), std::to_string(partition_num),
                        std::to_string(partition_unalive), std::to_string(replica_num), offline_path, offline_format,
                        symbolic_paths, error_msg});
    }

    // TODO(#1456): rich schema result set, and pretty-print numberic values (e.g timestamp) in cli
    return ResultSetSQL::MakeResultSet(GetTableStatusSchema(), data, status);
}

bool SQLClusterRouter::CheckTableStatus(const std::string& db, const std::string& table_name, uint32_t tid,
                                        const nameserver::TablePartition& partition_info, uint32_t replica_num,
                                        const TableStatusMap& statuses, std::string* msg) {
    bool check_succeed = true;
    auto& error_msg = *msg;
    uint32_t pid = partition_info.pid();
    auto append_error_msg = [&check_succeed](std::string& msg, uint32_t pid, int is_leader, const std::string& endpoint,
                                             const std::string& error) {
        absl::StrAppend(&msg, (msg.empty() ? "" : "\n"), "[pid=", std::to_string(pid), "]");
        if (is_leader >= 0) {
            absl::StrAppend(&msg, "[", (is_leader == 1 ? "leader" : "follower"), "]");
        }
        if (!endpoint.empty()) {
            absl::StrAppend(&msg, "[", endpoint, "]");
        }
        absl::StrAppend(&msg, ": ", error);
        check_succeed = false;
    };

    if (statuses.count(tid) && statuses.at(tid).count(pid)) {
        const auto& partition_statuses = statuses.at(tid).at(pid);
        if (partition_info.partition_meta_size() != static_cast<int>(partition_statuses.size())) {
            append_error_msg(
                error_msg, pid, -1, "",
                absl::StrCat("real replica number ", partition_statuses.size(),
                             " does not match the configured replicanum ", partition_info.partition_meta_size()));
        }

        for (auto& meta : partition_info.partition_meta()) {
            if (!partition_statuses.count(meta.endpoint())) {
                append_error_msg(error_msg, pid, meta.is_leader(), meta.endpoint(), "state is kNotFound");
                continue;
            }
            const auto& table_status = partition_statuses.at(meta.endpoint());

            // check leader/follower status
            bool is_leader =
                table_status.has_mode() ? table_status.mode() == ::openmldb::api::kTableLeader : meta.is_leader();
            if (is_leader != meta.is_leader()) {
                append_error_msg(error_msg, pid, is_leader, meta.endpoint(), "leader/follower mode inconsistent");
            }

            // check table status
            ::openmldb::api::TableState state =
                table_status.has_state() ? table_status.state() : ::openmldb::api::kTableUndefined;
            if (state == ::openmldb::api::kTableUndefined || (is_leader && state == ::openmldb::api::kTableLoading)) {
                append_error_msg(error_msg, pid, is_leader, meta.endpoint(),
                                 absl::StrCat("state is ", TableState_Name(state)));
            }
        }
    } else {
        append_error_msg(error_msg, pid, -1, "",
                         absl::StrCat("real replica number 0 does not match the configured replicanum ",
                                      partition_info.partition_meta_size()));
    }

    // check the followers' connections
    auto tablet_accessor = cluster_sdk_->GetTablet(db, table_name, pid);
    std::shared_ptr<client::TabletClient> tablet_client;
    if (tablet_accessor && (tablet_client = tablet_accessor->GetClient())) {
        uint64_t offset = 0;
        std::map<std::string, uint64_t> info_map;
        auto st = tablet_client->GetTableFollower(tid, pid, offset, info_map);
        // no followers is fine if replicanum == 1
        if (st.OK() || st.GetCode() == ::openmldb::base::ReturnCode::kNoFollower) {
            for (auto& meta : partition_info.partition_meta()) {
                if (meta.is_leader()) continue;

                if (info_map.count(meta.endpoint()) == 0) {
                    append_error_msg(error_msg, pid, false, meta.endpoint(), "not connected to leader");
                }
            }
        } else {
            append_error_msg(error_msg, pid, -1, "", absl::StrCat("fail to get from tablet: ", st.GetMsg()));
        }
    }

    return check_succeed;
}

std::map<std::string, std::string> SQLClusterRouter::ParseSparkConfigString(const std::string& input) {
    std::map<std::string, std::string> configMap;

    std::istringstream iss(input);
    std::string keyValue;

    while (std::getline(iss, keyValue, ';')) {
        // Split the key-value pair
        size_t equalPos = keyValue.find('=');
        if (equalPos != std::string::npos) {
            std::string key = keyValue.substr(0, equalPos);
            std::string value = keyValue.substr(equalPos + 1);

            // Check if the key starts with "spark."
            if (key.find("spark.") == 0) {
                // Add to the map
                configMap[key] = value;
            } else {
                std::cerr << "Error: Key does not start with 'spark.' - " << key << std::endl;
            }
        } else {
            std::cerr << "Error: Invalid key-value pair - " << keyValue << std::endl;
        }
    }

    return configMap;
}

void SQLClusterRouter::ReadSparkConfFromFile(std::string conf_file_path, std::map<std::string, std::string>* config) {
    if (!conf_file_path.empty()) {
        boost::property_tree::ptree pt;

        try {
            boost::property_tree::ini_parser::read_ini(conf_file_path, pt);
            LOG(INFO) << "Load Spark conf file: " << conf_file_path;
        } catch (...) {
            LOG(WARNING) << "Fail to load Spark conf file: " << conf_file_path;
            return;
        }

        if (pt.empty()) {
            LOG(WARNING) << "Spark conf file is empty";
        }

        for (auto& section : pt) {
            // Only supports Spark section
            if (section.first == "Spark") {
                for (auto& key : section.second) {
                    (*config).emplace(key.first, key.second.get_value<std::string>());
                }
            } else {
                LOG(WARNING) << "The section " + section.first + " is not supported, please use Spark section";
            }
        }
    }
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::GetJobResultSet(int job_id,
                                                                            ::hybridse::sdk::Status* status) {
    std::string db = openmldb::nameserver::INTERNAL_DB;
    std::string sql = "SELECT * FROM JOB_INFO WHERE id = " + std::to_string(job_id);

    auto rs = ExecuteSQLParameterized(db, sql, {}, status);
    if (!status->IsOK()) {
        return {};
    }
    if (rs->Size() == 0) {
        status->SetCode(::hybridse::common::StatusCode::kCmdError);
        status->SetMsg("Job not found: " + std::to_string(job_id));
        return {};
    }
    rs = JobTableHelper::MakeResultSet(rs, "", status);
    if (FLAGS_role == "sql_client") {
        return std::make_shared<ReadableResultSetSQL>(rs);
    } else {
        return rs;
    }
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::GetJobResultSet(::hybridse::sdk::Status* status) {
    std::string db = openmldb::nameserver::INTERNAL_DB;
    std::string sql = "SELECT * FROM JOB_INFO";
    auto rs = ExecuteSQLParameterized(db, sql, std::shared_ptr<openmldb::sdk::SQLRequestRow>(), status);
    if (!status->IsOK()) {
        return {};
    }
    rs = JobTableHelper::MakeResultSet(rs, "", status);
    if (FLAGS_role == "sql_client" && rs) {
        return std::make_shared<ReadableResultSetSQL>(rs);
    }
    return rs;
}
std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::GetTaskManagerJobResult(const std::string& like_pattern,
                                                                                    ::hybridse::sdk::Status* status) {
    bool like_match = JobTableHelper::NeedLikeMatch(like_pattern);
    if (!like_pattern.empty() && !like_match) {
        int job_id;
        if (!absl::SimpleAtoi(like_pattern, &job_id)) {
            *status = {StatusCode::kCmdError, "Failed to parse job id: " + like_pattern};
            return {};
        }
        return this->GetJobResultSet(job_id, status);
    }
    std::string db = openmldb::nameserver::INTERNAL_DB;
    std::string sql = "SELECT * FROM JOB_INFO;";
    auto rs = ExecuteSQLParameterized(db, sql, {}, status);
    if (!status->IsOK()) {
        return {};
    }
    rs = JobTableHelper::MakeResultSet(rs, like_pattern, status);
    if (FLAGS_role == "sql_client" && rs && status->IsOK()) {
        return std::make_shared<ReadableResultSetSQL>(rs);
    }
    return rs;
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::GetNameServerJobResult(const std::string& like_pattern,
                                                                                   ::hybridse::sdk::Status* status) {
    bool like_match = JobTableHelper::NeedLikeMatch(like_pattern);
    base::Status ret;
    nameserver::ShowOPStatusResponse response;
    if (!like_pattern.empty() && !like_match) {
        uint64_t id;
        if (!absl::SimpleAtoi(like_pattern, &id)) {
            *status = {-1, absl::StrCat("invalid job_id ", like_pattern)};
            return {};
        }
        ret = cluster_sdk_->GetNsClient()->ShowOPStatus(id, &response);
    } else {
        ret = cluster_sdk_->GetNsClient()->ShowOPStatus("", client::INVALID_PID, &response);
    }
    if (!ret.OK()) {
        *status = {ret.GetCode(), ret.GetMsg()};
        return {};
    }
    auto rs = JobTableHelper::MakeResultSet(response.op_status(), like_pattern, status);
    if (FLAGS_role == "sql_client" && rs && status->IsOK()) {
        return std::make_shared<ReadableResultSetSQL>(rs);
    }
    return rs;
}

absl::StatusOr<bool> SQLClusterRouter::GetUser(const std::string& name, UserInfo* user_info) {
    std::string sql = absl::StrCat("select * from ",  nameserver::USER_INFO_NAME);
    hybridse::sdk::Status status;
    auto rs = ExecuteSQLParameterized(nameserver::INTERNAL_DB, sql,
            std::shared_ptr<openmldb::sdk::SQLRequestRow>(), &status);
    if (rs == nullptr) {
        return absl::InternalError(status.msg);
    }
    while (rs->Next()) {
        if (rs->GetStringUnsafe(1) == name) {
            user_info->name = name;
            user_info->password = rs->GetStringUnsafe(2);
            user_info->create_time = rs->GetTimeUnsafe(5);
            user_info->update_time = rs->GetTimeUnsafe(6);
            return true;
        }
    }
    return false;
}

hybridse::sdk::Status SQLClusterRouter::AddUser(const std::string& name, const std::string& password) {
    auto real_password = password.empty() ? password : codec::Encrypt(password);
    uint64_t cur_ts = ::baidu::common::timer::get_micros() / 1000;
    std::string sql = absl::StrCat("insert into ", nameserver::USER_INFO_NAME, " values (",
            "'%',",                 // host
            "'", name, "','",       // user
            real_password, "',",    // password
            cur_ts, ",",            // password_last_changed
            "0,",                   // password_expired_time
            cur_ts, ", ",           // create_time
            cur_ts, ",",            // update_time
            1,                      // account_type
            ",'',",                 // privileges
            "null"                  // extra_info
            ");");
    hybridse::sdk::Status status;
    ExecuteInsert(nameserver::INTERNAL_DB, sql, &status);
    return status;
}

hybridse::sdk::Status SQLClusterRouter::UpdateUser(const UserInfo& user_info, const std::string& password) {
    auto real_password = password.empty() ? password : codec::Encrypt(password);
    uint64_t cur_ts = ::baidu::common::timer::get_micros() / 1000;
    std::string sql = absl::StrCat("insert into ", nameserver::USER_INFO_NAME, " values (",
            "'%',",                           // host
            "'", user_info.name, "','",       // user
            real_password, "',",              // password
            cur_ts, ",",                      // password_last_changed
            "0,",                             // password_expired_time
            user_info.create_time, ", ",      // create_time
            cur_ts, ",",                      // update_time
            1,                                // account_type
            ",'", user_info.privileges, "',", // privileges
            "null"                            // extra_info
            ");");
    hybridse::sdk::Status status;
    ExecuteInsert(nameserver::INTERNAL_DB, sql, &status);
    return status;
}

hybridse::sdk::Status SQLClusterRouter::DeleteUser(const std::string& name) {
    std::string sql = absl::StrCat("delete from ", nameserver::USER_INFO_NAME,
            " where host = '%' and user = '", name, "';");
    hybridse::sdk::Status status;
    ExecuteSQL(nameserver::INTERNAL_DB, sql, &status);
    return status;
}

void SQLClusterRouter::AddUserToConfig(std::map<std::string, std::string>* config) {
    config->emplace("spark.openmldb.user", GetRouterOptions()->user);
    if (!GetRouterOptions()->password.empty()) {
        config->emplace("spark.openmldb.password", GetRouterOptions()->password);
    }
}

::hybridse::sdk::Status SQLClusterRouter::RevertPut(const nameserver::TableInfo& table_info,
        uint32_t end_pid,
        const std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& dimensions,
        uint64_t ts,
        const base::Slice& value,
        const std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>>& tablets) {
    codec::RowView row_view(table_info.column_desc());
    std::map<std::string, uint32_t> column_map;
    for (int32_t i = 0; i < table_info.column_desc_size(); i++) {
        column_map.emplace(table_info.column_desc(i).name(), i);
    }
    const int8_t* data = reinterpret_cast<const int8_t*>(value.data());
    for (const auto& kv : dimensions) {
        if (static_cast<size_t>(kv.first) > tablets.size()) {
            return {StatusCode::kCmdError,
                    absl::StrCat("pid ", kv.first, " is greater than the tablets size ", tablets.size())};
        }
        auto tablet = tablets[kv.first];
        if (!tablet) {
            continue;
        }
        auto client = tablet->GetClient();
        for (const auto& val : kv.second) {
            if (val.second >= static_cast<uint32_t>(table_info.column_key_size())) {
                return {StatusCode::kCmdError, absl::StrCat("invalid index pos ", val.second)};
            }
            const auto& index = table_info.column_key(val.second);
            if (index.flag()) {
                continue;
            }
            int64_t cur_ts = ts;
            if (!index.ts_name().empty()) {
                if (auto it = column_map.find(index.ts_name()); it == column_map.end()) {
                    return {StatusCode::kCmdError, absl::StrCat("invalid ts name ", index.ts_name())};
                } else if (row_view.GetInteger(data, it->second, table_info.column_desc(it->second).data_type(),
                                               &cur_ts) != 0) {
                    return {StatusCode::kCmdError, "get ts failed"};
                }
            }
            std::map<uint32_t, std::string> index_val = {{val.second, val.first}};
            uint64_t end_ts = cur_ts > 0 ? cur_ts - 1 : 0;
            DeleteOption option(val.second, val.first, "", cur_ts, end_ts);
            option.enable_decode_value = false;
            client->Delete(table_info.tid(), kv.first, option, options_->request_timeout);
        }
        if (kv.first == end_pid) {
            break;
        }
    }
    return {};
}

common::ColumnKey Bias::AddBias(const common::ColumnKey& index) const {
    if (!index.has_ttl()) {
        LOG(WARNING) << "index has no ttl, skip bias";
        return index;
    }
    auto new_index = index;
    auto ttl = new_index.mutable_ttl();
    if (ttl->ttl_type() != type::TTLType::kLatestTime) {
        // add bias to ttl when abs / abs||. / abs&&.
        if (range_inf) {
            ttl->set_abs_ttl(0);
        } else if (ttl->abs_ttl() > 0) {
            // in ttl, 0 means unlimited, no need to add bias
            ttl->set_abs_ttl(ttl->abs_ttl() + range_bias);
        }
    }
    if (ttl->ttl_type() != type::TTLType::kAbsoluteTime) {
        // add bias to ttl when lat / .||lat / .&&lat
        if (rows_inf) {
            ttl->set_lat_ttl(0);
        } else if (ttl->lat_ttl() > 0) {
            // in ttl, 0 means unlimited, no need to add bias
            ttl->set_lat_ttl(ttl->lat_ttl() + rows_bias);
        }
    }
    return new_index;
}

bool Bias::Set(const hybridse::node::ConstNode* node, bool is_row_type) {
    if (node == nullptr) {
        return false;
    }
    // both range and rows can be int & string 'inf'
    if (node->IsNumber()) {
        SetBias(is_row_type, node->GetAsInt64());
        return true;
    }
    auto str = node->GetAsString();
    if (absl::EqualsIgnoreCase(str, "inf")) {
        SetInf(is_row_type);
        return true;
    }
    // row bias shouldn't be interval, rang bias can be 0d, 0h, ...
    if (is_row_type) {
        return false;
    }
    auto v = node->GetMillis();
    if (v == -1) {
        return false;
    }
    // abs time should be min, 0 means no bias, if has part which < 1 min, add 1 min
    SetBias(is_row_type, v);
    return true;
}

std::ostream& operator<<(std::ostream& os, const Bias& bias) {  // NOLINT
    os << "Bias[" << bias.ToString() << "]";
    return os;
}

}  // namespace sdk
}  // namespace openmldb
