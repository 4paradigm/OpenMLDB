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
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include "absl/strings/ascii.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/strip.h"
#include "absl/strings/substitute.h"
#include "base/ddl_parser.h"
#include "base/file_util.h"
#include "boost/none.hpp"
#include "boost/property_tree/ini_parser.hpp"
#include "boost/property_tree/ptree.hpp"
#include "brpc/channel.h"
#include "cmd/display.h"
#include "common/timer.h"
#include "glog/logging.h"
#include "nameserver/system_table.h"
#include "plan/plan_api.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "schema/schema_adapter.h"
#include "sdk/base.h"
#include "sdk/base_impl.h"
#include "sdk/batch_request_result_set_sql.h"
#include "sdk/file_option_parser.h"
#include "sdk/node_adapter.h"
#include "sdk/result_set_sql.h"
#include "sdk/split.h"

DECLARE_string(bucket_size);
DECLARE_uint32(replica_num);

namespace openmldb {
namespace sdk {

using hybridse::plan::PlanAPI;

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

class QueryFutureImpl : public QueryFuture {
 public:
    explicit QueryFutureImpl(openmldb::RpcCallback<openmldb::api::QueryResponse>* callback) : callback_(callback) {
        if (callback_) {
            callback_->Ref();
        }
    }
    ~QueryFutureImpl() {
        if (callback_) {
            callback_->UnRef();
        }
    }

    std::shared_ptr<hybridse::sdk::ResultSet> GetResultSet(hybridse::sdk::Status* status) override {
        if (!status) {
            return nullptr;
        }
        if (!callback_ || !callback_->GetResponse() || !callback_->GetController()) {
            status->code = hybridse::common::kRpcError;
            status->msg = "request error, response or controller null";
            return nullptr;
        }
        brpc::Join(callback_->GetController()->call_id());
        if (callback_->GetController()->Failed()) {
            status->code = hybridse::common::kRpcError;
            status->msg = "request error, " + callback_->GetController()->ErrorText();
            return nullptr;
        }
        if (callback_->GetResponse()->code() != ::openmldb::base::kOk) {
            status->code = callback_->GetResponse()->code();
            status->msg = "request error, " + callback_->GetResponse()->msg();
            return nullptr;
        }
        auto rs = ResultSetSQL::MakeResultSet(callback_->GetResponse(), callback_->GetController(), status);
        return rs;
    }

    bool IsDone() const override {
        if (callback_) return callback_->IsDone();
        return false;
    }

 private:
    openmldb::RpcCallback<openmldb::api::QueryResponse>* callback_;
};

class BatchQueryFutureImpl : public QueryFuture {
 public:
    explicit BatchQueryFutureImpl(openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback)
        : callback_(callback) {
        if (callback_) {
            callback_->Ref();
        }
    }

    ~BatchQueryFutureImpl() {
        if (callback_) {
            callback_->UnRef();
        }
    }

    std::shared_ptr<hybridse::sdk::ResultSet> GetResultSet(hybridse::sdk::Status* status) override {
        if (!status) {
            return nullptr;
        }
        if (!callback_ || !callback_->GetResponse() || !callback_->GetController()) {
            status->code = hybridse::common::kRpcError;
            status->msg = "request error, response or controller null";
            return nullptr;
        }
        brpc::Join(callback_->GetController()->call_id());
        if (callback_->GetController()->Failed()) {
            status->code = hybridse::common::kRpcError;
            status->msg = "request error. " + callback_->GetController()->ErrorText();
            return nullptr;
        }
        std::shared_ptr<::openmldb::sdk::SQLBatchRequestResultSet> rs =
            std::make_shared<openmldb::sdk::SQLBatchRequestResultSet>(callback_->GetResponse(),
                                                                      callback_->GetController());
        bool ok = rs->Init();
        if (!ok) {
            status->code = -1;
            status->msg = "request error, resuletSetSQL init failed";
            return nullptr;
        }
        return rs;
    }

    bool IsDone() const override { return callback_->IsDone(); }

 private:
    openmldb::RpcCallback<openmldb::api::SQLBatchRequestQueryResponse>* callback_;
};

SQLClusterRouter::SQLClusterRouter(const SQLRouterOptions& options)
    : options_(std::make_shared<SQLRouterOptions>(options)),
      is_cluster_mode_(true),
      interactive_(false),
      cluster_sdk_(nullptr),
      mu_(),
      rand_(::baidu::common::timer::now_time()) {}

SQLClusterRouter::SQLClusterRouter(const StandaloneOptions& options)
    : options_(std::make_shared<StandaloneOptions>(options)),
      is_cluster_mode_(false),
      interactive_(false),
      cluster_sdk_(nullptr),
      mu_(),
      rand_(::baidu::common::timer::now_time()) {}

SQLClusterRouter::SQLClusterRouter(DBSDK* sdk)
    : options_(),
      is_cluster_mode_(sdk->IsClusterMode()),
      interactive_(false),
      cluster_sdk_(sdk),
      mu_(),
      rand_(::baidu::common::timer::now_time()) {
    if (is_cluster_mode_) {
        options_ = std::make_shared<SQLRouterOptions>();
    } else {
        options_ = std::make_shared<StandaloneOptions>();
    }
}

SQLClusterRouter::~SQLClusterRouter() { delete cluster_sdk_; }

bool SQLClusterRouter::Init() {
    if (cluster_sdk_ == nullptr) {
        // init cluster_sdk_, require options_ or standalone_options_ is set
        if (is_cluster_mode_) {
            auto ops = std::dynamic_pointer_cast<SQLRouterOptions>(options_);
            ClusterOptions coptions;
            coptions.zk_cluster = ops->zk_cluster;
            coptions.zk_path = ops->zk_path;
            coptions.zk_session_timeout = ops->zk_session_timeout;
            coptions.zk_log_level = ops->zk_log_level;
            coptions.zk_log_file = ops->zk_log_file;
            cluster_sdk_ = new ClusterSDK(coptions);
            bool ok = cluster_sdk_->Init();
            if (!ok) {
                LOG(WARNING) << "fail to init cluster sdk";
                return false;
            }
        } else {
            auto ops = std::dynamic_pointer_cast<StandaloneOptions>(options_);
            cluster_sdk_ = new ::openmldb::sdk::StandAloneSDK(ops->host, ops->port);
            bool ok = cluster_sdk_->Init();
            if (!ok) {
                LOG(WARNING) << "fail to init standalone sdk";
                return false;
            }
        }
    } else {
        // init options_ or standalone_options_ if fileds not filled, they should be consistent with cluster_sdk_
        //
        // might better to refactor constructors & fileds for SQLClusterRouter
        // but will introduce breaking changes as well
        if (is_cluster_mode_) {
            auto ops = std::dynamic_pointer_cast<SQLRouterOptions>(options_);
            if (ops->zk_cluster.empty() || ops->zk_path.empty()) {
                auto* cluster_sdk = dynamic_cast<ClusterSDK*>(cluster_sdk_);
                DCHECK(cluster_sdk != nullptr);
                ops->zk_cluster = cluster_sdk->GetClusterOptions().zk_cluster;
                ops->zk_path = cluster_sdk->GetClusterOptions().zk_path;
            }
        } else {
            auto ops = std::dynamic_pointer_cast<StandaloneOptions>(options_);
            if (ops->host.empty() || ops->port == 0) {
                auto* standalone_sdk = dynamic_cast<StandAloneSDK*>(cluster_sdk_);
                DCHECK(standalone_sdk != nullptr);
                ops->host = standalone_sdk->GetHost();
                ops->port = standalone_sdk->GetPort();
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
        // if not allowed to create system table, init session here
        session_variables_.emplace("execute_mode", "offline");
        session_variables_.emplace("enable_trace", "false");
        session_variables_.emplace("sync_job", "false");
        session_variables_.emplace("job_timeout", "60000");  // rpc request timeout for taskmanager
    }
    return true;
}

std::shared_ptr<SQLRequestRow> SQLClusterRouter::GetRequestRow(const std::string& db, const std::string& sql,
                                                               ::hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return {};
    }
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
        *status = {-1, vm_status.msg};
        LOG(WARNING) << "fail to explain sql " << sql << " for " << vm_status.msg;
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
    if (status == nullptr) {
        return nullptr;
    }
    std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info = cluster_sdk_->GetProcedureInfo(db, sp_name, &status->msg);
    if (!sp_info) {
        status->code = -1;
        status->msg = "procedure not found, msg: " + status->msg;
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    const std::string& sql = sp_info->GetSql();
    return GetRequestRow(db, sql, status);
}

std::shared_ptr<openmldb::sdk::SQLDeleteRow> SQLClusterRouter::GetDeleteRow(const std::string& db,
                                                                            const std::string& sql,
                                                                            ::hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return {};
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    if (cache) {
        auto delete_cache = std::dynamic_pointer_cast<DeleteSQLCache>(cache);
        if (delete_cache) {
            status->code = 0;
            return std::make_shared<openmldb::sdk::SQLDeleteRow>(
                delete_cache->GetDatabase(), delete_cache->GetTableName(), delete_cache->GetIndexName(),
                delete_cache->GetColNames(), delete_cache->GetDefaultValue(), delete_cache->GetHoleMap());
        }
    }
    ::hybridse::node::NodeManager nm;
    ::hybridse::plan::PlanNodeList plans;
    bool ok = GetSQLPlan(sql, &nm, &plans);
    if (!ok || plans.empty()) {
        *status = {::hybridse::common::StatusCode::kCmdError, "fail to get sql plan " + sql};
        return {};
    }
    ::hybridse::node::PlanNode* plan = plans[0];
    if (plan->GetType() != hybridse::node::kPlanTypeDelete) {
        *status = {::hybridse::common::StatusCode::kCmdError, "invalid sql node expect delete"};
        return {};
    }
    auto delete_plan = dynamic_cast<::hybridse::node::DeletePlanNode*>(plan);
    auto condition = delete_plan->GetCondition();
    if (!condition) {
        *status = {::hybridse::common::StatusCode::kCmdError, "no condition in delete sql"};
        return {};
    }
    std::string database = delete_plan->GetDatabase().empty() ? db : delete_plan->GetDatabase();
    if (database.empty()) {
        *status = {::hybridse::common::StatusCode::kCmdError, " no db in sql and no default db"};
        return {};
    }
    const auto& table_name = delete_plan->GetTableName();
    auto table_info = cluster_sdk_->GetTableInfo(database, table_name);
    if (!table_info) {
        *status = {::hybridse::common::StatusCode::kCmdError,
                   absl::StrCat("table ", table_name, " in db", database, " does not exist")};
        return {};
    }
    auto col_map = schema::SchemaAdapter::GetColMap(*table_info);
    std::map<std::string, std::string> condition_map;
    std::map<std::string, int> parameter_map;
    auto binary_node = dynamic_cast<const hybridse::node::BinaryExpr*>(condition);
    *status = NodeAdapter::ParseExprNode(binary_node, col_map, &condition_map, &parameter_map);
    if (!status->IsOK()) {
        return {};
    }
    int index_pos = 0;
    bool found = true;
    for (; index_pos < table_info->column_key_size(); index_pos++) {
        const auto& column_key = table_info->column_key(index_pos);
        if (column_key.flag() != 0) {
            continue;
        }
        for (const auto& col : column_key.col_name()) {
            if (condition_map.count(col) == 0 && parameter_map.count(col) == 0) {
                found = false;
                break;
            }
        }
        if (found) {
            break;
        }
    }
    if (!found) {
        *status = {::hybridse::common::StatusCode::kCmdError, "no index col in sql"};
        return {};
    }
    auto delete_cache = std::make_shared<DeleteSQLCache>(
        db, table_info->tid(), table_name, table_info->column_key(index_pos), condition_map, parameter_map);

    SetCache(db, sql, hybridse::vm::kBatchMode, delete_cache);
    *status = {};
    return std::make_shared<openmldb::sdk::SQLDeleteRow>(delete_cache->GetDatabase(), delete_cache->GetTableName(),
                                                         delete_cache->GetIndexName(), delete_cache->GetColNames(),
                                                         delete_cache->GetDefaultValue(), delete_cache->GetHoleMap());
}

std::shared_ptr<SQLInsertRow> SQLClusterRouter::GetInsertRow(const std::string& db, const std::string& sql,
                                                             ::hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return {};
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    if (cache) {
        auto insert_cache = std::dynamic_pointer_cast<InsertSQLCache>(cache);
        if (insert_cache) {
            *status = {};
            return std::make_shared<SQLInsertRow>(insert_cache->GetTableInfo(), insert_cache->GetSchema(),
                                                  insert_cache->GetDefaultValue(), insert_cache->GetStrLength(),
                                                  insert_cache->GetHoleIdxArr());
        }
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    std::vector<uint32_t> stmt_column_idx_arr;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map, &str_length, &stmt_column_idx_arr)) {
        *status = {1, "get insert information failed"};
        return {};
    }
    auto schema = openmldb::schema::SchemaAdapter::ConvertSchema(table_info->column_desc());
    auto insert_cache =
        std::make_shared<InsertSQLCache>(table_info, schema, default_map, str_length,
                                         SQLInsertRow::GetHoleIdxArr(default_map, stmt_column_idx_arr, schema));
    SetCache(db, sql, hybridse::vm::kBatchMode, insert_cache);
    *status = {};
    return std::make_shared<SQLInsertRow>(insert_cache->GetTableInfo(), insert_cache->GetSchema(),
                                          insert_cache->GetDefaultValue(), insert_cache->GetStrLength(),
                                          insert_cache->GetHoleIdxArr());
}

bool SQLClusterRouter::GetMultiRowInsertInfo(const std::string& db, const std::string& sql,
                                             ::hybridse::sdk::Status* status,
                                             std::shared_ptr<::openmldb::nameserver::TableInfo>* table_info,
                                             std::vector<DefaultValueMap>* default_maps,
                                             std::vector<uint32_t>* str_lengths) {
    if (status == NULL || table_info == NULL || default_maps == NULL || str_lengths == NULL) {
        status->msg = "insert info is null";
        LOG(WARNING) << status->msg;
        return false;
    }
    ::hybridse::node::NodeManager nm;
    ::hybridse::plan::PlanNodeList plans;
    bool ok = GetSQLPlan(sql, &nm, &plans);
    if (!ok || plans.empty()) {
        LOG(WARNING) << "fail to get sql plan with sql " << sql;
        status->msg = "fail to get sql plan with";
        return false;
    }
    ::hybridse::node::PlanNode* plan = plans[0];
    if (plan->GetType() != hybridse::node::kPlanTypeInsert) {
        status->msg = "invalid sql node expect insert";
        LOG(WARNING) << "invalid sql node expect insert";
        return false;
    }
    auto* iplan = dynamic_cast<::hybridse::node::InsertPlanNode*>(plan);
    const ::hybridse::node::InsertStmt* insert_stmt = iplan->GetInsertNode();
    if (insert_stmt == nullptr) {
        LOG(WARNING) << "insert stmt is null";
        status->msg = "insert stmt is null";
        return false;
    }
    std::string db_name;
    if (!insert_stmt->db_name_.empty()) {
        db_name = insert_stmt->db_name_;
    } else {
        db_name = db;
    }
    if (db_name.empty()) {
        status->msg = "Please enter database first";
        return false;
    }
    *table_info = cluster_sdk_->GetTableInfo(db_name, insert_stmt->table_name_);
    if (!(*table_info)) {
        status->msg = "table with name " + insert_stmt->table_name_ + " in db " + db_name + " does not exist";
        LOG(WARNING) << status->msg;
        return false;
    }
    std::map<uint32_t, uint32_t> column_map;
    for (size_t j = 0; j < insert_stmt->columns_.size(); ++j) {
        const std::string& col_name = insert_stmt->columns_[j];
        bool find_flag = false;
        for (int i = 0; i < (*table_info)->column_desc_size(); ++i) {
            if (col_name == (*table_info)->column_desc(i).name()) {
                if (column_map.count(i) > 0) {
                    status->msg = "duplicate column of " + col_name;
                    LOG(WARNING) << status->msg;
                    return false;
                }
                column_map.insert(std::make_pair(i, j));
                find_flag = true;
                break;
            }
        }
        if (!find_flag) {
            status->msg = "can't find column " + col_name + " in table " + (*table_info)->name();
            LOG(WARNING) << status->msg;
            return false;
        }
    }
    size_t total_rows_size = insert_stmt->values_.size();
    for (size_t i = 0; i < total_rows_size; i++) {
        hybridse::node::ExprNode* value = insert_stmt->values_[i];
        if (value->GetExprType() != ::hybridse::node::kExprList) {
            status->msg = "fail to parse row [" + std::to_string(i) +
                          "]"
                          ": invalid row expression, expect kExprList but " +
                          hybridse::node::ExprTypeName(value->GetExprType());
            LOG(WARNING) << status->msg;
            return false;
        }
        uint32_t str_length = 0;
        default_maps->push_back(
            GetDefaultMap(*table_info, column_map, dynamic_cast<::hybridse::node::ExprListNode*>(value), &str_length));
        if (!default_maps->back()) {
            status->msg = "fail to parse row[" + std::to_string(i) + "]: " + value->GetExprString();
            LOG(WARNING) << status->msg;
            return false;
        }
        str_lengths->push_back(str_length);
    }
    if (default_maps->empty() || str_lengths->empty()) {
        status->msg = "default_maps or str_lengths are empty";
        status->code = 1;
        LOG(WARNING) << status->msg;
        return false;
    }
    if (default_maps->size() != str_lengths->size()) {
        status->msg = "default maps isn't match with str_lengths";
        status->code = 1;
        LOG(WARNING) << status->msg;
        return false;
    }
    return true;
}

bool SQLClusterRouter::GetInsertInfo(const std::string& db, const std::string& sql, ::hybridse::sdk::Status* status,
                                     std::shared_ptr<::openmldb::nameserver::TableInfo>* table_info,
                                     DefaultValueMap* default_map, uint32_t* str_length,
                                     std::vector<uint32_t>* stmt_column_idx_in_table) {
    if (status == nullptr || table_info == nullptr || default_map == nullptr || str_length == nullptr) {
        LOG(WARNING) << "insert info is null" << sql;
        return false;
    }
    ::hybridse::node::NodeManager nm;
    ::hybridse::plan::PlanNodeList plans;
    bool ok = GetSQLPlan(sql, &nm, &plans);
    if (!ok || plans.empty()) {
        LOG(WARNING) << "fail to get sql plan with sql " << sql;
        status->msg = "fail to get sql plan with";
        return false;
    }
    ::hybridse::node::PlanNode* plan = plans[0];
    if (plan->GetType() != hybridse::node::kPlanTypeInsert) {
        status->msg = "invalid sql node expect insert";
        LOG(WARNING) << "invalid sql node expect insert";
        return false;
    }
    auto* iplan = dynamic_cast<::hybridse::node::InsertPlanNode*>(plan);
    const ::hybridse::node::InsertStmt* insert_stmt = iplan->GetInsertNode();
    if (insert_stmt == nullptr) {
        LOG(WARNING) << "insert stmt is null";
        status->msg = "insert stmt is null";
        return false;
    }
    *table_info = cluster_sdk_->GetTableInfo(db, insert_stmt->table_name_);
    if (!(*table_info)) {
        status->msg = "table with name " + insert_stmt->table_name_ + " in db " + db + " does not exist";
        LOG(WARNING) << status->msg;
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
                    status->msg = "duplicate column of " + col_name;
                    LOG(WARNING) << status->msg;
                    return false;
                }
                column_map.insert(std::make_pair(i, j));
                stmt_column_idx_in_table->emplace_back(i);
                find_flag = true;
                break;
            }
        }
        if (!find_flag) {
            status->msg = "can't find column " + col_name + " in table " + (*table_info)->name();
            LOG(WARNING) << status->msg;
            return false;
        }
    }
    *default_map = GetDefaultMap(*table_info, column_map,
                                 dynamic_cast<::hybridse::node::ExprListNode*>(insert_stmt->values_[0]), str_length);
    if (!(*default_map)) {
        status->msg = "get default value map of " + sql + " failed";
        LOG(WARNING) << status->msg;
        return false;
    }
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
    if (status == nullptr) {
        return {};
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    std::shared_ptr<InsertSQLCache> insert_cache;
    if (cache) {
        insert_cache = std::dynamic_pointer_cast<InsertSQLCache>(cache);
        if (insert_cache) {
            status->code = 0;
            return std::make_shared<SQLInsertRows>(insert_cache->GetTableInfo(), insert_cache->GetSchema(),
                                                   insert_cache->GetDefaultValue(), insert_cache->GetStrLength(),
                                                   insert_cache->GetHoleIdxArr());
        }
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    std::vector<uint32_t> stmt_column_idx_arr;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map, &str_length, &stmt_column_idx_arr)) {
        return {};
    }
    auto col_schema = openmldb::schema::SchemaAdapter::ConvertSchema(table_info->column_desc());
    insert_cache =
        std::make_shared<InsertSQLCache>(table_info, col_schema, default_map, str_length,
                                         SQLInsertRow::GetHoleIdxArr(default_map, stmt_column_idx_arr, col_schema));
    SetCache(db, sql, hybridse::vm::kBatchMode, insert_cache);
    return std::make_shared<SQLInsertRows>(table_info, insert_cache->GetSchema(), default_map, str_length,
                                           insert_cache->GetHoleIdxArr());
}

bool SQLClusterRouter::ExecuteDDL(const std::string& db, const std::string& sql, hybridse::sdk::Status* status) {
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        status->code = -1;
        status->msg = "no nameserver exist";
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
        status->code = -1;
        status->msg = sql_status.msg;
        LOG(WARNING) << status->msg;
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
        status->msg = "fail to execute sql " + sql + " for error " + ret.msg;
        LOG(WARNING) << status->msg;
        status->code = -1;
        return false;
    }
    return true;
}

bool SQLClusterRouter::ShowDB(std::vector<std::string>* dbs, hybridse::sdk::Status* status) {
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        LOG(WARNING) << "no nameserver exist";
        return false;
    }
    std::string err;
    bool ok = ns_ptr->ShowDatabase(dbs, err);
    if (!ok) {
        status->msg = "fail to show databases: " + err;
        LOG(WARNING) << status->msg;
        status->code = -1;
        return false;
    }
    return true;
}

// get all table names in all DB
std::vector<std::string> SQLClusterRouter::GetAllTables() { return cluster_sdk_->GetAllTables(); }

bool SQLClusterRouter::CreateDB(const std::string& db, hybridse::sdk::Status* status) {
    if (status == NULL) {
        return false;
    }
    // We use hybridse parser to check db name, to ensure syntactic consistency.
    if (db.empty() || !CheckSQLSyntax("CREATE DATABASE `" + db + "`;")) {
        status->msg = "db name(" + db + ") is invalid";
        status->code = -2;
        LOG(WARNING) << status->msg;
        return false;
    }

    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        LOG(WARNING) << "no nameserver exist";
        status->msg = "no nameserver exist";
        status->code = -1;
        return false;
    }

    std::string err;
    bool ok = ns_ptr->CreateDatabase(db, err);
    if (!ok) {
        LOG(WARNING) << "fail to create db " << db << " for error " << err;
        status->msg = err;
        return false;
    }
    status->code = 0;
    return true;
}

bool SQLClusterRouter::DropDB(const std::string& db, hybridse::sdk::Status* status) {
    if (db.empty() || !CheckSQLSyntax("DROP DATABASE `" + db + "`;")) {
        status->msg = "db name(" + db + ") is invalid";
        status->code = -2;
        LOG(WARNING) << status->msg;
        return false;
    }

    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        LOG(WARNING) << "no nameserver exist";
        return false;
    }
    std::string err;
    bool ok = ns_ptr->DropDatabase(db, err);
    if (!ok) {
        LOG(WARNING) << "fail to drop db " << db << " for error " << err;
        return false;
    }
    return true;
}

bool SQLClusterRouter::DropTable(const std::string& db, const std::string& table, hybridse::sdk::Status* status) {
    if (db.empty() || table.empty()) {
        status->msg = "db name(" + db + ") or table name(" + table + ") is invalid";
        status->code = -2;
        return false;
    }

    // RefreshCatalog to avoid getting out-of-date table info
    if (!RefreshCatalog()) {
        status->msg = "Fail to refresh catalog";
        status->code = -1;
        return false;
    }

    auto tableInfo = GetTableInfo(db, table);

    // delete pre-aggr meta info if need
    if (tableInfo.base_table_tid() > 0) {
        std::string meta_db = openmldb::nameserver::INTERNAL_DB;
        std::string meta_table = openmldb::nameserver::PRE_AGG_META_NAME;
        std::string select_aggr_info =
            absl::StrCat("select base_db,base_table,aggr_func,aggr_col,partition_cols,order_by_col,filter_col from ",
                         meta_db, ".", meta_table, " where aggr_table = '", tableInfo.name(), "';");
        auto rs = ExecuteSQL("", select_aggr_info, status);
        if (!status->IsOK()) {
            return false;
        }
        if (rs->Size() != 1) {
            status->msg = "duplicate records generate with aggr table name: " + tableInfo.name();
            status->code = -1;
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
            status->msg = "access ResultSet failed";
            status->code = -1;
            return false;
        }
        auto tablet_accessor = cluster_sdk_->GetTablet(meta_db, meta_table, (uint32_t)0);
        if (!tablet_accessor) {
            status->msg = "get tablet accessor failed";
            status->code = -1;
            return false;
        }
        auto tablet_client = tablet_accessor->GetClient();
        if (!tablet_client) {
            status->msg = "get tablet client failed";
            status->code = -1;
            return false;
        }
        auto tid = cluster_sdk_->GetTableId(meta_db, meta_table);
        std::string msg;
        if (!tablet_client->Delete(tid, 0, tableInfo.name(), "aggr_table", msg) ||
            !tablet_client->Delete(tid, 0, idx_key, "unique_key", msg)) {
            status->msg = "delete aggr meta failed";
            status->code = -1;
            return false;
        }
    }

    // Check offline table info first
    if (tableInfo.has_offline_table_info()) {
        auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
        if (!taskmanager_client_ptr) {
            status->msg = "no TaskManager exist";
            status->code = -1;
            return false;
        }

        ::openmldb::base::Status rpcStatus = taskmanager_client_ptr->DropOfflineTable(db, table, GetJobTimeout());
        if (rpcStatus.code != 0) {
            status->msg = rpcStatus.msg;
            status->code = rpcStatus.code;
            return false;
        }
    }

    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        status->msg = "no nameserver exist";
        status->code = -2;
        return false;
    }
    std::string err;

    bool ok = ns_ptr->DropTable(db, table, err);
    if (!ok) {
        status->msg = "fail to drop, " + err;
        status->code = -2;
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
    ::hybridse::codec::Schema parameter_schema_raw;
    if (parameter) {
        for (int i = 0; i < parameter->GetSchema()->GetColumnCnt(); i++) {
            auto column = parameter_schema_raw.Add();
            hybridse::type::Type hybridse_type;
            if (!openmldb::schema::SchemaAdapter::ConvertType(parameter->GetSchema()->GetColumnType(i),
                                                              &hybridse_type)) {
                *status = {-1, "Invalid parameter type"};
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
            status->msg = base_status.GetMsg();
            status->trace = base_status.GetTraces();
            status->code = -1;
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
    auto cache = GetSQLCache(db, sql, engine_mode, parameter, status);
    if (!status->IsOK()) {
        return {};
    }
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
        status->msg = "fail to get tablet";
        status->code = hybridse::common::kRunError;
        LOG(WARNING) << "fail to get tablet";
        return {};
    }
    return tablet->GetClient();
}

// Get clients when online batch query in Cluster OpenMLDB
std::shared_ptr<::openmldb::client::TabletClient> SQLClusterRouter::GetTabletClientForBatchQuery(
    const std::string& db, const std::string& sql, const std::shared_ptr<SQLRequestRow>& parameter,
    hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return {};
    }
    auto cache = GetSQLCache(db, sql, hybridse::vm::kBatchMode, parameter, status);
    if (0 != status->code) {
        return {};
    }
    if (cache) {
        auto router_cache = std::dynamic_pointer_cast<RouterSQLCache>(cache);
        if (router_cache) {
            const auto& router = router_cache->GetRouter();
            const std::string& main_table = router.GetMainTable();
            const std::string main_db = router.GetMainDb().empty() ? db : router.GetMainDb();
            if (!main_table.empty()) {
                DLOG(INFO) << "get main table " << main_table;
                auto tablet_accessor = cluster_sdk_->GetTablet(main_db, main_table);
                if (tablet_accessor) {
                    *status = {};
                    return tablet_accessor->GetClient();
                }
            } else {
                auto tablet_accessor = cluster_sdk_->GetTablet();
                if (tablet_accessor) {
                    *status = {};
                    return tablet_accessor->GetClient();
                }
            }
        }
    }
    *status = {::hybridse::common::StatusCode::kCmdError, "fail to get tablet"};
    return {};
}

std::shared_ptr<TableReader> SQLClusterRouter::GetTableReader() {
    return std::make_shared<TableReaderImpl>(cluster_sdk_);
}

std::shared_ptr<openmldb::client::TabletClient> SQLClusterRouter::GetTablet(const std::string& db,
                                                                            const std::string& sp_name,
                                                                            hybridse::sdk::Status* status) {
    if (status == nullptr) return nullptr;
    std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info = cluster_sdk_->GetProcedureInfo(db, sp_name, &status->msg);
    if (!sp_info) {
        status->code = -1;
        status->msg = "procedure not found, msg: " + status->msg;
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    const std::string& table = sp_info->GetMainTable();
    const std::string& db_name = sp_info->GetMainDb().empty() ? db : sp_info->GetMainDb();
    auto tablet = cluster_sdk_->GetTablet(db_name, table);
    if (!tablet) {
        status->code = -1;
        status->msg = "fail to get tablet, table " + db_name + "." + table;
        LOG(WARNING) << status->msg;
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
    if (!row || !status) {
        LOG(WARNING) << "input is invalid";
        return {};
    }
    if (!row->OK()) {
        LOG(WARNING) << "make sure the request row is built before execute sql";
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
        status->msg = "not tablet found";
        return {};
    }
    if (!client->Query(db, sql, row->GetRow(), cntl.get(), response.get(), options_->enable_debug)) {
        status->msg = "request server error, msg: " + response->msg();
        return {};
    }
    if (response->code() != ::openmldb::base::kOk) {
        status->code = response->code();
        status->msg = "request error, " + response->msg();
        return {};
    }

    auto rs = ResultSetSQL::MakeResultSet(response, cntl, status);
    return rs;
}

std::shared_ptr<::hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQLParameterized(
    const std::string& db, const std::string& sql, std::shared_ptr<openmldb::sdk::SQLRequestRow> parameter,
    ::hybridse::sdk::Status* status) {
    std::vector<openmldb::type::DataType> parameter_types;
    if (parameter && !ExtractDBTypes(parameter->GetSchema(), &parameter_types)) {
        status->msg = "convert parameter types error";
        status->code = -1;
        return {};
    }
    auto client = GetTabletClientForBatchQuery(db, sql, parameter, status);
    if (!status->IsOK() || !client) {
        DLOG(INFO) << "no tablet available for sql '" << sql << "': " << status->msg;
        status->msg = absl::StrCat("no tablet available for sql: ", status->msg);
        status->code = -1;
        return {};
    }
    auto cntl = std::make_shared<::brpc::Controller>();
    cntl->set_timeout_ms(options_->request_timeout);
    DLOG(INFO) << " send query to tablet " << client->GetEndpoint();
    auto response = std::make_shared<::openmldb::api::QueryResponse>();
    if (!client->Query(db, sql, parameter_types, parameter ? parameter->GetRow() : "", cntl.get(), response.get(),
                       options_->enable_debug)) {
        status->msg = response->msg();
        status->code = -1;
        return {};
    }
    return ResultSetSQL::MakeResultSet(response, cntl, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQLBatchRequest(
    const std::string& db, const std::string& sql, std::shared_ptr<SQLRequestRowBatch> row_batch,
    hybridse::sdk::Status* status) {
    if (!row_batch || !status) {
        LOG(WARNING) << "input is invalid";
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
        status->code = -1;
        status->msg = "no tablet found";
        return nullptr;
    }
    if (!client->SQLBatchRequestQuery(db, sql, row_batch, cntl.get(), response.get(), options_->enable_debug)) {
        status->code = -1;
        status->msg = "request server error " + response->msg();
        return nullptr;
    }
    if (response->code() != ::openmldb::base::kOk) {
        status->code = -1;
        status->msg = response->msg();
        return nullptr;
    }
    auto rs = std::make_shared<openmldb::sdk::SQLBatchRequestResultSet>(response, cntl);
    if (!rs->Init()) {
        status->code = -1;
        status->msg = "batch request result set init fail";
        return nullptr;
    }
    return rs;
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db, const std::string& sql, ::hybridse::sdk::Status* status) {
    if (status == NULL) return false;
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    std::vector<DefaultValueMap> default_maps;
    std::vector<uint32_t> str_lengths;
    if (!GetMultiRowInsertInfo(db, sql, status, &table_info, &default_maps, &str_lengths)) {
        status->code = 1;
        LOG(WARNING) << "Fail to execute insert statement: " << status->msg;
        return false;
    }

    auto schema = ::openmldb::schema::SchemaAdapter::ConvertSchema(table_info->column_desc());
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
    bool ret = cluster_sdk_->GetTablet(table_info->db(), table_info->name(), &tablets);
    if (!ret || tablets.empty()) {
        status->msg = "Fail to execute insert statement: fail to get " + table_info->name() + " tablet";
        LOG(WARNING) << status->msg;
        return false;
    }
    size_t cnt = 0;
    for (size_t i = 0; i < default_maps.size(); i++) {
        auto row = std::make_shared<SQLInsertRow>(table_info, schema, default_maps[i], str_lengths[i]);
        if (!row) {
            LOG(WARNING) << "fail to parse row[" << i << "]";
            continue;
        }
        if (!row->Init(0)) {
            LOG(WARNING) << "fail to encode row[" << i << " for table " << table_info->name();
            continue;
        }
        if (!row->IsComplete()) {
            LOG(WARNING) << "fail to build row[" << i << "]";
            continue;
        }
        if (!PutRow(table_info->tid(), row, tablets, status)) {
            LOG(WARNING) << "fail to put row[" << i << "] due to: " << status->msg;
            continue;
        }
        cnt++;
    }
    if (cnt < default_maps.size()) {
        status->msg = "Error occur when execute insert, success/total: " + std::to_string(cnt) + "/" +
                      std::to_string(default_maps.size());
        status->code = 1;
        return false;
    }
    return true;
}

bool SQLClusterRouter::PutRow(uint32_t tid, const std::shared_ptr<SQLInsertRow>& row,
                              const std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>>& tablets,
                              ::hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return false;
    }
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
                    bool ret = client->Put(tid, pid, cur_ts, row->GetRow(), kv.second);
                    if (!ret) {
                        status->msg = "fail to make a put request to table. tid " + std::to_string(tid);
                        LOG(WARNING) << status->msg;
                        return false;
                    }
                    continue;
                }
            }
        }
        status->msg = "fail to get tablet client. pid " + std::to_string(pid);
        LOG(WARNING) << status->msg;
        return false;
    }
    return true;
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db, const std::string& sql, std::shared_ptr<SQLInsertRows> rows,
                                     hybridse::sdk::Status* status) {
    if (!rows || !status) {
        LOG(WARNING) << "input is invalid";
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
    if (!row || !status) {
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

bool SQLClusterRouter::GetSQLPlan(const std::string& sql, ::hybridse::node::NodeManager* nm,
                                  ::hybridse::node::PlanNodeList* plan) {
    if (nm == NULL || plan == NULL) return false;
    ::hybridse::base::Status sql_status;
    PlanAPI::CreatePlanTreeFromScript(sql, *plan, nm, sql_status);
    if (0 != sql_status.code) {
        LOG(WARNING) << sql_status.msg;
        return false;
    }
    return true;
}

bool SQLClusterRouter::RefreshCatalog() { return cluster_sdk_->Refresh(); }

std::shared_ptr<ExplainInfo> SQLClusterRouter::Explain(const std::string& db, const std::string& sql,
                                                       ::hybridse::sdk::Status* status) {
    ::hybridse::vm::ExplainOutput explain_output;
    ::hybridse::base::Status vm_status;
    ::hybridse::codec::Schema parameter_schema;
    bool ok = cluster_sdk_->GetEngine()->Explain(sql, db, ::hybridse::vm::kRequestMode, parameter_schema,
                                                 &explain_output, &vm_status);
    if (!ok) {
        status->code = -1;
        status->msg = vm_status.msg;
        status->trace = vm_status.GetTraces();
        LOG(WARNING) << "fail to explain sql " << sql;
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
    if (!row || !status) {
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    if (!row->OK()) {
        status->code = -1;
        status->msg = "make sure the request row is built before execute sql";
        LOG(WARNING) << "make sure the request row is built before execute sql";
        return nullptr;
    }
    auto tablet = GetTablet(db, sp_name, status);
    if (!tablet) {
        return nullptr;
    }

    auto cntl = std::make_shared<::brpc::Controller>();
    auto response = std::make_shared<::openmldb::api::QueryResponse>();
    bool ok = tablet->CallProcedure(db, sp_name, row->GetRow(), cntl.get(), response.get(), options_->enable_debug,
                                    options_->request_timeout);
    if (!ok) {
        status->code = -1;
        status->msg = "request server error" + response->msg();
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    if (response->code() != ::openmldb::base::kOk) {
        status->code = -1;
        status->msg = response->msg();
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    auto rs = ResultSetSQL::MakeResultSet(response, cntl, status);
    return rs;
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::CallSQLBatchRequestProcedure(
    const std::string& db, const std::string& sp_name, std::shared_ptr<SQLRequestRowBatch> row_batch,
    hybridse::sdk::Status* status) {
    if (!row_batch || !status) {
        return nullptr;
    }
    auto tablet = GetTablet(db, sp_name, status);
    if (!tablet) {
        return nullptr;
    }

    auto cntl = std::make_shared<::brpc::Controller>();
    auto response = std::make_shared<::openmldb::api::SQLBatchRequestQueryResponse>();
    bool ok = tablet->CallSQLBatchRequestProcedure(db, sp_name, row_batch, cntl.get(), response.get(),
                                                   options_->enable_debug, options_->request_timeout);
    if (!ok) {
        status->code = -1;
        status->msg = "request server error, msg: " + response->msg();
        return nullptr;
    }
    if (response->code() != ::openmldb::base::kOk) {
        status->code = -1;
        status->msg = response->msg();
        return nullptr;
    }
    auto rs = std::make_shared<::openmldb::sdk::SQLBatchRequestResultSet>(response, cntl);
    if (!rs->Init()) {
        status->code = -1;
        status->msg = "SQLBatchRequestResultSet init failed";
        return nullptr;
    }
    return rs;
}

std::shared_ptr<hybridse::sdk::ProcedureInfo> SQLClusterRouter::ShowProcedure(const std::string& db,
                                                                              const std::string& sp_name,
                                                                              hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return nullptr;
    }
    std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info = cluster_sdk_->GetProcedureInfo(db, sp_name, &status->msg);
    if (!sp_info) {
        status->code = -1;
        status->msg = "procedure not found, msg: " + status->msg;
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    return sp_info;
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::HandleSQLCmd(const hybridse::node::CmdPlanNode* cmd_node,
                                                                         const std::string& db,
                                                                         ::hybridse::sdk::Status* status) {
    if (cmd_node == nullptr || status == nullptr) {
        *status = {::hybridse::common::StatusCode::kCmdError, "null pointer"};
        return {};
    }
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        *status = {::hybridse::common::StatusCode::kCmdError, "no nameserver exist"};
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
                *status = {::hybridse::common::StatusCode::kCmdError, msg};
            }
            return {};
        }

        case hybridse::node::kCmdShowTables: {
            if (db.empty()) {
                *status = {::hybridse::common::StatusCode::kCmdError, "please enter database first"};
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

        case hybridse::node::kCmdDescTable: {
            if (db.empty()) {
                *status = {::hybridse::common::StatusCode::kCmdError, "please enter database first"};
                return {};
            }
            // TODO(denglong): Should support table name with database name
            auto table_name = cmd_node->GetArgs()[0];
            auto table = cluster_sdk_->GetTableInfo(db, table_name);
            if (table == nullptr) {
                *status = {::hybridse::common::StatusCode::kCmdError, "table " + table_name + " does not exist"};
                return {};
            }
            std::vector<std::vector<std::string>> result;
            std::stringstream ss;
            ::openmldb::cmd::PrintSchema(table->column_desc(), ss);
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
            options["storage_mode"] = StorageMode_Name(table->storage_mode());
            // remove the prefix 'k', i.e., change kMemory to Memory
            options["storage_mode"] = options["storage_mode"].substr(1, options["storage_mode"].size() - 1);
            ::openmldb::cmd::PrintTableOptions(options, ss);
            result.emplace_back(std::vector{ss.str()});
            return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, result, status);
        }

        case hybridse::node::kCmdCreateDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            if (ns_ptr->CreateDatabase(name, msg, cmd_node->IsIfNotExists())) {
                *status = {};
            } else {
                *status = {::hybridse::common::StatusCode::kCmdError, "Create database failed for " + msg};
            }
            return {};
        }
        case hybridse::node::kCmdUseDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            if (ns_ptr->Use(name, msg)) {
                db_ = name;
                *status = {::hybridse::common::kOk, "Database changed"};
            } else {
                *status = {::hybridse::common::StatusCode::kCmdError, "Use database failed for " + msg};
            }
            return {};
        }
        case hybridse::node::kCmdDropDatabase: {
            std::string name = cmd_node->GetArgs()[0];
            if (ns_ptr->DropDatabase(name, msg)) {
                *status = {};
            } else {
                *status = {::hybridse::common::StatusCode::kCmdError, msg};
            }
            return {};
        }
        case hybridse::node::kCmdShowFunctions: {
            std::vector<::openmldb::common::ExternalFun> funs;
            base::Status st = ns_ptr->ShowFunction("", &funs);
            if (!st.OK()) {
                *status = {::hybridse::common::StatusCode::kCmdError, "show udf function failed"};
                return {};
            }
            std::vector<std::vector<std::string>> lines;
            for (auto& fun_info : funs) {
                std::string is_aggregate = "false";
                if (fun_info.is_aggregate()) {
                    is_aggregate = "true";
                }
                std::string arg_type = "";
                for (int i = 0; i < fun_info.arg_type_size(); i++) {
                    arg_type = absl::StrCat(arg_type, openmldb::type::DataType_Name(fun_info.arg_type(i)).substr(1));
                    if (i != fun_info.arg_type_size() - 1) {
                        arg_type = absl::StrCat(arg_type, "|");
                    }
                }
                std::vector<std::string> vec = {fun_info.name(),
                                                openmldb::type::DataType_Name(fun_info.return_type()).substr(1),
                                                arg_type, is_aggregate, fun_info.file()};
                lines.push_back(vec);
            }
            return ResultSetSQL::MakeResultSet({"Name", "Return_type", "Arg_type", "Is_aggregate", "File"}, lines,
                                               status);
        }
        case hybridse::node::kCmdDropFunction: {
            std::string name = cmd_node->GetArgs()[0];
            auto base_status = ns_ptr->DropFunction(name, cmd_node->IsIfExists());
            if (base_status.OK()) {
                cluster_sdk_->RemoveExternalFun(name);
                auto taskmanager_client = cluster_sdk_->GetTaskManagerClient();
                if (taskmanager_client) {
                    base_status = taskmanager_client->DropFunction(name, GetJobTimeout());
                    if (!base_status.OK()) {
                        *status = {::hybridse::common::StatusCode::kCmdError, base_status.msg};
                        return {};
                    }
                }
                *status = {};
            } else {
                *status = {::hybridse::common::StatusCode::kCmdError, base_status.msg};
            }
            return {};
        }
        case hybridse::node::kCmdShowCreateSp: {
            auto& args = cmd_node->GetArgs();
            std::string db_name, sp_name;
            if (!ParseNamesFromArgs(db, args, &db_name, &sp_name).IsOK()) {
                *status = {::hybridse::common::StatusCode::kCmdError, msg};
                return {};
            }
            auto sp_info = cluster_sdk_->GetProcedureInfo(db_name, sp_name, &msg);
            if (!sp_info) {
                *status = {::hybridse::common::StatusCode::kCmdError, "Failed to show procedure, " + msg};
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
                *status = {::hybridse::common::StatusCode::kCmdError, "please enter database first"};
                return {};
            }
            std::string sp_name = cmd_node->GetArgs()[0];
            if (!CheckAnswerIfInteractive("procedure", sp_name)) {
                return {};
            }
            if (ns_ptr->DropProcedure(db, sp_name, msg)) {
                *status = {};
                RefreshCatalog();
            } else {
                *status = {::hybridse::common::StatusCode::kCmdError, "Failed to drop, " + msg};
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
            auto sp = cluster_sdk_->GetProcedureInfo(db_name, deploy_name, &msg);
            // check if deployment
            if (!sp || sp->GetType() != hybridse::sdk::kReqDeployment) {
                *status = {::hybridse::common::StatusCode::kCmdError, sp ? "not a deployment" : "not found"};
                return {};
            }
            std::stringstream ss;
            ::openmldb::cmd::PrintProcedureInfo(*sp, ss);
            std::vector<std::vector<std::string>> result;
            std::vector<std::string> vec = {ss.str()};
            result.emplace_back(std::move(vec));
            return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, result, status);
        }
        case hybridse::node::kCmdShowDeployments: {
            if (db.empty()) {
                *status = {::hybridse::common::StatusCode::kCmdError, "please enter database first"};
                return {};
            }
            // ns client get all procedures of one db
            std::vector<api::ProcedureInfo> sps;
            if (!ns_ptr->ShowProcedure(db, "", &sps, &msg)) {
                *status = {::hybridse::common::StatusCode::kCmdError, msg};
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
            if (db.empty()) {
                *status = {::hybridse::common::StatusCode::kCmdError, "please enter database first"};
                return {};
            }
            std::string deploy_name = cmd_node->GetArgs()[0];
            // check if deployment, avoid deleting the normal procedure
            auto sp = cluster_sdk_->GetProcedureInfo(db, deploy_name, &msg);
            if (!sp || sp->GetType() != hybridse::sdk::kReqDeployment) {
                *status = {::hybridse::common::StatusCode::kCmdError, sp ? "not a deployment" : "deployment not found"};
                return {};
            }
            if (!CheckAnswerIfInteractive("deployment", deploy_name)) {
                return {};
            }
            if (ns_ptr->DropProcedure(db, deploy_name, msg)) {
                RefreshCatalog();
                *status = {};
            } else {
                *status = {::hybridse::common::StatusCode::kCmdError, "Failed to drop. error: " + msg};
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
            exit(0);
        }
        case hybridse::node::kCmdShowJobs: {
            std::string db = "__INTERNAL_DB";
            std::string sql = "SELECT * FROM JOB_INFO";
            auto rs = ExecuteSQLParameterized(db, sql, std::shared_ptr<openmldb::sdk::SQLRequestRow>(), status);
            if (status->code != 0) {
                return {};
            }
            return rs;
        }
        case hybridse::node::kCmdShowJob: {
            int job_id;
            try {
                // Check argument type
                job_id = std::stoi(cmd_node->GetArgs()[0]);
            } catch (...) {
                *status = {::hybridse::common::StatusCode::kCmdError,
                           "Failed to parse job id: " + cmd_node->GetArgs()[0]};
                return {};
            }

            std::string db = "__INTERNAL_DB";
            std::string sql = "SELECT * FROM JOB_INFO WHERE id = " + std::to_string(job_id);

            auto rs = ExecuteSQLParameterized(db, sql, std::shared_ptr<openmldb::sdk::SQLRequestRow>(), status);
            if (status->code != 0) {
                return {};
            }
            if (rs->Size() == 0) {
                status->code = ::hybridse::common::StatusCode::kCmdError;
                status->msg = "Job not found: " + std::to_string(job_id);
                return {};
            }
            return rs;
        }
        case hybridse::node::kCmdStopJob: {
            int job_id;
            try {
                job_id = std::stoi(cmd_node->GetArgs()[0]);
            } catch (...) {
                *status = {::hybridse::common::StatusCode::kCmdError,
                           "Failed to parse job id: " + cmd_node->GetArgs()[0]};
                return {};
            }

            ::openmldb::taskmanager::JobInfo job_info;
            StopJob(job_id, &job_info);

            std::vector<::openmldb::taskmanager::JobInfo> job_infos;
            if (job_info.id() > 0) {
                job_infos.push_back(job_info);
            }
            std::stringstream ss;
            ::openmldb::cmd::PrintJobInfos(job_infos, ss);
            std::vector<std::vector<std::string>> result;
            std::vector<std::string> vec = {ss.str()};
            result.emplace_back(std::move(vec));
            return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, result, status);
        }
        case hybridse::node::kCmdDropTable: {
            *status = {};
            std::string db_name = db;
            std::string table_name;
            if (cmd_node->GetArgs().size() == 2) {
                db_name = cmd_node->GetArgs()[0];
                table_name = cmd_node->GetArgs()[1];
            } else if (cmd_node->GetArgs().size() == 1) {
                table_name = cmd_node->GetArgs()[0];
            } else {
                *status = {::hybridse::common::StatusCode::kCmdError, "Invalid Cmd Args size"};
            }
            if (!CheckAnswerIfInteractive("table", table_name)) {
                return {};
            }
            if (DropTable(db_name, table_name, status)) {
                RefreshCatalog();
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
                *status = {::hybridse::common::StatusCode::kCmdError, "Invalid Cmd Args size"};
                return {};
            }
            if (!CheckAnswerIfInteractive("index", index_name + " on " + table_name)) {
                return {};
            }
            ret = ns_ptr->DeleteIndex(db, table_name, index_name, msg);
            ret == true ? * status = {} : * status = {::hybridse::common::StatusCode::kCmdError, msg};
            return {};
        }
        case hybridse::node::kCmdShowComponents: {
            return ExecuteShowComponents(status);
        }
        case hybridse::node::kCmdShowTableStatus: {
            return ExecuteShowTableStatus(db, status);
        }
        default: {
            *status = {::hybridse::common::StatusCode::kCmdError, "fail to execute script with unsupported type"};
        }
    }
    return {};
}

base::Status SQLClusterRouter::HandleSQLCreateTable(hybridse::node::CreatePlanNode* create_node, const std::string& db,
                                                    std::shared_ptr<::openmldb::client::NsClient> ns_ptr) {
    if (create_node == nullptr || ns_ptr == nullptr) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "fail to execute plan : null pointer");
    }
    std::string db_name = create_node->GetDatabase().empty() ? db : create_node->GetDatabase();
    if (db_name.empty()) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "ERROR: Please use database first");
    }
    ::openmldb::nameserver::TableInfo table_info;
    table_info.set_db(db_name);

    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> all_tablet;
    all_tablet = cluster_sdk_->GetAllTablet();
    // set dafault value
    uint32_t default_replica_num = std::min(static_cast<uint32_t>(all_tablet.size()), FLAGS_replica_num);

    hybridse::base::Status sql_status;
    bool is_cluster_mode = cluster_sdk_->IsClusterMode();
    ::openmldb::sdk::NodeAdapter::TransformToTableDef(create_node, true, &table_info, default_replica_num,
                                                      is_cluster_mode, &sql_status);
    if (sql_status.code != 0) {
        return base::Status(sql_status.code, sql_status.msg);
    }
    std::string msg;
    if (!ns_ptr->CreateTable(table_info, create_node->GetIfNotExist(), msg)) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, msg);
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
    // get dependent tables, and fill sp_info
    std::set<std::pair<std::string, std::string>> tables;
    ::hybridse::base::Status status;
    if (!cluster_sdk_->GetEngine()->GetDependentTables(sql, db, ::hybridse::vm::kRequestMode, &tables, status)) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "fail to get dependent tables: " + status.msg);
    }
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
        LOG(WARNING) << sql_status.str();
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
    if (!row || !status) {
        return {};
    }
    if (!row->OK()) {
        status->code = -1;
        status->msg = "make sure the request row is built before execute sql";
        LOG(WARNING) << "make sure the request row is built before execute sql";
        return {};
    }
    auto tablet = GetTablet(db, sp_name, status);
    if (!tablet) {
        return {};
    }

    std::shared_ptr<openmldb::api::QueryResponse> response = std::make_shared<openmldb::api::QueryResponse>();
    std::shared_ptr<brpc::Controller> cntl = std::make_shared<brpc::Controller>();
    auto* callback = new openmldb::RpcCallback<openmldb::api::QueryResponse>(response, cntl);

    std::shared_ptr<openmldb::sdk::QueryFutureImpl> future = std::make_shared<openmldb::sdk::QueryFutureImpl>(callback);
    bool ok = tablet->CallProcedure(db, sp_name, row->GetRow(), timeout_ms, options_->enable_debug, callback);
    if (!ok) {
        status->code = -1;
        status->msg = "request server error, msg: " + response->msg();
        LOG(WARNING) << status->msg;
        return {};
    }
    return future;
}

std::shared_ptr<openmldb::sdk::QueryFuture> SQLClusterRouter::CallSQLBatchRequestProcedure(
    const std::string& db, const std::string& sp_name, int64_t timeout_ms,
    std::shared_ptr<SQLRequestRowBatch> row_batch, hybridse::sdk::Status* status) {
    if (!row_batch || !status) {
        return nullptr;
    }
    auto tablet = GetTablet(db, sp_name, status);
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
        status->code = -1;
        status->msg = "request server error, msg: " + response->msg();
        LOG(WARNING) << status->msg;
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
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ShowJobs(only_unfinished, GetJobTimeout(), job_infos);
}

::openmldb::base::Status SQLClusterRouter::ShowJob(const int id, ::openmldb::taskmanager::JobInfo* job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ShowJob(id, GetJobTimeout(), job_info);
}

::openmldb::base::Status SQLClusterRouter::StopJob(const int id, ::openmldb::taskmanager::JobInfo* job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->StopJob(id, GetJobTimeout(), job_info);
}

::openmldb::base::Status SQLClusterRouter::ExecuteOfflineQueryAsync(const std::string& sql,
                                                                    const std::map<std::string, std::string>& config,
                                                                    const std::string& default_db, int job_timeout,
                                                                    ::openmldb::taskmanager::JobInfo* job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->RunBatchAndShow(sql, config, default_db, false, job_timeout, job_info);
}

::openmldb::base::Status SQLClusterRouter::ExecuteOfflineQueryGetOutput(
    const std::string& sql, const std::map<std::string, std::string>& config, const std::string& default_db,
    int job_timeout, std::string* output) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
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
        return {-1, "Fail to get TaskManager client"};
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
        return {-1, "Fail to get TaskManager client"};
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
        return {-1, "Fail to get TaskManager client"};
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
    table_info.set_format_version(1);
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
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        status->code = -1;
        status->msg = "Fail to get TaskManager client";
        return "";
    }

    // TODO(tobe): Need to pass ::openmldb::base::Status* for TaskManagerClient
    auto openmldbStatus = std::make_shared<::openmldb::base::Status>();
    auto log = taskmanager_client_ptr->GetJobLog(id, GetJobTimeout(), openmldbStatus.get());
    status->code = openmldbStatus->code;
    status->msg = openmldbStatus->msg;
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
    return ExecuteSQL(db, sql, IsOnlineMode(), IsSyncJob(), GetJobTimeout(), status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQL(const std::string& db, const std::string& sql,
                                                                       bool is_online_mode, bool is_sync_job,
                                                                       int offline_job_timeout,
                                                                       hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return {};
    }
    hybridse::node::NodeManager node_manager;
    hybridse::node::PlanNodeList plan_trees;
    hybridse::base::Status sql_status;
    hybridse::plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);
    if (sql_status.code != 0) {
        *status = {::hybridse::common::StatusCode::kCmdError, sql_status.msg, sql_status.GetTraces()};
        return {};
    }
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        *status = {::hybridse::common::StatusCode::kCmdError, "no nameserver exist"};
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
            auto base_status = HandleSQLCreateTable(create_node, db, ns_ptr);
            if (base_status.OK()) {
                RefreshCatalog();
                *status = {};
            } else {
                *status = {::hybridse::common::StatusCode::kCmdError, base_status.msg};
            }
            return {};
        }
        case hybridse::node::kPlanTypeCreateSp: {
            if (db.empty()) {
                *status = {::hybridse::common::StatusCode::kCmdError, "Please use database first"};
                return {};
            }
            auto create_node = dynamic_cast<hybridse::node::CreateProcedurePlanNode*>(node);
            auto base_status = HandleSQLCreateProcedure(create_node, db, sql, ns_ptr);
            if (base_status.OK()) {
                RefreshCatalog();
                *status = {};
            } else {
                *status = {::hybridse::common::StatusCode::kCmdError, base_status.msg};
            }
            return {};
        }
        case hybridse::node::kPlanTypeCreateIndex: {
            if (db.empty()) {
                *status = {::hybridse::common::StatusCode::kCmdError, "Please use database first"};
                return {};
            }
            auto create_index_plan_node = dynamic_cast<hybridse::node::CreateIndexPlanNode*>(node);
            auto create_index_node = create_index_plan_node->create_index_node_;
            ::openmldb::common::ColumnKey column_key;
            hybridse::base::Status base_status;
            if (!::openmldb::sdk::NodeAdapter::TransformToColumnKey(create_index_node->index_, {}, &column_key,
                                                                    &base_status)) {
                *status = {::hybridse::common::StatusCode::kCmdError, base_status.msg};
                return {};
            }
            column_key.set_index_name(create_index_node->index_name_);
            if (ns_ptr->AddIndex(create_index_node->table_name_, column_key, nullptr, msg)) {
                *status = {};
            } else {
                *status = {::hybridse::common::StatusCode::kCmdError, msg};
            }
            return {};
        }
        case hybridse::node::kPlanTypeInsert: {
            if (cluster_sdk_->IsClusterMode() && !is_online_mode) {
                // Not support for inserting into offline storage
                *status = {::hybridse::common::StatusCode::kCmdError,
                           "Can not insert in offline mode, please set @@SESSION.execute_mode='online'"};
                return {};
            }
            // if db name has been specified in sql, db parameter will be ignored
            if (!ExecuteInsert(db, sql, status)) {
                status->code = ::hybridse::common::StatusCode::kCmdError;
            } else {
                *status = {};
            }
            return {};
        }
        case hybridse::node::kPlanTypeDeploy: {
            *status = HandleDeploy(db, dynamic_cast<hybridse::node::DeployPlanNode*>(node));
            if (status->IsOK()) {
                RefreshCatalog();
            }
            return {};
        }
        case hybridse::node::kPlanTypeFuncDef:
        case hybridse::node::kPlanTypeQuery: {
            if (!cluster_sdk_->IsClusterMode() || is_online_mode) {
                // Run online query
                return ExecuteSQLParameterized(db, sql, {}, status);
            } else {
                // Run offline query
                return ExecuteOfflineQuery(db, sql, is_sync_job, offline_job_timeout, status);
            }
        }
        case hybridse::node::kPlanTypeSelectInto: {
            if (!cluster_sdk_->IsClusterMode() || is_online_mode) {
                auto* select_into_plan_node = dynamic_cast<hybridse::node::SelectIntoPlanNode*>(node);
                const std::string& query_sql = select_into_plan_node->QueryStr();
                auto rs = ExecuteSQLParameterized(db, query_sql, {}, status);
                if (!rs) {
                    return {};
                }
                const std::string& file_path = select_into_plan_node->OutFile();
                const std::shared_ptr<hybridse::node::OptionsMap> options_map = select_into_plan_node->Options();
                auto base_status = SaveResultSet(file_path, options_map, rs.get());
                if (!base_status.OK()) {
                    *status = {::hybridse::common::StatusCode::kCmdError, base_status.msg};
                } else {
                    *status = {};
                }
            } else {
                ::openmldb::taskmanager::JobInfo job_info;
                std::map<std::string, std::string> config;

                ReadSparkConfFromFile(std::dynamic_pointer_cast<SQLRouterOptions>(options_)->spark_conf_path, &config);
                auto base_status = ExportOfflineData(sql, config, db, is_sync_job, offline_job_timeout, &job_info);
                if (base_status.OK()) {
                    *status = {};
                    if (job_info.id() > 0) {
                        std::stringstream ss;
                        ::openmldb::cmd::PrintJobInfos({job_info}, ss);
                        std::vector<std::string> value = {ss.str()};
                        return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, {value}, status);
                    }
                } else {
                    *status = {::hybridse::common::StatusCode::kCmdError, base_status.msg};
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
                *status = {::hybridse::common::StatusCode::kCmdError, " no db in sql and no default db"};
                return {};
            }
            if (cluster_sdk_->IsClusterMode()) {
                // Handle in cluster mode
                ::openmldb::taskmanager::JobInfo job_info;
                std::map<std::string, std::string> config;
                ReadSparkConfFromFile(std::dynamic_pointer_cast<SQLRouterOptions>(options_)->spark_conf_path, &config);

                ::openmldb::base::Status base_status;
                if (is_online_mode) {
                    // Handle in online mode
                    base_status = ImportOnlineData(sql, config, database, is_sync_job, offline_job_timeout, &job_info);
                } else {
                    // Handle in offline mode
                    base_status = ImportOfflineData(sql, config, database, is_sync_job, offline_job_timeout, &job_info);
                }
                if (base_status.OK() && job_info.id() > 0) {
                    std::stringstream ss;
                    ::openmldb::cmd::PrintJobInfos({job_info}, ss);
                    std::vector<std::string> value = {ss.str()};
                    *status = {};
                    return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, {value}, status);
                } else {
                    *status = {::hybridse::common::StatusCode::kCmdError, base_status.msg};
                }
            } else {
                // Handle in standalone mode
                *status = HandleLoadDataInfile(database, plan->Table(), plan->File(), plan->Options());
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
                *status = {::hybridse::common::StatusCode::kCmdError, " no db in sql and no default db"};
                return {};
            }
            *status = HandleDelete(database, plan->GetTableName(), plan->GetCondition());
            return {};
        }
        default: {
            *status = {::hybridse::common::StatusCode::kCmdError, "Unsupported command"};
            return {};
        }
    }
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteOfflineQuery(const std::string& db,
                                                                                const std::string& sql,
                                                                                bool is_sync_job, int job_timeout,
                                                                                ::hybridse::sdk::Status* status) {
    std::map<std::string, std::string> config;
    ReadSparkConfFromFile(std::dynamic_pointer_cast<SQLRouterOptions>(options_)->spark_conf_path, &config);

    if (is_sync_job) {
        // Run offline sql and wait to get output
        std::string output;
        auto base_status = ExecuteOfflineQueryGetOutput(sql, config, db, job_timeout, &output);
        if (!base_status.OK()) {
            *status = {::hybridse::common::StatusCode::kCmdError, base_status.msg};
            return {};
        }
        // Print the output from job output
        // TODO(tobe): return result set if want to format the output
        std::vector<std::string> value = {output};
        return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, {value}, status);
    } else {
        // Run offline sql and return job info immediately
        ::openmldb::taskmanager::JobInfo job_info;
        auto base_status = ExecuteOfflineQueryAsync(sql, config, db, job_timeout, &job_info);
        if (!base_status.OK()) {
            *status = {::hybridse::common::StatusCode::kCmdError, base_status.msg};
            return {};
        }

        // job info id should > 0
        if (job_info.id() <= 0) {
            *status = {::hybridse::common::StatusCode::kCmdError, "job info id should > 0"};
            return {};
        }

        std::stringstream ss;
        ::openmldb::cmd::PrintJobInfos({job_info}, ss);
        std::vector<std::string> value = {ss.str()};
        // TODO(tobe): Return the result set with multiple columns
        return ResultSetSQL::MakeResultSet({FORMAT_STRING_KEY}, {value}, status);
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
            return {::hybridse::common::StatusCode::kRunError, "set global variable failed"};
        }
        if (!cluster_sdk_->TriggerNotify(::openmldb::type::NotifyType::kGlobalVar)) {
            return {::hybridse::common::StatusCode::kRunError, "zk globlvar node not update"};
        }
    }
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    std::string value = node->Value()->GetExprString();
    std::transform(value.begin(), value.end(), value.begin(), ::tolower);
    // TODO(hw): validation can be simpler
    if (key == "execute_mode") {
        if (value != "online" && value != "offline") {
            return {::hybridse::common::StatusCode::kCmdError, "the value of execute_mode must be online|offline"};
        }
    } else if (key == "enable_trace" || key == "sync_job") {
        if (value != "true" && value != "false") {
            return {::hybridse::common::StatusCode::kCmdError, "the value of " + key + " must be true|false"};
        }
    } else if (key == "job_timeout") {
        // we only validate the value here, job timeout will be used in every offline call
        int new_timeout = 0;
        if (!absl::SimpleAtoi(value, &new_timeout)) {
            return {::hybridse::common::StatusCode::kCmdError, "Fail to parse value, can't set the request timeout"};
        }
    } else {
        return {};
    }
    session_variables_[key] = value;
    return {};
}

::hybridse::sdk::Status SQLClusterRouter::ParseNamesFromArgs(const std::string& db,
                                                             const std::vector<std::string>& args, std::string* db_name,
                                                             std::string* sp_name) {
    if (args.size() == 1) {
        // only sp name, no db_name
        if (db.empty()) {
            return {::hybridse::common::StatusCode::kCmdError, "Please enter database first"};
        }
        *db_name = db;
        *sp_name = args[0];
    } else if (args.size() == 2) {
        *db_name = args[0];
        *sp_name = args[1];
    } else {
        return {::hybridse::common::StatusCode::kCmdError, "Invalid args"};
    }
    return {};
}

bool SQLClusterRouter::CheckAnswerIfInteractive(const std::string& drop_type, const std::string& name) {
    if (interactive_) {
        printf("Drop %s %s? yes/no\n", drop_type.c_str(), name.c_str());
        std::string input;
        std::cin >> input;
        std::transform(input.begin(), input.end(), input.begin(), ::tolower);
        if (input != "yes") {
            printf("'Drop %s' cmd is canceled!\n", name.c_str());
            return false;
        }
    }
    return true;
}

std::string SQLClusterRouter::GetDatabase() {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    return db_;
}

void SQLClusterRouter::SetDatabase(const std::string& db) {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    db_ = db;
}

void SQLClusterRouter::SetInteractive(bool value) { interactive_ = value; }

::openmldb::base::Status SQLClusterRouter::SaveResultSet(const std::string& file_path,
                                                         const std::shared_ptr<hybridse::node::OptionsMap>& options_map,
                                                         ::hybridse::sdk::ResultSet* result_set) {
    if (!result_set) {
        return {openmldb::base::kSQLCmdRunError, "nullptr"};
    }
    openmldb::sdk::WriteFileOptionsParser options_parse;
    auto st = options_parse.Parse(options_map);
    if (!st.OK()) {
        return {st.code, st.msg};
    }
    // Check file
    std::ofstream fstream;
    if (options_parse.GetMode() == "error_if_exists") {
        if (access(file_path.c_str(), 0) == 0) {
            return {openmldb::base::kSQLCmdRunError, "File already exists"};
        } else {
            fstream.open(file_path);
        }
    } else if (options_parse.GetMode() == "overwrite") {
        fstream.open(file_path, std::ios::out);
    } else if (options_parse.GetMode() == "append") {
        fstream.open(file_path, std::ios::app);
        fstream << std::endl;
        if (options_parse.GetHeader()) {
            LOG(WARNING) << "In the middle of output file will have header";
        }
    }
    if (!fstream.is_open()) {
        return {openmldb::base::kSQLCmdRunError, "Failed to open file, please check file path"};
    }
    // Write data
    if (options_parse.GetFormat() == "csv") {
        auto* schema = result_set->GetSchema();
        // Add Header
        if (options_parse.GetHeader()) {
            std::string schemaString;
            for (int32_t i = 0; i < schema->GetColumnCnt(); i++) {
                schemaString.append(schema->GetColumnName(i));
                if (i != schema->GetColumnCnt() - 1) {
                    schemaString += options_parse.GetDelimiter();
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
                        rowString.append(options_parse.GetNullValue());
                    } else {
                        std::string val;
                        bool ok = result_set->GetAsString(i, val);
                        if (!ok) {
                            return {openmldb::base::kSQLCmdRunError, "Failed to get result set value"};
                        }
                        if (options_parse.GetQuote() != '\0' &&
                            schema->GetColumnType(i) == hybridse::sdk::kTypeString) {
                            rowString.append(options_parse.GetQuote() + val + options_parse.GetQuote());
                        } else {
                            rowString.append(val);
                        }
                    }
                    if (i != schema->GetColumnCnt() - 1) {
                        rowString += options_parse.GetDelimiter();
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
    }
    return {};
}

// Only csv format
hybridse::sdk::Status SQLClusterRouter::HandleLoadDataInfile(
    const std::string& database, const std::string& table, const std::string& file_path,
    const std::shared_ptr<hybridse::node::OptionsMap>& options) {
    if (database.empty()) {
        return {::hybridse::common::StatusCode::kCmdError, "database is empty"};
    }
    openmldb::sdk::ReadFileOptionsParser options_parse;
    auto st = options_parse.Parse(options);
    if (!st.OK()) {
        return {::hybridse::common::StatusCode::kCmdError, st.msg};
    }
    /*std::cout << "Load " << file_path << " to " << real_db << "-" << table << ", options: delimiter ["
              << options_parse.GetDelimiter() << "], has header[" << (options_parse.GetHeader() ? "true" : "false")
              << "], null_value[" << options_parse.GetNullValue() << "], format[" << options_parse.GetFormat()
              << "], quote[" << options_parse.GetQuote() << "]" << std::endl;*/
    // read csv
    if (!base::IsExists(file_path)) {
        return {::hybridse::common::StatusCode::kCmdError, "file not exist"};
    }
    std::ifstream file(file_path);
    if (!file.is_open()) {
        return {::hybridse::common::StatusCode::kCmdError, "open file failed"};
    }

    std::string line;
    if (!std::getline(file, line)) {
        return {::hybridse::common::StatusCode::kCmdError, "read from file failed"};
    }
    std::vector<std::string> cols;
    ::openmldb::sdk::SplitLineWithDelimiterForStrings(line, options_parse.GetDelimiter(), &cols,
                                                      options_parse.GetQuote());
    auto schema = GetTableSchema(database, table);
    if (!schema) {
        return {::hybridse::common::StatusCode::kCmdError, "table is not exist"};
    }
    if (static_cast<int>(cols.size()) != schema->GetColumnCnt()) {
        return {::hybridse::common::StatusCode::kCmdError, "mismatch column size"};
    }

    if (options_parse.GetHeader()) {
        // the first line is the column names, check if equal with table schema
        for (int i = 0; i < schema->GetColumnCnt(); ++i) {
            if (cols[i] != schema->GetColumnName(i)) {
                return {::hybridse::common::StatusCode::kCmdError, "mismatch column name"};
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
    uint64_t i = 0;
    do {
        cols.clear();
        std::string error;
        ::openmldb::sdk::SplitLineWithDelimiterForStrings(line, options_parse.GetDelimiter(), &cols,
                                                          options_parse.GetQuote());
        auto ret = InsertOneRow(database, insert_placeholder, str_cols_idx, options_parse.GetNullValue(), cols);
        if (!ret.IsOK()) {
            return {::hybridse::common::StatusCode::kCmdError, "line [" + line + "] insert failed, " + ret.msg};
        }
        ++i;
    } while (std::getline(file, line));
    return {0, "Load " + std::to_string(i) + " rows"};
}

hybridse::sdk::Status SQLClusterRouter::InsertOneRow(const std::string& database, const std::string& insert_placeholder,
                                                     const std::vector<int>& str_col_idx, const std::string& null_value,
                                                     const std::vector<std::string>& cols) {
    if (cols.empty()) {
        return {::hybridse::common::StatusCode::kCmdError, "cols is empty"};
    }
    if (database.empty()) {
        return {::hybridse::common::StatusCode::kCmdError, "database is empty"};
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
        return {::hybridse::common::StatusCode::kCmdError, "col size mismatch"};
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
            return {::hybridse::common::StatusCode::kCmdError, "translate to insert row failed"};
        }
    }
    if (!ExecuteInsert(database, insert_placeholder, row, &status)) {
        return {::hybridse::common::StatusCode::kCmdError, "insert row failed"};
    }
    return {};
}

hybridse::sdk::Status SQLClusterRouter::HandleDelete(const std::string& db, const std::string& table_name,
                                                     const hybridse::node::ExprNode* condition) {
    if (db.empty() || table_name.empty()) {
        return {::hybridse::common::StatusCode::kCmdError, "database or table is empty"};
    }
    if (condition == nullptr) {
        return {::hybridse::common::StatusCode::kCmdError, "has not where condition"};
    }
    auto table_info = cluster_sdk_->GetTableInfo(db, table_name);
    if (!table_info) {
        return {::hybridse::common::StatusCode::kCmdError, "table " + table_name + " in db " + db + " does not exist"};
    }
    std::map<std::string, std::string> condition_map;
    std::map<std::string, int> parameter_map;
    auto binary_node = dynamic_cast<const hybridse::node::BinaryExpr*>(condition);
    auto col_map = schema::SchemaAdapter::GetColMap(*table_info);
    auto status = NodeAdapter::ParseExprNode(binary_node, col_map, &condition_map, &parameter_map);
    if (!status.IsOK()) {
        return status;
    }
    if (!parameter_map.empty()) {
        return {::hybridse::common::StatusCode::kCmdError, "unsupport placeholder in sql"};
    }
    std::string index_name;
    std::string pk;
    for (const auto& column_key : table_info->column_key()) {
        if (column_key.flag() != 0) {
            continue;
        }
        pk.clear();
        bool found = true;
        for (const auto& col : column_key.col_name()) {
            auto iter = condition_map.find(col);
            if (iter == condition_map.end()) {
                found = false;
                break;
            }
            if (!pk.empty()) {
                pk.append("|");
            }
            if (iter->second.empty()) {
                pk.append(hybridse::codec::EMPTY_STRING);
            } else {
                pk.append(iter->second);
            }
        }
        if (found) {
            index_name = column_key.index_name();
            break;
        }
    }
    if (index_name.empty()) {
        return {::hybridse::common::StatusCode::kCmdError, "no index col in delete sql"};
    }
    uint32_t pid = ::openmldb::base::hash64(pk) % table_info->table_partition_size();
    auto tablet = cluster_sdk_->GetTablet(db, table_name, pk);
    if (!tablet) {
        return {::hybridse::common::StatusCode::kCmdError, "cannot connect tablet"};
    }
    auto tablet_client = tablet->GetClient();
    if (!tablet_client) {
        return {::hybridse::common::StatusCode::kCmdError, "tablet client is null"};
    }
    std::string msg;
    if (!tablet_client->Delete(table_info->tid(), pid, pk, index_name, msg)) {
        return {::hybridse::common::StatusCode::kCmdError, msg};
    }
    return {};
}

bool SQLClusterRouter::ExecuteDelete(std::shared_ptr<SQLDeleteRow> row, hybridse::sdk::Status* status) {
    if (!row || !status) {
        return false;
    }
    const auto& db = row->GetDatabase();
    const auto& table_name = row->GetTableName();
    auto table_info = cluster_sdk_->GetTableInfo(db, table_name);
    if (!table_info) {
        *status = {::hybridse::common::StatusCode::kCmdError,
                   "table " + table_name + " in db " + db + " does not exist"};
        return false;
    }
    const auto& pk = row->GetValue();
    const auto& index_name = row->GetIndexName();
    uint32_t pid = ::openmldb::base::hash64(pk) % table_info->table_partition_size();
    auto tablet = cluster_sdk_->GetTablet(db, table_name, pk);
    if (!tablet) {
        *status = {::hybridse::common::StatusCode::kCmdError, "cannot connect tablet"};
        return false;
    }
    auto tablet_client = tablet->GetClient();
    if (!tablet_client) {
        *status = {::hybridse::common::StatusCode::kCmdError, "tablet client is null"};
    }
    std::string msg;
    if (!tablet_client->Delete(table_info->tid(), pid, pk, index_name, msg)) {
        *status = {::hybridse::common::StatusCode::kCmdError, msg};
        return false;
    }
    *status = {};
    return true;
}

hybridse::sdk::Status SQLClusterRouter::HandleCreateFunction(const hybridse::node::CreateFunctionPlanNode* node) {
    if (node == nullptr) {
        return {::hybridse::common::StatusCode::kCmdError, "illegal create function statement"};
    }
    auto fun = std::make_shared<openmldb::common::ExternalFun>();
    fun->set_name(node->Name());
    auto type_node = dynamic_cast<const ::hybridse::node::TypeNode*>(node->GetReturnType());
    if (type_node == nullptr) {
        return {::hybridse::common::StatusCode::kCmdError, "illegal create function statement"};
    }
    openmldb::type::DataType data_type;
    if (!::openmldb::schema::SchemaAdapter::ConvertType(type_node->base(), &data_type)) {
        return {::hybridse::common::StatusCode::kCmdError, "illegal return type"};
    }
    fun->set_return_type(data_type);
    for (const auto arg_node : node->GetArgsType()) {
        type_node = dynamic_cast<const ::hybridse::node::TypeNode*>(arg_node);
        if (type_node == nullptr) {
            return {::hybridse::common::StatusCode::kCmdError, "illegal create function statement"};
        }
        if (!::openmldb::schema::SchemaAdapter::ConvertType(type_node->base(), &data_type)) {
            return {::hybridse::common::StatusCode::kCmdError, "illegal argument type"};
        }
        fun->add_arg_type(data_type);
    }
    if (node->IsAggregate()) {
        return {::hybridse::common::StatusCode::kCmdError, "unsupport udaf function"};
    }
    fun->set_is_aggregate(node->IsAggregate());
    auto option = node->Options();
    if (!option || option->find("FILE") == option->end()) {
        return {::hybridse::common::StatusCode::kCmdError, "missing FILE option"};
    }
    fun->set_file((*option)["FILE"]->GetExprString());
    if (cluster_sdk_->IsClusterMode()) {
        auto taskmanager_client = cluster_sdk_->GetTaskManagerClient();
        if (taskmanager_client) {
            auto ret = taskmanager_client->CreateFunction(fun, GetJobTimeout());
            if (!ret.OK()) {
                return {::hybridse::common::StatusCode::kCmdError, ret.msg};
            }
        }
    }
    auto ns = cluster_sdk_->GetNsClient();
    auto ret = ns->CreateFunction(*fun);
    if (!ret.OK()) {
        return {::hybridse::common::StatusCode::kCmdError, ret.msg};
    }
    cluster_sdk_->RegisterExternalFun(fun);
    return {};
}

hybridse::sdk::Status SQLClusterRouter::HandleDeploy(const std::string& db,
                                                     const hybridse::node::DeployPlanNode* deploy_node) {
    if (db.empty()) {
        return {::hybridse::common::StatusCode::kCmdError, "database is empty"};
    }
    if (deploy_node == nullptr) {
        return {::hybridse::common::StatusCode::kCmdError, "illegal deploy statement"};
    }

    std::string select_sql = deploy_node->StmtStr() + ";";
    hybridse::vm::ExplainOutput explain_output;
    hybridse::base::Status sql_status;
    if (!cluster_sdk_->GetEngine()->Explain(select_sql, db, hybridse::vm::kMockRequestMode, &explain_output,
                                            &sql_status)) {
        return {::hybridse::common::StatusCode::kCmdError, sql_status.GetMsg(), sql_status.GetTraces()};
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
        return {::hybridse::common::StatusCode::kCmdError, "convert schema failed"};
    }

    std::set<std::pair<std::string, std::string>> table_pair;
    if (!cluster_sdk_->GetEngine()->GetDependentTables(select_sql, db, ::hybridse::vm::kBatchMode, &table_pair,
                                                       sql_status)) {
        return {::hybridse::common::StatusCode::kCmdError, "get dependent table failed"};
    }
    std::set<std::string> db_set;
    for (auto& table : table_pair) {
        db_set.insert(table.first);
        auto db_table = sp_info.add_tables();
        db_table->set_db_name(table.first);
        db_table->set_table_name(table.second);
    }
    if (db_set.size() > 1) {
        return {::hybridse::common::StatusCode::kCmdError, "unsupport multi database"};
    }
    std::stringstream str_stream;
    str_stream << "CREATE PROCEDURE " << deploy_node->Name() << " (";
    for (int idx = 0; idx < input_schema->size(); idx++) {
        const auto& col = input_schema->Get(idx);
        auto it = codec::DATA_TYPE_STR_MAP.find(col.data_type());
        if (it == codec::DATA_TYPE_STR_MAP.end()) {
            return {::hybridse::common::StatusCode::kCmdError, "illegal data type"};
        }
        str_stream << col.name() << " " << it->second;
        if (idx != input_schema->size() - 1) {
            str_stream << ", ";
        }
    }
    str_stream << ") BEGIN " << select_sql << " END;";

    sp_info.set_sql(str_stream.str());
    sp_info.set_type(::openmldb::type::ProcedureType::kReqDeployment);

    auto index_status = HandleIndex(db, table_pair, select_sql);
    if (!index_status.IsOK()) {
        return index_status;
    }

    auto lw_status = HandleLongWindows(deploy_node, table_pair, select_sql);
    if (!lw_status.IsOK()) {
        return lw_status;
    }
    for (const auto& o : *deploy_node->Options()) {
        auto option = sp_info.add_options();
        option->set_name(o.first);
        option->mutable_value()->set_value(o.second->GetExprString());
    }

    auto status = cluster_sdk_->GetNsClient()->CreateProcedure(sp_info, options_->request_timeout);
    if (!status.OK()) {
        return {::hybridse::common::StatusCode::kCmdError, status.msg};
    }
    return {};
}

hybridse::sdk::Status SQLClusterRouter::HandleIndex(const std::string& db,
                                                    const std::set<std::pair<std::string, std::string>>& table_pair,
                                                    const std::string& select_sql) {
    // extract index from sql
    std::vector<::openmldb::nameserver::TableInfo> tables;
    auto ns = cluster_sdk_->GetNsClient();
    // TODO(denglong): support multi db
    auto ret = ns->ShowDBTable(db, &tables);
    if (!ret.OK()) {
        return {::hybridse::common::StatusCode::kCmdError, "get table failed " + ret.msg};
    }
    std::map<std::string, ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnDesc>> table_schema_map;
    std::map<std::string, ::openmldb::nameserver::TableInfo> table_map;
    for (const auto& table : tables) {
        for (const auto& pair : table_pair) {
            if (table.name() == pair.second) {
                table_schema_map.emplace(table.name(), table.column_desc());
                table_map.emplace(table.name(), table);
                break;
            }
        }
    }
    auto index_map = base::DDLParser::ExtractIndexes(select_sql, table_schema_map);
    std::map<std::string, std::vector<::openmldb::common::ColumnKey>> new_index_map;
    auto get_index_status = GetNewIndex(table_map, index_map, &new_index_map);
    if (!get_index_status.IsOK()) {
        return get_index_status;
    }

    auto add_index_status = AddNewIndex(db, table_map, new_index_map);
    if (!add_index_status.IsOK()) {
        return add_index_status;
    }
    return {};
}

hybridse::sdk::Status SQLClusterRouter::GetNewIndex(
    const std::map<std::string, ::openmldb::nameserver::TableInfo>& table_map,
    const std::map<std::string, std::vector<::openmldb::common::ColumnKey>>& index_map,
    std::map<std::string, std::vector<::openmldb::common::ColumnKey>>* new_index_map) {
    auto ns = cluster_sdk_->GetNsClient();
    for (auto& kv : index_map) {
        std::string table_name = kv.first;
        std::vector<::openmldb::common::ColumnKey> extract_column_keys = kv.second;
        auto it = table_map.find(table_name);
        if (it == table_map.end()) {
            return {::hybridse::common::StatusCode::kCmdError, "table " + table_name + "is not exist"};
        }
        auto& table = it->second;
        std::set<std::string> col_set;
        for (const auto& column_desc : table.column_desc()) {
            col_set.insert(column_desc.name());
        }
        std::map<std::string, ::openmldb::common::ColumnKey> id_columnkey_map;
        for (const auto& column_key : table.column_key()) {
            if (id_columnkey_map.find(openmldb::schema::IndexUtil::GetIDStr(column_key)) != id_columnkey_map.end()) {
                LOG(WARNING) << "exist two indexes which are the same id "
                             << openmldb::schema::IndexUtil::GetIDStr(column_key) << " in table " << table_name;
            }
            id_columnkey_map.emplace(openmldb::schema::IndexUtil::GetIDStr(column_key), column_key);
        }
        int cur_index_num = table.column_key_size();
        int add_index_num = 0;
        std::vector<::openmldb::common::ColumnKey> new_indexs;
        for (auto& column_key : extract_column_keys) {
            if (!column_key.has_ttl()) {
                return {::hybridse::common::StatusCode::kCmdError, "table " + table_name + " index has not ttl"};
            }
            if (!column_key.ts_name().empty() && col_set.count(column_key.ts_name()) == 0) {
                return {::hybridse::common::StatusCode::kCmdError,
                        "ts col " + column_key.ts_name() + " is not exist in table " + table_name};
            }
            for (const auto& col : column_key.col_name()) {
                if (col_set.count(col) == 0) {
                    return {::hybridse::common::StatusCode::kCmdError,
                            "col " + col + " is not exist in table " + table_name};
                }
            }
            std::string index_id = openmldb::schema::IndexUtil::GetIDStr(column_key);
            // index exist, if type match && new ttl greater than old ttl && old ttl != 0, update ttl, else skip
            auto it = id_columnkey_map.find(index_id);
            if (it != id_columnkey_map.end()) {
                auto& old_column_key = it->second;
                // type mismatch, return here and deploy failed
                if (old_column_key.ttl().ttl_type() != column_key.ttl().ttl_type()) {
                    return {::hybridse::common::StatusCode::kCmdError,
                            "new ttl type " + ::openmldb::type::TTLType_Name(column_key.ttl().ttl_type()) +
                                " doesn't match the old ttl type " +
                                ::openmldb::type::TTLType_Name(old_column_key.ttl().ttl_type())};
                } else {
                    // type match, if old ttl == 0, won't update
                    ::openmldb::type::TTLType type = column_key.ttl().ttl_type();
                    uint64_t old_abs_ttl = 0;
                    uint64_t old_lat_ttl = 0;
                    uint64_t new_abs_ttl = 0;
                    uint64_t new_lat_ttl = 0;
                    if (type == ::openmldb::type::TTLType::kAbsoluteTime) {
                        if (old_column_key.ttl().abs_ttl() != 0) {
                            old_abs_ttl = old_column_key.ttl().abs_ttl();
                            new_abs_ttl = column_key.ttl().abs_ttl();
                        }
                    } else if (type == ::openmldb::type::TTLType::kLatestTime) {
                        if (old_column_key.ttl().lat_ttl() != 0) {
                            old_lat_ttl = old_column_key.ttl().lat_ttl();
                            new_lat_ttl = column_key.ttl().lat_ttl();
                        }
                    } else {
                        // absandlat && absorlat should check abs_ttl and lat_ttl
                        if (old_column_key.ttl().abs_ttl() != 0) {
                            old_abs_ttl = old_column_key.ttl().abs_ttl();
                            new_abs_ttl = column_key.ttl().abs_ttl();
                        }
                        if (old_column_key.ttl().lat_ttl() != 0) {
                            old_lat_ttl = old_column_key.ttl().lat_ttl();
                            new_lat_ttl = column_key.ttl().lat_ttl();
                        }
                    }
                    if (new_abs_ttl > old_abs_ttl || new_lat_ttl > old_lat_ttl) {
                        // update ttl
                        auto ns_ptr = cluster_sdk_->GetNsClient();
                        std::string err;
                        if (!ns_ptr->UpdateTTL(table_name, type, new_abs_ttl, new_lat_ttl, old_column_key.index_name(),
                                               err)) {
                            return {::hybridse::common::StatusCode::kCmdError, "update ttl failed"};
                        }
                    }
                }
            } else {
                column_key.set_index_name("INDEX_" + std::to_string(cur_index_num + add_index_num) + "_" +
                                          std::to_string(::baidu::common::timer::now_time()));
                add_index_num++;
                new_indexs.emplace_back(column_key);
            }
        }
        if (!new_indexs.empty()) {
            if (cluster_sdk_->IsClusterMode()) {
                uint64_t record_cnt = 0;
                for (int idx = 0; idx < table.table_partition_size(); idx++) {
                    record_cnt += table.table_partition(idx).record_cnt();
                }
                if (record_cnt > 0) {
                    return {::hybridse::common::StatusCode::kCmdError,
                            "table " + table_name +
                                " has online data, cannot deploy. please drop this table and create a new one"};
                }
            }
            new_index_map->emplace(table_name, std::move(new_indexs));
        }
    }
    return {};
}

hybridse::sdk::Status SQLClusterRouter::AddNewIndex(
    const std::string& db, const std::map<std::string, ::openmldb::nameserver::TableInfo>& table_map,
    const std::map<std::string, std::vector<::openmldb::common::ColumnKey>>& new_index_map) {
    auto ns = cluster_sdk_->GetNsClient();
    if (cluster_sdk_->IsClusterMode()) {
        for (auto& kv : new_index_map) {
            auto status = ns->AddMultiIndex(db, kv.first, kv.second);
            if (!status.OK()) {
                return {::hybridse::common::StatusCode::kCmdError,
                        "table [" + db + "." + kv.first + "] add index failed. " + status.msg};
            }
        }
    } else {
        auto tablet_accessor = cluster_sdk_->GetTablet();
        if (!tablet_accessor) {
            return {::hybridse::common::StatusCode::kCmdError, "cannot connect tablet"};
        }
        auto tablet_client = tablet_accessor->GetClient();
        if (!tablet_client) {
            return {::hybridse::common::StatusCode::kCmdError, "tablet client is null"};
        }
        // add index
        for (auto& kv : new_index_map) {
            auto it = table_map.find(kv.first);
            for (auto& column_key : kv.second) {
                std::vector<openmldb::common::ColumnDesc> cols;
                for (const auto& col_name : column_key.col_name()) {
                    for (const auto& col : it->second.column_desc()) {
                        if (col.name() == col_name) {
                            cols.push_back(col);
                            break;
                        }
                    }
                }
                std::string msg;
                if (!ns->AddIndex(kv.first, column_key, &cols, msg)) {
                    return {::hybridse::common::StatusCode::kCmdError, "table " + kv.first + " add index failed"};
                }
            }
        }
        // load new index data to table
        for (auto& kv : new_index_map) {
            auto it = table_map.find(kv.first);
            if (it == table_map.end()) {
                continue;
            }
            uint32_t tid = it->second.tid();
            uint32_t pid = 0;
            if (!tablet_client->ExtractMultiIndexData(tid, pid, it->second.table_partition_size(), kv.second)) {
                return {::hybridse::common::StatusCode::kCmdError, "table " + kv.first + " load data failed"};
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
            return {base::ReturnCode::kError, "unsupport multi tables with long window options"};
        }
        std::string base_db = table_pair.begin()->first;
        std::string base_table = table_pair.begin()->second;
        std::vector<std::string> windows;
        boost::split(windows, long_window_param, boost::is_any_of(","));
        for (auto& window : windows) {
            std::vector<std::string> window_info;
            boost::split(window_info, window, boost::is_any_of(":"));

            if (window_info.size() == 2) {
                long_window_map[window_info[0]] = window_info[1];
            } else if (window_info.size() == 1) {
                long_window_map[window_info[0]] = FLAGS_bucket_size;
            } else {
                return {base::ReturnCode::kError, "illegal long window format"};
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
            return {base::ReturnCode::kError, "long_windows option doesn't match window in sql"};
        }
        auto ns_client = cluster_sdk_->GetNsClient();
        std::vector<::openmldb::nameserver::TableInfo> tables;
        std::string msg;
        ns_client->ShowTable(base_table, base_db, false, tables, msg);
        if (tables.size() != 1) {
            return {base::ReturnCode::kError, "base table not found"};
        }
        std::string meta_db = openmldb::nameserver::INTERNAL_DB;
        std::string meta_table = openmldb::nameserver::PRE_AGG_META_NAME;
        std::string aggr_db = openmldb::nameserver::PRE_AGG_DB;
        for (const auto& lw : long_window_infos) {
            if (absl::EndsWithIgnoreCase(lw.aggr_func_, "_where")) {
                // TOOD(ace): *_where op only support for memory base table
                if (tables[0].storage_mode() != common::StorageMode::kMemory) {
                    return {base::ReturnCode::kError,
                            absl::StrCat(lw.aggr_func_, " only support over memory base table")};
                }

                // TODO(#2313): *_where for rows bucket should support later
                if (openmldb::base::IsNumber(long_window_map.at(lw.window_name_))) {
                    return {base::ReturnCode::kError, absl::StrCat("unsupport *_where op (", lw.aggr_func_,
                                                                   ") for rows bucket type long window")};
                }

                // unsupport filter col of date/timestamp
                for (size_t i = 0; i < tables[0].column_desc_size(); ++i) {
                    if (lw.filter_col_ == tables[0].column_desc(i).name()) {
                        auto type = tables[0].column_desc(i).data_type();
                        if (type == type::DataType::kDate || type == type::DataType::kTimestamp) {
                            return {
                                base::ReturnCode::kError,
                                absl::Substitute("unsupport date or timestamp as filer column ($0)", lw.filter_col_)};
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
                return {base::ReturnCode::kError, "insert pre-aggr meta failed"};
            }

            // create pre-aggr table
            auto create_status = CreatePreAggrTable(aggr_db, aggr_table, lw, tables[0], ns_client);
            if (!create_status.OK()) {
                return {base::ReturnCode::kError, "create pre-aggr table failed"};
            }

            // create aggregator
            std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
            bool ret = cluster_sdk_->GetTablet(base_db, base_table, &tablets);
            if (!ret || tablets.empty()) {
                return {base::ReturnCode::kError, "get tablets failed"};
            }
            auto base_table_info = cluster_sdk_->GetTableInfo(base_db, base_table);
            if (!base_table_info) {
                return {base::ReturnCode::kError, "get table info failed"};
            }
            auto aggr_id = cluster_sdk_->GetTableId(aggr_db, aggr_table);
            ::openmldb::api::TableMeta base_table_meta;
            base_table_meta.set_db(base_table_info->db());
            base_table_meta.set_name(base_table_info->name());
            base_table_meta.set_tid(static_cast<::google::protobuf::int32>(base_table_info->tid()));
            base_table_meta.set_format_version(base_table_info->format_version());
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
                return {base::ReturnCode::kError, "index that associate to aggregator not found"};
            }
            for (uint32_t pid = 0; pid < tablets.size(); ++pid) {
                auto tablet_client = tablets[pid]->GetClient();
                if (tablet_client == nullptr) {
                    return {base::ReturnCode::kError, "get tablet client failed"};
                }
                base_table_meta.set_pid(pid);
                if (!tablet_client->CreateAggregator(base_table_meta, aggr_id, pid, index_pos, lw)) {
                    return {base::ReturnCode::kError, "create aggregator failed"};
                }
            }
        }
    }
    return {};
}

bool SQLClusterRouter::CheckPreAggrTableExist(const std::string& base_table, const std::string& base_db,
                                              const openmldb::base::LongWindowInfo& lw,
                                              ::hybridse::sdk::Status* status) {
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
        return false;
    }

    // Check if the bucket_size equal to the one in meta table with the same meta info.
    // Currently, we create pre-aggregated table for pre-aggr meta info that have different
    // bucket_size but the same other meta info.
    while (rs->Next()) {
        std::string exist_bucket_size;
        rs->GetString(0, &exist_bucket_size);
        if (exist_bucket_size == lw.bucket_size_) {
            LOG(INFO) << "Pre-aggregated table with same meta info already exist: " << meta_info;
            return true;
        }
    }

    return false;
}

static const std::initializer_list<std::string> GetComponetSchema() {
    static const std::initializer_list<std::string> schema = {"Endpoint", "Role", "Connect_time", "Status", "Ns_role"};
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
    DCHECK(status != nullptr);
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
    DCHECK(status != nullptr);

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

    std::vector<std::vector<std::string>> data(endpoint_map.size(), std::vector<std::string>(schema.size(), ""));

    auto begin = endpoint_map.cbegin();
    for (size_t i = 0; i < endpoint_map.size(); i++) {
        auto it = std::next(begin, i);
        // endpoint
        data[i][0] = it->first;
        // role
        data[i][1] = "nameserver";

        // connect time
        data[i][2] = std::to_string(it->second);

        // status
        // offlined nameserver won't register in zookeeper, so there is only online
        data[i][3] = "online";

        // ns_role
        // NSs runs as mater/standby mode
        if (i == 0) {
            data[i][4] = "master";
        } else {
            data[i][4] = "standby";
        }
    }

    return ResultSetSQL::MakeResultSet(schema, data, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteShowTablets(hybridse::sdk::Status* status) {
    DCHECK(status != nullptr);
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
    DCHECK(status != nullptr);

    auto zk_client = cluster_sdk_->GetZkClient();
    if (!cluster_sdk_->IsClusterMode() || zk_client == nullptr) {
        // standalone mode
        status->code = hybridse::common::kUnSupport;
        status->msg = "show taskmanagers not supported in standalone mode";
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
    static const std::initializer_list<std::string> schema = {
        "Table_id",         "Table_name",     "Database_name",    "Storage_type",      "Rows",
        "Memory_data_size", "Disk_data_size", "Partition",        "Partition_unalive", "Replica",
        "Offline_path",     "Offline_format", "Offline_deep_copy"};
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
// - partition_unalive: partition number that is unalive
// - Replica: replica number
// - Offline_path: data path for offline data
// - Offline_format: format for offline data
// - Offline_deep_copy: deep copy option for offline data
//
// if db is empty:
//   show table status in all databases except hidden databases
// else: show table status in current database, include hidden database
std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteShowTableStatus(const std::string& db,
                                                                                   hybridse::sdk::Status* status) {
    // NOTE: cluster_sdk_->GetTables(db) seems not accurate, query directly
    std::vector<nameserver::TableInfo> tables;
    std::string msg;
    cluster_sdk_->GetNsClient()->ShowTable("", "", true, tables, msg);

    std::vector<std::vector<std::string>> data;
    data.reserve(tables.size());

    std::for_each(tables.cbegin(), tables.cend(), [&data, &db](const nameserver::TableInfo& tinfo) {
        if (!db.empty()) {
            // rule 1: selected a db, show tables only inside the db
            if (db != tinfo.db()) {
                return;
            }
        } else if (nameserver::IsHiddenDb(tinfo.db())) {
            // rule 2: if no db selected, show all tables except those in hidden db
            return;
        }

        auto tid = tinfo.tid();
        auto table_name = tinfo.name();
        auto db = tinfo.db();
        auto& inner_storage_mode = StorageMode_Name(tinfo.storage_mode());
        std::string storage_type = absl::AsciiStrToLower(absl::StripPrefix(inner_storage_mode, "k"));

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
        }

        std::string offline_path = "NULL", offline_format = "NULL", offline_deep_copy = "NULL";
        if (tinfo.has_offline_table_info()) {
            offline_path = tinfo.offline_table_info().path();
            offline_format = tinfo.offline_table_info().format();
            offline_deep_copy = std::to_string(tinfo.offline_table_info().deep_copy());
        }

        data.push_back({std::to_string(tid), table_name, db, storage_type, std::to_string(rows),
                        std::to_string(mem_bytes), std::to_string(disk_bytes), std::to_string(partition_num),
                        std::to_string(partition_unalive), std::to_string(replica_num), offline_path, offline_format,
                        offline_deep_copy});
    });

    // TODO(#1456): rich schema result set, and pretty-print numberic values (e.g timestamp) in cli
    return ResultSetSQL::MakeResultSet(GetTableStatusSchema(), data, status);
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

}  // namespace sdk
}  // namespace openmldb
