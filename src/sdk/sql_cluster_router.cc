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

#include <memory>
#include <string>
#include <utility>

#include "boost/none.hpp"
#include "brpc/channel.h"
#include "common/timer.h"
#include "glog/logging.h"
#include "plan/plan_api.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "schema/schema_adapter.h"
#include "sdk/base.h"
#include "sdk/base_impl.h"
#include "sdk/batch_request_result_set_sql.h"
#include "sdk/node_adapter.h"
#include "sdk/result_set_sql.h"

DECLARE_int32(request_timeout_ms);

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
    : options_(options), cluster_sdk_(nullptr), mu_(), rand_(::baidu::common::timer::now_time()) {}

SQLClusterRouter::SQLClusterRouter(DBSDK* sdk)
    : options_(), cluster_sdk_(sdk), mu_(), rand_(::baidu::common::timer::now_time()) {}

SQLClusterRouter::~SQLClusterRouter() { delete cluster_sdk_; }

bool SQLClusterRouter::Init() {
    if (cluster_sdk_ == nullptr) {
        ClusterOptions coptions;
        coptions.zk_cluster = options_.zk_cluster;
        coptions.zk_path = options_.zk_path;
        coptions.session_timeout = options_.session_timeout;
        cluster_sdk_ = new ClusterSDK(coptions);
        bool ok = cluster_sdk_->Init();
        if (!ok) {
            LOG(WARNING) << "fail to init cluster sdk";
            return false;
        }
    }
    return true;
}

std::shared_ptr<SQLRequestRow> SQLClusterRouter::GetRequestRow(const std::string& db, const std::string& sql,
                                                               ::hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return {};
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kRequestMode);
    std::set<std::string> col_set;
    if (cache) {
        status->code = 0;
        const std::string& router_col = cache->router.GetRouterCol();
        if (!router_col.empty()) {
            col_set.insert(router_col);
        }
        return std::make_shared<SQLRequestRow>(cache->column_schema, col_set);
    }
    ::hybridse::vm::ExplainOutput explain;
    ::hybridse::base::Status vm_status;

    bool ok = cluster_sdk_->GetEngine()->Explain(sql, db, ::hybridse::vm::kRequestMode, &explain, &vm_status);
    if (!ok) {
        status->code = -1;
        status->msg = vm_status.msg;
        LOG(WARNING) << "fail to explain sql " << sql << " for " << vm_status.msg;
        return {};
    }
    std::shared_ptr<::hybridse::sdk::SchemaImpl> schema =
        std::make_shared<::hybridse::sdk::SchemaImpl>(explain.input_schema);
    SetCache(db, sql, hybridse::vm::kRequestMode, std::make_shared<SQLCache>(schema, explain.router));
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

std::shared_ptr<SQLInsertRow> SQLClusterRouter::GetInsertRow(const std::string& db, const std::string& sql,
                                                             ::hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return {};
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    if (cache) {
        status->code = 0;
        return std::make_shared<SQLInsertRow>(cache->table_info, cache->column_schema, cache->default_map,
                                              cache->str_length);
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map, &str_length)) {
        status->code = 1;
        LOG(WARNING) << "get insert information failed";
        return {};
    }
    cache = std::make_shared<SQLCache>(table_info, default_map, str_length, 0);
    SetCache(db, sql, hybridse::vm::kBatchMode, cache);
    return std::make_shared<SQLInsertRow>(table_info, cache->column_schema, default_map, str_length);
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
    *table_info = cluster_sdk_->GetTableInfo(db, insert_stmt->table_name_);
    if (!(*table_info)) {
        status->msg = "table with name " + insert_stmt->table_name_ + " in db " + db + " does not exist";
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
                                     DefaultValueMap* default_map, uint32_t* str_length) {
    if (status == NULL || table_info == NULL || default_map == NULL || str_length == NULL) {
        LOG(WARNING) << "insert info is null" << sql;
        return false;
    }
    ::hybridse::node::NodeManager nm;
    ::hybridse::plan::PlanNodeList plans;
    bool ok = GetSQLPlan(sql, &nm, &plans);
    if (!ok || plans.size() == 0) {
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
    if (insert_stmt == NULL) {
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
    *default_map = GetDefaultMap(*table_info, column_map,
                                 dynamic_cast<::hybridse::node::ExprListNode*>(insert_stmt->values_[0]), str_length);
    if (!(*default_map)) {
        status->msg = "get default value map of " + sql + " failed";
        LOG(WARNING) << status->msg;
        return false;
    }
    return true;
}

DefaultValueMap SQLClusterRouter::GetDefaultMap(std::shared_ptr<::openmldb::nameserver::TableInfo> table_info,
                                                const std::map<uint32_t, uint32_t>& column_map,
                                                ::hybridse::node::ExprListNode* row, uint32_t* str_length) {
    if (row == nullptr || str_length == nullptr) {
        LOG(WARNING) << "row or str length is NULL";
        return {};
    }
    DefaultValueMap default_map(new std::map<uint32_t, std::shared_ptr<::hybridse::node::ConstNode>>());
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
                default_map->insert(std::make_pair(idx, std::make_shared<::hybridse::node::ConstNode>()));
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
            ::hybridse::node::ConstNode* primary = dynamic_cast<::hybridse::node::ConstNode*>(row->children_.at(i));
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
            auto cached_info = value.value()->table_info;
            if (cached_info) {
                auto current_info = cluster_sdk_->GetTableInfo(db, cached_info->name());
                if (!current_info || cached_info->tid() != current_info->tid()) {
                    // just leave, this invalid value will be updated by SetCache()
                    return {};
                }
            }
            return value.value();
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
        decltype(it->second)::mapped_type value(options_.max_sql_cache_size);
        it->second.insert(std::make_pair(engine_mode, value));
        cache_it = it->second.find(engine_mode);
    }
    cache_it->second.upsert(sql, router_cache);
}

std::shared_ptr<SQLInsertRows> SQLClusterRouter::GetInsertRows(const std::string& db, const std::string& sql,
                                                               ::hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return {};
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    if (cache) {
        status->code = 0;
        return std::make_shared<SQLInsertRows>(cache->table_info, cache->column_schema, cache->default_map,
                                               cache->str_length);
    }
    std::shared_ptr<::openmldb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map, &str_length)) {
        return {};
    }
    cache = std::make_shared<SQLCache>(table_info, default_map, str_length);
    SetCache(db, sql, hybridse::vm::kBatchMode, cache);
    return std::make_shared<SQLInsertRows>(table_info, cache->column_schema, default_map, str_length);
}

bool SQLClusterRouter::ExecuteDDL(const std::string& db, const std::string& sql, hybridse::sdk::Status* status) {
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        status->code = -1;
        status->msg = "no nameserver exist";
        LOG(WARNING) << status->msg;
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
        ret = HandleSQLCmd(dynamic_cast<hybridse::node::CmdPlanNode*>(node), db, ns_ptr);
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
std::vector<std::string> SQLClusterRouter::GetAllTables() {
    return cluster_sdk_->GetAllTables();
}
 
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
        LOG(WARNING) << status->msg;
        return false;
    }

    // RefreshCatalog to avoid getting out-of-date table info
    if (!RefreshCatalog()) {
        status->msg = "Fail to refresh catalog";
        status->code = -1;
        LOG(WARNING) << status->msg;
        return false;
    }

    auto tableInfo = GetTableInfo(db, table);
    // Check offline table info first
    if (tableInfo.has_offline_table_info()) {
        auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
        if (!taskmanager_client_ptr) {
            status->msg = "no TaskManager exist";
            status->code = -1;
            LOG(WARNING) << status->msg;
            return false;
        }

        ::openmldb::base::Status rpcStatus = taskmanager_client_ptr->DropOfflineTable(db, table);
        if (rpcStatus.code != 0) {
            status->msg = rpcStatus.msg;
            status->code = rpcStatus.code;
            LOG(WARNING) << status->msg;
            return false;
        }
    }

    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        status->msg = "no nameserver exist";
        status->code = -2;
        LOG(WARNING) << status->msg;
        return false;
    }
    std::string err;

    bool ok = ns_ptr->DropTable(db, table, err);
    if (!ok) {
        status->msg = "fail to drop db " + db + " for error " + err;
        status->code = -2;
        LOG(WARNING) << status->msg;
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
                                                        const std::shared_ptr<SQLRequestRow>& parameter) {
    ::hybridse::codec::Schema parameter_schema_raw;
    if (parameter) {
        for (int i = 0; i < parameter->GetSchema()->GetColumnCnt(); i++) {
            auto column = parameter_schema_raw.Add();
            hybridse::type::Type hybridse_type;
            if (!openmldb::schema::SchemaAdapter::ConvertType(parameter->GetSchema()->GetColumnType(i),
                                                              &hybridse_type)) {
                LOG(WARNING) << "Invalid parameter type ";
                return {};
            }
            column->set_type(hybridse_type);
        }
    }
    auto cache = GetCache(db, sql, engine_mode);
    auto parameter_schema = std::make_shared<::hybridse::sdk::SchemaImpl>(parameter_schema_raw);
    if (cache && cache->IsCompatibleCache(parameter_schema)) {
        cache.reset();
    }
    if (!cache) {
        ::hybridse::vm::ExplainOutput explain;
        ::hybridse::base::Status vm_status;
        if (cluster_sdk_->GetEngine()->Explain(sql, db, engine_mode, parameter_schema_raw, &explain, &vm_status)) {
            std::shared_ptr<::hybridse::sdk::SchemaImpl> schema;
            if (!explain.input_schema.empty()) {
                schema = std::make_shared<::hybridse::sdk::SchemaImpl>(explain.input_schema);
            } else {
                const std::string& main_table = explain.router.GetMainTable();
                const std::string& main_db = explain.router.GetMainDb().empty() ? db : explain.router.GetMainDb();
                auto table_info = cluster_sdk_->GetTableInfo(main_db, main_table);
                ::hybridse::codec::Schema raw_schema;
                if (table_info &&
                    ::openmldb::schema::SchemaAdapter::ConvertSchema(table_info->column_desc(), &raw_schema)) {
                    schema = std::make_shared<::hybridse::sdk::SchemaImpl>(raw_schema);
                }
            }
            cache = std::make_shared<SQLCache>(schema, parameter_schema, explain.router, explain.limit_cnt);
            SetCache(db, sql, engine_mode, cache);
        }
    }
    return cache;
}
std::shared_ptr<::openmldb::client::TabletClient> SQLClusterRouter::GetTabletClient(
    const std::string& db, const std::string& sql, const ::hybridse::vm::EngineMode engine_mode,
    const std::shared_ptr<SQLRequestRow>& row) {
    return GetTabletClient(db, sql, engine_mode, row, std::shared_ptr<openmldb::sdk::SQLRequestRow>());
}
std::shared_ptr<::openmldb::client::TabletClient> SQLClusterRouter::GetTabletClient(
    const std::string& db, const std::string& sql, const ::hybridse::vm::EngineMode engine_mode,
    const std::shared_ptr<SQLRequestRow>& row, const std::shared_ptr<openmldb::sdk::SQLRequestRow>& parameter) {
    auto cache = GetSQLCache(db, sql, engine_mode, parameter);
    std::shared_ptr<::openmldb::catalog::TabletAccessor> tablet;
    if (cache) {
        const std::string& col = cache->router.GetRouterCol();
        const std::string& main_table = cache->router.GetMainTable();
        const std::string main_db = cache->router.GetMainDb().empty() ? db : cache->router.GetMainDb();
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
    if (!tablet) {
        tablet = cluster_sdk_->GetTablet();
    }
    if (!tablet) {
        LOG(WARNING) << "fail to get tablet";
        return {};
    }
    return tablet->GetClient();
}

// Get clients when online batch query in Cluster OpenMLDB
bool SQLClusterRouter::GetTabletClientsForClusterOnlineBatchQuery(
    const std::string& db, const std::string& sql, const std::shared_ptr<SQLRequestRow>& parameter,
    std::unordered_set<std::shared_ptr<::openmldb::client::TabletClient>>& clients) {
    auto cache = GetSQLCache(db, sql, hybridse::vm::kBatchMode, parameter);
    if (cache) {
        const std::string& main_table = cache->router.GetMainTable();
        const std::string main_db = cache->router.GetMainDb().empty() ? db : cache->router.GetMainDb();
        if (!main_table.empty()) {
            DLOG(INFO) << "get main table" << main_table;
            std::string val;
            std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;

            if (!cluster_sdk_->GetTablet(main_db, main_table, &tablets)) {
                LOG(WARNING) << "ERROR: Fail to get tablet clients for " << main_db << "." << main_table;
                return false;
            }

            for (auto tablet : tablets) {
                clients.insert(tablet->GetClient());
            }
            return true;
        } else {
            auto tablet = cluster_sdk_->GetTablet();
            if (!tablet) {
                return false;
            }
            clients.insert(tablet->GetClient());
            return true;
        }
    }

    LOG(WARNING) << "fail to get tablet";
    return false;
}
std::shared_ptr<TableReader> SQLClusterRouter::GetTableReader() {
    std::shared_ptr<TableReaderImpl> reader(new TableReaderImpl(cluster_sdk_));
    return reader;
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
    cntl->set_timeout_ms(options_.request_timeout);
    auto response = std::make_shared<::openmldb::api::QueryResponse>();
    auto client = GetTabletClient(db, sql, hybridse::vm::kRequestMode, row);
    if (!client) {
        status->msg = "not tablet found";
        return {};
    }
    if (!client->Query(db, sql, row->GetRow(), cntl.get(), response.get(), options_.enable_debug)) {
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
std::shared_ptr<::hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQL(const std::string& db, const std::string& sql,
                                                                         ::hybridse::sdk::Status* status) {
    return ExecuteSQLParameterized(db, sql, std::shared_ptr<openmldb::sdk::SQLRequestRow>(), status);
}
std::shared_ptr<::hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQLParameterized(
    const std::string& db, const std::string& sql, std::shared_ptr<openmldb::sdk::SQLRequestRow> parameter,
    ::hybridse::sdk::Status* status) {
    std::vector<openmldb::type::DataType> parameter_types;
    if (parameter && !ExtractDBTypes(parameter->GetSchema(), parameter_types)) {
        status->msg = "convert parameter types error";
        status->code = -1;
        return {};
    }

    std::unordered_set<std::shared_ptr<::openmldb::client::TabletClient>> clients;
    if (!GetTabletClientsForClusterOnlineBatchQuery(db, sql, parameter, clients)) {
        DLOG(INFO) << "no tablet avaliable for sql " << sql;
        return {};
    }
    if (clients.size() == 1) {
        // Batch query from single tablet
        auto cntl = std::make_shared<::brpc::Controller>();
        cntl->set_timeout_ms(options_.request_timeout);
        auto client = *(clients.begin());
        DLOG(INFO) << " send query to tablet " << client->GetEndpoint();
        auto response = std::make_shared<::openmldb::api::QueryResponse>();
        if (!client->Query(db, sql, parameter_types, parameter ? parameter->GetRow() : "", cntl.get(), response.get(),
                           options_.enable_debug)) {
            status->msg = response->msg();
            status->code = -1;
            return {};
        }
        auto rs = ResultSetSQL::MakeResultSet(response, cntl, status);
        return rs;
    } else {
        // Batch query from multiple tablets and merge the result set
        std::vector<std::shared_ptr<ResultSetSQL>> result_set_list;
        for (auto client : clients) {
            DLOG(INFO) << " send query to tablet " << client->GetEndpoint();
            auto cntl = std::make_shared<::brpc::Controller>();
            cntl->set_timeout_ms(options_.request_timeout);
            auto response = std::make_shared<::openmldb::api::QueryResponse>();
            if (!client->Query(db, sql, parameter_types, parameter ? parameter->GetRow() : "", cntl.get(),
                               response.get(), options_.enable_debug)) {
                status->msg = response->msg();
                status->code = -1;
                return {};
            }
            result_set_list.emplace_back(
                std::dynamic_pointer_cast<ResultSetSQL>(ResultSetSQL::MakeResultSet(response, cntl, status)));
            if (status->code != 0) {
                return {};
            }
        }
        auto cache = GetSQLCache(db, sql, hybridse::vm::kBatchMode, parameter);
        if (!cache) {
            return {};
        }
        auto rs = MultipleResultSetSQL::MakeResultSet(result_set_list, cache->limit_cnt, status);
        if (status->code != 0) {
            return {};
        }
        return rs;
    }
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQLBatchRequest(
    const std::string& db, const std::string& sql, std::shared_ptr<SQLRequestRowBatch> row_batch,
    hybridse::sdk::Status* status) {
    if (!row_batch || !status) {
        LOG(WARNING) << "input is invalid";
        return nullptr;
    }
    auto cntl = std::make_shared<::brpc::Controller>();
    cntl->set_timeout_ms(options_.request_timeout);
    auto response = std::make_shared<::openmldb::api::SQLBatchRequestQueryResponse>();
    auto client = GetTabletClient(db, sql, hybridse::vm::kBatchRequestMode, std::shared_ptr<SQLRequestRow>(),
                                  std::shared_ptr<SQLRequestRow>());
    if (!client) {
        status->code = -1;
        status->msg = "no tablet found";
        return nullptr;
    }
    if (!client->SQLBatchRequestQuery(db, sql, row_batch, cntl.get(), response.get(), options_.enable_debug)) {
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

    std::shared_ptr<::hybridse::sdk::Schema> schema = ::openmldb::sdk::ConvertToSchema(table_info);
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
    bool ret = cluster_sdk_->GetTablet(db, table_info->name(), &tablets);
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
                    bool ret = client->Put(tid, pid, cur_ts, row->GetRow(), kv.second, 1);
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
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info = cache->table_info;
        std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
        bool ret = cluster_sdk_->GetTablet(db, table_info->name(), &tablets);
        if (!ret || tablets.empty()) {
            status->msg = "fail to get table " + table_info->name() + " tablet";
            LOG(WARNING) << status->msg;
            return false;
        }
        for (uint32_t i = 0; i < rows->GetCnt(); ++i) {
            std::shared_ptr<SQLInsertRow> row = rows->GetRow(i);
            if (!PutRow(table_info->tid(), row, tablets, status)) {
                return false;
            }
        }
        return true;
    } else {
        status->msg = "please use getInsertRow with " + sql + " first";
        LOG(WARNING) << status->msg;
        return false;
    }
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db, const std::string& sql, std::shared_ptr<SQLInsertRow> row,
                                     hybridse::sdk::Status* status) {
    if (!row || !status) {
        LOG(WARNING) << "input is invalid";
        return false;
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql, hybridse::vm::kBatchMode);
    if (cache) {
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info = cache->table_info;
        std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> tablets;
        bool ret = cluster_sdk_->GetTablet(db, table_info->name(), &tablets);
        if (!ret || tablets.empty()) {
            status->msg = "fail to get table " + table_info->name() + " tablet";
            LOG(WARNING) << status->msg;
            return false;
        }
        if (!PutRow(table_info->tid(), row, tablets, status)) {
            return false;
        }
        return true;
    } else {
        status->msg = "please use getInsertRow with " + sql + " first";
        LOG(WARNING) << status->msg;
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
        LOG(WARNING) << "fail to explain sql " << sql;
        return std::shared_ptr<ExplainInfo>();
    }
    ::hybridse::sdk::SchemaImpl input_schema(explain_output.input_schema);
    ::hybridse::sdk::SchemaImpl output_schema(explain_output.output_schema);
    std::shared_ptr<ExplainInfoImpl> impl(
        new ExplainInfoImpl(input_schema, output_schema, explain_output.logical_plan, explain_output.physical_plan,
                            explain_output.ir, explain_output.request_db_name, explain_output.request_name));
    return impl;
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
    bool ok = tablet->CallProcedure(db, sp_name, row->GetRow(), cntl.get(), response.get(), options_.enable_debug,
                                    options_.request_timeout);
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
                                                   options_.enable_debug, options_.request_timeout);
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
        status->msg = "resuletSetSQL init failed";
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

base::Status SQLClusterRouter::HandleSQLCmd(const hybridse::node::CmdPlanNode* cmd_node, const std::string& db,
                                            std::shared_ptr<::openmldb::client::NsClient> ns_ptr) {
    if (cmd_node == nullptr || ns_ptr == nullptr) {
        return {base::ReturnCode::kSQLCmdRunError, "null pointer"};
    }
    bool ret = true;
    std::string msg;
    switch (cmd_node->GetCmdType()) {
        case hybridse::node::kCmdDropTable: {
            hybridse::sdk::Status status;
            if (cmd_node->GetArgs().size() == 2) {
                ret = this->DropTable(cmd_node->GetArgs()[0], cmd_node->GetArgs()[1], &status);
            } else if (cmd_node->GetArgs().size() == 1) {
                ret = this->DropTable(db, cmd_node->GetArgs()[0], &status);
            } else {
                return {base::ReturnCode::kSQLCmdRunError, "Invalid Cmd Args size"};
            }
            msg = status.msg;
            break;
        }
        case hybridse::node::kCmdDropIndex: {
            if (cmd_node->GetArgs().size() == 3) {
                ret = ns_ptr->DeleteIndex(cmd_node->GetArgs()[0], cmd_node->GetArgs()[1], cmd_node->GetArgs()[2], msg);
            } else if (cmd_node->GetArgs().size() == 2) {
                ret = ns_ptr->DeleteIndex(db, cmd_node->GetArgs()[1], cmd_node->GetArgs()[2], msg);
            } else {
                return {base::ReturnCode::kSQLCmdRunError, "Invalid Cmd Args size"};
            }
            break;
        }
        case hybridse::node::kCmdDropSp: {
            const std::string& sp_name = cmd_node->GetArgs()[0];
            ret = ns_ptr->DropProcedure(db, sp_name, msg);
            break;
        }
        default: {
            return {base::ReturnCode::kSQLCmdRunError, "fail to execute script with unsupported type"};
        }
    }
    if (!ret) {
        return {base::ReturnCode::kSQLCmdRunError, msg};
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
    hybridse::base::Status sql_status;
    ::openmldb::sdk::NodeAdapter::TransformToTableDef(create_node, true, &table_info, &sql_status);
    if (sql_status.code != 0) {
        return base::Status(sql_status.code, sql_status.msg);
    }
    std::string msg;
    if (!ns_ptr->CreateTable(table_info, msg)) {
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
    RtidbSchema* schema = sp_info.mutable_input_schema();
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
    RtidbSchema rtidb_input_schema;
    if (!openmldb::schema::SchemaAdapter::ConvertSchema(explain_output.input_schema, &rtidb_input_schema)) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "convert input schema failed");
    }
    if (!CheckParameter(*schema, rtidb_input_schema)) {
        return base::Status(base::ReturnCode::kSQLCmdRunError, "check input parameter failed");
    }
    sp_info.mutable_input_schema()->CopyFrom(*schema);
    // get output schema, and fill sp_info
    RtidbSchema rtidb_output_schema;
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
    return ns_ptr->CreateProcedure(sp_info, options_.request_timeout);
}

bool SQLClusterRouter::CheckParameter(const RtidbSchema& parameter, const RtidbSchema& input_schema) {
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
bool SQLClusterRouter::ExtractDBTypes(const std::shared_ptr<hybridse::sdk::Schema> schema,
                                      std::vector<openmldb::type::DataType>& db_types) {  // NOLINT
    if (schema) {
        for (int i = 0; i < schema->GetColumnCnt(); i++) {
            openmldb::type::DataType casted_type;
            if (!openmldb::schema::SchemaAdapter::ConvertType(schema->GetColumnType(i), &casted_type)) {
                LOG(WARNING) << "Invalid parameter type " << schema->GetColumnType(i);
                return false;
            }
            db_types.push_back(casted_type);
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
        return std::shared_ptr<openmldb::sdk::QueryFuture>();
    }
    if (!row->OK()) {
        status->code = -1;
        status->msg = "make sure the request row is built before execute sql";
        LOG(WARNING) << "make sure the request row is built before execute sql";
        return std::shared_ptr<openmldb::sdk::QueryFuture>();
    }
    auto tablet = GetTablet(db, sp_name, status);
    if (!tablet) {
        return std::shared_ptr<openmldb::sdk::QueryFuture>();
    }

    std::shared_ptr<openmldb::api::QueryResponse> response = std::make_shared<openmldb::api::QueryResponse>();
    std::shared_ptr<brpc::Controller> cntl = std::make_shared<brpc::Controller>();
    openmldb::RpcCallback<openmldb::api::QueryResponse>* callback =
        new openmldb::RpcCallback<openmldb::api::QueryResponse>(response, cntl);

    std::shared_ptr<openmldb::sdk::QueryFutureImpl> future = std::make_shared<openmldb::sdk::QueryFutureImpl>(callback);
    bool ok = tablet->CallProcedure(db, sp_name, row->GetRow(), timeout_ms, options_.enable_debug, callback);
    if (!ok) {
        status->code = -1;
        status->msg = "request server error, msg: " + response->msg();
        LOG(WARNING) << status->msg;
        return std::shared_ptr<openmldb::sdk::QueryFuture>();
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
    bool ok = tablet->CallSQLBatchRequestProcedure(db, sp_name, row_batch, options_.enable_debug, timeout_ms, callback);
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
    return cluster_sdk_->GetNsClient()->UpdateOfflineTableInfo(info);
}

::openmldb::base::Status SQLClusterRouter::ShowJobs(const bool only_unfinished,
                                                    std::vector<::openmldb::taskmanager::JobInfo>& job_infos) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ShowJobs(only_unfinished, job_infos);
}

::openmldb::base::Status SQLClusterRouter::ShowJob(const int id, ::openmldb::taskmanager::JobInfo& job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ShowJob(id, job_info);
}

::openmldb::base::Status SQLClusterRouter::StopJob(const int id, ::openmldb::taskmanager::JobInfo& job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->StopJob(id, job_info);
}

::openmldb::base::Status SQLClusterRouter::ExecuteOfflineQuery(const std::string& sql,
                                                               const std::map<std::string, std::string>& config,
                                                               const std::string& default_db,
                                                               ::openmldb::taskmanager::JobInfo& job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->RunBatchAndShow(sql, config, default_db, job_info);
}

::openmldb::base::Status SQLClusterRouter::ImportOnlineData(const std::string& sql,
                                                            const std::map<std::string, std::string>& config,
                                                            const std::string& default_db,
                                                            ::openmldb::taskmanager::JobInfo& job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ImportOnlineData(sql, config, default_db, job_info);
}

::openmldb::base::Status SQLClusterRouter::ImportOfflineData(const std::string& sql,
                                                             const std::map<std::string, std::string>& config,
                                                             const std::string& default_db,
                                                             ::openmldb::taskmanager::JobInfo& job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ImportOfflineData(sql, config, default_db, job_info);
}

::openmldb::base::Status SQLClusterRouter::ExportOfflineData(const std::string& sql,
                                                             const std::map<std::string, std::string>& config,
                                                             const std::string& default_db,
                                                             ::openmldb::taskmanager::JobInfo& job_info) {
    auto taskmanager_client_ptr = cluster_sdk_->GetTaskManagerClient();
    if (!taskmanager_client_ptr) {
        return {-1, "Fail to get TaskManager client"};
    }
    return taskmanager_client_ptr->ExportOfflineData(sql, config, default_db, job_info);
}

bool SQLClusterRouter::NotifyTableChange() {
    return cluster_sdk_->TriggerNotify();
}

}  // namespace sdk
}  // namespace openmldb
