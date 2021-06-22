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

#include "brpc/channel.h"
#include "glog/logging.h"
#include "plan/plan_api.h"
#include "proto/tablet.pb.h"
#include "sdk/base.h"
#include "sdk/base_impl.h"
#include "sdk/result_set_sql.h"
#include "sdk/batch_request_result_set_sql.h"
#include "common/timer.h"
#include "boost/none.hpp"
#include "rpc/rpc_client.h"

DECLARE_int32(request_timeout_ms);

namespace fedb {
namespace sdk {
using hybridse::plan::PlanAPI;
class ExplainInfoImpl : public ExplainInfo {
 public:
    ExplainInfoImpl(const ::hybridse::sdk::SchemaImpl& input_schema,
                    const ::hybridse::sdk::SchemaImpl& output_schema,
                    const std::string& logical_plan,
                    const std::string& physical_plan, const std::string& ir,
                    const std::string& request_name)
        : input_schema_(input_schema),
          output_schema_(output_schema),
          logical_plan_(logical_plan),
          physical_plan_(physical_plan),
          ir_(ir),
          request_name_(request_name) {}
    ~ExplainInfoImpl() {}

    const ::hybridse::sdk::Schema& GetInputSchema() override { return input_schema_; }

    const ::hybridse::sdk::Schema& GetOutputSchema() override { return output_schema_; }

    const std::string& GetLogicalPlan() override { return logical_plan_; }

    const std::string& GetPhysicalPlan() override { return physical_plan_; }

    const std::string& GetIR() override { return ir_; }

    const std::string& GetRequestName() override { return request_name_; }

 private:
    ::hybridse::sdk::SchemaImpl input_schema_;
    ::hybridse::sdk::SchemaImpl output_schema_;
    std::string logical_plan_;
    std::string physical_plan_;
    std::string ir_;
    std::string request_name_;
};

class QueryFutureImpl : public QueryFuture {
 public:
    explicit QueryFutureImpl(fedb::RpcCallback<fedb::api::QueryResponse>* callback)
        : callback_(callback) {
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
        if (callback_->GetResponse()->code() != ::fedb::base::kOk) {
            status->code = callback_->GetResponse()->code();
            status->msg = "request error, " + callback_->GetResponse()->msg();
            return nullptr;
        }
        auto rs = ResultSetSQL::MakeResultSet(callback_->GetResponse(),
                callback_->GetController(), status);
        return rs;
    }

    bool IsDone() const override {
        if (callback_)
        return callback_->IsDone();
        return false;
    }

 private:
    fedb::RpcCallback<fedb::api::QueryResponse>* callback_;
};

class BatchQueryFutureImpl : public QueryFuture {
 public:
    explicit BatchQueryFutureImpl(
            fedb::RpcCallback<fedb::api::SQLBatchRequestQueryResponse>* callback)
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
        std::shared_ptr<::fedb::sdk::SQLBatchRequestResultSet> rs =
            std::make_shared<fedb::sdk::SQLBatchRequestResultSet>(
                    callback_->GetResponse(), callback_->GetController());
        bool ok = rs->Init();
        if (!ok) {
            status->code = -1;
            status->msg = "request error, resuletSetSQL init failed";
            return nullptr;
        }
        return rs;
    }

    bool IsDone() const override {
        return callback_->IsDone();
    }

 private:
    fedb::RpcCallback<fedb::api::SQLBatchRequestQueryResponse>* callback_;
};

SQLClusterRouter::SQLClusterRouter(const SQLRouterOptions& options)
    : options_(options),
      cluster_sdk_(NULL),
      input_lru_cache_(),
      mu_(),
      rand_(::baidu::common::timer::now_time()) {}

SQLClusterRouter::SQLClusterRouter(ClusterSDK* sdk)
    : options_(),
      cluster_sdk_(sdk),
      input_lru_cache_(),
      mu_(),
      rand_(::baidu::common::timer::now_time()) {}

SQLClusterRouter::~SQLClusterRouter() {
    delete cluster_sdk_;
}

bool SQLClusterRouter::Init() {
    if (cluster_sdk_ == NULL) {
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

std::shared_ptr<SQLRequestRow> SQLClusterRouter::GetRequestRow(
    const std::string& db, const std::string& sql,
    ::hybridse::sdk::Status* status) {
    if (status == NULL) return std::shared_ptr<SQLRequestRow>();
    std::shared_ptr<SQLCache> cache = GetCache(db, sql);
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
        LOG(WARNING) << "fail to explain sql " << sql << " for "
                     << vm_status.msg;
        return std::shared_ptr<SQLRequestRow>();
    }
    std::shared_ptr<::hybridse::sdk::SchemaImpl> schema =
        std::make_shared<::hybridse::sdk::SchemaImpl>(explain.input_schema);
    SetCache(db, sql, std::make_shared<SQLCache>(schema, explain.router));
    const std::string& router_col = explain.router.GetRouterCol();
    if (!router_col.empty()) {
        col_set.insert(router_col);
    }
    return std::make_shared<SQLRequestRow>(schema, col_set);
}

std::shared_ptr<SQLRequestRow> SQLClusterRouter::GetRequestRowByProcedure(const std::string& db,
    const std::string& sp_name, ::hybridse::sdk::Status* status) {
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

std::shared_ptr<SQLInsertRow> SQLClusterRouter::GetInsertRow(
    const std::string& db, const std::string& sql,
    ::hybridse::sdk::Status* status) {
    if (status == NULL) return std::shared_ptr<SQLInsertRow>();
    std::shared_ptr<SQLCache> cache = GetCache(db, sql);
    if (cache) {
        status->code = 0;
        return std::make_shared<SQLInsertRow>(
            cache->table_info, cache->column_schema, cache->default_map,
            cache->str_length);
    }
    std::shared_ptr<::fedb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map,
                       &str_length)) {
        status->code = 1;
        LOG(WARNING) << "get insert information failed";
        return std::shared_ptr<SQLInsertRow>();
    }
    cache = std::make_shared<SQLCache>(table_info, default_map, str_length);
    SetCache(db, sql, cache);
    return std::make_shared<SQLInsertRow>(table_info, cache->column_schema,
                                          default_map, str_length);
}

bool SQLClusterRouter::GetInsertInfo(
    const std::string& db, const std::string& sql, ::hybridse::sdk::Status* status,
    std::shared_ptr<::fedb::nameserver::TableInfo>* table_info,
    DefaultValueMap* default_map, uint32_t* str_length) {
    if (status == NULL || table_info == NULL || default_map == NULL ||
        str_length == NULL) {
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
    ::hybridse::node::InsertPlanNode* iplan =
        dynamic_cast<::hybridse::node::InsertPlanNode*>(plan);
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
            status->msg = "can't find column " + col_name + " in table " +
                          (*table_info)->name();
            LOG(WARNING) << status->msg;
            return false;
        }
    }
    *default_map = GetDefaultMap(
        *table_info, column_map,
        dynamic_cast<::hybridse::node::ExprListNode*>(insert_stmt->values_[0]),
        str_length);
    if (!(*default_map)) {
        status->msg = "get default value map of " + sql + " failed";
        LOG(WARNING) << status->msg;
        return false;
    }
    return true;
}

std::shared_ptr<hybridse::node::ConstNode> SQLClusterRouter::GetDefaultMapValue(
    const hybridse::node::ConstNode& node, fedb::type::DataType column_type) {
    hybridse::node::DataType node_type = node.GetDataType();
    switch (column_type) {
        case fedb::type::kBool:
            if (node_type == hybridse::node::kInt32) {
                return std::make_shared<hybridse::node::ConstNode>(node.GetBool());
            } else if (node_type == hybridse::node::kBool) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case fedb::type::kSmallInt:
            if (node_type == hybridse::node::kInt16) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            } else if (node_type == hybridse::node::kInt32) {
                return std::make_shared<hybridse::node::ConstNode>(
                    node.GetAsInt16());
            }
            break;
        case fedb::type::kInt:
            if (node_type == hybridse::node::kInt16) {
                return std::make_shared<hybridse::node::ConstNode>(
                    node.GetAsInt32());
            } else if (node_type == hybridse::node::kInt32) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            } else if (node_type == hybridse::node::kInt64) {
                return std::make_shared<hybridse::node::ConstNode>(node.GetAsInt32());
            }
            break;
        case fedb::type::kBigInt:
            if (node_type == hybridse::node::kInt16 ||
                node_type == hybridse::node::kInt32) {
                return std::make_shared<hybridse::node::ConstNode>(
                    node.GetAsInt64());
            } else if (node_type == hybridse::node::kInt64) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case fedb::type::kFloat:
            if (node_type == hybridse::node::kDouble ||
                node_type == hybridse::node::kInt32 ||
                node_type == hybridse::node::kInt16) {
                return std::make_shared<hybridse::node::ConstNode>(
                    node.GetAsFloat());
            } else if (node_type == hybridse::node::kFloat) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case fedb::type::kDouble:
            if (node_type == hybridse::node::kFloat ||
                node_type == hybridse::node::kInt32 ||
                node_type == hybridse::node::kInt16) {
                return std::make_shared<hybridse::node::ConstNode>(
                    node.GetAsDouble());
            } else if (node_type == hybridse::node::kDouble) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case fedb::type::kDate:
            if (node_type == hybridse::node::kVarchar) {
                int32_t year;
                int32_t month;
                int32_t day;
                if (node.GetAsDate(&year, &month, &day)) {
                    if (year < 1900 || year > 9999) break;
                    if (month < 1 || month > 12) break;
                    if (day < 1 || day > 31) break;
                    int32_t date = (year - 1900) << 16;
                    date = date | ((month - 1) << 8);
                    date = date | day;
                    return std::make_shared<hybridse::node::ConstNode>(date);
                }
                break;
            } else if (node_type == hybridse::node::kDate) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case fedb::type::kTimestamp:
            if (node_type == hybridse::node::kInt16 ||
                node_type == hybridse::node::kInt32 ||
                node_type == hybridse::node::kTimestamp) {
                return std::make_shared<hybridse::node::ConstNode>(
                    node.GetAsInt64());
            } else if (node_type == hybridse::node::kInt64) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        case fedb::type::kVarchar:
        case fedb::type::kString:
            if (node_type == hybridse::node::kVarchar) {
                return std::make_shared<hybridse::node::ConstNode>(node);
            }
            break;
        default:
            return std::shared_ptr<hybridse::node::ConstNode>();
    }
    return std::shared_ptr<hybridse::node::ConstNode>();
}

DefaultValueMap SQLClusterRouter::GetDefaultMap(
    std::shared_ptr<::fedb::nameserver::TableInfo> table_info,
    const std::map<uint32_t, uint32_t>& column_map,
    ::hybridse::node::ExprListNode* row, uint32_t* str_length) {
    if (row == NULL || str_length == NULL) {
        LOG(WARNING) << "row or str length is NULL";
        return DefaultValueMap();
    }
    DefaultValueMap default_map(
        new std::map<uint32_t, std::shared_ptr<::hybridse::node::ConstNode>>());
    if ((column_map.empty() && static_cast<int32_t>(row->children_.size()) <
                                   table_info->column_desc_size()) ||
        (!column_map.empty() && row->children_.size() < column_map.size())) {
        LOG(WARNING) << "insert value number less than column number";
        return DefaultValueMap();
    }
    for (int32_t idx = 0; idx < table_info->column_desc_size(); idx++) {
        if (!column_map.empty() && (column_map.count(idx) == 0)) {
            if (table_info->column_desc(idx).not_null()) {
                LOG(WARNING)
                    << "column " << table_info->column_desc(idx).name()
                    << " can't be null";
                return DefaultValueMap();
            }
            default_map->insert(std::make_pair(
                idx, std::make_shared<::hybridse::node::ConstNode>()));
            continue;
        }

        auto column = table_info->column_desc(idx);
        uint32_t i = idx;
        if (!column_map.empty()) {
            i = column_map.at(idx);
        }
        ::hybridse::node::ConstNode* primary =
            dynamic_cast<::hybridse::node::ConstNode*>(row->children_.at(i));
        if (!primary->IsPlaceholder()) {
            std::shared_ptr<::hybridse::node::ConstNode> val;
            if (primary->IsNull()) {
                if (column.not_null()) {
                    LOG(WARNING)
                        << "column " << column.name() << " can't be null";
                    return DefaultValueMap();
                }
                val = std::make_shared<::hybridse::node::ConstNode>(*primary);
            } else {
                val = GetDefaultMapValue(*primary, column.data_type());
                if (!val) {
                    LOG(WARNING) << "default value type mismatch, column "
                                 << column.name();
                    return DefaultValueMap();
                }
            }
            default_map->insert(std::make_pair(idx, val));
            if (!primary->IsNull() &&
                (column.data_type() == ::fedb::type::kVarchar ||
                 column.data_type() == ::fedb::type::kString)) {
                *str_length += strlen(primary->GetStr());
            }
        }
    }
    return default_map;
}

std::shared_ptr<SQLCache> SQLClusterRouter::GetCache(
    const std::string& db, const std::string& sql) {
    std::lock_guard<::fedb::base::SpinMutex> lock(mu_);
    auto it = input_lru_cache_.find(db);
    if (it != input_lru_cache_.end()) {
        auto value = it->second.get(sql);
        if (value != boost::none) {
            return value.value();
        }
    }
    return std::shared_ptr<SQLCache>();
}

void SQLClusterRouter::SetCache(const std::string& db, const std::string& sql,
                                std::shared_ptr<SQLCache> router_cache) {
    std::lock_guard<::fedb::base::SpinMutex> lock(mu_);
    auto it = input_lru_cache_.find(db);
    if (it == input_lru_cache_.end()) {
        boost::compute::detail::lru_cache<std::string, std::shared_ptr<::fedb::sdk::SQLCache>>
            sql_cache(options_.max_sql_cache_size);
        input_lru_cache_.insert(std::make_pair(db, sql_cache));
        it = input_lru_cache_.find(db);
    }
    it->second.insert(sql, router_cache);
}

std::shared_ptr<SQLInsertRows> SQLClusterRouter::GetInsertRows(
    const std::string& db, const std::string& sql,
    ::hybridse::sdk::Status* status) {
    if (status == NULL) return std::shared_ptr<SQLInsertRows>();
    std::shared_ptr<SQLCache> cache = GetCache(db, sql);
    if (cache) {
        status->code = 0;
        return std::make_shared<SQLInsertRows>(
            cache->table_info, cache->column_schema, cache->default_map,
            cache->str_length);
    }
    std::shared_ptr<::fedb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map,
                       &str_length)) {
        return std::shared_ptr<SQLInsertRows>();
    }
    cache = std::make_shared<SQLCache>(table_info, default_map, str_length);
    SetCache(db, sql, cache);
    return std::make_shared<SQLInsertRows>(table_info, cache->column_schema,
                                           default_map, str_length);
}

bool SQLClusterRouter::ExecuteDDL(const std::string& db, const std::string& sql,
                                  hybridse::sdk::Status* status) {
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        status->code = -1;
        status->msg = "no nameserver exist";
        LOG(WARNING) << status->msg;
        return false;
    }
    // TODO(wangtaize) update ns client to thread safe
    std::string err;
    bool ok = false;

    // parse sql to judge whether is create procedure case
    hybridse::node::NodeManager node_manager;
    DLOG(INFO) << "start to execute script from dbms:\n" << sql;
    hybridse::base::Status sql_status;
    hybridse::node::NodePointVector parser_trees;
    PlanAPI::CreateSyntaxTreeFromScript(sql, parser_trees, &node_manager, sql_status);
    if (parser_trees.empty() || sql_status.code != 0) {
        status->code = -1;
        status->msg = sql_status.msg;
        LOG(WARNING) << status->msg;
        return false;
    }
    hybridse::node::SqlNode* node = parser_trees[0];
    if (node->GetType() == hybridse::node::kCreateSpStmt) {
        ok = HandleSQLCreateProcedure(parser_trees, db, sql,
                ns_ptr, &node_manager, &err);
    } else {
        ok = ns_ptr->ExecuteSQL(db, sql, err);
    }
    if (!ok) {
        status->msg = "fail to execute sql " + sql + " for error " + err;
        LOG(WARNING) << status->msg;
        status->code = -1;
        return false;
    }
    return true;
}

bool SQLClusterRouter::ShowDB(std::vector<std::string>* dbs,
                              hybridse::sdk::Status* status) {
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
bool SQLClusterRouter::CreateDB(const std::string& db,
                                hybridse::sdk::Status* status) {
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

bool SQLClusterRouter::DropDB(const std::string& db,
                              hybridse::sdk::Status* status) {
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

std::shared_ptr<::fedb::client::TabletClient> SQLClusterRouter::GetTabletClient(
    const std::string& db, const std::string& sql, const std::shared_ptr<SQLRequestRow>& row) {
    std::shared_ptr<::fedb::catalog::TabletAccessor> tablet;
    auto cache = GetCache(db, sql);
    if (!cache) {
        ::hybridse::vm::ExplainOutput explain;
        ::hybridse::base::Status vm_status;
        if (cluster_sdk_->GetEngine()->Explain(sql, db, ::hybridse::vm::kBatchMode, &explain, &vm_status)) {
            std::shared_ptr<::hybridse::sdk::SchemaImpl> schema;
            if (explain.input_schema.size() > 0) {
                schema = std::make_shared<::hybridse::sdk::SchemaImpl>(explain.input_schema);
            } else {
                auto table_info = cluster_sdk_->GetTableInfo(db, explain.router.GetMainTable());
                ::hybridse::vm::Schema raw_schema;
                if (table_info &&
                        ::fedb::catalog::SchemaAdapter::ConvertSchema(table_info->column_desc(), &raw_schema)) {
                    schema = std::make_shared<::hybridse::sdk::SchemaImpl>(raw_schema);
                }
            }
            if (schema) {
                cache = std::make_shared<SQLCache>(schema, explain.router);
                SetCache(db, sql, cache);
            }
        }
    }
    if (cache) {
        const std::string& col = cache->router.GetRouterCol();
        const std::string& main_table = cache->router.GetMainTable();
        if (!main_table.empty()) {
            DLOG(INFO) << "get main table" << main_table;
            std::string val;
            if (!col.empty() && row && row->GetRecordVal(col, &val)) {
                tablet = cluster_sdk_->GetTablet(db, main_table, val);
            }
            if (!tablet) {
                tablet = cluster_sdk_->GetTablet(db, main_table);
            }
        }
    }
    if (!tablet) {
        tablet = cluster_sdk_->GetTablet();
    }
    if (!tablet) {
        LOG(WARNING) << "fail to get tablet";
        return std::shared_ptr<::fedb::client::TabletClient>();
    }
    return tablet->GetClient();
}

std::shared_ptr<TableReader> SQLClusterRouter::GetTableReader() {
    std::shared_ptr<TableReaderImpl> reader(new TableReaderImpl(cluster_sdk_));
    return reader;
}

std::shared_ptr<fedb::client::TabletClient> SQLClusterRouter::GetTablet(
        const std::string& db, const std::string& sp_name, hybridse::sdk::Status* status) {
    if (status == nullptr) return nullptr;
    std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info =
        cluster_sdk_->GetProcedureInfo(db, sp_name, &status->msg);
    if (!sp_info) {
        status->code = -1;
        status->msg = "procedure not found, msg: " + status->msg;
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    const std::string& table = sp_info->GetMainTable();
    auto tablet = cluster_sdk_->GetTablet(db, table);
    if (!tablet) {
        status->code = -1;
        status->msg = "fail to get tablet, table "  + table;
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
void SQLClusterRouter::GetTables(::hybridse::vm::PhysicalOpNode* node,
                                 std::set<std::string>* tables) {
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

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQL(
    const std::string& db, const std::string& sql,
    std::shared_ptr<SQLRequestRow> row, hybridse::sdk::Status* status) {
    if (!row || !status) {
        LOG(WARNING) << "input is invalid";
        return std::shared_ptr<::hybridse::sdk::ResultSet>();
    }
    if (!row->OK()) {
        LOG(WARNING) << "make sure the request row is built before execute sql";
        return std::shared_ptr<::hybridse::sdk::ResultSet>();
    }
    auto cntl = std::make_shared<::brpc::Controller>();
    cntl->set_timeout_ms(options_.request_timeout);
    auto response = std::make_shared<::fedb::api::QueryResponse>();
    auto client = GetTabletClient(db, sql, row);
    if (!client) {
        status->msg = "not tablet found";
        return std::shared_ptr<::hybridse::sdk::ResultSet>();
    }
    if (!client->Query(db, sql, row->GetRow(), cntl.get(), response.get(),
                             options_.enable_debug)) {
        status->msg = "request server error, msg: " + response->msg();
        return std::shared_ptr<::hybridse::sdk::ResultSet>();
    }
    if (response->code() != ::fedb::base::kOk) {
        status->code = response->code();
        status->msg = "request error, " + response->msg();
        return std::shared_ptr<::hybridse::sdk::ResultSet>();
    }

    auto rs = ResultSetSQL::MakeResultSet(response, cntl, status);
    return rs;
}

std::shared_ptr<::hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQL(
    const std::string& db, const std::string& sql,
    ::hybridse::sdk::Status* status) {
    auto cntl = std::make_shared<::brpc::Controller>();
    cntl->set_timeout_ms(options_.request_timeout);
    auto response = std::make_shared<::fedb::api::QueryResponse>();
    auto client = GetTabletClient(db, sql, std::shared_ptr<SQLRequestRow>());
    if (!client) {
        DLOG(INFO) << "no tablet avilable for sql " << sql;
        return std::shared_ptr<::hybridse::sdk::ResultSet>();
    }
    DLOG(INFO) << " send query to tablet " << client->GetEndpoint();
    if (!client->Query(db, sql, cntl.get(), response.get(),
                           options_.enable_debug)) {
        return std::shared_ptr<::hybridse::sdk::ResultSet>();
    }
    auto rs = ResultSetSQL::MakeResultSet(response, cntl, status);
    return rs;
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::ExecuteSQLBatchRequest(
    const std::string& db, const std::string& sql,
    std::shared_ptr<SQLRequestRowBatch> row_batch, hybridse::sdk::Status* status) {
    if (!row_batch || !status) {
        LOG(WARNING) << "input is invalid";
        return nullptr;
    }
    auto cntl = std::make_shared<::brpc::Controller>();
    cntl->set_timeout_ms(options_.request_timeout);
    auto response = std::make_shared<::fedb::api::SQLBatchRequestQueryResponse>();
    auto client = GetTabletClient(db, sql, std::shared_ptr<SQLRequestRow>());
    if (!client) {
        status->code = -1;
        status->msg = "no tablet found";
        return nullptr;
    }
    if (!client->SQLBatchRequestQuery(db, sql, row_batch, cntl.get(),
                                            response.get(),
                                            options_.enable_debug)) {
        status->code = -1;
        status->msg = "request server error " + response->msg();
        return nullptr;
    }
    if (response->code() != ::fedb::base::kOk) {
        status->code = -1;
        status->msg = response->msg();
        return nullptr;
    }
    auto rs = std::make_shared<fedb::sdk::SQLBatchRequestResultSet>(response, cntl);
    if (!rs->Init()) {
        status->code = -1;
        status->msg = "batch request result set init fail";
        return nullptr;
    }
    return rs;
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db,
                                     const std::string& sql,
                                     ::hybridse::sdk::Status* status) {
    if (status == NULL) return false;
    std::shared_ptr<::fedb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map,
                       &str_length)) {
        status->code = 1;
        LOG(WARNING) << "get insert information failed";
        return false;
    }
    std::shared_ptr<SQLInsertRow> row = std::make_shared<SQLInsertRow>(
        table_info, ::fedb::sdk::ConvertToSchema(table_info), default_map,
        str_length);
    if (!row) {
        status->msg = "fail to parse row from sql " + sql;
        LOG(WARNING) << status->msg;
        return false;
    }
    if (!row->Init(0)) {
        status->msg = "fail to encode row for table " + table_info->name();
        LOG(WARNING) << status->msg;
        return false;
    }
    if (!row->IsComplete()) {
        status->msg = "insert value isn't complete";
        LOG(WARNING) << status->msg;
        return false;
    }
    std::vector<std::shared_ptr<::fedb::catalog::TabletAccessor>> tablets;
    bool ret = cluster_sdk_->GetTablet(db, table_info->name(), &tablets);
    if (!ret || tablets.empty()) {
        status->msg = "fail to get table " + table_info->name() + " tablet";
        LOG(WARNING) << status->msg;
        return false;
    }
    return PutRow(table_info->tid(), row, tablets, status);
}

bool SQLClusterRouter::PutRow(uint32_t tid, const std::shared_ptr<SQLInsertRow>& row,
        const std::vector<std::shared_ptr<::fedb::catalog::TabletAccessor>>& tablets,
        ::hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return false;
    }
    const auto& dimensions = row->GetDimensions();
    const auto& ts_dimensions = row->GetTs();
    uint64_t cur_ts = 0;
    if (ts_dimensions.empty()) {
        cur_ts = ::baidu::common::timer::get_micros() / 1000;
    }
    for (const auto& kv : dimensions) {
        uint32_t pid = kv.first;
        if (pid < tablets.size()) {
            auto tablet = tablets[pid];
            if (tablet) {
                auto client = tablet->GetClient();
                if (client) {
                    DLOG(INFO) << "put data to endpoint " << client->GetEndpoint()
                               << " with dimensions size " << kv.second.size();
                    bool ret = false;
                    if (ts_dimensions.empty()) {
                        ret = client->Put(tid, pid, cur_ts, row->GetRow(), kv.second, 1);
                    } else {
                        ret = client->Put(tid, pid, kv.second, row->GetTs(), row->GetRow(), 1);
                    }
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

bool SQLClusterRouter::ExecuteInsert(const std::string& db,
                                     const std::string& sql,
                                     std::shared_ptr<SQLInsertRows> rows,
                                     hybridse::sdk::Status* status) {
    if (!rows || !status) {
        LOG(WARNING) << "input is invalid";
        return false;
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql);
    if (cache) {
        std::shared_ptr<::fedb::nameserver::TableInfo> table_info =
            cache->table_info;
        std::vector<std::shared_ptr<::fedb::catalog::TabletAccessor>> tablets;
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

bool SQLClusterRouter::ExecuteInsert(const std::string& db,
                                     const std::string& sql,
                                     std::shared_ptr<SQLInsertRow> row,
                                     hybridse::sdk::Status* status) {
    if (!row || !status) {
        LOG(WARNING) << "input is invalid";
        return false;
    }
    std::shared_ptr<SQLCache> cache = GetCache(db, sql);
    if (cache) {
        std::shared_ptr<::fedb::nameserver::TableInfo> table_info = cache->table_info;
        std::vector<std::shared_ptr<::fedb::catalog::TabletAccessor>> tablets;
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

bool SQLClusterRouter::GetSQLPlan(const std::string& sql,
                                  ::hybridse::node::NodeManager* nm,
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

bool SQLClusterRouter::RefreshCatalog() {
    return cluster_sdk_->Refresh();
}

std::shared_ptr<ExplainInfo> SQLClusterRouter::Explain(
    const std::string& db, const std::string& sql,
    ::hybridse::sdk::Status* status) {
    ::hybridse::vm::ExplainOutput explain_output;
    ::hybridse::base::Status vm_status;
    bool ok = cluster_sdk_->GetEngine()->Explain(sql, db, ::hybridse::vm::kRequestMode, &explain_output, &vm_status);
    if (!ok) {
        status->code = -1;
        status->msg = vm_status.msg;
        LOG(WARNING) << "fail to explain sql " << sql;
        return std::shared_ptr<ExplainInfo>();
    }
    ::hybridse::sdk::SchemaImpl input_schema(explain_output.input_schema);
    ::hybridse::sdk::SchemaImpl output_schema(explain_output.output_schema);
    std::shared_ptr<ExplainInfoImpl> impl(new ExplainInfoImpl(
        input_schema, output_schema, explain_output.logical_plan,
        explain_output.physical_plan, explain_output.ir, explain_output.request_name));
    return impl;
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::CallProcedure(
    const std::string& db, const std::string& sp_name,
    std::shared_ptr<SQLRequestRow> row, hybridse::sdk::Status* status) {
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
    auto response = std::make_shared<::fedb::api::QueryResponse>();
    bool ok = tablet->CallProcedure(db, sp_name, row->GetRow(), cntl.get(), response.get(),
                             options_.enable_debug, options_.request_timeout);
    if (!ok) {
        status->code = -1;
        status->msg = "request server error" + response->msg();
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    if (response->code() != ::fedb::base::kOk) {
        status->code = -1;
        status->msg = response->msg();
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    auto rs = ResultSetSQL::MakeResultSet(response, cntl, status);
    return rs;
}

std::shared_ptr<hybridse::sdk::ResultSet> SQLClusterRouter::CallSQLBatchRequestProcedure(
        const std::string& db, const std::string& sp_name,
        std::shared_ptr<SQLRequestRowBatch> row_batch, hybridse::sdk::Status* status) {
    if (!row_batch || !status) {
        return nullptr;
    }
    auto tablet = GetTablet(db, sp_name, status);
    if (!tablet) {
        return nullptr;
    }

    auto cntl = std::make_shared<::brpc::Controller>();
    auto response = std::make_shared<::fedb::api::SQLBatchRequestQueryResponse>();
    bool ok = tablet->CallSQLBatchRequestProcedure(
            db, sp_name, row_batch, cntl.get(), response.get(),
            options_.enable_debug, options_.request_timeout);
    if (!ok) {
        status->code = -1;
        status->msg = "request server error, msg: " + response->msg();
        return nullptr;
    }
    if (response->code() != ::fedb::base::kOk) {
        status->code = -1;
        status->msg = response->msg();
        return nullptr;
    }
    auto rs = std::make_shared<::fedb::sdk::SQLBatchRequestResultSet>(response, cntl);
    if (!rs->Init()) {
        status->code = -1;
        status->msg = "resuletSetSQL init failed";
        return nullptr;
    }
    return rs;
}

std::shared_ptr<hybridse::sdk::ProcedureInfo> SQLClusterRouter::ShowProcedure(
        const std::string& db, const std::string& sp_name, hybridse::sdk::Status* status) {
    if (status == nullptr) {
        return nullptr;
    }
    std::shared_ptr<hybridse::sdk::ProcedureInfo> sp_info =
        cluster_sdk_->GetProcedureInfo(db, sp_name, &status->msg);
    if (!sp_info) {
        status->code = -1;
        status->msg = "procedure not found, msg: " + status->msg;
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    return sp_info;
}

bool SQLClusterRouter::HandleSQLCreateProcedure(const hybridse::node::NodePointVector& parser_trees,
        const std::string& db, const std::string& sql,
        std::shared_ptr<::fedb::client::NsClient> ns_ptr,
        hybridse::node::NodeManager* node_manager, std::string* msg) {
    if (node_manager == nullptr || msg == nullptr) {
        return false;
    }
    hybridse::node::PlanNodeList plan_trees;
    hybridse::base::Status sql_status;
    PlanAPI::CreatePlanTreeFromSyntaxTree(parser_trees, plan_trees, node_manager, sql_status);
    if (plan_trees.empty() || sql_status.code != 0) {
        *msg = sql_status.msg;
        return false;
    }

    hybridse::node::PlanNode* plan = plan_trees[0];
    if (plan == nullptr) {
        *msg = "fail to execute plan : PlanNode null";
        return false;
    }
    switch (plan->GetType()) {
        case hybridse::node::kPlanTypeCreateSp: {
            hybridse::node::CreateProcedurePlanNode* create_sp =
                dynamic_cast<hybridse::node::CreateProcedurePlanNode*>(plan);
            if (create_sp == nullptr) {
                *msg = "cast CreateProcedurePlanNode failed";
                return false;
            }
            // construct sp_info
            fedb::api::ProcedureInfo sp_info;
            sp_info.set_db_name(db);
            sp_info.set_sp_name(create_sp->GetSpName());
            sp_info.set_sql(sql);
            RtidbSchema* schema = sp_info.mutable_input_schema();
            for (auto input : create_sp->GetInputParameterList()) {
                if (input == nullptr) {
                    *msg = "fail to execute plan : InputParameterNode null";
                    return false;
                }
                if (input->GetType() == hybridse::node::kInputParameter) {
                    hybridse::node::InputParameterNode* input_ptr =
                        (hybridse::node::InputParameterNode*)input;
                    if (input_ptr == nullptr) {
                        *msg = "cast InputParameterNode failed";
                        return false;
                    }
                    fedb::common::ColumnDesc* col_desc = schema->Add();
                    col_desc->set_name(input_ptr->GetColumnName());
                    fedb::type::DataType rtidb_type;
                    bool ok = ::fedb::catalog::SchemaAdapter::ConvertType(input_ptr->GetColumnType(),
                            &rtidb_type);
                    if (!ok) {
                        *msg = "convert type failed";
                        return false;
                    }
                    col_desc->set_data_type(rtidb_type);
                    col_desc->set_is_constant(input_ptr->GetIsConstant());
                } else {
                    *msg = "fail to execute script with unsupported type" +
                        hybridse::node::NameOfSqlNodeType(input->GetType());
                    return false;
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
                ok = cluster_sdk_->GetEngine()->Explain(
                    sql, db, hybridse::vm::kRequestMode, &explain_output, &sql_status);
            } else {
                ok = cluster_sdk_->GetEngine()->Explain(
                    sql, db, hybridse::vm::kBatchRequestMode, input_common_column_indices,
                    &explain_output, &sql_status);
            }
            if (!ok) {
                *msg = "fail to explain sql" + sql_status.msg;
                return false;
            }
            RtidbSchema rtidb_input_schema;
            if (!fedb::catalog::SchemaAdapter::ConvertSchema(explain_output.input_schema, &rtidb_input_schema)) {
                *msg = "convert input schema failed";
                return false;
            }
            if (!CheckParameter(*schema, rtidb_input_schema)) {
                *msg = "check input parameter failed";
                return false;
            }
            sp_info.mutable_input_schema()->CopyFrom(*schema);
            // get output schema, and fill sp_info
            RtidbSchema rtidb_output_schema;
            if (!fedb::catalog::SchemaAdapter::ConvertSchema(explain_output.output_schema, &rtidb_output_schema)) {
                *msg = "convert output schema failed";
                return false;
            }
            sp_info.mutable_output_schema()->CopyFrom(rtidb_output_schema);
            sp_info.set_main_table(explain_output.request_name);
            // get dependent tables, and fill sp_info
            std::set<std::string> tables;
            ::hybridse::base::Status status;
            if (!cluster_sdk_->GetEngine()->GetDependentTables(sql, db, ::hybridse::vm::kRequestMode, &tables,
                                                               status)) {
                LOG(WARNING) << "fail to get dependent tables: " << status.msg;
                return false;
            }
            for (auto& table : tables) {
                sp_info.add_tables(table);
            }
            // send request to ns client
            if (!ns_ptr->CreateProcedure(sp_info, options_.request_timeout, msg)) {
                *msg = "create procedure failed, msg: " + *msg;
                return false;
            }
            break;
        }
        default: {
            *msg = "fail to execute script with unsupported type " +
                              hybridse::node::NameOfPlanNodeType(plan->GetType());
            return false;
        }
    }
    return true;
}

bool SQLClusterRouter::CheckParameter(const RtidbSchema& parameter,
        const RtidbSchema& input_schema) {
    if (parameter.size() != input_schema.size()) {
        return false;
    }
    for (int32_t i = 0; i < parameter.size(); i++) {
        if (parameter.Get(i).name() != input_schema.Get(i).name()) {
            LOG(WARNING) << "check column name failed, expect " << input_schema.Get(i).name()
                << ", but " << parameter.Get(i).name();
            return false;
        }
        if (parameter.Get(i).data_type() != input_schema.Get(i).data_type()) {
            LOG(WARNING) << "check column type failed, expect "
                << fedb::type::DataType_Name(input_schema.Get(i).data_type())
                << ", but " << fedb::type::DataType_Name(parameter.Get(i).data_type());
            return false;
        }
    }
    return true;
}

bool SQLClusterRouter::CheckSQLSyntax(const std::string& sql) {
    hybridse::node::NodeManager node_manager;
    hybridse::base::Status sql_status;
    hybridse::node::NodePointVector parser_trees;
    hybridse::plan::PlanAPI::CreateSyntaxTreeFromScript(sql, parser_trees, &node_manager, sql_status);
    if (0 != sql_status.code) {
        LOG(WARNING) << sql_status.str();
        return false;
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

std::shared_ptr<hybridse::sdk::ProcedureInfo> SQLClusterRouter::ShowProcedure(
        const std::string& db, const std::string& sp_name, std::string* msg) {
    if (msg == nullptr) {
        *msg = "null ptr";
        return nullptr;
    }
    return cluster_sdk_->GetProcedureInfo(db, sp_name, msg);
}

std::shared_ptr<fedb::sdk::QueryFuture> SQLClusterRouter::CallProcedure(
    const std::string& db, const std::string& sp_name, int64_t timeout_ms,
    std::shared_ptr<SQLRequestRow> row, hybridse::sdk::Status* status) {
    if (!row || !status) {
        return std::shared_ptr<fedb::sdk::QueryFuture>();
    }
    if (!row->OK()) {
        status->code = -1;
        status->msg = "make sure the request row is built before execute sql";
        LOG(WARNING) << "make sure the request row is built before execute sql";
        return std::shared_ptr<fedb::sdk::QueryFuture>();
    }
    auto tablet = GetTablet(db, sp_name, status);
    if (!tablet) {
        return std::shared_ptr<fedb::sdk::QueryFuture>();
    }

    std::shared_ptr<fedb::api::QueryResponse> response =
        std::make_shared<fedb::api::QueryResponse>();
    std::shared_ptr<brpc::Controller> cntl = std::make_shared<brpc::Controller>();
    fedb::RpcCallback<fedb::api::QueryResponse>* callback =
            new fedb::RpcCallback<fedb::api::QueryResponse>(response, cntl);

    std::shared_ptr<fedb::sdk::QueryFutureImpl> future =
        std::make_shared<fedb::sdk::QueryFutureImpl>(callback);
    bool ok = tablet->CallProcedure(db, sp_name, row->GetRow(), timeout_ms,
            options_.enable_debug, callback);
    if (!ok) {
        status->code = -1;
        status->msg = "request server error, msg: " + response->msg();
        LOG(WARNING) << status->msg;
        return std::shared_ptr<fedb::sdk::QueryFuture>();
    }
    return future;
}

std::shared_ptr<fedb::sdk::QueryFuture> SQLClusterRouter::CallSQLBatchRequestProcedure(
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
    auto response = std::make_shared<fedb::api::SQLBatchRequestQueryResponse>();
    fedb::RpcCallback<fedb::api::SQLBatchRequestQueryResponse>* callback =
           new fedb::RpcCallback<fedb::api::SQLBatchRequestQueryResponse>(response, cntl);

    std::shared_ptr<fedb::sdk::BatchQueryFutureImpl> future =
        std::make_shared<fedb::sdk::BatchQueryFutureImpl>(callback);
    bool ok = tablet->CallSQLBatchRequestProcedure(
            db, sp_name, row_batch, options_.enable_debug, timeout_ms, callback);
    if (!ok) {
        status->code = -1;
        status->msg = "request server error, msg: " + response->msg();
        LOG(WARNING) << status->msg;
        return nullptr;
    }
    return future;
}

}  // namespace sdk
}  // namespace fedb
