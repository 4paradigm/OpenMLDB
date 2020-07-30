/*
 * sql_cluster_router.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
#include "parser/parser.h"
#include "plan/planner.h"
#include "proto/tablet.pb.h"
#include "sdk/base.h"
#include "sdk/base_impl.h"
#include "sdk/result_set_sql.h"
#include "timer.h"  //NOLINT

namespace rtidb {
namespace sdk {

class ExplainInfoImpl : public ExplainInfo {
 public:
    ExplainInfoImpl(const ::fesql::sdk::SchemaImpl& input_schema,
                    const ::fesql::sdk::SchemaImpl& output_schema,
                    const std::string& logical_plan,
                    const std::string& physical_plan, const std::string& ir)
        : input_schema_(input_schema),
          output_schema_(output_schema),
          logical_plan_(logical_plan),
          physical_plan_(physical_plan),
          ir_(ir) {}
    ~ExplainInfoImpl() {}

    const ::fesql::sdk::Schema& GetInputSchema() { return input_schema_; }

    const ::fesql::sdk::Schema& GetOutputSchema() { return output_schema_; }

    const std::string& GetLogicalPlan() { return logical_plan_; }

    const std::string& GetPhysicalPlan() { return physical_plan_; }

    const std::string& GetIR() { return ir_; }

 private:
    ::fesql::sdk::SchemaImpl input_schema_;
    ::fesql::sdk::SchemaImpl output_schema_;
    std::string logical_plan_;
    std::string physical_plan_;
    std::string ir_;
};

SQLClusterRouter::SQLClusterRouter(const SQLRouterOptions& options)
    : options_(options),
      cluster_sdk_(NULL),
      engine_(NULL),
      input_cache_(),
      mu_(),
      rand_(::baidu::common::timer::now_time()) {}

SQLClusterRouter::SQLClusterRouter(ClusterSDK* sdk)
    : options_(),
      cluster_sdk_(sdk),
      engine_(NULL),
      input_cache_(),
      mu_(),
      rand_(::baidu::common::timer::now_time()) {}

SQLClusterRouter::~SQLClusterRouter() {
    if (NULL != cluster_sdk_) {
        delete cluster_sdk_;
    }
    if (NULL != engine_) {
        delete engine_;
    }
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
    ::fesql::vm::Engine::InitializeGlobalLLVM();
    ::fesql::vm::EngineOptions eopt;
    eopt.set_compile_only(true);
    eopt.set_plan_only(true);
    engine_ = new ::fesql::vm::Engine(
        std::make_shared<::rtidb::catalog::SDKCatalog>(), eopt);
    engine_->UpdateCatalog(cluster_sdk_->GetCatalog());
    return true;
}

std::shared_ptr<SQLRequestRow> SQLClusterRouter::GetRequestRow(
    const std::string& db, const std::string& sql,
    ::fesql::sdk::Status* status) {
    if (status == NULL) return std::shared_ptr<SQLRequestRow>();
    std::shared_ptr<RouterCache> cache = GetCache(db, sql);
    if (cache) {
        status->code = 0;
        return std::make_shared<SQLRequestRow>(cache->column_schema);
    }
    ::fesql::vm::ExplainOutput explain;
    ::fesql::base::Status vm_status;
    bool ok = engine_->Explain(sql, db, false, &explain, &vm_status);
    if (!ok) {
        status->code = -1;
        status->msg = vm_status.msg;
        LOG(WARNING) << "fail to explain sql " << sql << " for "
                     << vm_status.msg;
        return std::shared_ptr<SQLRequestRow>();
    }
    std::shared_ptr<::fesql::sdk::SchemaImpl> schema =
        std::make_shared<::fesql::sdk::SchemaImpl>(explain.input_schema);
    SetCache(db, sql, std::make_shared<RouterCache>(schema));
    return std::make_shared<SQLRequestRow>(schema);
}

std::shared_ptr<SQLInsertRow> SQLClusterRouter::GetInsertRow(
    const std::string& db, const std::string& sql,
    ::fesql::sdk::Status* status) {
    if (status == NULL) return std::shared_ptr<SQLInsertRow>();
    std::shared_ptr<RouterCache> cache = GetCache(db, sql);
    if (cache) {
        status->code = 0;
        return std::make_shared<SQLInsertRow>(
            cache->table_info, cache->column_schema, cache->default_map,
            cache->str_length);
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map,
                       &str_length)) {
        status->code = 1;
        LOG(WARNING) << "get insert information failed";
        return std::shared_ptr<SQLInsertRow>();
    }
    cache = std::make_shared<RouterCache>(table_info, default_map, str_length);
    SetCache(db, sql, cache);
    return std::make_shared<SQLInsertRow>(table_info, cache->column_schema,
                                          default_map, str_length);
}

bool SQLClusterRouter::GetInsertInfo(
    const std::string& db, const std::string& sql, ::fesql::sdk::Status* status,
    std::shared_ptr<::rtidb::nameserver::TableInfo>* table_info,
    DefaultValueMap* default_map, uint32_t* str_length) {
    if (status == NULL || table_info == NULL || default_map == NULL ||
        str_length == NULL) {
        LOG(WARNING) << "insert info is null" << sql;
        return false;
    }
    ::fesql::node::NodeManager nm;
    ::fesql::plan::PlanNodeList plans;
    bool ok = GetSQLPlan(sql, &nm, &plans);
    if (!ok || plans.size() == 0) {
        LOG(WARNING) << "fail to get sql plan with sql " << sql;
        status->msg = "fail to get sql plan with";
        return false;
    }
    ::fesql::node::PlanNode* plan = plans[0];
    if (plan->GetType() != fesql::node::kPlanTypeInsert) {
        status->msg = "invalid sql node expect insert";
        LOG(WARNING) << "invalid sql node expect insert";
        return false;
    }
    ::fesql::node::InsertPlanNode* iplan =
        dynamic_cast<::fesql::node::InsertPlanNode*>(plan);
    const ::fesql::node::InsertStmt* insert_stmt = iplan->GetInsertNode();
    if (insert_stmt == NULL) {
        LOG(WARNING) << "insert stmt is null";
        status->msg = "insert stmt is null";
        return false;
    }
    *table_info = cluster_sdk_->GetTableInfo(db, insert_stmt->table_name_);
    if (!(*table_info)) {
        status->msg =
            "table with name " + insert_stmt->table_name_ + " does not exist";
        LOG(WARNING) << status->msg;
        return false;
    }
    std::map<uint32_t, uint32_t> column_map;
    for (size_t j = 0; j < insert_stmt->columns_.size(); ++j) {
        const std::string& col_name = insert_stmt->columns_[j];
        bool find_flag = false;
        for (int i = 0; i < (*table_info)->column_desc_v1_size(); ++i) {
            if (col_name == (*table_info)->column_desc_v1(i).name()) {
                if (column_map.count(i)) {
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
        dynamic_cast<::fesql::node::ExprListNode*>(insert_stmt->values_[0]),
        str_length);
    if (!(*default_map)) {
        status->msg = "get default value map of " + sql + " failed";
        LOG(WARNING) << status->msg;
        return false;
    }
    return true;
}

std::shared_ptr<fesql::node::ConstNode> SQLClusterRouter::GetDefaultMapValue(
    const fesql::node::ConstNode& node, rtidb::type::DataType column_type) {
    fesql::node::DataType node_type = node.GetDataType();
    switch (column_type) {
        case rtidb::type::kBool:
            if (node_type == fesql::node::kInt32) {
                return std::make_shared<fesql::node::ConstNode>(node);
            }
            break;
        case rtidb::type::kSmallInt:
            if (node_type == fesql::node::kInt16) {
                return std::make_shared<fesql::node::ConstNode>(node);
            } else if (node_type == fesql::node::kInt32) {
                return std::make_shared<fesql::node::ConstNode>(
                    node.GetAsInt16());
            }
            break;
        case rtidb::type::kInt:
            if (node_type == fesql::node::kInt16) {
                return std::make_shared<fesql::node::ConstNode>(
                    node.GetAsInt32());
            } else if (node_type == fesql::node::kInt32) {
                return std::make_shared<fesql::node::ConstNode>(node);
            }
            break;
        case rtidb::type::kBigInt:
            if (node_type == fesql::node::kInt16 ||
                node_type == fesql::node::kInt32) {
                return std::make_shared<fesql::node::ConstNode>(
                    node.GetAsInt64());
            } else if (node_type == fesql::node::kInt64) {
                return std::make_shared<fesql::node::ConstNode>(node);
            }
            break;
        case rtidb::type::kFloat:
            if (node_type == fesql::node::kDouble ||
                node_type == fesql::node::kInt32 ||
                node_type == fesql::node::kInt16) {
                return std::make_shared<fesql::node::ConstNode>(
                    node.GetAsFloat());
            } else if (node_type == fesql::node::kFloat) {
                return std::make_shared<fesql::node::ConstNode>(node);
            }
            break;
        case rtidb::type::kDouble:
            if (node_type == fesql::node::kFloat ||
                node_type == fesql::node::kInt32 ||
                node_type == fesql::node::kInt16) {
                return std::make_shared<fesql::node::ConstNode>(
                    node.GetAsDouble());
            } else if (node_type == fesql::node::kDouble) {
                return std::make_shared<fesql::node::ConstNode>(node);
            }
            break;
        case rtidb::type::kDate:
            if (node_type == fesql::node::kVarchar) {
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
                    return std::make_shared<fesql::node::ConstNode>(date);
                }
                break;
            } else if (node_type == fesql::node::kDate) {
                return std::make_shared<fesql::node::ConstNode>(node);
            }
            break;
        case rtidb::type::kTimestamp:
            if (node_type == fesql::node::kInt16 ||
                node_type == fesql::node::kInt32 ||
                node_type == fesql::node::kTimestamp) {
                return std::make_shared<fesql::node::ConstNode>(
                    node.GetAsInt64());
            } else if (node_type == fesql::node::kInt64) {
                return std::make_shared<fesql::node::ConstNode>(node);
            }
            break;
        case rtidb::type::kVarchar:
        case rtidb::type::kString:
            if (node_type == fesql::node::kVarchar) {
                return std::make_shared<fesql::node::ConstNode>(node);
            }
            break;
        default:
            return std::shared_ptr<fesql::node::ConstNode>();
    }
    return std::shared_ptr<fesql::node::ConstNode>();
}

DefaultValueMap SQLClusterRouter::GetDefaultMap(
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info,
    const std::map<uint32_t, uint32_t>& column_map,
    ::fesql::node::ExprListNode* row, uint32_t* str_length) {
    if (row == NULL || str_length == NULL) {
        LOG(WARNING) << "row or str length is NULL";
        return DefaultValueMap();
    }
    DefaultValueMap default_map(
        new std::map<uint32_t, std::shared_ptr<::fesql::node::ConstNode>>());
    if ((column_map.empty() && static_cast<int32_t>(row->children_.size()) <
                                   table_info->column_desc_v1_size()) ||
        (!column_map.empty() && row->children_.size() < column_map.size())) {
        LOG(WARNING) << "insert value number less than column number";
        return DefaultValueMap();
    }
    for (int32_t idx = 0; idx < table_info->column_desc_v1_size(); idx++) {
        if (!column_map.empty() && !column_map.count(idx)) {
            if (table_info->column_desc_v1(idx).not_null()) {
                LOG(WARNING)
                    << "column " << table_info->column_desc_v1(idx).name()
                    << " can't be null";
                return DefaultValueMap();
            }
            default_map->insert(std::make_pair(
                idx, std::make_shared<::fesql::node::ConstNode>()));
            continue;
        }

        auto column = table_info->column_desc_v1(idx);
        uint32_t i = idx;
        if (!column_map.empty()) {
            i = column_map.at(idx);
        }
        ::fesql::node::ConstNode* primary =
            dynamic_cast<::fesql::node::ConstNode*>(row->children_.at(i));
        if (!primary->IsPlaceholder()) {
            std::shared_ptr<::fesql::node::ConstNode> val;
            if (primary->IsNull()) {
                if (column.not_null()) {
                    LOG(WARNING)
                        << "column " << column.name() << " can't be null";
                    return DefaultValueMap();
                }
                val = std::make_shared<::fesql::node::ConstNode>(*primary);
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
                (column.data_type() == ::rtidb::type::kVarchar ||
                 column.data_type() == ::rtidb::type::kString)) {
                *str_length += strlen(primary->GetStr());
            }
        }
    }
    return default_map;
}

std::shared_ptr<RouterCache> SQLClusterRouter::GetCache(
    const std::string& db, const std::string& sql) {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    auto it = input_cache_.find(db);
    if (it != input_cache_.end()) {
        auto iit = it->second.find(sql);
        if (iit != it->second.end()) {
            return iit->second;
        }
    }
    return std::shared_ptr<RouterCache>();
}

void SQLClusterRouter::SetCache(const std::string& db, const std::string& sql,
                                std::shared_ptr<RouterCache> router_cache) {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    auto it = input_cache_.find(db);
    if (it == input_cache_.end()) {
        std::map<std::string, std::shared_ptr<::rtidb::sdk::RouterCache>>
            sql_cache;
        sql_cache.insert(std::make_pair(sql, router_cache));
        input_cache_.insert(std::make_pair(db, sql_cache));
    } else {
        it->second.insert(std::make_pair(sql, router_cache));
    }
}

std::shared_ptr<SQLInsertRows> SQLClusterRouter::GetInsertRows(
    const std::string& db, const std::string& sql,
    ::fesql::sdk::Status* status) {
    if (status == NULL) return std::shared_ptr<SQLInsertRows>();
    std::shared_ptr<RouterCache> cache = GetCache(db, sql);
    if (cache) {
        status->code = 0;
        return std::make_shared<SQLInsertRows>(
            cache->table_info, cache->column_schema, cache->default_map,
            cache->str_length);
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map,
                       &str_length)) {
        return std::shared_ptr<SQLInsertRows>();
    }
    cache = std::make_shared<RouterCache>(table_info, default_map, str_length);
    SetCache(db, sql, cache);
    return std::make_shared<SQLInsertRows>(table_info, cache->column_schema,
                                           default_map, str_length);
}

bool SQLClusterRouter::ExecuteDDL(const std::string& db, const std::string& sql,
                                  fesql::sdk::Status* status) {
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        LOG(WARNING) << "no nameserver exist";
        return false;
    }
    // TODO(wangtaize) update ns client to thread safe
    std::string err;
    bool ok = ns_ptr->ExecuteSQL(db, sql, err);
    if (!ok) {
        status->msg = "fail to execute sql " + sql + " for error " + err;
        LOG(WARNING) << status->msg;
        status->code = -1;
        return false;
    }
    return true;
}
bool SQLClusterRouter::ShowDB(std::vector<std::string>* dbs,
                              fesql::sdk::Status* status) {
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
                                fesql::sdk::Status* status) {
    if (status == NULL) {
        return false;
    }

    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        LOG(WARNING) << "no nameserver exist";
        status->msg = "no nameserver exist";
        status->code = -1;
        return false;
    }
    if (db.empty()) {
        LOG(WARNING) << "db is empty";
        status->msg = "db is emtpy";
        status->code = -2;
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
                              fesql::sdk::Status* status) {
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

bool SQLClusterRouter::GetTablet(
    const std::string& db, const std::string& sql,
    std::vector<std::shared_ptr<::rtidb::client::TabletClient>>* tablets) {
    if (tablets == NULL) return false;
    // TODO(wangtaize) cache compile result
    ::fesql::vm::BatchRunSession session;
    ::fesql::base::Status status;
    bool ok = engine_->Get(sql, db, session, status);
    if (!ok || status.code != 0) {
        LOG(WARNING) << "fail to compile sql " << sql << " in db " << db;
        return false;
    }
    ::fesql::vm::PhysicalOpNode* physical_plan = session.GetPhysicalPlan();
    std::set<std::string> tables;
    GetTables(physical_plan, &tables);
    auto it = tables.begin();
    for (; it != tables.end(); ++it) {
        ok = cluster_sdk_->GetTabletByTable(db, *it, tablets);
        if (!ok) {
            LOG(WARNING) << "fail to get table " << *it << " tablet";
            return false;
        }
    }
    return true;
}

void SQLClusterRouter::GetTables(::fesql::vm::PhysicalOpNode* node,
                                 std::set<std::string>* tables) {
    if (node == NULL || tables == NULL) return;
    if (node->type_ == ::fesql::vm::kPhysicalOpDataProvider) {
        ::fesql::vm::PhysicalDataProviderNode* data_node =
            reinterpret_cast<::fesql::vm::PhysicalDataProviderNode*>(node);
        if (data_node->provider_type_ == ::fesql::vm::kProviderTypeTable ||
            data_node->provider_type_ == ::fesql::vm::kProviderTypePartition) {
            tables->insert(data_node->table_handler_->GetName());
        }
    }
    if (node->GetProducerCnt() <= 0) return;
    for (size_t i = 0; i < node->GetProducerCnt(); i++) {
        GetTables(node->GetProducer(i), tables);
    }
}

std::shared_ptr<fesql::sdk::ResultSet> SQLClusterRouter::ExecuteSQL(
    const std::string& db, const std::string& sql,
    std::shared_ptr<SQLRequestRow> row, fesql::sdk::Status* status) {
    if (!row || status == NULL) {
        LOG(WARNING) << "input is invalid";
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    if (!row->OK()) {
        LOG(WARNING) << "make sure the request row is built before execute sql";
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    std::unique_ptr<::brpc::Controller> cntl(new ::brpc::Controller());
    std::unique_ptr<::rtidb::api::QueryResponse> response(
        new ::rtidb::api::QueryResponse());
    std::vector<std::shared_ptr<::rtidb::client::TabletClient>> tablets;
    bool ok = GetTablet(db, sql, &tablets);
    if (!ok || tablets.size() <= 0) {
        status->msg = "not tablet found";
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    uint32_t idx = rand_.Uniform(tablets.size());
    ok = tablets[idx]->Query(db, sql, row->GetRow(), cntl.get(), response.get(),
                             options_.enbale_debug);
    if (!ok) {
        status->msg = "request server error";
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    if (response->code() != ::rtidb::base::kOk) {
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    std::shared_ptr<::rtidb::sdk::ResultSetSQL> rs(
        new rtidb::sdk::ResultSetSQL(std::move(response), std::move(cntl)));
    ok = rs->Init();
    if (!ok) {
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    return rs;
}

std::shared_ptr<::fesql::sdk::ResultSet> SQLClusterRouter::ExecuteSQL(
    const std::string& db, const std::string& sql,
    ::fesql::sdk::Status* status) {
    std::unique_ptr<::brpc::Controller> cntl(new ::brpc::Controller());
    std::unique_ptr<::rtidb::api::QueryResponse> response(
        new ::rtidb::api::QueryResponse());
    std::vector<std::shared_ptr<::rtidb::client::TabletClient>> tablets;
    bool ok = GetTablet(db, sql, &tablets);
    if (!ok || tablets.size() <= 0) {
        DLOG(INFO) << "no tablet avilable for sql " << sql;
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    DLOG(INFO) << " send query to tablet " << tablets[0]->GetEndpoint();
    ok = tablets[0]->Query(db, sql, cntl.get(), response.get(),
                           options_.enbale_debug);
    if (!ok) {
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    std::shared_ptr<::rtidb::sdk::ResultSetSQL> rs(
        new rtidb::sdk::ResultSetSQL(std::move(response), std::move(cntl)));
    ok = rs->Init();
    if (!ok) {
        DLOG(INFO) << "fail to init result set for sql " << sql;
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    return rs;
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db,
                                     const std::string& sql,
                                     ::fesql::sdk::Status* status) {
    if (status == NULL) return false;
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map,
                       &str_length)) {
        status->code = 1;
        LOG(WARNING) << "get insert information failed";
        return false;
    }
    std::shared_ptr<SQLInsertRow> row = std::make_shared<SQLInsertRow>(
        table_info, ::rtidb::sdk::ConvertToSchema(table_info), default_map,
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
    std::shared_ptr<::rtidb::client::TabletClient> tablet =
        cluster_sdk_->GetLeaderTabletByTable(db, table_info->name());
    if (!tablet) {
        status->msg = "fail to get table " + table_info->name() + " tablet";
        LOG(WARNING) << status->msg;
        return false;
    }
    DLOG(INFO) << "put data to endpoint " << tablet->GetEndpoint()
               << " with dimensions size " << row->GetDimensions().size();
    bool ok = tablet->Put(table_info->tid(), 0, row->GetDimensions(),
                          row->GetTs(), row->GetRow(), 1);
    if (!ok) {
        status->msg = "fail to get table " + table_info->name() + " tablet";
        LOG(WARNING) << status->msg;
        return false;
    }
    return true;
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db,
                                     const std::string& sql,
                                     std::shared_ptr<SQLInsertRows> rows,
                                     fesql::sdk::Status* status) {
    if (!rows || status == NULL) {
        LOG(WARNING) << "input is invalid";
        return false;
    }
    std::shared_ptr<RouterCache> cache = GetCache(db, sql);
    if (cache) {
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info =
            cache->table_info;
        std::shared_ptr<::rtidb::client::TabletClient> tablet =
            cluster_sdk_->GetLeaderTabletByTable(db, table_info->name());
        if (!tablet) {
            status->msg = "fail to get table " + table_info->name() + " tablet";
            LOG(WARNING) << status->msg;
            return false;
        }
        DLOG(INFO) << "put data to endpoint " << tablet->GetEndpoint();
        for (uint32_t i = 0; i < rows->GetCnt(); ++i) {
            std::shared_ptr<SQLInsertRow> row = rows->GetRow(i);
            bool ok = tablet->Put(table_info->tid(), 0, row->GetDimensions(),
                                  row->GetTs(), row->GetRow(), 1);
            if (!ok) {
                status->msg =
                    "fail to get table " + table_info->name() + " tablet";
                LOG(WARNING) << status->msg;
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
                                     fesql::sdk::Status* status) {
    if (!row || status == NULL) {
        status->msg = "input is invalid";
        LOG(WARNING) << "input is invalid";
        return false;
    }
    std::shared_ptr<RouterCache> cache = GetCache(db, sql);
    if (cache) {
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info =
            cache->table_info;
        std::shared_ptr<::rtidb::client::TabletClient> tablet =
            cluster_sdk_->GetLeaderTabletByTable(db, table_info->name());
        if (!tablet) {
            status->msg = "fail to get table " + table_info->name() + " tablet";
            LOG(WARNING) << status->msg;
            return false;
        }
        DLOG(INFO) << "put data to endpoint " << tablet->GetEndpoint();
        bool ok = tablet->Put(table_info->tid(), 0, row->GetDimensions(),
                              row->GetTs(), row->GetRow(), 1);
        if (!ok) {
            status->msg = "fail to get table " + table_info->name() + " tablet";
            LOG(WARNING) << status->msg;
            return false;
        }
        return ok;
    } else {
        status->msg = "please use getInsertRow with " + sql + " first";
        LOG(WARNING) << status->msg;
        return false;
    }
}

bool SQLClusterRouter::GetSQLPlan(const std::string& sql,
                                  ::fesql::node::NodeManager* nm,
                                  ::fesql::node::PlanNodeList* plan) {
    if (nm == NULL || plan == NULL) return false;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::plan::SimplePlanner planner(nm);
    ::fesql::base::Status sql_status;
    ::fesql::node::NodePointVector parser_trees;
    parser.parse(sql, parser_trees, nm, sql_status);
    if (0 != sql_status.code) {
        LOG(WARNING) << sql_status.msg;
        return false;
    }
    planner.CreatePlanTree(parser_trees, *plan, sql_status);
    if (0 != sql_status.code) {
        LOG(WARNING) << sql_status.msg;
        return false;
    }
    return true;
}

bool SQLClusterRouter::RefreshCatalog() {
    bool ok = cluster_sdk_->Refresh();
    if (ok) {
        engine_->UpdateCatalog(cluster_sdk_->GetCatalog());
    }
    return ok;
}

std::shared_ptr<ExplainInfo> SQLClusterRouter::Explain(
    const std::string& db, const std::string& sql,
    ::fesql::sdk::Status* status) {
    ::fesql::vm::ExplainOutput explain_output;
    ::fesql::base::Status vm_status;
    bool ok = engine_->Explain(sql, db, false, &explain_output, &vm_status);
    if (!ok) {
        LOG(WARNING) << "fail to explain sql " << sql;
        return std::shared_ptr<ExplainInfo>();
    }
    ::fesql::sdk::SchemaImpl input_schema(explain_output.input_schema);
    ::fesql::sdk::SchemaImpl output_schema(explain_output.output_schema);
    std::shared_ptr<ExplainInfoImpl> impl(new ExplainInfoImpl(
        input_schema, output_schema, explain_output.logical_plan,
        explain_output.physical_plan, explain_output.ir));
    return impl;
}

}  // namespace sdk
}  // namespace rtidb
