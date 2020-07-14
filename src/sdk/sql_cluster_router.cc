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
    delete cluster_sdk_;
    delete engine_;
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
    std::shared_ptr<::fesql::vm::Catalog> catalog = cluster_sdk_->GetCatalog();
    ::fesql::vm::EngineOptions eopt;
    eopt.set_compile_only(true);
    eopt.set_plan_only(true);
    engine_ = new ::fesql::vm::Engine(catalog, eopt);
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
            cache->table_info, cache->default_map, cache->str_length);
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
    SetCache(
        db, sql,
        std::make_shared<RouterCache>(table_info, default_map, str_length));
    return std::make_shared<SQLInsertRow>(table_info, default_map, str_length);
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
    *default_map = GetDefaultMap(
        *table_info,
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
    ::fesql::node::ExprListNode* row, uint32_t* str_length) {
    if (row == NULL || str_length == NULL) {
        LOG(WARNING) << "row or str length is NULL";
        return DefaultValueMap();
    }
    DefaultValueMap default_map(
        new std::map<uint32_t, std::shared_ptr<::fesql::node::ConstNode>>());
    for (int32_t idx = 0; idx < table_info->column_desc_v1_size(); idx++) {
        auto column = table_info->column_desc_v1(idx);
        ::fesql::node::ConstNode* primary =
            dynamic_cast<::fesql::node::ConstNode*>(row->children_.at(idx));
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
            if (column.data_type() == ::rtidb::type::kVarchar ||
                column.data_type() == ::rtidb::type::kString) {
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
            cache->table_info, cache->default_map, cache->str_length);
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info;
    DefaultValueMap default_map;
    uint32_t str_length = 0;
    if (!GetInsertInfo(db, sql, status, &table_info, &default_map,
                       &str_length)) {
        return std::shared_ptr<SQLInsertRows>();
    }
    SetCache(
        db, sql,
        std::make_shared<RouterCache>(table_info, default_map, str_length));
    return std::make_shared<SQLInsertRows>(table_info, default_map, str_length);
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
        LOG(WARNING) << "fail to execute sql " << sql << " for error " << err;
        return false;
    }
    return true;
}

bool SQLClusterRouter::CreateDB(const std::string& db,
                                fesql::sdk::Status* status) {
    auto ns_ptr = cluster_sdk_->GetNsClient();
    if (!ns_ptr) {
        LOG(WARNING) << "no nameserver exist";
        return false;
    }
    std::string err;
    bool ok = ns_ptr->CreateDatabase(db, err);
    if (!ok) {
        LOG(WARNING) << "fail to create db " << db << " for error " << err;
        return false;
    }
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
    if (node == NULL) return;
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
    ok =
        tablets[idx]->Query(db, sql, row->GetRow(), cntl.get(), response.get());
    if (!ok) {
        status->msg = "request server error";
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    std::shared_ptr<::rtidb::sdk::ResultSetSQL> rs(
        new rtidb::sdk::ResultSetSQL(std::move(response), std::move(cntl)));
    rs->Init();
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
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    DLOG(INFO) << " send query to tablet " << tablets[0]->GetEndpoint();
    ok = tablets[0]->Query(db, sql, cntl.get(), response.get());
    if (!ok) {
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    std::shared_ptr<::rtidb::sdk::ResultSetSQL> rs(
        new rtidb::sdk::ResultSetSQL(std::move(response), std::move(cntl)));
    ok = rs->Init();
    if (!ok) {
        DLOG(INFO) << "fail to init result set for sql " << sql;
    }
    return rs;
}

bool SQLClusterRouter::ExecuteInsert(const std::string& db,
                                     const std::string& sql,
                                     ::fesql::sdk::Status* status) {
    ::fesql::node::NodeManager nm;
    ::fesql::plan::PlanNodeList plans;
    bool ok = GetSQLPlan(sql, &nm, &plans);
    if (!ok || plans.size() == 0) {
        status->msg = "fail to get sql plan with " + sql;
        LOG(WARNING) << status->msg;
        return false;
    }
    ::fesql::node::PlanNode* plan = plans[0];
    if (plan->GetType() != fesql::node::kPlanTypeInsert) {
        status->msg = "invalid sql node expect insert";
        LOG(WARNING) << status->msg;
        return false;
    }
    ::fesql::node::InsertPlanNode* iplan =
        dynamic_cast<::fesql::node::InsertPlanNode*>(plan);
    const ::fesql::node::InsertStmt* insert_stmt = iplan->GetInsertNode();
    if (insert_stmt == NULL) {
        status->msg = "insert stmt is null";
        LOG(WARNING) << "insert stmt is null";
        return false;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info =
        cluster_sdk_->GetTableInfo(db, insert_stmt->table_name_);
    if (!table_info) {
        status->msg =
            "table with name " + insert_stmt->table_name_ + " does not exist";
        LOG(WARNING) << status->msg;
        return false;
    }
    std::string value;
    std::vector<std::pair<std::string, uint32_t>> dimensions;
    std::vector<uint64_t> ts;
    ok = EncodeFullColumns(table_info->column_desc_v1(), iplan, &value,
                           &dimensions, &ts);
    if (!ok) {
        status->msg =
            "fail to encode row for table " + insert_stmt->table_name_;
        LOG(WARNING) << status->msg;
        return false;
    }
    std::shared_ptr<::rtidb::client::TabletClient> tablet =
        cluster_sdk_->GetLeaderTabletByTable(db, insert_stmt->table_name_);
    if (!tablet) {
        status->msg =
            "fail to get table " + insert_stmt->table_name_ + " tablet";
        LOG(WARNING) << status->msg;
        return false;
    }
    DLOG(INFO) << "put data to endpoint " << tablet->GetEndpoint()
               << " with dimensions size " << dimensions.size();
    ok = tablet->Put(table_info->tid(), 0, dimensions, ts, value, 1);
    if (!ok) {
        status->msg =
            "fail to get table " + insert_stmt->table_name_ + " tablet";
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
        DefaultValueMap default_map = cache->default_map;
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
        DefaultValueMap default_map = cache->default_map;
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

bool SQLClusterRouter::EncodeFullColumns(
    const catalog::RtiDBSchema& schema,
    const ::fesql::node::InsertPlanNode* plan, std::string* value,
    std::vector<std::pair<std::string, uint32_t>>* dimensions,
    std::vector<uint64_t>* ts_dimensions) {
    if (plan == NULL || value == NULL || dimensions == NULL ||
        ts_dimensions == NULL)
        return false;
    auto insert_stmt = plan->GetInsertNode();
    if (nullptr == insert_stmt || insert_stmt->values_.size() <= 0) {
        LOG(WARNING) << "insert stmt is null";
        return false;
    }
    // TODO(wangtaize) support batch put
    ::fesql::node::ExprListNode* row =
        dynamic_cast<::fesql::node::ExprListNode*>(insert_stmt->values_[0]);
    if (row->children_.size() != schema.size()) {
        LOG(WARNING) << "schema mismatch";
        return false;
    }
    uint32_t str_size = 0;
    for (uint32_t idx = 0; idx < row->children_.size(); idx++) {
        const ::rtidb::common::ColumnDesc& column = schema.Get(idx);
        auto val = row->children_.at(idx);
        if (val->GetExprType() != ::fesql::node::kExprPrimary) {
            LOG(WARNING) << "unsupported  data type";
            return false;
        }
        ::fesql::node::ConstNode* primary =
            dynamic_cast<::fesql::node::ConstNode*>(val);
        if (primary->IsNull()) {
            continue;
        }
        if (column.data_type() == ::rtidb::type::kVarchar ||
            column.data_type() == ::rtidb::type::kString) {
            str_size += strlen(primary->GetStr());
        }
    }
    DLOG(INFO) << "row str size " << str_size;
    ::rtidb::codec::RowBuilder rb(schema);
    uint32_t row_size = rb.CalTotalLength(str_size);
    value->resize(row_size);
    char* buf = reinterpret_cast<char*>(&(value->at(0)));
    bool ok = rb.SetBuffer(reinterpret_cast<int8_t*>(buf), row_size);
    if (!ok) {
        return false;
    }
    int32_t idx_cnt = 0;
    for (int32_t i = 0; i < schema.size(); i++) {
        const ::rtidb::common::ColumnDesc& column = schema.Get(i);
        const ::fesql::node::ConstNode* primary =
            dynamic_cast<const ::fesql::node::ConstNode*>(row->children_.at(i));
        if (primary->IsNull()) {
            if (column.not_null()) {
                return false;
            }
            rb.AppendNULL();
            continue;
        }
        switch (column.data_type()) {
            case ::rtidb::type::kBool: {
                ok = rb.AppendBool(primary->GetInt());
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetInt();
                if (column.add_ts_idx()) {
                    dimensions->push_back(std::make_pair(
                        primary->GetInt() ? "true" : "false", idx_cnt));
                    idx_cnt++;
                }
                break;
            }
            case ::rtidb::type::kSmallInt: {
                ok = rb.AppendInt16(primary->GetSmallInt());
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetSmallInt();
                if (column.add_ts_idx()) {
                    dimensions->push_back(std::make_pair(
                        std::to_string(primary->GetSmallInt()), idx_cnt));
                    idx_cnt++;
                }
                break;
            }
            case ::rtidb::type::kInt: {
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetInt();
                ok = rb.AppendInt32(primary->GetInt());
                if (column.add_ts_idx()) {
                    dimensions->push_back(std::make_pair(
                        std::to_string(primary->GetInt()), idx_cnt));
                    idx_cnt++;
                }
                break;
            }
            case ::rtidb::type::kBigInt: {
                if (column.is_ts_col()) {
                    ts_dimensions->push_back(primary->GetAsInt64());
                }
                if (column.add_ts_idx()) {
                    dimensions->push_back(std::make_pair(
                        std::to_string(primary->GetAsInt64()), idx_cnt));
                    idx_cnt++;
                }
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetAsInt64();
                ok = rb.AppendInt64(primary->GetAsInt64());
                break;
            }
            case ::rtidb::type::kFloat: {
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetDouble();
                ok = rb.AppendFloat(static_cast<float>(primary->GetDouble()));
                break;
            }
            case ::rtidb::type::kDouble: {
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetDouble();
                ok = rb.AppendDouble(primary->GetDouble());
                break;
            }
            case ::rtidb::type::kVarchar:
            case ::rtidb::type::kString: {
                ok = rb.AppendString(primary->GetStr(),
                                     strlen(primary->GetStr()));
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << std::string(primary->GetStr(),
                                          strlen(primary->GetStr()));
                if (column.add_ts_idx()) {
                    dimensions->push_back(
                        std::make_pair(std::string(primary->GetStr(),
                                                   strlen(primary->GetStr())),
                                       idx_cnt));
                    idx_cnt++;
                }
                break;
            }
            case ::rtidb::type::kTimestamp: {
                if (column.is_ts_col()) {
                    ts_dimensions->push_back(primary->GetAsInt64());
                }
                ok = rb.AppendTimestamp(primary->GetAsInt64());
                break;
            }
            case ::rtidb::type::kDate: {
                int32_t year;
                int32_t month;
                int32_t day;
                if (!primary->GetAsDate(&year, &month, &day)) {
                    ok = false;
                } else {
                    ok = rb.AppendDate(year, month, day);
                }
                break;
            }
            default: {
                LOG(WARNING) << " not supported type";
                return false;
            }
        }
        if (!ok) {
            LOG(WARNING) << "fail encode column " << column.type();
            return false;
        }
    }
    return true;
}

bool SQLClusterRouter::EncodeFormat(
    const catalog::RtiDBSchema& schema,
    const ::fesql::node::InsertPlanNode* plan, std::string* value,
    std::vector<std::pair<std::string, uint32_t>>* dimensions,
    std::vector<uint64_t>* ts_dimensions) {
    if (plan == NULL || value == NULL || dimensions == NULL ||
        ts_dimensions == NULL)
        return false;
    auto insert_stmt = plan->GetInsertNode();
    if (nullptr == insert_stmt ||
        insert_stmt->values_.size() != schema.size()) {
        LOG(WARNING) << "insert stmt is null or schema mismatch";
        return false;
    }
    std::map<std::string, uint32_t> column_idx;
    uint32_t str_size = 0;
    // TODO(wangtaize) use a safe way
    for (auto value : insert_stmt->values_) {
        if (value->GetExprType() == ::fesql::node::kExprPrimary) {
            ::fesql::node::ConstNode* primary =
                dynamic_cast<::fesql::node::ConstNode*>(value);
            if (primary->GetDataType() == ::fesql::node::kVarchar) {
                str_size += strlen(primary->GetStr());
            }
        }
    }
    DLOG(INFO) << "row str size " << str_size;
    ::rtidb::codec::RowBuilder rb(schema);
    uint32_t row_size = rb.CalTotalLength(str_size);
    value->resize(row_size);
    char* buf = reinterpret_cast<char*>(&(value->at(0)));
    bool ok = rb.SetBuffer(reinterpret_cast<int8_t*>(buf), row_size);
    if (!ok) {
        return false;
    }
    int32_t idx_cnt = 0;
    for (int32_t i = 0; i < schema.size(); i++) {
        const ::rtidb::common::ColumnDesc& column = schema.Get(i);
        const ::fesql::node::ConstNode* primary =
            dynamic_cast<const ::fesql::node::ConstNode*>(
                insert_stmt->values_.at(i));
        switch (column.data_type()) {
            case ::rtidb::type::kSmallInt: {
                ok = rb.AppendInt16(primary->GetSmallInt());
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetSmallInt();
                if (column.add_ts_idx()) {
                    dimensions->push_back(std::make_pair(
                        std::to_string(primary->GetSmallInt()), idx_cnt));
                    idx_cnt++;
                }
                break;
            }
            case ::rtidb::type::kInt: {
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetInt();
                ok = rb.AppendInt32(primary->GetInt());
                if (column.add_ts_idx()) {
                    dimensions->push_back(std::make_pair(
                        std::to_string(primary->GetInt()), idx_cnt));
                    idx_cnt++;
                }
                break;
            }
            case ::rtidb::type::kBigInt: {
                if (column.is_ts_col()) {
                    ts_dimensions->push_back(primary->GetInt());
                }
                if (column.add_ts_idx()) {
                    dimensions->push_back(std::make_pair(
                        std::to_string(primary->GetInt()), idx_cnt));
                    idx_cnt++;
                }
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetInt();
                ok = rb.AppendInt64(primary->GetInt());
                break;
            }
            case ::rtidb::type::kFloat: {
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetDouble();
                ok = rb.AppendFloat(static_cast<float>(primary->GetDouble()));
                break;
            }
            case ::rtidb::type::kDouble: {
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << primary->GetDouble();
                ok = rb.AppendDouble(primary->GetDouble());
                break;
            }
            case ::rtidb::type::kVarchar:
            case ::rtidb::type::kString: {
                ok = rb.AppendString(primary->GetStr(),
                                     strlen(primary->GetStr()));
                DLOG(INFO) << "parse column " << column.name() << " with value "
                           << std::string(primary->GetStr(),
                                          strlen(primary->GetStr()));
                if (column.add_ts_idx()) {
                    dimensions->push_back(
                        std::make_pair(std::string(primary->GetStr(),
                                                   strlen(primary->GetStr())),
                                       idx_cnt));
                    idx_cnt++;
                }
                break;
            }
            case ::rtidb::type::kTimestamp: {
                if (column.is_ts_col()) {
                    ts_dimensions->push_back(primary->GetInt());
                }
                ok = rb.AppendTimestamp(primary->GetInt());
                break;
            }
            default: {
                LOG(WARNING) << " not supported type";
                return false;
            }
        }
        if (!ok) {
            LOG(WARNING) << "fail encode column " << column.type();
            return false;
        }
    }
    return true;
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
