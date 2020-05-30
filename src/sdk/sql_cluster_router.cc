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

namespace rtidb {
namespace sdk {

SQLClusterRouter::SQLClusterRouter(const SQLRouterOptions& options)
    : options_(options),
      cluster_sdk_(NULL),
      engine_(NULL),
      input_schema_map_(),
      mu_() {}

SQLClusterRouter::SQLClusterRouter(ClusterSDK* sdk)
    : options_(),
      cluster_sdk_(sdk),
      engine_(NULL),
      input_schema_map_(),
      mu_() {}


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
    engine_ = new ::fesql::vm::Engine(catalog, eopt);
    return true;
}

std::shared_ptr<SQLRequestRow> SQLClusterRouter::GetRequestRow(
    const std::string& db, const std::string& sql,
    ::fesql::sdk::Status* status) {
    if (status == NULL) return std::shared_ptr<SQLRequestRow>();
    {
        std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
        auto it = input_schema_map_.find(db);
        if (it != input_schema_map_.end()) {
            auto iit = it->second.find(sql);
            if (iit != it->second.end()) {
                status->code = 0;
                std::shared_ptr<SQLRequestRow> row(
                    new SQLRequestRow(iit->second));
                return row;
            }
        }
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
    std::shared_ptr<::fesql::sdk::SchemaImpl> schema(
        new ::fesql::sdk::SchemaImpl(explain.input_schema));
    {
        std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
        auto it = input_schema_map_.find(db);
        if (it == input_schema_map_.end()) {
            std::map<std::string, std::shared_ptr<::fesql::sdk::Schema>>
                sql_schema;
            sql_schema.insert(std::make_pair(sql, schema));
            input_schema_map_.insert(std::make_pair(db, sql_schema));
        } else {
            it->second.insert(std::make_pair(sql, schema));
        }
    }
    std::shared_ptr<SQLRequestRow> row(new SQLRequestRow(schema));
    return row;
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
    ok = tablets[0]->Query(db, sql, row->GetRow(), cntl.get(), response.get());
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
        return false;
    }
    std::shared_ptr<::rtidb::nameserver::TableInfo> table_info =
        cluster_sdk_->GetTableInfo(db, insert_stmt->table_name_);
    if (!table_info) {
        LOG(WARNING) << "table with name " << insert_stmt->table_name_
                     << " does not exist";
        return false;
    }
    std::string value;
    std::vector<std::pair<std::string, uint32_t>> dimensions;
    std::vector<uint64_t> ts;
    ok = EncodeFromat(table_info->column_desc_v1(), iplan, &value, &dimensions,
                      &ts);
    if (!ok) {
        LOG(WARNING) << "fail to encode row for table "
                     << insert_stmt->table_name_;
        return false;
    }
    std::shared_ptr<::rtidb::client::TabletClient> tablet =
        cluster_sdk_->GetLeaderTabletByTable(db, insert_stmt->table_name_);
    if (!tablet) {
        LOG(WARNING) << "fail to get table " << insert_stmt->table_name_
                     << " tablet";
        return false;
    }
    DLOG(INFO) << "put data to endpoint " << tablet->GetEndpoint()
               << " with dimensions size " << dimensions.size();
    ok = tablet->Put(table_info->tid(), 0, dimensions, ts, value, 1);
    if (!ok) return false;
    return true;
}

bool SQLClusterRouter::EncodeFromat(
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

}  // namespace sdk
}  // namespace rtidb
