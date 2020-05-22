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

#include <string>
#include <memory>
#include <utility>
#include "brpc/channel.h"
#include "glog/logging.h"
#include "proto/tablet.pb.h"
#include "sdk/base.h"
#include "sdk/result_set_sql.h"

namespace rtidb {
namespace sdk {

SQLClusterRouter::SQLClusterRouter(const SQLRouterOptions& options)
    : options_(options), cluster_sdk_(NULL), engine_(NULL) {}

SQLClusterRouter::~SQLClusterRouter() {}

bool SQLClusterRouter::Init() {
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
    ::fesql::vm::Engine::InitializeGlobalLLVM();
    std::shared_ptr<::fesql::vm::Catalog> catalog = cluster_sdk_->GetCatalog();
    ::fesql::vm::EngineOptions eopt;
    eopt.set_compile_only(true);
    engine_ = new ::fesql::vm::Engine(catalog, eopt);
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
    ok = tablets[0]->Query(db, sql, cntl.get(), response.get());
    if (!ok) {
        return std::shared_ptr<::fesql::sdk::ResultSet>();
    }
    std::shared_ptr<::rtidb::sdk::ResultSetSQL> rs(
        new rtidb::sdk::ResultSetSQL(std::move(response), std::move(cntl)));
    rs->Init();
    return rs;
}

}  // namespace sdk
}  // namespace rtidb
