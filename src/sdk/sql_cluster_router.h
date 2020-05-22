/*
 * sql_cluster_router.h
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

#ifndef SRC_SDK_SQL_CLUSTER_ROUTER_H_
#define SRC_SDK_SQL_CLUSTER_ROUTER_H_

#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "client/tablet_client.h"
#include "sdk/cluster_sdk.h"
#include "sdk/sql_router.h"
#include "vm/engine.h"

namespace rtidb {
namespace sdk {

class SQLClusterRouter : public SQLRouter {
 public:
    explicit SQLClusterRouter(const SQLRouterOptions& options);
    ~SQLClusterRouter();

    bool Init();

    bool ExecuteInsert(const std::string& db, const std::string& sql,
                       ::fesql::sdk::Status* status);

    std::shared_ptr<::fesql::sdk::ResultSet> ExecuteSQL(
        const std::string& db, const std::string& sql,
        ::fesql::sdk::Status* status);

 private:
    bool GetTablet(
        const std::string& db, const std::string& sql,
        std::vector<std::shared_ptr<::rtidb::client::TabletClient>>* tablets);

    void GetTables(::fesql::vm::PhysicalOpNode* node,
                   std::set<std::string>* tables);

 private:
    SQLRouterOptions options_;
    ClusterSDK* cluster_sdk_;
    ::fesql::vm::Engine* engine_;
};

}  // namespace sdk
}  // namespace rtidb
#endif  // SRC_SDK_SQL_CLUSTER_ROUTER_H_
