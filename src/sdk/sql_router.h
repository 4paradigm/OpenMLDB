/*
 * sql_router.h
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

#ifndef SRC_SDK_SQL_ROUTER_H_
#define SRC_SDK_SQL_ROUTER_H_

#include "sdk/base.h"
#include "sdk/result_set.h"

namespace rtidb {
namespace sdk {

struct SQLRouterOptions {
    std::string zk_cluster;
    std::string zk_path;
    uint32_t session_timeout = 2000;
};

class SQLRouter {
 public:
    SQLRouter() {}
    virtual ~SQLRouter() {}
    virtual bool ExecuteInsert(const std::string& db, const std::string& sql,
            fesql::sdk::Status* status) = 0;

    virtual std::shared_ptr<fesql::sdk::ResultSet> ExecuteSQL(
        const std::string& db, const std::string& sql,
        fesql::sdk::Status* status) = 0;
};

std::shared_ptr<SQLRouter> NewClusterSQLRouter(const SQLRouterOptions& options);

}  // namespace sdk
}  // namespace rtidb
#endif  // SRC_SDK_SQL_ROUTER_H_
