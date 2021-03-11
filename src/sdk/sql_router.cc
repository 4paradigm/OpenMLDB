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


#include "sdk/sql_router.h"

#include "glog/logging.h"
#include "sdk/sql_cluster_router.h"

namespace rtidb {
namespace sdk {

std::shared_ptr<SQLRouter> NewClusterSQLRouter(
    const SQLRouterOptions& options) {
    auto router = std::make_shared<SQLClusterRouter>(options);
    if (!router->Init()) {
        LOG(WARNING) << "fail to init sql cluster router";
        return std::shared_ptr<SQLRouter>();
    }
    return router;
}

}  // namespace sdk
}  // namespace rtidb
