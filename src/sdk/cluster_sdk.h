/*
 * cluster_sdk.h
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

#ifndef SRC_SDK_CLUSTER_SDK_H_
#define SRC_SDK_CLUSTER_SDK_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "base/spinlock.h"
#include "catalog/sdk_catalog.h"
#include "client/ns_client.h"
#include "client/tablet_client.h"
#include "thread_pool.h"  // NOLINT
#include "vm/catalog.h"
#include "zk/zk_client.h"

namespace rtidb {
namespace sdk {

struct ClusterOptions {
    std::string zk_cluster;
    std::string zk_path;
    int32_t session_timeout = 2000;
};

class ClusterSDK {
 public:
    explicit ClusterSDK(const ClusterOptions& options);

    ~ClusterSDK();

    bool Init();

    bool Refresh();

    inline uint64_t GetClusterVersion() {
        return cluster_version_.load(std::memory_order_relaxed);
    }

    inline std::shared_ptr<::fesql::vm::Catalog> GetCatalog() {
        std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
        return catalog_;
    }
    bool GetTabletByTable(
        const std::string& db, const std::string& tname,
        std::vector<std::shared_ptr<::rtidb::client::TabletClient>>* tablets);

    std::shared_ptr<::rtidb::client::TabletClient> GetLeaderTabletByTable(
        const std::string& db, const std::string& name);

    uint32_t GetTableId(const std::string& db, const std::string& tname);

    std::shared_ptr<::rtidb::nameserver::TableInfo> GetTableInfo(
        const std::string& db, const std::string& tname);
    std::vector<std::shared_ptr<::rtidb::nameserver::TableInfo>> GetTables(
        const std::string& db);

    inline std::shared_ptr<::rtidb::client::NsClient> GetNsClient() {
        if (ns_client_) return ns_client_;
        CreateNsClient();
        return ns_client_;
    }

 private:
    bool InitCatalog();
    bool RefreshCatalog(const std::vector<std::string>& table_datas);
    bool InitTabletClient();
    bool CreateNsClient();
    void WatchNotify();
    void CheckZk();

 private:
    std::atomic<uint64_t> cluster_version_;
    ClusterOptions options_;
    std::string nodes_root_path_;
    std::string table_root_path_;
    std::string notify_path_;
    ::rtidb::zk::ZkClient* zk_client_;
    ::rtidb::base::SpinMutex mu_;
    std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>>
        alive_tablets_;
    std::map<
        std::string,
        std::map<std::string, std::shared_ptr<::rtidb::nameserver::TableInfo>>>
        table_to_tablets_;
    std::shared_ptr<::rtidb::catalog::SDKCatalog> catalog_;
    std::shared_ptr<::rtidb::client::NsClient> ns_client_;
    ::baidu::common::ThreadPool pool_;
    uint64_t session_id_;
    std::atomic<bool> running_;
};

}  // namespace sdk
}  // namespace rtidb
#endif  // SRC_SDK_CLUSTER_SDK_H_
