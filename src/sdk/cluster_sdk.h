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
#include "common/thread_pool.h"
#include "vm/catalog.h"
#include "vm/engine.h"
#include "zk/zk_client.h"

namespace openmldb {
namespace sdk {

using openmldb::catalog::Procedures;

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

    inline uint64_t GetClusterVersion() { return cluster_version_.load(std::memory_order_relaxed); }

    inline std::shared_ptr<::openmldb::catalog::SDKCatalog> GetCatalog() {
        std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
        return catalog_;
    }
    uint32_t GetTableId(const std::string& db, const std::string& tname);

    std::shared_ptr<::openmldb::nameserver::TableInfo> GetTableInfo(const std::string& db, const std::string& tname);
    std::vector<std::shared_ptr<::openmldb::nameserver::TableInfo>> GetTables(const std::string& db);

    inline std::shared_ptr<::openmldb::client::NsClient> GetNsClient() {
        auto ns_client = std::atomic_load_explicit(&ns_client_, std::memory_order_relaxed);
        if (ns_client) return ns_client;
        CreateNsClient();
        return std::atomic_load_explicit(&ns_client_, std::memory_order_relaxed);
    }
    bool GetRealEndpoint(const std::string& endpoint, std::string* real_endpoint);

    std::shared_ptr<::openmldb::catalog::TabletAccessor> GetTablet();
    bool GetTablet(const std::string& db, const std::string& name,
                   std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>>* tablets);
    std::shared_ptr<::openmldb::catalog::TabletAccessor> GetTablet(const std::string& db, const std::string& name);
    std::shared_ptr<::openmldb::catalog::TabletAccessor> GetTablet(const std::string& db, const std::string& name,
                                                                   uint32_t pid);
    std::shared_ptr<::openmldb::catalog::TabletAccessor> GetTablet(const std::string& db, const std::string& name,
                                                                   const std::string& pk);

    std::shared_ptr<hybridse::sdk::ProcedureInfo> GetProcedureInfo(const std::string& db, const std::string& sp_name,
                                                                   std::string* msg);

    std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> GetProcedureInfo(std::string* msg);

    inline ::hybridse::vm::Engine* GetEngine() { return engine_; }

 private:
    bool InitCatalog();
    bool RefreshCatalog(const std::vector<std::string>& table_datas, const std::vector<std::string>& sp_datas);
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
    ::openmldb::zk::ZkClient* zk_client_;
    ::openmldb::base::SpinMutex mu_;
    std::shared_ptr<::openmldb::catalog::ClientManager> client_manager_;
    std::map<std::string, std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>> table_to_tablets_;
    std::shared_ptr<::openmldb::catalog::SDKCatalog> catalog_;
    std::shared_ptr<::openmldb::client::NsClient> ns_client_;
    ::baidu::common::ThreadPool pool_;
    uint64_t session_id_;
    ::openmldb::base::Random rand_;
    std::string sp_root_path_;
    ::hybridse::vm::Engine* engine_;
};

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_CLUSTER_SDK_H_
