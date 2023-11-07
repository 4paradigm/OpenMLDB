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

#ifndef SRC_SDK_DB_SDK_H_
#define SRC_SDK_DB_SDK_H_

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base/spinlock.h"
#include "catalog/sdk_catalog.h"
#include "client/ns_client.h"
#include "client/tablet_client.h"
#include "client/taskmanager_client.h"
#include "common/thread_pool.h"
#include "vm/catalog.h"
#include "vm/engine.h"
#include "zk/zk_client.h"

namespace openmldb::sdk {

using openmldb::catalog::Procedures;

struct ClusterOptions {
    std::string zk_cluster;
    std::string zk_path;
    int32_t zk_session_timeout = 2000;
    int32_t zk_log_level = 3;
    std::string zk_log_file;
    std::string zk_auth_schema;
    std::string zk_cert;
    std::string to_string() {
        std::stringstream ss;
        ss << "zk options [cluster:" << zk_cluster << ", path:" << zk_path
           << ", zk_session_timeout:" << zk_session_timeout
           << ", log_level:" << zk_log_level << ", log_file:" << zk_log_file
           << ", zk_auth_schema:" << zk_auth_schema << ", zk_cert:" << zk_cert << "]";
        return ss.str();
    }
};

class DBSDK {
 public:
    virtual ~DBSDK() { delete engine_; }
    // create engine, then build the catalog
    // TODO(hw): should prevent double init
    virtual bool Init() = 0;

    virtual bool IsClusterMode() const = 0;

    virtual zk::ZkClient* GetZkClient() = 0;

    bool Refresh() { return BuildCatalog(); }

    inline uint64_t GetClusterVersion() { return cluster_version_.load(std::memory_order_relaxed); }

    inline std::shared_ptr<::openmldb::catalog::SDKCatalog> GetCatalog() {
        std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
        return catalog_;
    }
    inline ::hybridse::vm::Engine* GetEngine() { return engine_; }

    std::shared_ptr<::openmldb::client::NsClient> GetNsClient();

    std::shared_ptr<::openmldb::client::TaskManagerClient> GetTaskManagerClient();

    std::vector<std::string> GetAllDbs();
    uint32_t GetTableId(const std::string& db, const std::string& tname);
    std::shared_ptr<::openmldb::nameserver::TableInfo> GetTableInfo(const std::string& db, const std::string& tname);
    std::vector<std::shared_ptr<::openmldb::nameserver::TableInfo>> GetTables(const std::string& db);
    std::vector<std::string> GetAllTables();
    std::vector<std::string> GetTableNames(const std::string& db);
    std::shared_ptr<::openmldb::catalog::TabletAccessor> GetTablet();
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> GetAllTablet();
    bool GetTablet(const std::string& db, const std::string& name,
                   std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>>* tablets);
    std::shared_ptr<::openmldb::catalog::TabletAccessor> GetTablet(const std::string& db, const std::string& name);
    std::shared_ptr<::openmldb::catalog::TabletAccessor> GetTablet(const std::string& db, const std::string& name,
                                                                   uint32_t pid);
    std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>> GetTabletFollowers(const std::string& db,
                                                                                         const std::string& name,
                                                                                         uint32_t pid);
    std::shared_ptr<::openmldb::catalog::TabletAccessor> GetTablet(const std::string& db, const std::string& name,
                                                                   const std::string& pk);

    std::shared_ptr<hybridse::sdk::ProcedureInfo> GetProcedureInfo(const std::string& db, const std::string& sp_name,
                                                                   std::string* msg);
    std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> GetProcedureInfo(std::string* msg);
    virtual bool TriggerNotify(::openmldb::type::NotifyType type) const = 0;

    virtual bool GetNsAddress(std::string* endpoint, std::string* real_endpoint) = 0;

    bool RegisterExternalFun(const std::shared_ptr<openmldb::common::ExternalFun>& fun);
    bool RemoveExternalFun(const std::string& name);

 protected:
    virtual bool GetTaskManagerAddress(std::string* endpoint, std::string* real_endpoint) = 0;
    // build client_manager, then create a new catalog, replace the catalog in engine
    virtual bool BuildCatalog() = 0;

    DBSDK() : client_manager_(new catalog::ClientManager), catalog_(new catalog::SDKCatalog(client_manager_)) {}

    static std::string GetFunSignature(const openmldb::common::ExternalFun& fun);
    bool InitExternalFun();

 protected:
    std::atomic<uint64_t> cluster_version_{0};
    ::openmldb::base::Random rand_{0xdeadbeef};

    ::openmldb::base::SpinMutex mu_;
    std::shared_ptr<::openmldb::catalog::ClientManager> client_manager_;
    std::shared_ptr<::openmldb::catalog::SDKCatalog> catalog_;
    std::map<std::string, std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>> table_to_tablets_;

    ::hybridse::vm::Engine* engine_ = nullptr;
    std::map<std::string, std::shared_ptr<openmldb::common::ExternalFun>> external_fun_;

    // get/set op should be atomic(actually no reset now)
    std::shared_ptr<::openmldb::client::NsClient> ns_client_;
    std::shared_ptr<::openmldb::client::TaskManagerClient> taskmanager_client_;
};

class ClusterSDK : public DBSDK {
 public:
    explicit ClusterSDK(const ClusterOptions& options);

    ~ClusterSDK() override;
    bool Init() override;
    bool IsClusterMode() const override { return true; }
    bool TriggerNotify(::openmldb::type::NotifyType type) const override;

    zk::ZkClient* GetZkClient() override { return zk_client_; }
    const ClusterOptions& GetClusterOptions() const { return options_; }

    bool GetNsAddress(std::string* endpoint, std::string* real_endpoint) override;

    void RefreshExternalFun(const std::vector<std::string>& funs);

 protected:
    bool BuildCatalog() override;
    bool GetTaskManagerAddress(std::string* endpoint, std::string* real_endpoint) override;

 private:
    bool GetRealEndpointFromZk(const std::string& endpoint, std::string* real_endpoint);
    bool UpdateCatalog(const std::vector<std::string>& table_datas, const std::vector<std::string>& sp_datas);
    bool InitTabletClient();
    void WatchNotify();
    void CheckZk();
    void RefreshNsClient(const std::vector<std::string>& leader_children);
    void RefreshTaskManagerClient();

 private:
    ClusterOptions options_;
    uint64_t session_id_;
    std::string table_root_path_;
    std::string sp_root_path_;
    std::string notify_path_;
    std::string globalvar_changed_notify_path_;
    std::string leader_path_;
    std::string taskmanager_leader_path_;

    ::openmldb::zk::ZkClient* zk_client_;
    ::baidu::common::ThreadPool pool_;
};

class StandAloneSDK : public DBSDK {
 public:
    StandAloneSDK(std::string host, int port) : host_(std::move(host)), port_(port) {}

    ~StandAloneSDK() override { pool_.Stop(false); }
    bool Init() override;

    zk::ZkClient* GetZkClient() override { return nullptr; }

    bool IsClusterMode() const override { return false; }
    // kTable for normal table and kGlobalVar for global var table, return true directly in standalone
    bool TriggerNotify(::openmldb::type::NotifyType type) const override {
        if (type == ::openmldb::type::kTable) {
            return true;
        } else if (type == ::openmldb::type::kGlobalVar) {
            return true;
        }
        DLOG(ERROR) << "unsupport notify type";
        return false;
    }

    const std::string& GetHost() const { return host_; }

    int GetPort() const { return port_; }

    // Before connecting to ns, we only have the host&port
    // NOTICE: when we call this method, we do not have the correct ns client, do not GetNsClient.
    bool GetNsAddress(std::string* endpoint, std::string* real_endpoint) override {
        std::stringstream ss;
        ss << host_ << ":" << port_;
        *endpoint = ss.str();
        *real_endpoint = ss.str();
        return true;
    }

 protected:
    bool GetTaskManagerAddress(std::string* endpoint, std::string* real_endpoint) override {
        // Standalone mode does not provide TaskManager service
        return false;
    }

    bool BuildCatalog() override;

 private:
    bool PeriodicRefresh() {
        auto ok = BuildCatalog();
        // periodic refreshing
        pool_.DelayTask(2000, [this] { PeriodicRefresh(); });
        return ok;
    }

 private:
    std::string host_;
    int port_;
    ::baidu::common::ThreadPool pool_{1};
};

}  // namespace openmldb::sdk
#endif  // SRC_SDK_DB_SDK_H_
