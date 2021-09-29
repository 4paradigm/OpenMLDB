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
#include <utility>
#include <vector>

#include "base/spinlock.h"
#include "catalog/sdk_catalog.h"
#include "client/ns_client.h"
#include "client/tablet_client.h"
#include "common/thread_pool.h"
#include "vm/catalog.h"
#include "vm/engine.h"
#include "zk/zk_client.h"

namespace openmldb::sdk {

using openmldb::catalog::Procedures;

struct ClusterOptions {
    std::string zk_cluster;
    std::string zk_path;
    int32_t session_timeout = 2000;
};

class ClusterSDK {
 public:
    virtual ~ClusterSDK() { delete engine_; }
    // bind catalog and engine, then build the catalog
    virtual bool Init() = 0;
    bool Refresh() { return BuildCatalog(); }

    inline uint64_t GetClusterVersion() { return cluster_version_.load(std::memory_order_relaxed); }

    inline std::shared_ptr<::openmldb::catalog::SDKCatalog> GetCatalog() {
        std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
        return catalog_;
    }
    inline ::hybridse::vm::Engine* GetEngine() { return engine_; }

    inline std::shared_ptr<::openmldb::client::NsClient> GetNsClient() {
        auto ns_client = std::atomic_load_explicit(&ns_client_, std::memory_order_relaxed);
        if (ns_client) return ns_client;
        CreateNsClient();
        return std::atomic_load_explicit(&ns_client_, std::memory_order_relaxed);
    }
    virtual bool GetRealEndpoint(const std::string& endpoint, std::string* real_endpoint) = 0;

    uint32_t GetTableId(const std::string& db, const std::string& tname);
    std::shared_ptr<::openmldb::nameserver::TableInfo> GetTableInfo(const std::string& db, const std::string& tname);
    std::vector<std::shared_ptr<::openmldb::nameserver::TableInfo>> GetTables(const std::string& db);
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

 protected:
    // build client_manager, then create a new catalog, replace the catalog in engine
    // TODO(hw): recreate is more readable?
    virtual bool BuildCatalog() = 0;
    virtual bool CreateNsClient() = 0;

    ClusterSDK() : client_manager_(new catalog::ClientManager), catalog_(new catalog::SDKCatalog(client_manager_)) {}

    bool SetNsClient(const std::string& endpoint);

 protected:
    std::atomic<uint64_t> cluster_version_{0};
    ::openmldb::base::Random rand_{0xdeadbeef};

    ::openmldb::base::SpinMutex mu_;
    std::shared_ptr<::openmldb::catalog::ClientManager> client_manager_;
    std::shared_ptr<::openmldb::catalog::SDKCatalog> catalog_;
    std::map<std::string, std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>> table_to_tablets_;

    ::hybridse::vm::Engine* engine_ = nullptr;

 private:
    std::shared_ptr<::openmldb::client::NsClient> ns_client_;
};

class NormalClusterSDK : public ClusterSDK {
 public:
    explicit NormalClusterSDK(const ClusterOptions& options);

    ~NormalClusterSDK() override;
    bool Init() override;
    bool GetRealEndpoint(const std::string& endpoint, std::string* real_endpoint) override;

 protected:
    bool BuildCatalog() override;
    bool CreateNsClient() override;

 private:
    bool UpdateCatalog(const std::vector<std::string>& table_datas, const std::vector<std::string>& sp_datas);
    bool InitTabletClient();
    void WatchNotify();
    void CheckZk();

 private:
    ClusterOptions options_;
    uint64_t session_id_;
    std::string table_root_path_;
    std::string sp_root_path_;
    std::string notify_path_;
    ::openmldb::zk::ZkClient* zk_client_;
    ::baidu::common::ThreadPool pool_;
};

class StandAloneClusterSDK : public ClusterSDK {
 public:
    StandAloneClusterSDK(std::string host, int port) : host_(std::move(host)), port_(port) {}

    ~StandAloneClusterSDK() override = default;
    bool Init() override;
    bool GetRealEndpoint(const std::string& endpoint, std::string* real_endpoint) override {
        std::vector<client::TabletInfo> tablets;
        std::string msg;
        if (!GetNsClient()->ShowSdkEndpoint(tablets, msg)) {
            LOG(WARNING) << msg;
            return false;
        }
        for (const auto& tablet : tablets) {
            if (tablet.endpoint == endpoint) {
                *real_endpoint = tablet.real_endpoint;
                return true;
            }
        }
        // TODO(hw): RegisterName /map/names, how to get it from ns?

        return false;
    }

 protected:
    bool BuildCatalog() override {
        // InitTabletClients
        std::vector<client::TabletInfo> tablets;
        std::string msg;
        if (!GetNsClient()->ShowTablet(tablets, msg)) {
            LOG(WARNING) << msg;
            return false;
        }
        std::map<std::string, std::string> real_ep_map;
        for (const auto& tablet : tablets) {
            std::string cur_endpoint = ::openmldb::base::ExtractEndpoint(tablet.endpoint);
            // TODO(hw): use tablet.real_endpoint?
            std::string real_endpoint;
            if (!GetRealEndpoint(cur_endpoint, &real_endpoint)) {
                return false;
            }
            real_ep_map.emplace(cur_endpoint, real_endpoint);
        }
        client_manager_->UpdateClient(real_ep_map);

        // tables
        std::vector<::openmldb::nameserver::TableInfo> tables;
        if (!GetNsClient()->ShowAllTable(tables, msg)) {
            LOG(WARNING) << msg;
            return false;
        }
        // table_to_tablets_
        std::map<std::string, std::map<std::string, std::shared_ptr<nameserver::TableInfo>>> mapping;
        auto new_catalog = std::make_shared<catalog::SDKCatalog>(client_manager_);
        for (const auto& table : tables) {
            auto& db_map = mapping[table.db()];
            db_map[table.name()] = std::make_shared<nameserver::TableInfo>(table);
            DLOG(INFO) << "load table info with name " << table.name() << " in db " << table.db();
        }
        // TODO(hw): no show procedure api

        // TODO(hw): use tables and sp map(no sp map now) to init a new catalog
        if (!new_catalog->Init(tables, {})) {
            LOG(WARNING) << "fail to init catalog";
            return false;
        }
        {
            std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
            table_to_tablets_ = mapping;
            catalog_ = new_catalog;
        }
        engine_->UpdateCatalog(new_catalog);

        return true;
    }
    bool CreateNsClient() override {
        std::stringstream ss;
        ss << host_ << ":" << port_;
        return SetNsClient(ss.str());
    }

 private:
    std::string host_;
    int port_;
};

}  // namespace openmldb::sdk
#endif  // SRC_SDK_CLUSTER_SDK_H_
