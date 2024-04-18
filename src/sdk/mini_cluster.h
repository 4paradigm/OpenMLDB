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

#ifndef SRC_SDK_MINI_CLUSTER_H_
#define SRC_SDK_MINI_CLUSTER_H_

#include <sched.h>
#include <unistd.h>

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "auth/brpc_authenticator.h"
#include "auth/user_access_manager.h"
#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "brpc/server.h"
#include "client/ns_client.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "rpc/rpc_client.h"
#include "sdk/db_sdk.h"
#include "tablet/tablet_impl.h"
#include "test/util.h"

DECLARE_string(endpoint);
DECLARE_string(tablet);
DECLARE_string(db_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(request_timeout_ms);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_uint32(name_server_task_max_concurrency);
DECLARE_bool(auto_failover);
DECLARE_bool(enable_distsql);
DECLARE_uint32(system_table_replica_num);
DECLARE_uint32(get_table_diskused_interval);
DECLARE_uint32(sync_deploy_stats_timeout);

namespace openmldb {
namespace sdk {

constexpr int MAX_TABLET_NUM = 3;
#ifdef __linux__
#pragma pack(8)
#endif
class MiniCluster {
 public:
    explicit MiniCluster(int32_t zk_port)
        : zk_port_(zk_port), ns_(), tablet_num_(2), zk_cluster_(), zk_path_(), ns_client_(NULL) {}

    ~MiniCluster() {
        if (tablet_user_access_manager_) {
            delete tablet_user_access_manager_;
            tablet_user_access_manager_ = nullptr;
        }

        if (user_access_manager_) {
            delete user_access_manager_;
            user_access_manager_ = nullptr;
        }

        if (ns_authenticator_) {
            delete ns_authenticator_;
            ns_authenticator_ = nullptr;
        }

        for (const auto& kv : tb_clients_) {
            delete kv.second;
        }

        if (ns_client_) {
            delete ns_client_;
        }

        if (ts_authenticator_) {
            delete ts_authenticator_;
        }
    }

    bool SetUp(int tablet_num = 2) {
        LOG(INFO) << "start tablet number " << tablet_num;
        if (tablet_num > MAX_TABLET_NUM) {
            return false;
        }
        FLAGS_system_table_replica_num = 1;
        // lower diskused pull interval needed by SHOW TABLE STATUS tests
        FLAGS_get_table_diskused_interval = 2000;
        FLAGS_sync_deploy_stats_timeout = 2000;
        srand(time(NULL));
        zk_cluster_ = "127.0.0.1:" + std::to_string(zk_port_);
        FLAGS_zk_cluster = zk_cluster_;
        std::string ns_endpoint = "127.0.0.1:" + GenRand();
        zk_path_ = "/mini_cluster_" + GenRand();
        sleep(1);
        LOG(INFO) << "zk cluster " << zk_cluster_ << " zk path " << zk_path_
                  << " enable_distsql = " << FLAGS_enable_distsql;
        tablet_num_ = tablet_num;
        for (int i = 0; i < tablet_num; i++) {
            if (!StartTablet(&tb_servers_[i])) {
                LOG(WARNING) << "fail to start tablet";
                return false;
            }
        }
        sleep(4);
        nameserver = new ::openmldb::nameserver::NameServerImpl();
        bool ok = nameserver->Init(zk_cluster_, zk_path_, ns_endpoint, "");
        if (!ok) {
            return false;
        }
        if (!nameserver->GetTableInfo(::openmldb::nameserver::USER_INFO_NAME, ::openmldb::nameserver::INTERNAL_DB,
                                      &user_table_info_)) {
            PDLOG(WARNING, "Failed to get table info for user table");
            return false;
        }
        user_access_manager_ = new openmldb::auth::UserAccessManager(
            nameserver->GetSystemTableIterator(),
            std::make_unique<::openmldb::codec::Schema>(user_table_info_->column_desc()));
        ns_authenticator_ = new openmldb::authn::BRPCAuthenticator(
            [this](const std::string& host, const std::string& username, const std::string& password) {
                return user_access_manager_->IsAuthenticated(host, username, password);
            });
        brpc::ServerOptions options;
        options.auth = ns_authenticator_;
        if (ns_.AddService(nameserver, brpc::SERVER_OWNS_SERVICE) != 0) {
            LOG(WARNING) << "fail to add ns";
            return false;
        }
        if (ns_.Start(ns_endpoint.c_str(), &options) != 0) {
            LOG(WARNING) << "fail to start ns";
            return false;
        }
        sleep(2);
        ns_client_ = new ::openmldb::client::NsClient(ns_endpoint, "");
        if (ns_client_->Init() != 0) {
            LOG(WARNING) << "fail to init ns client";
            return false;
        }
        ns_endpoint_ = ns_endpoint;
        LOG(INFO) << "start mini cluster with zk cluster " << zk_cluster_ << " and zk path " << zk_path_;
        LOG(INFO) << "----- ns " << ns_endpoint;
        for (auto tb_endpoint : tb_endpoints_) {
            LOG(INFO) << "----- tb " << tb_endpoint;
        }
        return true;
    }

    void Close() {
        if (tablet_user_access_manager_) {
            delete tablet_user_access_manager_;
            tablet_user_access_manager_ = nullptr;
        }

        if (user_access_manager_) {
            delete user_access_manager_;
            user_access_manager_ = nullptr;
        }

        if (ns_authenticator_) {
            delete ns_authenticator_;
            ns_authenticator_ = nullptr;
        }
        nameserver->CloseThreadpool();
        ns_.Stop(10);
        ns_.Join();

        for (int i = 0; i < tablet_num_; i++) {
            tb_servers_[i].Stop(10);
            tb_servers_[i].Join();
        }
    }

    std::string GetZkCluster() { return zk_cluster_; }

    std::string GetZkPath() { return zk_path_; }

    ::openmldb::client::NsClient* GetNsClient() { return ns_client_; }

    ::openmldb::tablet::TabletImpl* GetTablet(const std::string& endpoint) {
        auto iter = tablets_.find(endpoint);
        if (iter != tablets_.end()) {
            return iter->second;
        }
        return nullptr;
    }

    ::openmldb::client::TabletClient* GetTabletClient(const std::string& endpoint) {
        auto iter = tb_clients_.find(endpoint);
        if (iter != tb_clients_.end()) {
            return iter->second;
        }
        return nullptr;
    }

    std::string GenRand() {
        return std::to_string(rand() % 1000 + 10000);  // NOLINT
    }

    const std::vector<std::string>& GetTbEndpoint() const { return tb_endpoints_; }

    const std::string& GetNsEndpoint() const { return ns_endpoint_; }

 private:
    bool StartTablet(brpc::Server* tb_server) {
        std::string tb_endpoint = "127.0.0.1:" + GenRand();
        tb_endpoints_.push_back(tb_endpoint);
        ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
        bool ok = tablet->Init(zk_cluster_, zk_path_, tb_endpoint, "");
        if (!ok) {
            return false;
        }

        tablet_user_access_manager_ = new openmldb::auth::UserAccessManager(
            tablet->GetSystemTableIterator(), tablet->GetSystemTableColumnDesc(::openmldb::nameserver::USER_INFO_NAME));
        ts_authenticator_ = new openmldb::authn::BRPCAuthenticator(
            [this](const std::string& host, const std::string& username, const std::string& password) {
                return tablet_user_access_manager_->IsAuthenticated(host, username, password);
            });

        brpc::ServerOptions options;
        options.auth = ts_authenticator_;

        if (tb_server->AddService(tablet, brpc::SERVER_OWNS_SERVICE) != 0) {
            LOG(WARNING) << "fail to add tablet";
            return false;
        }
        if (tb_server->Start(tb_endpoint.c_str(), &options) != 0) {
            LOG(WARNING) << "fail to start tablet";
            return false;
        }
        ok = tablet->RegisterZK();
        if (!ok) {
            return false;
        }
        LOG(INFO) << "start tablet " << tb_endpoint;
        tablets_.emplace(tb_endpoint, tablet);
        sleep(2);
        auto* client = new ::openmldb::client::TabletClient(tb_endpoint, tb_endpoint);
        if (client->Init() < 0) {
            LOG(WARNING) << "fail to init client";
            return false;
        }
        tb_clients_.emplace(tb_endpoint, client);
        return true;
    }
    ::openmldb::nameserver::NameServerImpl* nameserver;
    int32_t zk_port_;
    brpc::Server ns_;
    int32_t tablet_num_;
    brpc::Server tb_servers_[MAX_TABLET_NUM];
    std::string ns_endpoint_;
    std::vector<std::string> tb_endpoints_;
    std::string zk_cluster_;
    std::string zk_path_;
    ::openmldb::client::NsClient* ns_client_;
    std::map<std::string, ::openmldb::tablet::TabletImpl*> tablets_;
    std::map<std::string, ::openmldb::client::TabletClient*> tb_clients_;
    openmldb::authn::BRPCAuthenticator* ns_authenticator_;
    openmldb::authn::BRPCAuthenticator* ts_authenticator_;
    openmldb::auth::UserAccessManager* user_access_manager_;
    openmldb::auth::UserAccessManager* tablet_user_access_manager_;
    std::shared_ptr<::openmldb::nameserver::TableInfo> user_table_info_;
};

class StandaloneEnv {
 public:
    StandaloneEnv() : ns_(), ns_client_(nullptr), tb_client_(nullptr) {}
    ~StandaloneEnv() {
        if (tablet_user_access_manager_) {
            delete tablet_user_access_manager_;
            tablet_user_access_manager_ = nullptr;
        }

        if (user_access_manager_) {
            delete user_access_manager_;
            user_access_manager_ = nullptr;
        }

        if (ns_authenticator_) {
            delete ns_authenticator_;
            ns_authenticator_ = nullptr;
        }
        if (tb_client_) {
            delete tb_client_;
        }
        if (ns_client_) {
            delete ns_client_;
        }
        if (ts_authenticator_) {
            delete ts_authenticator_;
        }
    }

    bool SetUp() {
        srand(time(nullptr));
        // shit happens, cluster & standalone require distinct db_root_path
        test::TempPath tmp;
        FLAGS_db_root_path = tmp.GetTempPath();
        if (!StartTablet(&tb_server_)) {
            LOG(WARNING) << "fail to start tablet";
            return false;
        }
        sleep(1);
        FLAGS_tablet = tb_endpoint_;
        // lower diskused pull interval needed by SHOW TABLE STATUS tests
        FLAGS_get_table_diskused_interval = 2000;
        FLAGS_sync_deploy_stats_timeout = 2000;
        ns_port_ = GenRand();
        std::string ns_endpoint = "127.0.0.1:" + std::to_string(ns_port_);
        nameserver = new ::openmldb::nameserver::NameServerImpl();
        bool ok = nameserver->Init("", "", ns_endpoint, "");
        if (!ok) {
            return false;
        }
        if (!nameserver->GetTableInfo(::openmldb::nameserver::USER_INFO_NAME, ::openmldb::nameserver::INTERNAL_DB,
                                      &user_table_info_)) {
            PDLOG(WARNING, "Failed to get table info for user table");
            return false;
        }
        user_access_manager_ = new openmldb::auth::UserAccessManager(
            nameserver->GetSystemTableIterator(),
            std::make_unique<::openmldb::codec::Schema>(user_table_info_->column_desc()));
        ns_authenticator_ = new openmldb::authn::BRPCAuthenticator(
            [this](const std::string& host, const std::string& username, const std::string& password) {
                return user_access_manager_->IsAuthenticated(host, username, password);
            });
        brpc::ServerOptions options;
        options.auth = ns_authenticator_;
        if (ns_.AddService(nameserver, brpc::SERVER_OWNS_SERVICE) != 0) {
            LOG(WARNING) << "fail to add ns";
            return false;
        }
        if (ns_.Start(ns_endpoint.c_str(), &options) != 0) {
            LOG(WARNING) << "fail to start ns";
            return false;
        }
        sleep(2);
        ns_client_ = new ::openmldb::client::NsClient(ns_endpoint, "");
        if (ns_client_->Init() != 0) {
            LOG(WARNING) << "fail to init ns client";
            return false;
        }
        ns_endpoint_ = ns_endpoint;
        LOG(INFO) << "start standalone env";
        LOG(INFO) << "----- ns " << ns_endpoint;
        LOG(INFO) << "----- tb " << tb_endpoint_;
        return true;
    }

    void Close() {
        if (tablet_user_access_manager_) {
            delete tablet_user_access_manager_;
            tablet_user_access_manager_ = nullptr;
        }

        if (user_access_manager_) {
            delete user_access_manager_;
            user_access_manager_ = nullptr;
        }

        if (ns_authenticator_) {
            delete ns_authenticator_;
            ns_authenticator_ = nullptr;
        }
        nameserver->CloseThreadpool();
        ns_.Stop(10);
        ns_.Join();
        tb_server_.Stop(10);
        tb_server_.Join();
    }

    ::openmldb::client::NsClient* GetNsClient() { return ns_client_; }

    ::openmldb::client::TabletClient* GetTabletClient() { return tb_client_; }

    uint64_t GetNsPort() const { return ns_port_; }

    const std::string& GetTbEndpoint() const { return tb_endpoint_; }

    const std::string& GetNsEndpoint() const { return ns_endpoint_; }

 private:
    uint64_t GenRand() { return rand() % 1000 + 10000; }

    bool StartTablet(brpc::Server* tb_server) {
        std::string tb_endpoint = "127.0.0.1:" + std::to_string(GenRand());
        tb_endpoint_ = tb_endpoint;
        ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
        bool ok = tablet->Init("", "", tb_endpoint, "");
        if (!ok) {
            return false;
        }

        tablet_user_access_manager_ = new openmldb::auth::UserAccessManager(
            tablet->GetSystemTableIterator(), tablet->GetSystemTableColumnDesc(::openmldb::nameserver::USER_INFO_NAME));
        ts_authenticator_ = new openmldb::authn::BRPCAuthenticator(
            [this](const std::string& host, const std::string& username, const std::string& password) {
                return tablet_user_access_manager_->IsAuthenticated(host, username, password);
            });
        brpc::ServerOptions options;
        options.auth = ts_authenticator_;
        if (tb_server->AddService(tablet, brpc::SERVER_OWNS_SERVICE) != 0) {
            LOG(WARNING) << "fail to add tablet";
            return false;
        }
        if (tb_server->Start(tb_endpoint.c_str(), &options) != 0) {
            LOG(WARNING) << "fail to start tablet";
            return false;
        }
        sleep(2);
        auto* client = new ::openmldb::client::TabletClient(tb_endpoint, tb_endpoint);
        if (client->Init() < 0) {
            LOG(WARNING) << "fail to init client";
            return false;
        }
        tb_client_ = client;
        return true;
    }
    ::openmldb::nameserver::NameServerImpl* nameserver;
    brpc::Server ns_;
    brpc::Server tb_server_;
    std::string ns_endpoint_;
    std::string tb_endpoint_;
    uint64_t ns_port_ = 0;
    ::openmldb::client::NsClient* ns_client_;
    ::openmldb::client::TabletClient* tb_client_;
    openmldb::authn::BRPCAuthenticator* ns_authenticator_;
    openmldb::authn::BRPCAuthenticator* ts_authenticator_;
    openmldb::auth::UserAccessManager* user_access_manager_;
    openmldb::auth::UserAccessManager* tablet_user_access_manager_;
    std::shared_ptr<::openmldb::nameserver::TableInfo> user_table_info_;
};

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_MINI_CLUSTER_H_
