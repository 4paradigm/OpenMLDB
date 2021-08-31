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
#include <string>
#include <vector>

#include "base/file_util.h"
#include "base/glog_wapper.h"
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
#include "sdk/cluster_sdk.h"
#include "tablet/tablet_impl.h"

DECLARE_string(endpoint);
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
    ~MiniCluster() {}
    bool SetUp(int tablet_num = 2) {
        if (tablet_num > MAX_TABLET_NUM) {
            return false;
        }
        srand(time(NULL));
        FLAGS_db_root_path = "/tmp/mini_cluster" + GenRand();
        zk_cluster_ = "127.0.0.1:" + std::to_string(zk_port_);
        FLAGS_zk_cluster = zk_cluster_;
        std::string ns_endpoint = "127.0.0.1:" + GenRand();
        zk_path_ = "/mini_cluster_" + GenRand();
        sleep(1);
        LOG(INFO) << "zk cluster " << zk_cluster_ << " zk path " << zk_path_
                  << " enable_distsql = " << FLAGS_enable_distsql;
        ::openmldb::nameserver::NameServerImpl* nameserver = new ::openmldb::nameserver::NameServerImpl();
        bool ok = nameserver->Init(zk_cluster_, zk_path_, ns_endpoint, "");
        if (!ok) {
            return false;
        }
        brpc::ServerOptions options;
        if (ns_.AddService(nameserver, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(WARNING) << "fail to start ns";
            return false;
        }
        if (ns_.Start(ns_endpoint.c_str(), &options) != 0) {
            return false;
        }
        sleep(2);
        ns_client_ = new ::openmldb::client::NsClient(ns_endpoint, "");
        if (ns_client_->Init() != 0) {
            LOG(WARNING) << "fail to init ns client";
            return false;
        }
        tablet_num_ = tablet_num;
        for (int i = 0; i < tablet_num; i++) {
            if (!StartTablet(&tb_servers_[i])) {
                LOG(WARNING) << "fail to start tablet";
                return false;
            }
        }
        LOG(INFO) << "start mini cluster with zk cluster " << zk_cluster_ << " and zk path " << zk_path_;
        LOG(INFO) << "----- ns " << ns_endpoint;
        for (auto tb_endpoint : tb_endpoints_) {
            LOG(INFO) << "----- tb " << tb_endpoint;
        }
        return true;
    }

    void Close() {
        ns_.Stop(10);
        for (int i = 0; i < tablet_num_; i++) {
            tb_servers_[i].Stop(10);
        }
        for (const auto& kv : tb_clients_) {
            delete kv.second;
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

 private:
    bool StartTablet(brpc::Server* tb_server) {
        std::string tb_endpoint = "127.0.0.1:" + GenRand();
        tb_endpoints_.push_back(tb_endpoint);
        ::openmldb::tablet::TabletImpl* tablet = new ::openmldb::tablet::TabletImpl();
        bool ok = tablet->Init(zk_cluster_, zk_path_, tb_endpoint, "");
        if (!ok) {
            return false;
        }
        brpc::ServerOptions ts_opt;
        if (tb_server->AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(WARNING) << "fail to start tablet";
            return false;
        }
        if (tb_server->Start(tb_endpoint.c_str(), &ts_opt) != 0) {
            return false;
        }
        ok = tablet->RegisterZK();
        if (!ok) {
            return false;
        }
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

    int32_t zk_port_;
    brpc::Server ns_;
    int32_t tablet_num_;
    brpc::Server tb_servers_[MAX_TABLET_NUM];
    std::vector<std::string> tb_endpoints_;
    std::string zk_cluster_;
    std::string zk_path_;
    ::openmldb::client::NsClient* ns_client_;
    std::map<std::string, ::openmldb::tablet::TabletImpl*> tablets_;
    std::map<std::string, ::openmldb::client::TabletClient*> tb_clients_;
};

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_MINI_CLUSTER_H_
