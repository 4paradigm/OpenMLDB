/*
 * mini_cluster.h
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

#ifndef SRC_SDK_MINI_CLUSTER_H_
#define SRC_SDK_MINI_CLUSTER_H_

#include <sched.h>
#include <unistd.h>

#include <string>

#include "base/file_util.h"
#include "base/glog_wapper.h"  // NOLINT
#include "brpc/server.h"
#include "client/ns_client.h"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "rpc/rpc_client.h"
#include "sdk/cluster_sdk.h"
#include "tablet/tablet_impl.h"
#include "timer.h"  // NOLINT

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
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);

namespace rtidb {
namespace sdk {

class MiniCluster {
 public:
    explicit MiniCluster(int32_t zk_port)
        : zk_port_(zk_port),
          ns_(NULL),
          ts_(NULL),
          zk_cluster_(),
          zk_path_(),
          ns_client_(NULL) {}
    ~MiniCluster() {}
    bool SetUp() {
        ns_ = new brpc::Server();
        ts_ = new brpc::Server();
        srand(time(NULL));
        FLAGS_db_root_path = "/tmp/mini_cluster" + GenRand();
        zk_cluster_ = "127.0.0.1:" + std::to_string(zk_port_);
        std::string ns_endpoint = "127.0.0.1:" + GenRand();
        zk_path_ = "/mini_cluster_" + GenRand();
        sleep(1);
        LOG(INFO) << "zk cluster " << zk_cluster_ << " zk path " << zk_path_;
        ::rtidb::nameserver::NameServerImpl* nameserver =
            new ::rtidb::nameserver::NameServerImpl();
        bool ok = nameserver->Init(zk_cluster_, zk_path_, ns_endpoint, "");
        if (!ok) {
            return false;
        }
        brpc::ServerOptions options;
        if (ns_->AddService(nameserver, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(WARNING) << "fail to start ns";
            return false;
        }
        if (ns_->Start(ns_endpoint.c_str(), &options) != 0) {
            return false;
        }
        sleep(2);
        ns_client_ = new ::rtidb::client::NsClient(ns_endpoint, "");
        if (ns_client_->Init() != 0) {
            return false;
        }
        std::string ts_endpoint = "127.0.0.1:" + GenRand();
        ::rtidb::tablet::TabletImpl* tablet = new ::rtidb::tablet::TabletImpl();
        ok = tablet->Init(zk_cluster_, zk_path_, ts_endpoint, "");
        if (!ok) {
            return false;
        }
        brpc::ServerOptions ts_opt;
        if (ts_->AddService(tablet, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
            LOG(WARNING) << "fail to start ns";
            return false;
        }
        if (ts_->Start(ts_endpoint.c_str(), &ts_opt) != 0) {
            return false;
        }
        ok = tablet->RegisterZK();
        if (!ok) {
            return false;
        }
        sleep(2);
        LOG(INFO) << "start mini cluster with zk cluster " << zk_cluster_
                  << " and zk path " << zk_path_;
        LOG(INFO) << "----- ns " << ns_endpoint;
        LOG(INFO) << "----- ts " << ts_endpoint;
        return true;
    }

    void Close() {
        ns_->Stop(10);
        ts_->Stop(10);
        delete ns_;
        delete ts_;
    }

    std::string GetZkCluster() { return zk_cluster_; }

    std::string GetZkPath() { return zk_path_; }

    ::rtidb::client::NsClient* GetNsClient() { return ns_client_; }

    std::string GenRand() {
        return std::to_string(rand() % 1000 + 10000);  // NOLINT
    }

 private:
    int32_t zk_port_;
    brpc::Server* ns_;
    brpc::Server* ts_;
    std::string zk_cluster_;
    std::string zk_path_;
    ::rtidb::client::NsClient* ns_client_;
};

}  // namespace sdk
}  // namespace rtidb
#endif  // SRC_SDK_MINI_CLUSTER_H_
