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

#include "nameserver/cluster_info.h"

#include <algorithm>
#include <utility>
#include "base/glog_wrapper.h"
#include "boost/bind.hpp"
#include "common/timer.h"
#include "gflags/gflags.h"

DECLARE_int32(zk_session_timeout);
DECLARE_bool(use_name);

namespace openmldb {
namespace nameserver {

ClusterInfo::ClusterInfo(const ::openmldb::nameserver::ClusterAddress& cd)
    : client_(), last_status(), zk_client_(), session_term_() {
    cluster_add_.CopyFrom(cd);
    state_ = kClusterOffline;
    ctime_ = ::baidu::common::timer::get_micros() / 1000;
}

void ClusterInfo::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        PDLOG(WARNING, "reconnect zk");
        if (zk_client_->Reconnect()) {
            PDLOG(INFO, "reconnect zk ok");
        }
    }
    if (session_term_ != zk_client_->GetSessionTerm()) {
        if (zk_client_->WatchNodes()) {
            session_term_ = zk_client_->GetSessionTerm();
            PDLOG(INFO, "watch node ok");
        } else {
            PDLOG(WARNING, "watch node failed");
        }
    }
}

void ClusterInfo::UpdateNSClient(const std::vector<std::string>& children) {
    if (children.empty()) {
        PDLOG(INFO, "children is empty on UpdateNsClient");
        return;
    }
    std::vector<std::string> tmp_children(children.begin(), children.end());
    std::sort(tmp_children.begin(), tmp_children.end());
    std::string endpoint;
    if (tmp_children[0] == client_->GetEndpoint()) {
        return;
    }
    if (!zk_client_->GetNodeValue(cluster_add_.zk_path() + "/leader/" + tmp_children[0], endpoint)) {
        PDLOG(WARNING, "get replica cluster leader ns failed");
        return;
    }
    std::string real_endpoint;
    if (FLAGS_use_name) {
        std::vector<std::string> vec;
        const std::string name_path = cluster_add_.zk_path() + "/map/names/" + endpoint;
        if (zk_client_->IsExistNode(name_path) != 0) {
            LOG(WARNING) << endpoint << " not in name vec";
            return;
        }
        if (!zk_client_->GetNodeValue(name_path, real_endpoint)) {
            LOG(WARNING) << "get real_endpoint failed for name " << endpoint;
            return;
        }
    }
    std::shared_ptr<::openmldb::client::NsClient> tmp_ptr =
        std::make_shared<::openmldb::client::NsClient>(endpoint, real_endpoint);
    if (tmp_ptr->Init() < 0) {
        PDLOG(WARNING, "replica cluster ns client init failed");
        return;
    }
    std::atomic_store_explicit(&client_, tmp_ptr, std::memory_order_relaxed);
    ctime_ = ::baidu::common::timer::get_micros() / 1000;
    state_.store(kClusterHealthy, std::memory_order_relaxed);
}

int ClusterInfo::Init(std::string& msg) {
    zk_client_ = std::make_shared<::openmldb::zk::ZkClient>(cluster_add_.zk_endpoints(), FLAGS_zk_session_timeout, "",
                                            cluster_add_.zk_path(), cluster_add_.zk_path() + "/leader");
    bool ok = zk_client_->Init();
    for (int i = 1; i < 3; i++) {
        if (ok) {
            break;
        }
        PDLOG(WARNING, "count %d fail to init zookeeper with cluster %s %s", i, cluster_add_.zk_endpoints().c_str(),
              cluster_add_.zk_path().c_str());
        ok = zk_client_->Init();
    }
    if (!ok) {
        msg = "connect relica cluster zk failed";
        return 401;
    }
    session_term_ = zk_client_->GetSessionTerm();
    std::vector<std::string> children;
    if (!zk_client_->GetChildren(cluster_add_.zk_path() + "/leader", children) || children.empty()) {
        msg = "get zk failed";
        PDLOG(WARNING, "get zk failed, get children");
        return 451;
    }
    std::string endpoint;
    if (!zk_client_->GetNodeValue(cluster_add_.zk_path() + "/leader/" + children[0], endpoint)) {
        msg = "get zk failed";
        PDLOG(WARNING, "get zk failed, get replica cluster leader ns failed");
        return 451;
    }
    std::string real_endpoint;
    if (FLAGS_use_name) {
        std::vector<std::string> vec;
        const std::string name_path = cluster_add_.zk_path() + "/map/names/" + endpoint;
        if (zk_client_->IsExistNode(name_path) != 0) {
            msg = "name not in names_vec";
            LOG(WARNING) << endpoint << " not in name vec";
            return -1;
        }
        if (!zk_client_->GetNodeValue(name_path, real_endpoint)) {
            msg = "get zk failed";
            LOG(WARNING) << "get real_endpoint failed for name " << endpoint;
            return 451;
        }
    }
    client_ = std::make_shared<::openmldb::client::NsClient>(endpoint, real_endpoint);
    if (client_->Init() < 0) {
        msg = "connect ns failed";
        PDLOG(WARNING, "connect ns failed, replica cluster ns");
        return 403;
    }
    zk_client_->WatchNodes(boost::bind(&ClusterInfo::UpdateNSClient, this, _1));
    zk_client_->WatchNodes();
    if (FLAGS_use_name) {
        UpdateRemoteRealEpMap();
        bool ok = zk_client_->WatchItem(cluster_add_.zk_path() + "/nodes",
                                        boost::bind(&ClusterInfo::UpdateRemoteRealEpMap, this));
        if (!ok) {
            zk_client_->CloseZK();
            msg = "zk watch nodes failed";
            PDLOG(WARNING, "zk watch nodes failed");
            return -1;
        }
    }
    return 0;
}

bool ClusterInfo::DropTableRemote(const ::openmldb::api::TaskInfo& task_info, const std::string& name,
                                  const std::string& db, const ::openmldb::nameserver::ZoneInfo& zone_info) {
    std::string msg;
    if (!std::atomic_load_explicit(&client_, std::memory_order_relaxed)
             ->DropTableRemote(task_info, name, db, zone_info, msg)) {
        PDLOG(WARNING, "drop table for replica cluster failed!, msg is: %s", msg.c_str());
        return false;
    }
    return true;
}

bool ClusterInfo::CreateTableRemote(const ::openmldb::api::TaskInfo& task_info,
                                    const ::openmldb::nameserver::TableInfo& table_info,
                                    const ::openmldb::nameserver::ZoneInfo& zone_info) {
    std::string msg;
    if (!std::atomic_load_explicit(&client_, std::memory_order_relaxed)
             ->CreateTableRemote(task_info, table_info, zone_info, msg)) {
        PDLOG(WARNING, "create table for replica cluster failed!, msg is: %s", msg.c_str());
        return false;
    }
    return true;
}

bool ClusterInfo::UpdateRemoteRealEpMap() {
    if (!FLAGS_use_name) {
        return true;
    }
    decltype(remote_real_ep_map_) tmp_map = std::make_shared<std::map<std::string, std::string>>();
    std::vector<std::string> vec;
    if (!zk_client_->GetChildren(cluster_add_.zk_path() + "/map/names", vec) || vec.empty()) {
        PDLOG(WARNING, "get zk failed, get remote children");
        return false;
    }
    for (const auto& ep : vec) {
        std::string real_endpoint;
        if (!zk_client_->GetNodeValue(cluster_add_.zk_path() + "/map/names/" + ep, real_endpoint)) {
            PDLOG(WARNING, "get zk failed, get real_endpoint failed");
            continue;
        }
        tmp_map->insert(std::make_pair(ep, real_endpoint));
    }
    std::atomic_store_explicit(&remote_real_ep_map_, tmp_map, std::memory_order_release);
    return true;
}

bool ClusterInfo::AddReplicaClusterByNs(const std::string& alias, const std::string& zone_name, const uint64_t term,
                                        std::string& msg) {
    if (!std::atomic_load_explicit(&client_, std::memory_order_relaxed)
             ->AddReplicaClusterByNs(alias, zone_name, term, msg)) {
        PDLOG(WARNING, "send MakeReplicaCluster request failed");
        return false;
    }
    return true;
}

bool ClusterInfo::RemoveReplicaClusterByNs(const std::string& alias, const std::string& zone_name, const uint64_t term,
                                           int& code, std::string& msg) {
    return std::atomic_load_explicit(&client_, std::memory_order_relaxed)
        ->RemoveReplicaClusterByNs(alias, zone_name, term, code, msg);
}

}  // namespace nameserver
}  // namespace openmldb
