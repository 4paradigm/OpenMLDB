/*
 * cluster_sdk.cc
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

#include "sdk/cluster_sdk.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/strings.h"
#include "boost/algorithm/string.hpp"
#include "boost/function.hpp"
#include "glog/logging.h"

namespace rtidb {
namespace sdk {

ClusterSDK::ClusterSDK(const ClusterOptions& options)
    : cluster_version_(0),
      options_(options),
      nodes_root_path_(options.zk_path + "/nodes"),
      table_root_path_(options.zk_path + "/table/db_table_data"),
      notify_path_(options.zk_path + "/table/notify"),
      zk_client_(NULL),
      mu_(),
      alive_tablets_(),
      table_to_tablets_(),
      catalog_(new ::rtidb::catalog::SDKCatalog()),
      pool_(1),
      session_id_(0),
      running_(true) {}

ClusterSDK::~ClusterSDK() {
    running_.store(false, std::memory_order_relaxed);
    pool_.Stop(false);
    if (zk_client_ != NULL) {
        zk_client_->CloseZK();
        delete zk_client_;
        zk_client_ = NULL;
    }
}

void ClusterSDK::CheckZk() {
    if (!running_.load(std::memory_order_relaxed)) {
        return;
    }
    if (session_id_ == 0) {
        WatchNotify();
    } else if (session_id_ != zk_client_->GetSessionTerm()) {
        LOG(WARNING) << "session changed rewatch notity";
        WatchNotify();
    }
    pool_.DelayTask(2000, boost::bind(&ClusterSDK::CheckZk, this));
}

bool ClusterSDK::Init() {
    zk_client_ = new ::rtidb::zk::ZkClient(options_.zk_cluster, "",
            options_.session_timeout, "", options_.zk_path);
    bool ok = zk_client_->Init();
    if (!ok) {
        LOG(WARNING) << "fail to init zk client with zk cluster "
                     << options_.zk_cluster << " , zk path " << options_.zk_path
                     << " and session timeout " << options_.session_timeout;
        return false;
    }
    LOG(WARNING) << "init zk client with zk cluster " << options_.zk_cluster
                 << " , zk path " << options_.zk_path << ",session timeout "
                 << options_.session_timeout << " and session id "
                 << zk_client_->GetSessionTerm();
    ok = InitCatalog();
    if (!ok) return false;
    ok = InitTabletClient();
    if (!ok) return false;
    CheckZk();
    return true;
}

bool ClusterSDK::Refresh() { return InitCatalog(); }

void ClusterSDK::WatchNotify() {
    LOG(INFO) << "start to watch table notify";
    session_id_ = zk_client_->GetSessionTerm();
    zk_client_->CancelWatchItem(notify_path_);
    zk_client_->WatchItem(notify_path_,
                          boost::bind(&ClusterSDK::Refresh, this));
}

bool ClusterSDK::CreateNsClient() {
    std::string ns_node = options_.zk_path + "/leader";
    std::vector<std::string> children;
    if (!zk_client_->GetChildren(ns_node, children) || children.empty()) {
        LOG(WARNING) << "no nameserver exists";
        return false;
    }
    std::string real_path = ns_node + "/" + children[0];
    std::string endpoint;
    if (!zk_client_->GetNodeValue(real_path, endpoint)) {
        LOG(WARNING) << "fail to get zk value with path " << real_path;
        return false;
    }
    DLOG(INFO) << "leader path " << real_path << " with value " << endpoint;

    std::string real_endpoint;
    if (!GetRealEndpoint(endpoint, &real_endpoint)) {
        return false;
    }
    std::shared_ptr<::rtidb::client::NsClient> ns_client(
        new ::rtidb::client::NsClient(endpoint, real_endpoint));
    int ret = ns_client->Init();
    if (ret != 0) {
        LOG(WARNING) << "fail to init ns client with endpoint " << endpoint;
        return false;
    } else {
        LOG(INFO) << "init ns client with endpoint " << endpoint << " done";
        std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
        ns_client_ = ns_client;
        return true;
    }
}

bool ClusterSDK::RefreshCatalog(const std::vector<std::string>& table_datas) {
    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::map<
        std::string,
        std::map<std::string, std::shared_ptr<::rtidb::nameserver::TableInfo>>>
        mapping;
    std::shared_ptr<::rtidb::catalog::SDKCatalog> new_catalog(
        new ::rtidb::catalog::SDKCatalog());
    for (uint32_t i = 0; i < table_datas.size(); i++) {
        if (table_datas[i].empty()) continue;
        std::string value;
        bool ok = zk_client_->GetNodeValue(
            table_root_path_ + "/" + table_datas[i], value);
        if (!ok) {
            LOG(WARNING) << "fail to get table data";
            continue;
        }
        std::shared_ptr<::rtidb::nameserver::TableInfo> table_info(
            new ::rtidb::nameserver::TableInfo());
        ok = table_info->ParseFromString(value);
        if (!ok) {
            LOG(WARNING) << "fail to parse table proto with " << value;
            return false;
        }
        DLOG(INFO) << "parse table " << table_info->name() << " ok";
        if (table_info->format_version() != 1) {
            continue;
        }
        tables.push_back(*(table_info.get()));
        auto it = mapping.find(table_info->db());
        if (it == mapping.end()) {
            std::map<std::string,
                     std::shared_ptr<::rtidb::nameserver::TableInfo>>
                table_in_db;
            table_in_db.insert(std::make_pair(table_info->name(), table_info));
            mapping.insert(std::make_pair(table_info->db(), table_in_db));
        } else {
            it->second.insert(std::make_pair(table_info->name(), table_info));
        }
        DLOG(INFO) << "load table info with name " << table_info->name()
                   << " in db " << table_info->db();
    }
    bool ok = new_catalog->Init(tables);
    if (!ok) {
        LOG(WARNING) << "fail to init catalog";
        return false;
    }
    {
        std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
        table_to_tablets_ = mapping;
        catalog_ = new_catalog;
        return true;
    }
}

bool ClusterSDK::InitTabletClient() {
    std::vector<std::string> tablets;
    bool ok = zk_client_->GetNodes(tablets);
    if (!ok) {
        LOG(WARNING) << "fail to get tablet";
        return false;
    }
    std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>>
        tablet_clients;
    for (uint32_t i = 0; i < tablets.size(); i++) {
        if (boost::starts_with(tablets[i], ::rtidb::base::BLOB_PREFIX))
            continue;

        std::string real_endpoint;
        if (!GetRealEndpoint(tablets[i], &real_endpoint)) {
            return false;
        }
        std::shared_ptr<::rtidb::client::TabletClient> client(
            new ::rtidb::client::TabletClient(tablets[i], real_endpoint));
        int ret = client->Init();
        if (ret != 0) {
            LOG(WARNING) << "fail to init tablet client " << tablets[i];
            return false;
        }
        LOG(INFO) << "add alive tablet " << tablets[i];
        tablet_clients.insert(std::make_pair(tablets[i], client));
    }
    {
        std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
        alive_tablets_ = tablet_clients;
    }
    return true;
}

bool ClusterSDK::InitCatalog() {
    std::vector<std::string> table_datas;
    if (zk_client_->IsExistNode(table_root_path_) == 0) {
        bool ok = zk_client_->GetChildren(table_root_path_, table_datas);
        if (!ok) {
            LOG(WARNING) << "fail to get table list with path "
                         << table_root_path_;
            return false;
        }
    } else {
        LOG(INFO) << "no tables in db";
    }
    return RefreshCatalog(table_datas);
}

std::shared_ptr<::rtidb::client::TabletClient>
ClusterSDK::GetLeaderTabletByTable(const std::string& db,
                                   const std::string& name) {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    auto it = table_to_tablets_.find(db);
    if (it == table_to_tablets_.end()) {
        return std::shared_ptr<::rtidb::client::TabletClient>();
    }
    auto sit = it->second.find(name);
    if (sit == it->second.end()) {
        return std::shared_ptr<::rtidb::client::TabletClient>();
    }
    auto table_info = sit->second;
    for (int32_t i = 0; i < table_info->table_partition_size(); i++) {
        const ::rtidb::nameserver::TablePartition& partition =
            table_info->table_partition(i);
        for (int32_t j = 0; j < partition.partition_meta_size(); j++) {
            if (!partition.partition_meta(j).is_leader()) continue;
            std::string endpoint = partition.partition_meta(j).endpoint();
            if (!partition.partition_meta(j).is_alive()) {
                return std::shared_ptr<::rtidb::client::TabletClient>();
            }
            auto ait = alive_tablets_.find(endpoint);
            if (ait != alive_tablets_.end()) {
                return ait->second;
            }
        }
    }
    return std::shared_ptr<::rtidb::client::TabletClient>();
}
std::shared_ptr<::rtidb::client::TabletClient> ClusterSDK::PickOneTablet() {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);

    if (alive_tablets_.empty()) {
        LOG(WARNING) << "no alive tablets exist!";
        return std::shared_ptr<::rtidb::client::TabletClient>();
    }
    auto ait = alive_tablets_.begin();
    if (ait != alive_tablets_.end()) {
        return ait->second;
    }
    return std::shared_ptr<::rtidb::client::TabletClient>();
}
bool ClusterSDK::GetTabletByTable(
    const std::string& db, const std::string& name,
    std::vector<std::shared_ptr<::rtidb::client::TabletClient>>* tablets) {
    if (tablets == NULL) return false;
    {
        std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
        auto it = table_to_tablets_.find(db);
        if (it == table_to_tablets_.end()) {
            return false;
        }
        auto sit = it->second.find(name);
        if (sit == it->second.end()) {
            return false;
        }
        std::set<std::string> endpoints;
        auto table_info = sit->second;
        for (int32_t i = 0; i < table_info->table_partition_size(); i++) {
            const ::rtidb::nameserver::TablePartition& partition =
                table_info->table_partition(i);
            for (int32_t j = 0; j < partition.partition_meta_size(); j++) {
                std::string endpoint = partition.partition_meta(j).endpoint();
                if (endpoints.find(endpoint) != endpoints.end()) continue;
                if (!partition.partition_meta(j).is_alive()) {
                    continue;
                }
                endpoints.insert(endpoint);
                auto ait = alive_tablets_.find(endpoint);
                if (ait != alive_tablets_.end()) {
                    tablets->push_back(ait->second);
                }
            }
        }
        return true;
    }
}

uint32_t ClusterSDK::GetTableId(const std::string& db,
                                const std::string& tname) {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    auto it = table_to_tablets_.find(db);
    if (it == table_to_tablets_.end()) {
        return 0;
    }
    auto sit = it->second.find(tname);
    if (sit == it->second.end()) {
        return 0;
    }
    auto table_info = sit->second;
    return table_info->tid();
}

std::shared_ptr<::rtidb::nameserver::TableInfo> ClusterSDK::GetTableInfo(
    const std::string& db, const std::string& tname) {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    auto it = table_to_tablets_.find(db);
    if (it == table_to_tablets_.end()) {
        return std::shared_ptr<::rtidb::nameserver::TableInfo>();
    }
    auto sit = it->second.find(tname);
    if (sit == it->second.end()) {
        return std::shared_ptr<::rtidb::nameserver::TableInfo>();
    }
    auto table_info = sit->second;
    return table_info;
}

std::vector<std::shared_ptr<::rtidb::nameserver::TableInfo>>
ClusterSDK::GetTables(const std::string& db) {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    std::vector<std::shared_ptr<::rtidb::nameserver::TableInfo>> tables;
    auto it = table_to_tablets_.find(db);
    if (it == table_to_tablets_.end()) {
        return tables;
    }
    auto iit = it->second.begin();
    for (; iit != it->second.end(); ++iit) {
        tables.push_back(iit->second);
    }
    return tables;
}

bool ClusterSDK::GetRealEndpoint(const std::string& endpoint,
        std::string* real_endpoint) {
    if (real_endpoint == nullptr) {
        return false;
    }
    std::string sdk_path = options_.zk_path + "/map/sdkendpoints/" + endpoint;
    if (zk_client_->IsExistNode(sdk_path) == 0) {
        if (!zk_client_->GetNodeValue(sdk_path, *real_endpoint)) {
            DLOG(WARNING) << "get zk failed! : sdk_path: " << sdk_path;
            return false;
        }
    }
    if (real_endpoint->empty()) {
        std::string sname_path = options_.zk_path + "/map/names/" + endpoint;
        if (zk_client_->IsExistNode(sname_path) == 0) {
            if (!zk_client_->GetNodeValue(sname_path, *real_endpoint)) {
                DLOG(WARNING) << "get zk failed! : sname_path: " << sname_path;
                return false;
            }
        }
    }
    return true;
}

}  // namespace sdk
}  // namespace rtidb
