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
#include "glog/logging.h"

namespace rtidb {
namespace sdk {


ClusterSDK::ClusterSDK(
        const ClusterOptions& options):cluster_version_(0),
    options_(options), nodes_root_path_(options.zk_path +"/nodes"),
    table_root_path_(options.zk_path + "/table/table_data"),
    notify_path_(options.zk_path + "/table/notify"),
    zk_client_(NULL), mu_(), alive_tablets_(),
    table_to_tablets_(), catalog_(new ::rtidb::catalog::SDKCatalog()) {}

ClusterSDK::~ClusterSDK() {
    if (zk_client_ != NULL) {
        zk_client_->CloseZK();
        delete zk_client_;
        zk_client_ = NULL;
    }
}

bool ClusterSDK::Init() {
    zk_client_ = new ::rtidb::zk::ZkClient(options_.zk_cluster, options_.session_timeout,
            "", options_.zk_path);
    bool ok = zk_client_->Init();
    if (!ok) {
        LOG(WARNING) << "fail to init zk client with zk cluster " << options_.zk_cluster
            << " , zk path " << options_.zk_path  << " and session timeout " << options_.session_timeout;
        return false;
    }
    LOG(WARNING) << "init zk client with zk cluster " << options_.zk_cluster
                 << " , zk path " << options_.zk_path  << " and session timeout ok" << options_.session_timeout;
    InitCatalog();
    InitTabletClient();
    return true;
}

bool ClusterSDK::RefreshCatalog(const std::vector<std::string>& table_datas) {
    std::vector<::rtidb::nameserver::TableInfo> tables;
    std::map<std::string, std::map<std::string, ::rtidb::nameserver::TableInfo>> mapping;
    for (uint32_t i = 0; i < table_datas.size(); i++) {
        ::rtidb::nameserver::TableInfo table_info;
        bool ok = table_info.ParseFromString(table_datas[i]);
        if (!ok) {
            LOG(WARNING) << "fail to parse table proto";
            return false;
        }
        if (table_info.format_version() != 1) {
            continue;
        }
        tables.push_back(table_info);
        auto it = mapping.find(table_info.db());
        if (it == mapping.end()) {
            std::map<std::string, ::rtidb::nameserver::TableInfo> table_in_db;
            table_in_db.insert(std::make_pair(table_info.name(), table_info));
            mapping.insert(std::make_pair(table_info.db(), table_in_db));
        }else {
            it->second.insert(std::make_pair(table_info.name(), table_info));
        }
        LOG(INFO) << "load table info with name " << table_info.name() << " in db " << table_info.db();
    }
    bool ok = catalog_->Init(tables);
    if (!ok) {
        LOG(WARNING) << "fail to init catalog";
        return false;
    }
    {
        std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
        table_to_tablets_ = mapping;
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
    std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>> tablet_clients;
    for (uint32_t i = 0; i < tablets.size(); i++) {
        std::shared_ptr<::rtidb::client::TabletClient> client(new ::rtidb::client::TabletClient(tablets[i]));
        ok = client->Init();
        if (!ok) {
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
    bool ok = zk_client_->GetChildren(table_root_path_, table_datas);
    if (!ok) {
        LOG(WARNING) << "fail to get table list with path " << table_root_path_;
        return false;
    }
    return RefreshCatalog(table_datas);
}

bool ClusterSDK::GetTabletByTable(const std::string& db, const std::string& name,
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
        const ::rtidb::nameserver::TableInfo& table_info = sit->second;
        for (int32_t i = 0; i < table_info.table_partition_size(); i++) {
            const ::rtidb::nameserver::TablePartition& partition = table_info.table_partition(i);
            for (int32_t j = 0; j < partition.partition_meta_size(); j++) {
                std::string endpoint = partition.partition_meta(j).endpoint();
                auto ait = alive_tablets_.find(endpoint);
                if (ait != alive_tablets_.end()) {
                    tablets->push_back(ait->second);
                }
            }

        }
        return true;
    }
}


}  // sdk
}  // rtidb
