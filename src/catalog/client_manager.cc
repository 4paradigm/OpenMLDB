/*
 * client_manager.cc
 * Copyright (C) 4paradigm.com
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

#include "catalog/client_manager.h"

namespace rtidb {
namespace catalog {

PartitionClientManager::PartitionClientManager(uint32_t pid, const std::shared_ptr<ClientWrapper>& leader,
                                               const std::vector<std::shared_ptr<ClientWrapper>>& followers)
    : pid_(pid), leader_(leader), followers_(followers), rand_(0xdeadbeef) {}

std::shared_ptr<::rtidb::client::TabletClient> PartitionClientManager::GetFollower() {
    if (!followers_.empty()) {
        uint32_t it = rand_.Next() % followers_.size();
        return followers_[it]->GetClient();
    }
    return std::shared_ptr<::rtidb::client::TabletClient>();
}

TableClientManager::TableClientManager(const TablePartitions& partitions,
                                       const ClientManager& client_manager) {
    for (const auto& table_partition : partitions) {
        uint32_t pid = table_partition.pid();
        std::shared_ptr<ClientWrapper> leader;
        std::vector<std::shared_ptr<ClientWrapper>> follower;
        for (const auto& meta : table_partition.partition_meta()) {
            if (meta.is_alive()) {
                auto client = client_manager.GetClient(meta.endpoint());
                if (!client) {
                    continue;
                }
                if (meta.is_leader()) {
                    leader = client;
                } else {
                    follower.push_back(client);
                }
            }
        }
        partition_managers_.push_back(std::make_shared<PartitionClientManager>(pid, leader, follower));
    }
}

TableClientManager::TableClientManager(const ::rtidb::storage::TableSt& table_st,
                                       const ClientManager& client_manager) {
    for (const auto& partition_st : *(table_st.GetPartitions())) {
        uint32_t pid = partition_st.GetPid();
        std::shared_ptr<ClientWrapper> leader = client_manager.GetClient(partition_st.GetLeader());
        std::vector<std::shared_ptr<ClientWrapper>> follower;
        for (const auto& endpoint : partition_st.GetFollower()) {
            auto client = client_manager.GetClient(endpoint);
            if (client) {
                follower.push_back(client);
            }
        }
        partition_managers_.push_back(std::make_shared<PartitionClientManager>(pid, leader, follower));
    }
}

bool TableClientManager::UpdatePartitionClientManager(const ::rtidb::storage::PartitionSt& partition,
                                                      const ClientManager& client_manager) {
    auto leader = client_manager.GetClient(partition.GetLeader());
    if (!leader) {
        return false;
    }
    std::vector<std::shared_ptr<ClientWrapper>> followers;
    for (const auto& endpoint : partition.GetFollower()) {
        auto client = client_manager.GetClient(endpoint);
        if (!client) {
            return false;
        }
        followers.push_back(client);
    }
    uint32_t pid = partition.GetPid();
    auto partition_manager = std::make_shared<PartitionClientManager>(pid, leader, followers);
    std::atomic_store_explicit(&partition_managers_[pid], partition_manager, std::memory_order_relaxed);
    return true;
}

std::shared_ptr<ClientWrapper> ClientManager::GetClient(const std::string& name) const {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    auto it = clients_.find(name);
    if (it == clients_.end()) {
        return std::shared_ptr<ClientWrapper>();
    }
    return it->second;
}

bool ClientManager::UpdateClient(const std::map<std::string, std::string>& endpoint_map) {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    for (const auto& kv : endpoint_map) {
        auto it = real_endpoint_map_.find(kv.first);
        if (it == real_endpoint_map_.end()) {
            auto wrapper = std::make_shared<ClientWrapper>(kv.first);
            if (!wrapper->UpdateClient(kv.second)) {
                LOG(WARNING) << "add client failed. name " << kv.first << " , endpoint " << kv.second;
                continue;
            }
            LOG(INFO) << "add client. name " << kv.first << " , endpoint " << kv.second;
            clients_.emplace(kv.first, wrapper);
            real_endpoint_map_.emplace(kv.first, kv.second);
            continue;
        }
        if (it->second != kv.second) {
            auto client_it = clients_.find(kv.first);
            LOG(INFO) << "update client " << kv.first << "from " << it->second << " to " << kv.second;
            if (!client_it->second->UpdateClient(kv.second)) {
                LOG(WARNING) << "update client failed. name " << kv.first << " , endpoint " << kv.second;
                continue;
            }
            it->second = kv.second;
        }
    }
    return true;
}

bool ClientManager::UpdateClient(
    const std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>>& tablet_clients) {
    std::lock_guard<::rtidb::base::SpinMutex> lock(mu_);
    for (const auto& kv : tablet_clients) {
        auto it = real_endpoint_map_.find(kv.first);
        if (it == real_endpoint_map_.end()) {
            auto wrapper = std::make_shared<ClientWrapper>(kv.first, kv.second);
            LOG(INFO) << "add client. name " << kv.first << " , endpoint " << kv.second;
            clients_.emplace(kv.first, wrapper);
            real_endpoint_map_.emplace(kv.first, kv.second->GetRealEndpoint());
            continue;
        }
        if (it->second != kv.second->GetRealEndpoint()) {
            auto client_it = clients_.find(kv.first);
            LOG(INFO) << "update client " << kv.first << "from " << it->second << " to " << kv.second;
            if (!client_it->second->UpdateClient(kv.second)) {
                LOG(WARNING) << "update client failed. name " << kv.first << " , endpoint " << kv.second;
                continue;
            }
            it->second = kv.second->GetRealEndpoint();
        }
    }
    return true;
}

}  // namespace catalog
}  // namespace rtidb
