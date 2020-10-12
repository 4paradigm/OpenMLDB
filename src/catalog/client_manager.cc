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

PartitionClientManager::PartitionClientManager(uint32_t pid,
        const std::shared_ptr<ClientWrapper>& leader,
        const std::vector<std::shared_ptr<ClientWrapper>>& followers) :
    pid_(pid), leader_(leader), followers_(followers), rand_(0xdeadbeef) {}

std::shared_ptr<::rtidb::client::TabletClient> PartitionClientManager::GetFollower() {
    if (!followers_.empty()) {
        uint32_t it = rand_.Next() % followers_.size();
        return followers_[it]->GetClient();
    }
    return std::shared_ptr<::rtidb::client::TabletClient>();
}

TableClientManager::TableClientManager(const TablePartitions& partitions,
    const std::shared_ptr<ClientManager>& client_manager) {
    for (const auto& table_partition : partitions) {
        uint32_t pid = table_partition.pid();
        std::shared_ptr<ClientWrapper> leader;
        std::vector<std::shared_ptr<ClientWrapper>> follower;
        for (const auto& meta : table_partition.partition_meta()) {
            if (meta.is_alive()) {
                auto client = client_manager->GetClient(meta.endpoint());
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

std::shared_ptr<ClientWrapper> ClientManager::GetClient(const std::string& name) {
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
                continue;
            }
            clients_.emplace(kv.first, wrapper);
            real_endpoint_map_.emplace(kv.first, kv.second);
            continue;
        }
        if (it->second != kv.second) {
            auto client_it = clients_.find(kv.first);
            if (!client_it->second->UpdateClient(kv.second)) {
                continue;
            }
            it->second = kv.second;
        }
    }
    return true;
}

}  // namespace catalog
}  // namespace rtidb
