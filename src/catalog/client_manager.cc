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
        const std::shared_ptr<::rtidb::client::TabletClient>& leader,
        const std::vector<std::shared_ptr<::rtidb::client::TabletClient>>& followers) :
    pid_(pid), leader_(leader), followers_(followers), rand_(0xdeadbeef) {}

std::shared_ptr<::rtidb::client::TabletClient> PartitionClientManager::GetFollower() {
    if (!followers_.empty()) {
        uint32_t pos = rand_.Next() % followers_.size();
        return followers_[pos];
    }
    return std::shared_ptr<::rtidb::client::TabletClient>();
}

TableClientManager::TableClientManager(const TablePartitions& partitions,
    const std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>>& tablet_clients) {
    for (const auto& table_partition : partitions) {
        uint32_t pid = table_partition.pid();
        std::shared_ptr<::rtidb::client::TabletClient> leader;
        std::vector<std::shared_ptr<::rtidb::client::TabletClient>> follower;
        for (const auto& meta : table_partition.partition_meta()) {
            if (meta.is_alive()) {
                auto iter = tablet_clients.find(meta.endpoint());
                if (iter == tablet_clients.end()) {
                    continue;
                }
                if (meta.is_leader()) {
                    leader = iter->second;
                } else {
                    follower.push_back(iter->second);
                }
            }
        }
        partition_managers_.push_back(std::make_shared<PartitionClientManager>(pid, leader, follower));
    }
}

}  // namespace catalog
}  // namespace rtidb
