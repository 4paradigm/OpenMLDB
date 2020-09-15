/*
 * client_manager.h
 * Copyright (C) 4paradigm.com 2020
 * Author denglong
 * Date 2020-09-14
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

#ifndef SRC_CATALOG_CLIENT_MANAGER_H_
#define SRC_CATALOG_CLIENT_MANAGER_H_

#include <atomic>
#include <memory>
#include <vector>

#include "base/random.h"

namespace rtidb {
namespace catalog {

class PartitionClientManager {
 public:
    PartitionClientManager(uint32_t pid, const std::shared_ptr<TabletClient>& leader,
                           const std::vector<std::shared_ptr<TabletClient>>& followers);

    inline std::shared_ptr<TabletClient> GetLeader() const {
        return std::atomic_load_explicit(&leader_, std::memory_order_relaxed);
    }

    void SetLeader(const std::shared_ptr<TabletClient>& leader) {
        std::atomic_store_explicit(&leader_, leader, std::memory_order_relaxed);
    }

    std::shared_ptr<TabletClient> GetFollower();

    void SetFollower(const std::vector<std::shared_ptr<TabletClient>>& followers) {
        auto new_followers = std::make_shared<std::vector<std::shared_ptr<TabletClient>>>(followers);
        std::atomic_store_explicit(&followers_, new_followers, std::memory_order_relaxed);
    }

 private:
    uint32_t pid;
    std::shared_ptr<TabletClient> leader_;
    std::shared_ptr<std::vector<std::shared_ptr<TabletClient>>> followers_;
    ::rtidb::base::Random rand_;
};

class TableClientManager {
 public:
    TableClientManager();
    std::shared_ptr<PartitionClientManager> GetPartitionClientManager(uint32_t pid) const {
        if (pid < partition_managers_.size()) {
            return partition_managers_[pid];
        }
        return std::shared_ptr<PartitionClientManager>();
    }

 private:
    std::vector<std::shared_ptr<PartitionClientManager>> partition_managers_;
};

}  // namespace catalog
}  // namespace rtidb
#endif  // SRC_CATALOG_CLIENT_MANAGER_H_
