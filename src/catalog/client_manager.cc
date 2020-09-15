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
        const std::shared_ptr<TabletClient>& leader,
        const std::vector<std::shared_ptr<TabletClient>>& followers) :
    pid_(pid), leader_(leader), followers_(), rand_(0xdeadbeef) {
    followers_ = std::make_shared<std::vector<std::shared_ptr<TabletClient>>>(followers);
}

std::shared_ptr<TabletClient> PartitionClientManager::GetFollower() {
    auto followers = std::atomic_load_explicit(&followers_, std::memory_order_relaxed);
    if (!followers->empty()) {
        uint32_t pos = rand_.Next() % followers.size();
        return followers[pos];
    }
    return std::shared_ptr<TabletClient>();
}

}  // namespace catalog
}  // namespace rtidb
