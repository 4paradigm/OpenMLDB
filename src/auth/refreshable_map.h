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

#ifndef SRC_AUTH_REFRESHABLE_MAP_H_
#define SRC_AUTH_REFRESHABLE_MAP_H_

#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>
#include <utility>

namespace openmldb::auth {

template <typename Key, typename Value>
class RefreshableMap {
 public:
    std::optional<Value> Get(const Key& key) const {
        std::shared_lock<std::shared_mutex> lock(mutex_);
        if (auto it = map_->find(key); it != map_->end()) {
            return it->second;
        }
        return std::nullopt;
    }

    void Refresh(std::unique_ptr<std::unordered_map<Key, Value>> new_map) {
        std::unique_lock<std::shared_mutex> lock(mutex_);
        map_ = std::move(new_map);
    }

 private:
    mutable std::shared_mutex mutex_;
    std::shared_ptr<std::unordered_map<Key, Value>> map_;
};

}  // namespace openmldb::auth

#endif  // SRC_AUTH_REFRESHABLE_MAP_H_
