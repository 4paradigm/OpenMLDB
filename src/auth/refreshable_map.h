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
