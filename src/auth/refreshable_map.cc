#include "refreshable_map.h"

#include <utility>  // For std::move
#include <string> 

namespace openmldb::auth {
// Get method implementation
template <typename Key, typename Value>
std::optional<Value> RefreshableMap<Key, Value>::Get(const Key& key) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    if (auto it = map_->find(key); it != map_->end()) {
        return it->second;
    }
    return std::nullopt;
}

// Refresh method implementation
template <typename Key, typename Value>
void RefreshableMap<Key, Value>::Refresh(std::unique_ptr<std::unordered_map<Key, Value>> new_map) {
    std::unique_lock<std::shared_mutex> lock(mutex_);  // Exclusive lock for writing
    map_ = std::move(new_map);
}
}  // namespace openmldb::auth

template class openmldb::auth::RefreshableMap<std::string, std::string>;
