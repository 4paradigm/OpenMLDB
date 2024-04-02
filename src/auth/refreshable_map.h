#ifndef SRC_REFRESHABLE_MAP_H_
#define SRC_REFRESHABLE_MAP_H_

#include <memory>
#include <mutex>
#include <optional>
#include <shared_mutex>
#include <unordered_map>

namespace openmldb::auth {

template <typename Key, typename Value>
class RefreshableMap {
 public:
    std::optional<Value> Get(const Key& key) const;
    void Refresh(std::unique_ptr<std::unordered_map<Key, Value>> new_map);

 private:
    mutable std::shared_mutex mutex_;
    std::shared_ptr<std::unordered_map<Key, Value>> map_;
};

}  // namespace openmldb::auth

#endif  // SRC_REFRESHABLE_MAP_H_
