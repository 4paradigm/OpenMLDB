#ifndef SRC_AUTH_USER_ACCESS_MANAGER_H_
#define SRC_AUTH_USER_ACCESS_MANAGER_H_

#include <functional>
#include <memory>
#include <string>

#include "catalog/distribute_iterator.h"
#include "refreshable_map.h"

namespace openmldb::auth {
class UserAccessManager {
 public:
    using IteratorFactory =
        std::function<std::unique_ptr<::openmldb::catalog::FullTableIterator>(const std::string& table_name)>;

    UserAccessManager(IteratorFactory iterator_factory, std::shared_ptr<nameserver::TableInfo> user_table_info);

    void SyncWithDB();

    bool IsAuthenticated(const std::string& host, const std::string& username, const std::string& password);

 private:
    IteratorFactory user_table_iterator_factory_;
    std::shared_ptr<nameserver::TableInfo> user_table_info_;
    RefreshableMap<std::string, std::string> user_map_;
};
}  // namespace openmldb::auth

#endif  // SRC_AUTH_USER_ACCESS_MANAGER_H_
