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

#ifndef SRC_AUTH_USER_ACCESS_MANAGER_H_
#define SRC_AUTH_USER_ACCESS_MANAGER_H_

#include <chrono>
#include <functional>
#include <memory>
#include <string>
#include <thread>

#include "catalog/distribute_iterator.h"
#include "refreshable_map.h"

namespace openmldb::auth {
class UserAccessManager {
 public:
    using IteratorFactory =
        std::function<std::unique_ptr<::openmldb::catalog::FullTableIterator>(const std::string& table_name)>;

    UserAccessManager(IteratorFactory iterator_factory, std::shared_ptr<nameserver::TableInfo> user_table_info);
    ~UserAccessManager();
    bool IsAuthenticated(const std::string& host, const std::string& username, const std::string& password);

 private:
    IteratorFactory user_table_iterator_factory_;
    std::shared_ptr<nameserver::TableInfo> user_table_info_;
    RefreshableMap<std::string, std::string> user_map_;
    std::atomic<bool> sync_task_running_{false};
    std::thread sync_task_thread_;

    void SyncWithDB();
    void StartSyncTask();
    void StopSyncTask();
};
}  // namespace openmldb::auth

#endif  // SRC_AUTH_USER_ACCESS_MANAGER_H_
