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
#include <future>
#include <memory>
#include <string>
#include <thread>
#include <utility>

#include "catalog/distribute_iterator.h"
#include "proto/name_server.pb.h"
#include "refreshable_map.h"

namespace openmldb::auth {
struct UserRecord {
    std::string password;
    ::openmldb::nameserver::PrivilegeLevel privilege_level;
};

class UserAccessManager {
 public:
    using IteratorFactory = std::function<std::optional<
        std::pair<std::unique_ptr<::openmldb::catalog::FullTableIterator>, std::unique_ptr<openmldb::codec::Schema>>>(
        const std::string& table_name)>;

    explicit UserAccessManager(IteratorFactory iterator_factory);

    ~UserAccessManager();
    bool IsAuthenticated(const std::string& host, const std::string& username, const std::string& password);
    ::openmldb::nameserver::PrivilegeLevel GetPrivilegeLevel(const std::string& user_at_host);
    void SyncWithDB();
    std::optional<std::string> GetUserPassword(const std::string& host, const std::string& user);

 private:
    IteratorFactory user_table_iterator_factory_;
    RefreshableMap<std::string, UserRecord> user_map_;
    std::thread sync_task_thread_;
    std::promise<void> stop_promise_;
    void StartSyncTask();
    void StopSyncTask();
};
}  // namespace openmldb::auth

#endif  // SRC_AUTH_USER_ACCESS_MANAGER_H_
