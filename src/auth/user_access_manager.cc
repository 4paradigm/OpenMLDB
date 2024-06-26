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

#include "user_access_manager.h"

#include <unordered_map>
#include <utility>

#include "auth_utils.h"
#include "nameserver/system_table.h"

namespace openmldb::auth {

UserAccessManager::UserAccessManager(IteratorFactory iterator_factory)
    : user_table_iterator_factory_(std::move(iterator_factory)) {
    StartSyncTask();
}

UserAccessManager::~UserAccessManager() { StopSyncTask(); }

void UserAccessManager::StartSyncTask() {
    sync_task_thread_ = std::thread([this, fut = stop_promise_.get_future()] {
        while (true) {
            SyncWithDB();
            if (fut.wait_for(std::chrono::minutes(15)) != std::future_status::timeout) return;
        }
    });
}

void UserAccessManager::StopSyncTask() {
    stop_promise_.set_value();
    sync_task_thread_.join();
}

void UserAccessManager::SyncWithDB() {
    if (auto it_pair = user_table_iterator_factory_(::openmldb::nameserver::USER_INFO_NAME); it_pair) {
        auto new_user_map = std::make_unique<std::unordered_map<std::string, UserRecord>>();
        auto it = it_pair->first.get();
        it->SeekToFirst();
        while (it->Valid()) {
            auto row = it->GetValue();
            auto buf = it->GetValue().buf();
            auto size = it->GetValue().size();
            codec::RowView row_view(*it_pair->second.get(), buf, size);
            std::string host, user, password;
            std::string privilege_level_str;
            row_view.GetStrValue(0, &host);
            row_view.GetStrValue(1, &user);
            row_view.GetStrValue(2, &password);
            row_view.GetStrValue(5, &privilege_level_str);
            openmldb::nameserver::PrivilegeLevel privilege_level;
            ::openmldb::nameserver::PrivilegeLevel_Parse(privilege_level_str, &privilege_level);
            UserRecord user_record = {password, privilege_level};
            if (host == "%") {
                new_user_map->emplace(user, user_record);
            } else {
                new_user_map->emplace(FormUserHost(user, host), user_record);
            }
            it->Next();
        }
        user_map_.Refresh(std::move(new_user_map));
    }
}

std::optional<std::string> UserAccessManager::GetUserPassword(const std::string& host, const std::string& user) {
    if (auto user_record = user_map_.Get(FormUserHost(user, host)); user_record.has_value()) {
        return user_record.value().password;
    } else if (auto stored_password = user_map_.Get(user); stored_password.has_value()) {
        return stored_password.value().password;
    } else {
        return std::nullopt;
    }
}

bool UserAccessManager::IsAuthenticated(const std::string& host, const std::string& user, const std::string& password) {
    if (auto user_record = user_map_.Get(FormUserHost(user, host)); user_record.has_value()) {
        return user_record.value().password == password;
    } else if (auto stored_password = user_map_.Get(user); stored_password.has_value()) {
        return stored_password.value().password == password;
    }
    return false;
}

::openmldb::nameserver::PrivilegeLevel UserAccessManager::GetPrivilegeLevel(const std::string& user_at_host) {
    std::size_t at_pos = user_at_host.find('@');
    if (at_pos != std::string::npos) {
        std::string user = user_at_host.substr(0, at_pos);
        std::string host = user_at_host.substr(at_pos + 1);
        if (auto user_record = user_map_.Get(FormUserHost(user, host)); user_record.has_value()) {
            return user_record.value().privilege_level;
        } else if (auto stored_password = user_map_.Get(user); stored_password.has_value()) {
            return stored_password.value().privilege_level;
        }
    }
    return ::openmldb::nameserver::PrivilegeLevel::NO_PRIVILEGE;
}
}  // namespace openmldb::auth
