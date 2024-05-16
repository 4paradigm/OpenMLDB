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
    std::thread([this] {
        while (true) {
            SyncWithDB();
            std::this_thread::sleep_for(std::chrono::minutes(15));
        }
    }).detach();
}

void UserAccessManager::SyncWithDB() {
    if (auto it_pair = user_table_iterator_factory_(::openmldb::nameserver::USER_INFO_NAME); it_pair) {
        auto new_user_map = std::make_unique<std::unordered_map<std::string, std::string>>();
        auto it = it_pair->first.get();
        it->SeekToFirst();
        while (it->Valid()) {
            auto row = it->GetValue();
            auto buf = it->GetValue().buf();
            auto size = it->GetValue().size();
            codec::RowView row_view(*it_pair->second.get(), buf, size);
            std::string host, user, password;
            row_view.GetStrValue(0, &host);
            row_view.GetStrValue(1, &user);
            row_view.GetStrValue(2, &password);
            if (host == "%") {
                new_user_map->emplace(user, password);
            } else {
                new_user_map->emplace(FormUserHost(user, host), password);
            }
            it->Next();
        }
        user_map_.Refresh(std::move(new_user_map));
    }
}

bool UserAccessManager::IsAuthenticated(const std::string& host, const std::string& user, const std::string& password) {
    if (auto stored_password = user_map_.Get(FormUserHost(user, host)); stored_password.has_value()) {
        return stored_password.value() == password;
    } else if (auto stored_password = user_map_.Get(user); stored_password.has_value()) {
        return stored_password.value() == password;
    }
    return false;
}
}  // namespace openmldb::auth
