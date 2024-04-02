#include "user_access_manager.h"

#include "auth_utils.h"
#include "nameserver/system_table.h"

namespace openmldb::auth {
UserAccessManager::UserAccessManager(IteratorFactory iterator_factory,
                                     std::shared_ptr<nameserver::TableInfo> user_table_info)
    : user_table_iterator_factory_(std::move(iterator_factory)), user_table_info_(user_table_info) {}

void UserAccessManager::SyncWithDB() {
    auto new_user_map = std::make_unique<std::unordered_map<std::string, std::string>>();
    auto it = user_table_iterator_factory_(::openmldb::nameserver::USER_INFO_NAME);
    it->SeekToFirst();
    while (it->Valid()) {
        auto row = it->GetValue();
        auto buf = it->GetValue().buf();
        auto size = it->GetValue().size();
        codec::RowView row_view(user_table_info_->column_desc(), buf, size);
        std::string host, user, password;
        row_view.GetStrValue(0, &host);
        row_view.GetStrValue(1, &user);
        row_view.GetStrValue(2, &password);
        new_user_map->emplace(FormUserHost(user, host), password);
        it->Next();
    }
    user_map_.Refresh(std::move(new_user_map));
}

bool UserAccessManager::IsAuthenticated(const std::string& host, const std::string& user, const std::string& password) {
    if (auto stored_password = user_map_.Get(FormUserHost(user, host)); stored_password.has_value()) {
        return stored_password.value() == password;
    }
    return false;
}
}  // namespace openmldb::auth