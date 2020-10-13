/*
 * sdk_catalog.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "catalog/sdk_catalog.h"

#include "base/hash.h"
#include "catalog/schema_adapter.h"
#include "glog/logging.h"

namespace rtidb {
namespace catalog {

SDKTableHandler::SDKTableHandler(const ::rtidb::nameserver::TableInfo& meta,
        const ClientManager& client_manager)
    : meta_(meta), schema_(), name_(meta.name()), db_(meta.db()),
    table_client_manager_(std::make_shared<TableClientManager>(meta.table_partition(), client_manager)),
    partition_key_() {}

bool SDKTableHandler::Init() {
    if (meta_.format_version() != 1) {
        LOG(WARNING) << "bad format version " << meta_.format_version();
        return false;
    }
    bool ok = SchemaAdapter::ConvertSchema(meta_.column_desc_v1(), &schema_);
    if (!ok) {
        LOG(WARNING) << "fail to covert schema to sql schema";
        return false;
    }

    ok = SchemaAdapter::ConvertIndex(meta_.column_key(), &index_list_);
    if (!ok) {
        LOG(WARNING) << "fail to conver index to sql index";
        return false;
    }

    // init types var
    for (int32_t i = 0; i < schema_.size(); i++) {
        const ::fesql::type::ColumnDef& column = schema_.Get(i);
        ::fesql::vm::ColInfo col_info;
        col_info.type = column.type();
        col_info.idx = i;
        col_info.name = column.name();
        types_.insert(std::make_pair(column.name(), col_info));
    }

    // init index hint
    for (int32_t i = 0; i < index_list_.size(); i++) {
        const ::fesql::type::IndexDef& index_def = index_list_.Get(i);
        ::fesql::vm::IndexSt index_st;
        index_st.index = i;
        int32_t pos = GetColumnIndex(index_def.second_key());
        if (pos < 0) {
            LOG(WARNING) << "fail to get second key " << index_def.second_key();
            return false;
        }
        index_st.ts_pos = pos;
        index_st.name = index_def.name();
        for (int32_t j = 0; j < index_def.first_keys_size(); j++) {
            const std::string& key = index_def.first_keys(j);
            auto it = types_.find(key);
            if (it == types_.end()) {
                LOG(WARNING)
                    << "column " << key << " does not exist in table " << name_;
                return false;
            }
            index_st.keys.push_back(it->second);
        }
        index_hint_.insert(std::make_pair(index_st.name, index_st));
    }

    for (const auto& name : meta_.partition_key()) {
        partition_key_.push_back(name);
    }
    DLOG(INFO) << "init table handler for table " << name_ << " in db " << db_
               << " done";
    return true;
}

bool SDKTableHandler::GetTablets(const std::string& index_name, const std::string& pk,
        std::vector<std::shared_ptr<::rtidb::client::TabletClient>>* tablets) {
    if (index_name.empty() || pk.empty() || tablets == nullptr) {
        return false;
    }
    tablets->clear();
    uint32_t pid_num = (uint32_t)meta_.table_partition_size();
    if (!partition_key_.empty()) {
        auto iter = index_hint_.find(index_name);
        if (iter == index_hint_.end()) {
            return false;
        }
        bool match_partition_key = true;
        if (partition_key_.size() == iter->second.keys.size()) {
            for (const auto& col : iter->second.keys) {
                if (std::find(partition_key_.begin(), partition_key_.end(), col.name) == partition_key_.end()) {
                    match_partition_key = false;
                    break;
                }
            }
        }
        if (!match_partition_key) {
            for (uint32_t pid = 0; pid < pid_num; pid++) {
                tablets->push_back(table_client_manager_->GetTablets(pid));
            }
            return true;
        }
    }
    uint32_t pid = 0;
    if (pid_num > 0) {
        pid = (uint32_t)(::rtidb::base::hash64(pk) % pid_num);
    }
    tablets->push_back(table_client_manager_->GetTablets(pid));
    return true;
}

bool SDKCatalog::Init(
    const std::vector<::rtidb::nameserver::TableInfo>& tables,
    const std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>>& tablet_clients) {
    table_metas_ = tables;
    client_manager_.UpdateClient(tablet_clients);
    for (size_t i = 0; i < tables.size(); i++) {
        const ::rtidb::nameserver::TableInfo& table_meta = tables[i];
        std::shared_ptr<SDKTableHandler> table = std::make_shared<SDKTableHandler>(table_meta, client_manager_);
        if (!table->Init()) {
            LOG(WARNING) << "fail to init table " << table_meta.name();
            return false;
        }
        auto db_it = tables_.find(table->GetDatabase());
        if (db_it == tables_.end()) {
            auto result_pair = tables_.insert(std::make_pair(
                    table->GetDatabase(),
                    std::map<std::string, std::shared_ptr<SDKTableHandler>>()));
            db_it = result_pair.first;
        }
        db_it->second.insert(std::make_pair(table->GetName(), table));
    }
    return true;
}

std::shared_ptr<::fesql::vm::TableHandler> SDKCatalog::GetTable(
    const std::string& db, const std::string& table_name) {
    auto db_it = tables_.find(db);
    if (db_it == tables_.end()) {
        return std::shared_ptr<::fesql::vm::TableHandler>();
    }
    auto it = db_it->second.find(table_name);
    if (it == db_it->second.end()) {
        return std::shared_ptr<::fesql::vm::TableHandler>();
    }
    return it->second;
}

}  // namespace catalog
}  // namespace rtidb
