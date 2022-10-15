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

#include "catalog/sdk_catalog.h"

#include "base/hash.h"
#include "glog/logging.h"
#include "schema/index_util.h"
#include "schema/schema_adapter.h"

namespace openmldb {
namespace catalog {

SDKTableHandler::SDKTableHandler(const ::openmldb::nameserver::TableInfo& meta, const ClientManager& client_manager)
    : meta_(meta),
      schema_map_(),
      name_(meta.name()),
      db_(meta.db()),
      table_client_manager_(std::make_shared<TableClientManager>(meta.table_partition(), client_manager)) {}

bool SDKTableHandler::Init() {
    auto schema = std::make_shared<::hybridse::vm::Schema>();
    bool ok = schema::SchemaAdapter::ConvertSchema(meta_.column_desc(), schema.get());
    if (!ok) {
        LOG(WARNING) << "fail to covert schema to sql schema";
        return false;
    }
    schema_map_.emplace(1, schema);
    if (meta_.added_column_desc_size() > 0) {
        auto added_schema = std::make_shared<::hybridse::vm::Schema>();
        bool ok = schema::SchemaAdapter::ConvertSchema(meta_.added_column_desc(), added_schema.get());
        if (!ok) {
            LOG(WARNING) << "fail to covert schema to sql schema";
            return false;
        }
        for (int idx = 0; idx < meta_.schema_versions_size(); idx++) {
            auto new_schema = std::make_shared<::hybridse::vm::Schema>(*schema);
            for (int pos = 0; pos < meta_.schema_versions(idx).field_count() - meta_.column_desc_size(); pos++) {
                auto new_column = new_schema->Add();
                new_column->CopyFrom(added_schema->Get(pos));
            }
            schema_map_.emplace(meta_.schema_versions(idx).id(), new_schema);
        }
    }

    // init types var
    auto cur_schema = schema_map_.rbegin()->second;
    for (int32_t i = 0; i < cur_schema->size(); i++) {
        const ::hybridse::type::ColumnDef& column = cur_schema->Get(i);
        ::hybridse::vm::ColInfo col_info;
        col_info.type = column.type();
        col_info.idx = i;
        col_info.name = column.name();
        types_.emplace(column.name(), col_info);
    }

    // init index hint
    for (int32_t i = 0; i < meta_.column_key_size(); i++) {
        const auto& column_key = meta_.column_key(i);
        ::hybridse::vm::IndexSt index_st;
        index_st.index = i;
        index_st.ts_pos = ::hybridse::vm::INVALID_POS;
        if (!column_key.ts_name().empty()) {
            int32_t pos = GetColumnIndex(column_key.ts_name());
            if (pos < 0) {
                LOG(WARNING) << "fail to get second key " << column_key.ts_name();
                return false;
            }
            index_st.ts_pos = pos;
        }
        index_st.name = column_key.index_name();
        for (int32_t j = 0; j < column_key.col_name_size(); j++) {
            const std::string& key = column_key.col_name(j);
            auto it = types_.find(key);
            if (it == types_.end()) {
                LOG(WARNING) << "column " << key << " does not exist in table " << name_;
                return false;
            }
            index_st.keys.push_back(it->second);
        }
        index_hint_.emplace(index_st.name, index_st);
    }
    VLOG(5) << "init table handler for table " << name_ << " in db " << db_ << " done";
    return true;
}

std::shared_ptr<::hybridse::vm::Tablet> SDKTableHandler::GetTablet(const std::string& index_name,
                                                                   const std::string& pk) {
    if (index_name.empty() || pk.empty()) {
        return std::shared_ptr<::hybridse::vm::Tablet>();
    }
    uint32_t pid = 0;
    uint32_t pid_num = meta_.table_partition_size();
    if (pid_num > 0) {
        pid = (uint32_t)(::openmldb::base::hash64(pk) % pid_num);
    }
    return table_client_manager_->GetTablet(pid);
}

std::shared_ptr<TabletAccessor> SDKTableHandler::GetTablet(uint32_t pid) {
    return table_client_manager_->GetTablet(pid);
}

bool SDKTableHandler::GetTablet(std::vector<std::shared_ptr<TabletAccessor>>* tablets) {
    if (tablets == nullptr) {
        return false;
    }
    tablets->clear();
    for (uint32_t pid = 0; pid < (uint32_t)meta_.table_partition_size(); pid++) {
        auto tablet = table_client_manager_->GetTablet(pid);
        if (tablet == nullptr) {
            LOG(WARNING) << "fail to get tablet for pid " << pid;
            return false;
        }
        tablets->push_back(tablet);
    }
    return true;
}

bool SDKCatalog::Init(const std::vector<::openmldb::nameserver::TableInfo>& tables, const Procedures& db_sp_map) {
    for (size_t i = 0; i < tables.size(); i++) {
        const ::openmldb::nameserver::TableInfo& table_meta = tables[i];
        std::shared_ptr<SDKTableHandler> table = std::make_shared<SDKTableHandler>(table_meta, *client_manager_);
        if (!table->Init()) {
            LOG(WARNING) << "fail to init table " << table_meta.name();
            return false;
        }
        auto db_it = tables_.find(table->GetDatabase());
        if (db_it == tables_.end()) {
            auto result_pair = tables_.insert(
                std::make_pair(table->GetDatabase(), std::map<std::string, std::shared_ptr<SDKTableHandler>>()));
            db_it = result_pair.first;
        }
        db_it->second.insert(std::make_pair(table->GetName(), table));
    }
    db_sp_map_ = db_sp_map;
    return true;
}

std::shared_ptr<::hybridse::vm::TableHandler> SDKCatalog::GetTable(const std::string& db,
                                                                   const std::string& table_name) {
    auto db_it = tables_.find(db);
    if (db_it == tables_.end()) {
        return std::shared_ptr<::hybridse::vm::TableHandler>();
    }
    auto it = db_it->second.find(table_name);
    if (it == db_it->second.end()) {
        return std::shared_ptr<::hybridse::vm::TableHandler>();
    }
    return it->second;
}

std::shared_ptr<TabletAccessor> SDKCatalog::GetTablet() const { return client_manager_->GetTablet(); }

std::vector<std::shared_ptr<TabletAccessor>> SDKCatalog::GetAllTablet() const {
    return client_manager_->GetAllTablet();
}

std::shared_ptr<::hybridse::sdk::ProcedureInfo> SDKCatalog::GetProcedureInfo(const std::string& db,
                                                                             const std::string& sp_name) {
    auto db_sp_it = db_sp_map_.find(db);
    if (db_sp_it == db_sp_map_.end()) {
        return nullptr;
    }
    auto& map = db_sp_it->second;
    auto sp_it = map.find(sp_name);
    if (sp_it == map.end()) {
        return nullptr;
    }
    return sp_it->second;
}

}  // namespace catalog
}  // namespace openmldb
