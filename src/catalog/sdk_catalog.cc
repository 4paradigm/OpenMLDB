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

#include <mutex>

#include "catalog/schema_adapter.h"
#include "glog/logging.h"

namespace rtidb {
namespace catalog {

SDKTableHandler::SDKTableHandler(const ::rtidb::nameserver::TableInfo& meta)
    : meta_(meta), schema_(), name_(meta.name()), db_(meta.db()) {}

SDKTableHandler::~SDKTableHandler() {}

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
        col_info.pos = i;
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
    DLOG(INFO) << "init table handler for table " << name_ << " in db " << db_
               << " done";
    return true;
}

bool SDKCatalog::Init(
    const std::vector<::rtidb::nameserver::TableInfo>& tables) {
    table_metas_ = tables;
    for (size_t i = 0; i < tables.size(); i++) {
        const ::rtidb::nameserver::TableInfo& table_meta = tables[i];
        std::shared_ptr<SDKTableHandler> table(new SDKTableHandler(table_meta));
        bool ok = table->Init();
        if (!ok) {
            LOG(WARNING) << "fail to init table " << table_meta.name();
            return false;
        }
        auto db_it = tables_.find(table->GetDatabase());
        if (db_it == tables_.end()) {
            tables_.insert(std::make_pair(
                table->GetDatabase(),
                std::map<std::string, std::shared_ptr<SDKTableHandler>>()));
            db_it = tables_.find(table->GetDatabase());
        }
        db_it->second.insert(std::make_pair(table->GetName(), table));
    }
    return true;
}

std::shared_ptr<::fesql::vm::TableHandler> SDKCatalog::GetTable(
    const std::string& db, const std::string& table_name) {
    std::lock_guard<::rtidb::base::SpinMutex> spin_lock(mu_);
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
