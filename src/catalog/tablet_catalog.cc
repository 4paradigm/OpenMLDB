/*
 * tablet_catalog.cc
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

#include "catalog/tablet_catalog.h"

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "catalog/schema_adapter.h"
#include "catalog/table_iterator_adapter.h"
#include "catalog/distribute_iterator.h"
#include "codec/list_iterator_codec.h"
#include "glog/logging.h"

namespace rtidb {
namespace catalog {

TabletTableHandler::TabletTableHandler(const ::rtidb::api::TableMeta& meta)
    : meta_(meta),
      schema_(),
      name_(meta.name()),
      db_(meta.db()),
      tables_(std::make_shared<Tables>()),
      types_(),
      index_list_(),
      index_hint_() {}

bool TabletTableHandler::Init() {
    bool ok = SchemaAdapter::ConvertSchema(meta_.column_desc(), &schema_);
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
                LOG(WARNING) << "column " << key << " does not exist in table";
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

std::unique_ptr<::fesql::codec::RowIterator> TabletTableHandler::GetIterator() const {
    auto tables = std::atomic_load_explicit(&tables_, std::memory_order_relaxed);
    if (!tables->empty()) {
        return std::unique_ptr<catalog::FullTableIterator>(
            new catalog::FullTableIterator(tables));
    }
    return std::unique_ptr<::fesql::codec::RowIterator>();
}

std::unique_ptr<::fesql::codec::WindowIterator>
TabletTableHandler::GetWindowIterator(const std::string& idx_name) {
    auto iter = index_hint_.find(idx_name);
    if (iter == index_hint_.end()) {
        LOG(WARNING) << "index name " << idx_name << " not exist";
        return std::unique_ptr<::fesql::codec::WindowIterator>();
    }
    DLOG(INFO) << "get window it with index " << idx_name;
    return std::unique_ptr<::fesql::codec::WindowIterator> (new DistributeWindowIterator(tables_, iter->second.index));
}

// TODO(chenjing): 基于segment 优化Get(int pos) 操作
const ::fesql::codec::Row TabletTableHandler::Get(int32_t pos) {
    auto iter = GetIterator();
    while (pos-- > 0 && iter->Valid()) {
        iter->Next();
    }
    return iter->Valid() ? iter->GetValue() : ::fesql::codec::Row();
}

::fesql::codec::RowIterator* TabletTableHandler::GetIterator(
    int8_t* addr) const {
    return NULL;
}

const uint64_t TabletTableHandler::GetCount() {
    auto iter = GetIterator();
    uint64_t cnt = 0;
    while (iter->Valid()) {
        iter->Next();
        cnt++;
    }
    return cnt;
}

::fesql::codec::Row TabletTableHandler::At(uint64_t pos) {
    auto iter = GetIterator();
    while (pos-- > 0 && iter->Valid()) {
        iter->Next();
    }
    return iter->Valid() ? iter->GetValue() : ::fesql::codec::Row();
}

std::shared_ptr<::fesql::vm::PartitionHandler> TabletTableHandler::GetPartition(
        std::shared_ptr<::fesql::vm::TableHandler> table_hander,
        const std::string &index_name) const {
    if (!table_hander) {
        LOG(WARNING) << "fail to get partition for tablet table handler: "
                        "table handler is null";
        return std::shared_ptr<::fesql::vm::PartitionHandler>();
    }
    if (table_hander->GetIndex().find(index_name) ==
        table_hander->GetIndex().cend()) {
        LOG(WARNING)
            << "fail to get partition for tablet table handler, index name "
            << index_name;
        return std::shared_ptr<::fesql::vm::PartitionHandler>();
    }
    return std::make_shared<TabletPartitionHandler>(table_hander, index_name);
}

void TabletTableHandler::AddTable(std::shared_ptr<::rtidb::storage::Table> table) {
    auto old_tables = std::atomic_load_explicit(&tables_, std::memory_order_relaxed);
    auto new_tables = std::make_shared<Tables>(*old_tables);
    new_tables->emplace(table->GetPid(), table);
    std::atomic_store_explicit(&tables_, new_tables, std::memory_order_relaxed);
}

int TabletTableHandler::DeleteTable(uint32_t pid) {
    auto old_tables = std::atomic_load_explicit(&tables_, std::memory_order_relaxed);
    auto new_tables = std::make_shared<Tables>(*old_tables);
    new_tables->erase(pid);
    std::atomic_store_explicit(&tables_, new_tables, std::memory_order_relaxed);
    return new_tables->size();
}

TabletCatalog::TabletCatalog() : mu_(), tables_(), db_() {}

TabletCatalog::~TabletCatalog() {}

bool TabletCatalog::Init() { return true; }

std::shared_ptr<::fesql::type::Database> TabletCatalog::GetDatabase(
    const std::string& db) {
    std::lock_guard<::rtidb::base::SpinMutex> spin_lock(mu_);
    auto it = db_.find(db);
    if (it == db_.end()) {
        return std::shared_ptr<::fesql::type::Database>();
    }
    return it->second;
}

std::shared_ptr<::fesql::vm::TableHandler> TabletCatalog::GetTable(
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

bool TabletCatalog::AddTable(const ::rtidb::api::TableMeta& meta,
        std::shared_ptr<::rtidb::storage::Table> table) {
    if (!table) {
        LOG(WARNING) << "input table is null";
        return false;
    }
    const std::string& db_name = meta.db();
    std::shared_ptr<TabletTableHandler> handler;
    std::lock_guard<::rtidb::base::SpinMutex> spin_lock(mu_);
    auto db_it = tables_.find(db_name);
    if (db_it == tables_.end()) {
        auto result = tables_.emplace(
            db_name, std::map<std::string, std::shared_ptr<TabletTableHandler>>());
        db_it = result.first;
    }
    const std::string& table_name = meta.name();
    auto it = db_it->second.find(table_name);
    if (it == db_it->second.end()) {
        handler = std::make_shared<TabletTableHandler>(meta);
        if (!handler->Init()) {
            LOG(WARNING) << "tablet handler init failed";
            return false;
        }
        db_it->second.emplace(table_name, handler);
    } else {
        handler = it->second;
    }
    handler->AddTable(table);
    return true;
}

bool TabletCatalog::AddDB(const ::fesql::type::Database& db) {
    std::lock_guard<::rtidb::base::SpinMutex> spin_lock(mu_);
    TabletDB::iterator it = db_.find(db.name());
    if (it != db_.end()) {
        return false;
    }
    tables_.insert(std::make_pair(
        db.name(),
        std::map<std::string, std::shared_ptr<TabletTableHandler>>()));
    return true;
}

bool TabletCatalog::DeleteTable(const std::string& db,
                                const std::string& table_name,
                                uint32_t pid) {
    std::lock_guard<::rtidb::base::SpinMutex> spin_lock(mu_);
    auto db_it = tables_.find(db);
    if (db_it == tables_.end()) {
        return false;
    }
    auto it = db_it->second.find(table_name);
    if (it == db_it->second.end()) {
        return false;
    }
    if (it->second->DeleteTable(pid) < 1) {
        db_it->second.erase(it);
    }
    return true;
}

bool TabletCatalog::DeleteDB(const std::string& db) {
    std::lock_guard<::rtidb::base::SpinMutex> spin_lock(mu_);
    return db_.erase(db) > 0;
}

bool TabletCatalog::IndexSupport() { return true; }

}  // namespace catalog
}  // namespace rtidb
