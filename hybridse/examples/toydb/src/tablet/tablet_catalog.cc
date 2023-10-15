/*
 * Copyright 2021 4Paradigm
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

#include "tablet/tablet_catalog.h"
#include <map>
#include <memory>
#include <string>
#include <utility>
#include "codec/list_iterator_codec.h"
#include "glog/logging.h"
#include "storage/table_iterator.h"

namespace hybridse {
namespace tablet {
using hybridse::codec::RowIterator;
using hybridse::codec::WindowIterator;

TabletTableHandler::TabletTableHandler(const vm::Schema schema,
                                       const std::string& name,
                                       const std::string& db,
                                       const vm::IndexList& index_list,
                                       std::shared_ptr<storage::Table> table)
    : schema_(schema),
      name_(name),
      db_(db),
      table_(table),
      types_(),
      index_list_(index_list),
      tablet_() {}

TabletTableHandler::TabletTableHandler(const vm::Schema schema,
                                       const std::string& name,
                                       const std::string& db,
                                       const vm::IndexList& index_list,
                                       std::shared_ptr<storage::Table> table,
                                       std::shared_ptr<vm::Tablet> tablet)
    : schema_(schema),
      name_(name),
      db_(db),
      table_(table),
      types_(),
      index_list_(index_list),
      tablet_(tablet) {}
TabletTableHandler::~TabletTableHandler() {}

bool TabletTableHandler::Init() {
    // init types var
    for (int32_t i = 0; i < schema_.size(); i++) {
        const type::ColumnDef& column = schema_.Get(i);
        codec::ColInfo col_info(column.name(), column.type(), i, 0);
        types_.insert(std::make_pair(column.name(), col_info));
    }

    // init index hint
    for (int32_t i = 0; i < index_list_.size(); i++) {
        const type::IndexDef& index_def = index_list_.Get(i);
        vm::IndexSt index_st;
        index_st.index = i;
        index_st.ts_pos = ::hybridse::vm::INVALID_POS;
        if (!index_def.second_key().empty()) {
            int32_t pos = GetColumnIndex(index_def.second_key());
            if (pos < 0) {
                LOG(WARNING)
                    << "fail to get second key " << index_def.second_key();
                return false;
            }
            index_st.ts_pos = pos;
        } else {
            DLOG(INFO) << "init table with empty second key";
        }
        index_st.name = index_def.name();
        for (int32_t j = 0; j < index_def.first_keys_size(); j++) {
            const std::string& key = index_def.first_keys(j);
            auto it = types_.find(key);
            if (it == types_.end()) {
                LOG(WARNING) << "column " << key << " does not exist in table "
                             << table_;
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

std::unique_ptr<RowIterator> TabletTableHandler::GetIterator() {
    std::unique_ptr<storage::FullTableIterator> it(
        new storage::FullTableIterator(table_->GetSegments(),
                                       table_->GetSegCnt(), table_));
    return std::move(it);
}

std::unique_ptr<WindowIterator> TabletTableHandler::GetWindowIterator(
    const std::string& idx_name) {
    auto iter = index_hint_.find(idx_name);
    if (iter == index_hint_.end()) {
        LOG(WARNING) << "index name " << idx_name << " not exist";
        return std::unique_ptr<storage::WindowTableIterator>();
    }
    if (nullptr == table_->GetSegments()) {
        return std::unique_ptr<storage::WindowTableIterator>();
    }
    std::unique_ptr<storage::WindowTableIterator> it(
        new storage::WindowTableIterator(table_->GetSegments(),
                                         table_->GetSegCnt(),
                                         iter->second.index, table_));
    return std::move(it);
}

// TODP(chenjing): 基于segment 优化Get(int pos) 操作
const Row TabletTableHandler::Get(int32_t pos) {
    auto iter = GetIterator();

    while (pos-- > 0 && iter->Valid()) {
        iter->Next();
    }
    return iter->Valid() ? iter->GetValue() : Row();
}
RowIterator* TabletTableHandler::GetRawIterator() {
    return new storage::FullTableIterator(table_->GetSegments(),
                                          table_->GetSegCnt(), table_);
}

TabletCatalog::TabletCatalog() : tables_(), db_() {}

TabletCatalog::~TabletCatalog() {}

bool TabletCatalog::Init() { return true; }

std::shared_ptr<type::Database> TabletCatalog::GetDatabase(
    const std::string& db) {
    auto it = db_.find(db);
    if (it == db_.end()) {
        return std::shared_ptr<type::Database>();
    }
    return it->second;
}

std::shared_ptr<vm::TableHandler> TabletCatalog::GetTable(
    const std::string& db, const std::string& table_name) {
    auto db_it = tables_.find(db);
    if (db_it == tables_.end()) {
        return std::shared_ptr<vm::TableHandler>();
    }
    auto it = db_it->second.find(table_name);
    if (it == db_it->second.end()) {
        return std::shared_ptr<vm::TableHandler>();
    }
    return it->second;
}

bool TabletCatalog::AddTable(std::shared_ptr<TabletTableHandler> table) {
    if (!table) {
        LOG(WARNING) << "input table is null";
        return false;
    }
    auto db_it = tables_.find(table->GetDatabase());
    if (db_it == tables_.end()) {
        tables_.insert(std::make_pair(
            table->GetDatabase(),
            std::map<std::string, std::shared_ptr<TabletTableHandler>>()));
        db_it = tables_.find(table->GetDatabase());
    }
    auto it = db_it->second.find(table->GetName());
    if (it != db_it->second.end()) {
        return false;
    }
    db_it->second.insert(std::make_pair(table->GetName(), table));
    return true;
}

bool TabletCatalog::AddDB(const type::Database& db) {
    TabletDB::iterator it = db_.find(db.name());
    if (it != db_.end()) {
        return false;
    }
    tables_.insert(std::make_pair(
        db.name(),
        std::map<std::string, std::shared_ptr<TabletTableHandler>>()));
    db_.insert(std::make_pair(db.name(), std::make_shared<type::Database>(db)));
    return true;
}
bool TabletCatalog::IndexSupport() { return true; }

TabletSegmentHandler::TabletSegmentHandler(
    std::shared_ptr<vm::PartitionHandler> partition_hander,
    const std::string& key)
    : TableHandler(), partition_hander_(partition_hander), key_(key) {}
TabletSegmentHandler::~TabletSegmentHandler() {}
std::unique_ptr<RowIterator> TabletSegmentHandler::GetIterator() {
    auto iter = partition_hander_->GetWindowIterator();
    if (iter) {
        iter->Seek(key_);
        if (iter->Valid() &&
            0 == iter->GetKey().compare(hybridse::codec::Row(key_))) {
            return iter->GetValue();
        } else {
            return std::unique_ptr<::hybridse::vm::RowIterator>();
        }
    }
    return std::unique_ptr<RowIterator>();
}
RowIterator* TabletSegmentHandler::GetRawIterator() {
    auto iter = partition_hander_->GetWindowIterator();
    if (iter) {
        iter->Seek(key_);
        if (iter->Valid() &&
            0 == iter->GetKey().compare(hybridse::codec::Row(key_))) {
            return iter->GetRawValue();
        } else {
            return nullptr;
        }
    }
    return nullptr;
}
std::unique_ptr<WindowIterator> TabletSegmentHandler::GetWindowIterator(
    const std::string& idx_name) {
    return std::unique_ptr<WindowIterator>();
}

const uint64_t TabletPartitionHandler::GetCount() {
    auto iter = GetWindowIterator();
    uint64_t cnt = 0;
    while (iter->Valid()) {
        cnt++;
        iter->Next();
    }
    return cnt;
}

}  // namespace tablet
}  // namespace hybridse
