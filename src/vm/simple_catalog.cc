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
#include "vm/simple_catalog.h"

namespace fesql {
namespace vm {

SimpleCatalog::SimpleCatalog(const bool enable_index)
    : enable_index_(enable_index) {}
SimpleCatalog::~SimpleCatalog() {}

void SimpleCatalog::AddDatabase(const fesql::type::Database &db) {
    auto &dict = table_handlers_[db.name()];
    for (int k = 0; k < db.tables_size(); ++k) {
        auto tbl = db.tables(k);
        dict[tbl.name()] =
            std::make_shared<SimpleCatalogTableHandler>(db.name(), tbl);
    }
    databases_[db.name()] = std::make_shared<fesql::type::Database>(db);
}

std::shared_ptr<type::Database> SimpleCatalog::GetDatabase(
    const std::string &db_name) {
    return databases_[db_name];
}

std::shared_ptr<TableHandler> SimpleCatalog::GetTable(
    const std::string &db_name, const std::string &table_name) {
    auto &dict = table_handlers_[db_name];
    return dict[table_name];
}
bool SimpleCatalog::IndexSupport() { return enable_index_; }

SimpleCatalogTableHandler::SimpleCatalogTableHandler(
    const std::string &db_name, const fesql::type::TableDef &table_def)
    : db_name_(db_name), table_def_(table_def) {
    // build col info and index info
    // init types var
    for (int32_t i = 0; i < table_def.columns_size(); i++) {
        const type::ColumnDef& column = table_def.columns(i);
        codec::ColInfo col_info(column.name(), column.type(), i, 0);
        types_dict_.insert(std::make_pair(column.name(), col_info));
    }

    // init index hint
    for (int32_t i = 0; i < table_def.indexes().size(); i++) {
        const type::IndexDef& index_def = table_def.indexes().Get(i);
        vm::IndexSt index_st;
        index_st.index = i;
        index_st.ts_pos = ::fesql::vm::INVALID_POS;
        if (!index_def.second_key().empty()) {
            int32_t pos = GetColumnIndex(index_def.second_key());
            if (pos < 0) {
                LOG(WARNING)
                    << "fail to get second key " << index_def.second_key();
                return;
            }
            index_st.ts_pos = pos;
        } else {
            DLOG(INFO) << "init table with empty second key";
        }
        index_st.name = index_def.name();
        for (int32_t j = 0; j < index_def.first_keys_size(); j++) {
            const std::string& key = index_def.first_keys(j);
            auto it = types_dict_.find(key);
            if (it == types_dict_.end()) {
                LOG(WARNING) << "column " << key << " does not exist in table "
                             << table_def.name();
                return;
            }
            index_st.keys.push_back(it->second);
        }
        index_hint_.insert(std::make_pair(index_st.name, index_st));
    }
}

const Types &SimpleCatalogTableHandler::GetTypes() { return this->types_dict_; }

const IndexHint &SimpleCatalogTableHandler::GetIndex() {
    return this->index_hint_;
}

const Schema *SimpleCatalogTableHandler::GetSchema() {
    return &this->table_def_.columns();
}

const std::string &SimpleCatalogTableHandler::GetName() {
    return this->table_def_.name();
}

const std::string &SimpleCatalogTableHandler::GetDatabase() {
    return this->db_name_;
}

std::unique_ptr<WindowIterator> SimpleCatalogTableHandler::GetWindowIterator(
    const std::string &) {
    LOG(ERROR) << "Unsupported operation: GetWindowIterator()";
    return nullptr;
}

const uint64_t SimpleCatalogTableHandler::GetCount() { return 0; }

fesql::codec::Row SimpleCatalogTableHandler::At(uint64_t pos) {
    LOG(ERROR) << "Unsupported operation: At()";
    return fesql::codec::Row();
}

std::shared_ptr<PartitionHandler> SimpleCatalogTableHandler::GetPartition(
    const std::string &index_name) {
    LOG(ERROR) << "Unsupported operation: GetPartition()";
    return nullptr;
}

std::unique_ptr<RowIterator> SimpleCatalogTableHandler::GetIterator() {
    LOG(ERROR) << "Unsupported operation: GetRawIterator()";
    return nullptr;
}

RowIterator *SimpleCatalogTableHandler::GetRawIterator() {
    LOG(ERROR) << "Unsupported operation: GetRawIterator()";
    return nullptr;
}

}  // namespace vm
}  // namespace fesql
