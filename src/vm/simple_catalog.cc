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

SimpleCatalog::SimpleCatalog() {}
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
bool SimpleCatalog::IndexSupport() { return false; }

SimpleCatalogTableHandler::SimpleCatalogTableHandler(
    const std::string &db_name, const fesql::type::TableDef &table_def)
    : db_name_(db_name), table_def_(table_def) {
    // build col info and index info
    for (int k = 0; k < table_def.columns_size(); ++k) {
        auto column = table_def.columns(k);
        ColInfo col_info(column.name(), column.type(), k, 0);
        this->types_dict_[column.name()] = col_info;
    }
    for (int k = 0; k < table_def.indexes_size(); ++k) {
        auto index = table_def_.indexes(k);
        IndexSt hint;
        hint.index = k;
        hint.name = index.name();
        // set ts col
        auto iter = types_dict_.find(index.second_key());
        if (iter != types_dict_.end()) {
            hint.ts_pos = iter->second.idx;
        } else {
            LOG(ERROR) << "Fail to find ts index: " << index.second_key();
        }
        // set keys
        iter = types_dict_.find(index.first_keys(0));
        if (iter != types_dict_.end()) {
            hint.keys.push_back(iter->second);
        } else {
            LOG(ERROR) << "Fail to find key: " << index.first_keys(0);
        }
        this->index_hint_[index.name()] = hint;
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
