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
#include <utility>

namespace hybridse {
namespace vm {

SimpleCatalog::SimpleCatalog(const bool enable_index)
    : enable_index_(enable_index) {}
SimpleCatalog::~SimpleCatalog() {}

void SimpleCatalog::AddDatabase(const hybridse::type::Database &db) {
    auto &dict = table_handlers_[db.name()];
    for (int k = 0; k < db.tables_size(); ++k) {
        auto tbl = db.tables(k);
        dict[tbl.name()] =
            std::make_shared<SimpleCatalogTableHandler>(db.name(), tbl);
    }
    databases_[db.name()] = std::make_shared<hybridse::type::Database>(db);
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

bool SimpleCatalog::InsertRows(const std::string &db_name,
                               const std::string &table_name,
                               const std::vector<Row> &rows) {
    auto table = GetTable(db_name, table_name);
    if (!table) {
        LOG(WARNING) << "table:" << table_name
                     << " isn't exist in db:" << db_name;
    }
    for (auto &row : rows) {
        if (!std::dynamic_pointer_cast<SimpleCatalogTableHandler>(table)
                 ->AddRow(row)) {
            return false;
        }
    }
    return true;
}
SimpleCatalogTableHandler::SimpleCatalogTableHandler(
    const std::string &db_name, const hybridse::type::TableDef &table_def)
    : db_name_(db_name),
      table_def_(table_def),
      row_view_(table_def_.columns()) {
    // build col info and index info
    // init types var
    for (int32_t i = 0; i < table_def.columns_size(); i++) {
        const type::ColumnDef &column = table_def.columns(i);
        if (column.has_schema()) {
            // new schema field
            types_dict_.emplace(column.name(), ColInfo(column.name(), column.schema(), i, 0));
        } else {
            // old type field
            codec::ColInfo col_info(column.name(), column.type(), i, 0);
            types_dict_.emplace(column.name(), col_info);
        }
    }

    // init index hint
    for (int32_t i = 0; i < table_def.indexes().size(); i++) {
        const type::IndexDef &index_def = table_def.indexes().Get(i);
        vm::IndexSt index_st;
        index_st.index = i;
        index_st.ts_pos = ::hybridse::vm::INVALID_POS;
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
            const std::string &key = index_def.first_keys(j);
            auto it = types_dict_.find(key);
            if (it == types_dict_.end()) {
                LOG(WARNING) << "column " << key << " does not exist in table "
                             << table_def.name();
                return;
            }
            index_st.keys.push_back(it->second);
        }
        index_hint_.insert(std::make_pair(index_st.name, index_st));
        table_storage.insert(std::make_pair(
            index_st.name, std::make_shared<MemPartitionHandler>()));
    }
    full_table_storage_ = std::make_shared<MemTableHandler>();
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
    const std::string &index_name) {
    if (table_storage.find(index_name) == table_storage.end()) {
        return nullptr;
    } else {
        return table_storage[index_name]->GetWindowIterator();
    }
}

const uint64_t SimpleCatalogTableHandler::GetCount() { return 0; }

hybridse::codec::Row SimpleCatalogTableHandler::At(uint64_t pos) {
    LOG(ERROR) << "Unsupported operation: At()";
    return hybridse::codec::Row();
}

std::shared_ptr<PartitionHandler> SimpleCatalogTableHandler::GetPartition(
    const std::string &index_name) {
    if (table_storage.find(index_name) == table_storage.end()) {
        return nullptr;
    } else {
        return table_storage[index_name];
    }
}

std::unique_ptr<RowIterator> SimpleCatalogTableHandler::GetIterator() {
    return full_table_storage_->GetIterator();
}

RowIterator *SimpleCatalogTableHandler::GetRawIterator() {
    return full_table_storage_->GetRawIterator();
}

bool SimpleCatalogTableHandler::DecodeKeysAndTs(const IndexSt &index,
                                                const int8_t *buf,
                                                uint32_t size, std::string &key,
                                                int64_t *time_ptr) {
    for (const auto &col : index.keys) {
        // expect keys and ts as base types, so calling 'type()' is generally safe
        assert(col.schema.has_base_type());

        if (!key.empty()) {
            key.append("|");
        }
        if (row_view_.IsNULL(buf, col.idx)) {
            key.append(codec::NONETOKEN);
        } else if (col.type() == ::hybridse::type::kVarchar) {
            const char *val = NULL;
            uint32_t length = 0;
            row_view_.GetValue(buf, col.idx, &val, &length);
            if (length != 0) {
                key.append(val, length);
            } else {
                key.append(codec::EMPTY_STRING);
            }
        } else {
            int64_t value = 0;
            row_view_.GetInteger(buf, col.idx, col.type(), &value);
            key.append(std::to_string(value));
        }
    }

    if (hybridse::vm::INVALID_POS == index.ts_pos ||
        row_view_.IsNULL(buf, index.ts_pos)) {
        *time_ptr = 0;
        return true;
    }
    row_view_.GetInteger(buf, index.ts_pos,
                         table_def_.columns(index.ts_pos).type(), time_ptr);
    return true;
}
bool SimpleCatalogTableHandler::AddRow(const Row row) {
    if (row.GetRowPtrCnt() != 1) {
        LOG(ERROR) << "Invalid row";
    }
    full_table_storage_->AddRow(row);
    for (const auto &kv : index_hint_) {
        auto partition = table_storage[kv.first];
        if (!partition) {
            LOG(WARNING) << "Invalid index " << kv.first;
            return false;
        }
        std::string key;
        int64_t time = 1;
        if (!DecodeKeysAndTs(kv.second, row.buf(), row.size(), key, &time)) {
            LOG(ERROR) << "Invalid row";
            return false;
        }
        partition->AddRow(key, time, row);
    }
    return true;
}

std::vector<AggrTableInfo> SimpleCatalog::GetAggrTables(const std::string &base_db, const std::string &base_table,
                                                        const std::string &aggr_func, const std::string &aggr_col,
                                                        const std::string &partition_cols, const std::string &order_col,
                                                        const std::string &filter_col) {
    ::hybridse::vm::AggrTableInfo info = {"aggr_" + base_table, "aggr_db", base_db, base_table, aggr_func, aggr_col,
                                          partition_cols,       order_col, "1000",  filter_col};
    return {info};
}

}  // namespace vm
}  // namespace hybridse
