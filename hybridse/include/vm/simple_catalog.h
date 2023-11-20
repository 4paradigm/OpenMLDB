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

#ifndef HYBRIDSE_INCLUDE_VM_SIMPLE_CATALOG_H_
#define HYBRIDSE_INCLUDE_VM_SIMPLE_CATALOG_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "proto/fe_type.pb.h"
#include "vm/catalog.h"
#include "vm/mem_catalog.h"

namespace hybridse {
namespace vm {

class SimpleCatalogTableHandler : public TableHandler {
 public:
    explicit SimpleCatalogTableHandler(const std::string &db_name,
                                       const hybridse::type::TableDef &);

    const Schema *GetSchema() override;

    const std::string &GetName() override;

    const std::string &GetDatabase() override;

    const Types &GetTypes() override;

    const IndexHint &GetIndex() override;

    std::unique_ptr<hybridse::codec::WindowIterator> GetWindowIterator(
        const std::string &) override;

    const uint64_t GetCount() override;

    hybridse::codec::Row At(uint64_t pos) override;

    std::shared_ptr<PartitionHandler> GetPartition(
        const std::string &index_name) override;

    std::unique_ptr<RowIterator> GetIterator() override;

    RowIterator *GetRawIterator() override;

    bool AddRow(const Row row);
    bool DecodeKeysAndTs(const IndexSt &index, const int8_t *buf, uint32_t size,
                         std::string &key, int64_t *time_ptr);  // NOLINT

 private:
    inline int32_t GetColumnIndex(const std::string &column) {
        auto it = types_dict_.find(column);
        if (it != types_dict_.end()) {
            return it->second.idx;
        }
        return -1;
    }
    std::string db_name_;
    hybridse::type::TableDef table_def_;
    Types types_dict_;
    IndexHint index_hint_;
    codec::RowView row_view_;
    std::map<std::string, std::shared_ptr<MemPartitionHandler>> table_storage;
    std::shared_ptr<MemTableHandler> full_table_storage_;
};

/**
 * Simple Catalog without actual data bindings.
 */
class SimpleCatalog : public Catalog {
 public:
    explicit SimpleCatalog(const bool enable_index = false);
    SimpleCatalog(const SimpleCatalog &) = delete;
    ~SimpleCatalog();

    void AddDatabase(const hybridse::type::Database &db);
    std::shared_ptr<type::Database> GetDatabase(const std::string &db) override;
    std::shared_ptr<TableHandler> GetTable(
        const std::string &db, const std::string &table_name) override;
    bool IndexSupport() override;

    bool InsertRows(const std::string &db, const std::string &table,
                    const std::vector<Row> &row);

    std::vector<AggrTableInfo> GetAggrTables(const std::string &base_db, const std::string &base_table,
                                             const std::string &aggr_func, const std::string &aggr_col,
                                             const std::string &partition_cols, const std::string &order_col,
                                             const std::string &filter_col) override;

 private:
    bool enable_index_;
    std::map<std::string,
             std::map<std::string, std::shared_ptr<SimpleCatalogTableHandler>>>
        table_handlers_;

    std::map<std::string, std::shared_ptr<type::Database>> databases_;
};

}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_VM_SIMPLE_CATALOG_H_
