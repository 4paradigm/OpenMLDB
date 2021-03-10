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

#ifndef SRC_VM_SIMPLE_CATALOG_H_
#define SRC_VM_SIMPLE_CATALOG_H_

#include <map>
#include <memory>
#include <string>

#include "glog/logging.h"
#include "proto/fe_type.pb.h"
#include "vm/catalog.h"

namespace fesql {
namespace vm {

class SimpleCatalogTableHandler : public TableHandler {
 public:
    explicit SimpleCatalogTableHandler(const std::string &db_name,
                                       const fesql::type::TableDef &);

    const Schema *GetSchema() override;

    const std::string &GetName() override;

    const std::string &GetDatabase() override;

    const Types &GetTypes() override;

    const IndexHint &GetIndex() override;

    std::unique_ptr<fesql::codec::WindowIterator> GetWindowIterator(
        const std::string &) override;

    const uint64_t GetCount() override;

    fesql::codec::Row At(uint64_t pos) override;

    std::shared_ptr<PartitionHandler> GetPartition(
        const std::string &index_name) override;

    std::unique_ptr<RowIterator> GetIterator() override;

    RowIterator *GetRawIterator() override;

 private:
    inline int32_t GetColumnIndex(const std::string &column) {
        auto it = types_dict_.find(column);
        if (it != types_dict_.end()) {
            return it->second.idx;
        }
        return -1;
    }
    std::string db_name_;
    fesql::type::TableDef table_def_;

    Types types_dict_;
    IndexHint index_hint_;
};

/**
 * Simple Catalog without actual data bindings.
 */
class SimpleCatalog : public Catalog {
 public:
    explicit SimpleCatalog(const bool enable_index = false);
    SimpleCatalog(const SimpleCatalog &) = delete;
    ~SimpleCatalog();

    void AddDatabase(const fesql::type::Database &db);
    std::shared_ptr<type::Database> GetDatabase(const std::string &db) override;
    std::shared_ptr<TableHandler> GetTable(
        const std::string &db, const std::string &table_name) override;
    bool IndexSupport() override;

 private:
    bool enable_index_;
    std::map<std::string,
             std::map<std::string, std::shared_ptr<SimpleCatalogTableHandler>>>
        table_handlers_;

    std::map<std::string, std::shared_ptr<type::Database>> databases_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_SIMPLE_CATALOG_H_
