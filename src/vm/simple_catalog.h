/*
 * simple_catalog.h
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

#ifndef SRC_VM_SIMPLE_CATALOG_H_
#define SRC_VM_SIMPLE_CATALOG_H_

#include <string>
#include <map>
#include <memory>

#include "glog/logging.h"
#include "proto/type.pb.h"
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
        std::shared_ptr<TableHandler> table_hander,
        const std::string &index_name) const override;

    std::unique_ptr<IteratorV<uint64_t, fesql::codec::Row>>
        GetIterator() const override;

    IteratorV<uint64_t, fesql::codec::Row>*
        GetIterator(int8_t *addr) const override;

 private:
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
    SimpleCatalog();
    SimpleCatalog(const SimpleCatalog &) = delete;
    ~SimpleCatalog();

    void AddDatabase(const fesql::type::Database &db);
    std::shared_ptr<type::Database> GetDatabase(const std::string &db) override;
    std::shared_ptr<TableHandler> GetTable(
        const std::string &db, const std::string &table_name) override;
    bool IndexSupport() override;

 private:
    std::map<std::string,
             std::map<std::string, std::shared_ptr<SimpleCatalogTableHandler>>>
        table_handlers_;

    std::map<std::string, std::shared_ptr<type::Database>> databases_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_SIMPLE_CATALOG_H_
