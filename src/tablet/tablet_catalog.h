/*
 * tablet_catalog.h
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

#ifndef SRC_TABLET_TABLET_CATALOG_H_
#define SRC_TABLET_TABLET_CATALOG_H_

#include "base/spin_lock.h"
#include "storage/table.h"
#include "vm/catalog.h"

namespace fesql {
namespace tablet {

class TabletTableHandler : public vm::TableHandler {
 public:
    TabletTableHandler(const vm::Schema& schema, const std::string& name,
                       const std::string& db, const vm::IndexList& index_list,
                       std::shared_ptr<storage::Table> table);

    ~TabletTableHandler();

    bool Init();

    inline const vm::Schema& GetSchema() { return schema_; }

    inline const std::string& GetName() { return name_; }

    inline const std::string& GetDatabase() { return db_; }

    inline const vm::Types& GetTypes() { return types_; }

    inline const vm::IndexHint& GetIndex() { return index_hint_; }

    inline std::shared_ptr<storage::Table> GetTable() { return table_; }

    std::unique_ptr<vm::Iterator> GetIterator();

    std::unique_ptr<vm::WindowIterator> GetWindowIterator(
        const std::string& idx_name);

 private:
    inline int32_t GetColumnIndex(const std::string& column) {
        auto it = types_.find(column);
        if (it != types_.end()) {
            return it->second.pos;
        }
        return -1;
    }

 private:
    vm::Schema schema_;
    std::string name_;
    std::string db_;
    std::shared_ptr<storage::Table> table_;
    vm::Types types_;
    vm::IndexList index_list_;
    vm::IndexHint index_hint_;
};

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<TabletTableHandler>>>
    TabletTables;
typedef std::map<std::string, std::shared_ptr<type::Database>> TabletDB;

class TabletCatalog : public vm::Catalog {
 public:
    TabletCatalog();

    ~TabletCatalog();

    // TODO(wangtaize) add delete method

    bool Init();

    bool AddDB(const type::Database& db);

    bool AddTable(std::shared_ptr<TabletTableHandler> table);

    std::shared_ptr<type::Database> GetDatabase(const std::string& db);

    std::shared_ptr<vm::TableHandler> GetTable(const std::string& db,
                                               const std::string& table_name);

 private:
    TabletTables tables_;
    TabletDB db_;
};

}  // namespace tablet
}  // namespace fesql
#endif  // SRC_TABLET_TABLET_CATALOG_H_
