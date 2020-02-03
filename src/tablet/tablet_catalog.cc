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

#include "tablet/tablet_catalog.h"

#include <memory>

namespace fesql {
namespace tablet {

TabletTableHandler::TabletTableHandler(const vm::Schema& schema, const std::string& name,
        const std::string& db, std::shared_ptr<storage::Table> table):schema_(schema),
    name_(name), db_(db), table_(table), types_(), index_list_() {
}

TabletCatalog::TabletCatalog():tables_(), db_(), slock_() {}

TabletCatalog::~TabletCatalog() {}

bool TabletCatalog::Init() {
    return true;
}

std::shared_ptr<type::Database> TabletCatalog::GetDatabase(const std::string& db) {
    std::lock_guard<base::SpinMutex> lock(slock_);
    auto it = db_.find(db);
    if (it == db_.end()) {
        return std::shared_ptr<type::Database>();
    }
    return it->second;
}

std::shared_ptr<TableHandler> TabletCatalog::GetTable(const std::string& db, 
        const std::string& table_name) {
    std::lock_guard<base::SpinMutex> lock(slock_);
    auto table_in_db = tables_[db];
    auto it = table_in_db.find(table_name);
    if (it == table_in_db.end()) {
        return std::shared_ptr<TableHandler>();
    }
    return it->second;
}

bool TabletCatalog::AddTable(std::shared_ptr<TabletTableHandler> table) {
    bool ok = false;
    do {
        std::lock_guard<base::SpinMutex> lock(slock_);
        auto table_in_db = tables_[table->GetDatabase()];
        auto it = table_in_db.find(table->GetName());
        if (it != table_in_db.end()) {
            break;
        }
        table_in_db.insert(std::make_pair(table->GetName(), table));
        ok = true;
    } while(false);
    if (!ok) {
        LOG(WARNING) << "table " << table->GetName() << " exist";
    }else {
        LOG(INFO) << "add table " << table->GetName() << " to database " << table->GetDatabase() << " ok";
    }
    return ok;
}

bool TabletCatalog::AddDB(const type::Database& db) {
    bool ok = false;
    do{
        std::lock_guard<base::SpinMutex> lock(slock_);
        TabletDB::iterator it = db_.find(db.name());
        if (it != db_.end()) {
            break;
        }
        db_.insert(std::make_pair(db.name(), db));
        tables_.insert(db.name(), std::map<std::string, std::shared_ptr<TabletTableHandler> >());
        ok = true;
    }while(false)
    if (ok) {
        LOG(INFO) << "add database " << db.name() << " ok";
    }else {
        LOG(WARNING) << "database " << db.name() << " exist";
    }
    return ok;
}


}  // namespace tablet
}  // namespace fesql



