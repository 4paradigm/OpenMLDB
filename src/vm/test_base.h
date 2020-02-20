/*
 * test_base.h
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

#ifndef SRC_VM_TEST_BASE_H_
#define SRC_VM_TEST_BASE_H_

#include <memory>
#include <sstream>
#include "glog/logging.h"
#include "tablet/tablet_catalog.h"
#include "vm/catalog.h"

namespace fesql {
namespace vm {

std::shared_ptr<tablet::TabletCatalog> BuildCommonCatalog(
    const fesql::type::TableDef& table_def,
    std::shared_ptr<fesql::storage::Table> table) {
    std::shared_ptr<tablet::TabletCatalog> catalog(new tablet::TabletCatalog());
    bool ok = catalog->Init();
    if (!ok) {
        return std::shared_ptr<tablet::TabletCatalog>();
    }

    std::shared_ptr<tablet::TabletTableHandler> handler(
        new tablet::TabletTableHandler(table_def.columns(), table_def.name(),
                                       table_def.catalog(), table_def.indexes(),
                                       table));
    ok = handler->Init();
    if (!ok) {
        return std::shared_ptr<tablet::TabletCatalog>();
    }
    catalog->AddTable(handler);
    return catalog;
}

std::shared_ptr<tablet::TabletCatalog> BuildCommonCatalog(
    const fesql::type::TableDef& table_def) {
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    return BuildCommonCatalog(table_def, table);
}

void PrintSchema(const Schema& schema) {
    std::stringstream ss;
    for (int32_t i = 0; i < schema.size(); i++) {
        if (i > 0) {
            ss << "\n";
        }
        const type::ColumnDef& column = schema.Get(i);
        ss << column.name() << " " << type::Type_Name(column.type());
    }
    LOG(INFO) << "\n" << ss.str();
}

}  // namespace vm
}  // namespace fesql

#endif  // SRC_VM_TEST_BASE_H_
