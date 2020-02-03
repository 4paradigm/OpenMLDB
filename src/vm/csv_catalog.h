/*
 * csv_catalog.h
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

#ifndef SRC_VM_CSV_CATALOG_H_
#define SRC_VM_CSV_CATALOG_H_

#include "vm/catalog.h"

namespace fesql {
namespace vm {

class SchemaParser {

 public:
    SchemaParser() {}
    ~SchemaParser() {}

    bool Parse(const std::string& path, 
            Schema* schema);
};

class CSVTableHandler : public TableHandler {
 public:

    CSVTableHandler(const std::string& table_dir,
            const std::string& db);

    ~CSVTableHandler();

    bool Init();
    const Schema& GetSchema();
    const std::string& GetName();
    const Types& GetTypes();
    const IndexList& GetIndex();

    std::unique_ptr<Iterator> GetIterator();

    std::unique_ptr<WindowIterator> GetWindowIterator(const std::string& idx_name);
 private:
    std::string table_dir_;
    std::shared_ptr<arrow::Table> table_;
};

// csv catalog is local dbms
// the dir layout should be 
// root
//   |
//   db1
//    \t1
//      |
//       -schema
//       -data.csv
//       -index
//     t2
class CSVCatalog : public Catalog {

 public:
    CSVCatalog(const std::string& root_dir) {}
    ~CSVCatalog() {}
    bool Init();
    std::shared_ptr<type::Database> GetDatabase(const std::string& db);
    std::shared_ptr<TableHandler> GetTable(const std::string& db, const std::string& table_name);
};

}  // namespace vm
}  // namespace fesql
#endif   // SRC_VM_CSV_CATALOG_H_
