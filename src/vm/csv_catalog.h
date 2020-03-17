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

#include <map>
#include <string>
#include <memory>
#include "vm/catalog.h"

#include "arrow/array.h"
#include "arrow/csv/api.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "glog/logging.h"

namespace fesql {
namespace vm {

class SchemaParser {
 public:
    SchemaParser() {}
    ~SchemaParser() {}

    bool Parse(const std::string& path, Schema* schema);

    bool Convert(const std::string& type, type::Type* db_type);
};

class IndexParser {
 public:
    IndexParser() {}
    ~IndexParser() {}
    bool Parse(const std::string& path, IndexList* index_list);
};

struct RowLocation {
    uint64_t chunk_offset;
    uint64_t array_offset;
};

typedef std::map<std::string,
                 std::map<std::string, std::map<uint64_t, RowLocation>>>
    IndexDatas;
typedef std::map<std::string, std::map<uint64_t, RowLocation>>::const_iterator
    FirstKeyIterator;
typedef std::map<uint64_t, RowLocation>::const_iterator SecondKeyIterator;

class CSVTableHandler : public TableHandler {
 public:
    CSVTableHandler(const std::string& table_dir, const std::string& table_name,
                    const std::string& db,
                    std::shared_ptr<::arrow::fs::FileSystem> fs);

    ~CSVTableHandler();

    bool Init();

    inline const Schema& GetSchema() { return schema_; }
    inline const std::string& GetName() { return table_name_; }
    inline const Types& GetTypes() { return types_; }

    inline const IndexHint& GetIndex() { return index_hint_; }

    std::unique_ptr<Iterator> GetIterator();

    inline const std::string& GetDatabase() { return db_; }

    std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name);

 private:
    bool InitConfig();

    bool InitTable();

    bool InitIndex();

    bool InitOptions(arrow::csv::ConvertOptions* options);

    int32_t GetColumnIndex(const std::string& name);

 private:
    std::string table_dir_;
    std::string table_name_;
    std::string db_;
    Schema schema_;
    std::shared_ptr<arrow::Table> table_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    Types types_;
    IndexList index_list_;
    IndexDatas* index_datas_;
    IndexHint index_hint_;
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

typedef std::map<std::string,
                 std::map<std::string, std::shared_ptr<CSVTableHandler>>>
    CSVTables;
typedef std::map<std::string, std::shared_ptr<type::Database>> Databases;

class CSVCatalog : public Catalog {
 public:
    explicit CSVCatalog(const std::string& root_dir);

    ~CSVCatalog();

    bool Init();

    std::shared_ptr<type::Database> GetDatabase(const std::string& db);
    std::shared_ptr<TableHandler> GetTable(const std::string& db,
                                           const std::string& table_name);

 private:
    bool InitDatabase(const std::string& db);

 private:
    std::string root_dir_;
    CSVTables tables_;
    Databases dbs_;
    std::shared_ptr<::arrow::fs::FileSystem> fs_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_CSV_CATALOG_H_
