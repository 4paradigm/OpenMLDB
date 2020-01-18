/*
 * batch_catalog.h
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

#ifndef SRC_CATALOG_BATCH_CATALOG_H_
#define SRC_CATALOG_BATCH_CATALOG_H_

#include <memory>
#include "catalog/catalog.h"
#include "arrow/filesystem/filesystem.h"

namespace fesql {
namespace catalog {

struct Partition {
    std::string path;
}

class BatchTableHandler : public TableHandler {
 public:

    BatchTableHandler(const Schema& schema, 
            const std::string& name,
            const std::string& db,
            const std::vector<Partition>& partitons):schema_(schema), name_(name),
    db_(db), partitions_(partitons){}

    ~BatchTableHandler() {}

    inline Schema& GetSchema() {
        return schema_;
    }

    inline const std::string& GetName() {
        return name_;
    }

    inline const std::string& GetDatabase() {
        return db_;
    }

    inline const std::vector<Partition>& GetPartitions() {
        return partitions_;
    }

 private:
    Schema schema_;
    std::string name_;
    std::string db_;
    std::vector<Partition> partitions_;
};

// the table and file path pairs
typedef std::vector<std::pair<std::string, std::string> > InputTables;
class BatchCatalog : public BatchCatalog {
 public:

    BatchCatalog(std::unique_ptr<::arrow::io::FileSystem> fs,
                 const InputTables& tables);

    ~BatchCatalog();

    // init catalog from filesystem
    bool Init();

    std::share_ptr<type::Database> GetDatabase(const std::string& db);

    std::share_ptr<TableHandler> GetTable(const std::string& db, 
                                          const std::string& table_name);
 public:
    std::unique_ptr<::arrow::io::FileSystem> fs_;
    InputTables input_tables_;
};

}  // namespace catalog
}  // namespace fesql
#endif  // SRC_CATALOG_BATCH_CATALOG_H_
