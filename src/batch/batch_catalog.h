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

#ifndef SRC_VM_BATCH_CATALOG_H_
#define SRC_VM_BATCH_CATALOG_H_

#include <memory>
#include "arrow/filesystem/filesystem.h"
#include "vm/catalog.h"
#include "parquet/schema.h"

namespace fesql {
namespace vm {

struct Partition {
    std::string path;
};

class BatchTableHandler : public TableHandler {
 public:
    BatchTableHandler(const Schema& schema, const std::string& name,
                      const std::string& db,
                      const std::vector<Partition>& partitons)
        : schema_(schema), name_(name), db_(db), partitions_(partitons) {}

    ~BatchTableHandler() {}

    inline Schema& GetSchema() { return schema_; }

    inline const std::string& GetName() { return name_; }

    inline const std::string& GetDatabase() { return db_; }

    inline const std::vector<Partition>& GetPartitions() { return partitions_; }

    inline const Types& GetTypes() {
        return types_;
    }

    inline const IndexList& GetIndex() {
        return index_list_;
    }

    std::unique_ptr<Iterator> GetIterator();

    std::unique_ptr<WindowIterator> GetWindowIterator();
 private:
    Schema schema_;
    std::string name_;
    std::string db_;
    std::vector<Partition> partitions_;
    IndexList index_list_;
    Types types_;
};

// the table and file path pairs
typedef std::vector<std::pair<std::string, std::string>> InputTables;
typedef std::map<std::string, std::map<std::string, std::shared_ptr<BatchTableHandler> > > BatchDB;


// NOTE not thread safe
class BatchCatalog : public Catalog {
 public:

    BatchCatalog(std::shared_ptr<::arrow::fs::FileSystem> fs,
                 const InputTables& tables);

    ~BatchCatalog();

    // init catalog from filesystem
    bool Init();

    std::shared_ptr<type::Database> GetDatabase(const std::string& db);

    std::shared_ptr<TableHandler> GetTable(const std::string& db,
                                          const std::string& table_name);

 private:

    // get parquet schema and map it to fesql schema
    bool GetSchemaFromParquet(const std::string& path, 
            Schema& schema);

    // map parquet schema to fesql schema
    bool MapParquetSchema(const parquet::SchemaDescriptor* input_schema,
            Schema& output_schema);

 private:
    std::shared_ptr<::arrow::fs::FileSystem> fs_;
    InputTables input_tables_;
    BatchDB db_;
};

}  // namespace vm
}  // namespace fesql
#endif  // SRC_vm_BATCH_CATALOG_H_
