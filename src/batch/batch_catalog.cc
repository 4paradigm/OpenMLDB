/*
 * batch_catalog.cc
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

#include "batch/batch_catalog.h"

#include <vector>
#include <utility>
#include <string>
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/status.h"
#include "base/parquet_util.h"
#include "gflags/gflags.h"
#include "glog/logging.h"
#include "parquet/api/reader.h"
#include "parquet/file_reader.h"

DECLARE_string(default_db_name);

namespace fesql {
namespace batch {

BatchCatalog::BatchCatalog(std::shared_ptr<::arrow::fs::FileSystem> fs,
                           const InputTables& tables)
    : fs_(fs), input_tables_(tables), db_() {}

BatchCatalog::~BatchCatalog() {}

bool BatchCatalog::Init() {
    LOG(INFO) << "add default db with name " << FLAGS_default_db_name;
    auto it = input_tables_.begin();
    for (; it != input_tables_.end(); ++it) {
        std::pair<std::string, std::string>& table = *it;
        // partiton information
        std::vector<Partition> partitions;
        Partition p;
        p.path = table.second;
        partitions.push_back(p);

        // schema information
        vm::Schema schema;
        bool ok = GetSchemaFromParquet(table.second, schema);
        if (!ok) {
            return false;
        }
        // TODO(wangtaize) support dir
        std::shared_ptr<BatchTableHandler> table_handler(new BatchTableHandler(
            schema, table.first, FLAGS_default_db_name, partitions));
        db_[FLAGS_default_db_name].insert(
            std::make_pair(table.first, table_handler));
        LOG(INFO) << "add table " << table.first << " " << table.second
                  << " ok";
    }

    LOG(INFO) << "init batch catalog successfully";
    return true;
}

std::shared_ptr<type::Database> BatchCatalog::GetDatabase(
    const std::string& db) {
    return std::shared_ptr<type::Database>();
}

std::shared_ptr<vm::TableHandler> BatchCatalog::GetTable(
    const std::string& db, const std::string& table_name) {
    // ignore the input db name
    auto it = db_[FLAGS_default_db_name].find(table_name);
    if (it == db_[FLAGS_default_db_name].end()) {
        return std::shared_ptr<vm::TableHandler>();
    }
    return it->second;
}

bool BatchCatalog::GetSchemaFromParquet(const std::string& path,
                                        vm::Schema& schema) {
    arrow::fs::FileStats stats;
    arrow::Status ok = fs_->GetTargetStats(path, &stats);

    if (!ok.ok()) {
        LOG(WARNING) << "fail to get path " << path << " stats "
                     << " with error " << ok.message();
        return false;
    }

    if (!stats.IsFile()) {
        LOG(WARNING) << "file is required but it's dir with path " << path;
        return false;
    }

    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ok = fs_->OpenInputFile(path, &input);
    if (!ok.ok() || !input) {
        LOG(WARNING) << "fail to open file with path " << path << " and error "
                     << ok.message();
        return false;
    }
    std::unique_ptr<::parquet::ParquetFileReader> reader =
        ::parquet::ParquetFileReader::Open(input);
    std::shared_ptr<parquet::FileMetaData> metadata = reader->metadata();
    bool done = MapParquetSchema(metadata->schema(), schema);
    if (!done) {
        LOG(WARNING) << "fail to get path " << path << " schema";
        return false;
    }
    return true;
}

bool BatchCatalog::MapParquetSchema(
    const parquet::SchemaDescriptor* input_schema, vm::Schema& output_schema) {
    if (input_schema == nullptr) {
        LOG(WARNING) << "input schema is nullptr";
        return false;
    }

    for (int32_t i = 0; i < input_schema->num_columns(); i++) {
        const parquet::ColumnDescriptor* column_desc = input_schema->Column(i);
        type::Type type;
        bool ok = base::MapParquetType(column_desc, &type);
        if (!ok) {
            return false;
        }
        type::ColumnDef* column_def = output_schema.Add();
        column_def->set_name(column_desc->name());
        column_def->set_type(type);
    }
    return true;
}

}  // namespace batch
}  // namespace fesql
