/*
 * csv_catalog.cc
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

#include "vm/csv_catalog.h"

#include <fstream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "arrow/csv/api.h"
#include "arrow/status.h"
#include "arrow/type.h"
#include "base/fs_util.h"
#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/predicate.hpp"
#include "boost/lexical_cast.hpp"
#include "glog/logging.h"
#include "vm/csv_table_iterator.h"
#include "vm/csv_window_iterator.h"

namespace fesql {
namespace vm {

bool SchemaParser::Convert(const std::string& type, type::Type* db_type) {
    if (db_type == nullptr) return false;
    if (type == "bool") {
        *db_type = type::kBool;
    } else if (type == "int16") {
        *db_type = type::kInt16;
    } else if (type == "int32") {
        *db_type = type::kInt32;
    } else if (type == "int64") {
        *db_type = type::kInt64;
    } else if (type == "float") {
        *db_type = type::kFloat;
    } else if (type == "double") {
        *db_type = type::kDouble;
    } else if (type == "varchar") {
        *db_type = type::kVarchar;
    } else if (type == "timestamp") {
        *db_type = type::kTimestamp;
    } else if (type == "date") {
        *db_type = type::kDate;
    } else {
        LOG(WARNING) << type << " is not supported";
        return false;
    }
    return true;
}

bool SchemaParser::Parse(const std::string& path, Schema* schema) {
    if (schema == nullptr) {
        LOG(WARNING) << "schema is nullptr";
        return false;
    }

    std::ifstream ifs;
    ifs.open(path.c_str(), std::ifstream::binary);
    while (ifs.good() && !ifs.eof()) {
        std::string line;
        std::getline(ifs, line);
        if (line.empty()) continue;
        std::vector<std::string> parts;
        boost::split(parts, line, boost::is_any_of(" "));
        if (parts.size() == 2) {
            type::Type type;
            bool ok = Convert(parts[1], &type);
            if (!ok) {
                ifs.close();
                LOG(WARNING) << "fail to get fesql type from " << parts[1];
                return false;
            }
            type::ColumnDef* column_def = schema->Add();
            column_def->set_name(parts[0]);
            column_def->set_type(type);
            LOG(INFO) << "add column " << column_def->name() << " with type "
                      << type::Type_Name(column_def->type());
        } else {
            LOG(WARNING) << "invalid line " << line;
        }
    }
    ifs.close();
    return true;
}

bool IndexParser::Parse(const std::string& path, IndexList* index_list) {
    if (index_list == nullptr) {
        LOG(WARNING) << "index_list is nullptr";
        return false;
    }
    std::ifstream ifs;
    ifs.open(path.c_str(), std::ifstream::binary);
    while (ifs.good() && !ifs.eof()) {
        std::string line;
        std::getline(ifs, line);
        if (line.empty()) continue;
        std::vector<std::string> parts;
        boost::split(parts, line, boost::is_any_of(" "));
        if (parts.size() == 3) {
            type::IndexDef* index_def = index_list->Add();
            index_def->set_name(parts[0]);
            index_def->add_first_keys(parts[1]);
            index_def->set_second_key(parts[2]);
            LOG(INFO) << "add index with name " << parts[0] << " fk "
                      << parts[1] << " with sk " << parts[2];
        } else {
            LOG(WARNING) << "invalid line " << line;
        }
    }
    return true;
}

CSVTableHandler::CSVTableHandler(const std::string& table_dir,
                                 const std::string& table_name,
                                 const std::string& db,
                                 std::shared_ptr<arrow::fs::FileSystem> fs)
    : table_dir_(table_dir),
      table_name_(table_name),
      db_(db),
      schema_(),
      table_(),
      fs_(fs),
      types_(),
      index_list_(),
      index_datas_(new IndexDatas()),
      index_hint_() {}

CSVTableHandler::~CSVTableHandler() { delete index_datas_; }

bool CSVTableHandler::Init() {
    bool ok = InitConfig();
    if (!ok) {
        LOG(WARNING) << "fail to parse schema for table " << table_dir_;
        return false;
    }
    for (int32_t i = 0; i < schema_.size(); i++) {
        const type::ColumnDef& column = schema_.Get(i);
        ColInfo col_info;
        col_info.type = column.type();
        col_info.pos = i;
        col_info.name = column.name();
        types_.insert(std::make_pair(column.name(), col_info));
    }
    ok = InitTable();
    if (!ok) {
        LOG(WARNING) << "fail to load csv for table " << table_dir_;
        return false;
    }
    ok = InitIndex();
    return ok;
}

bool CSVTableHandler::InitTable() {
    std::string path = table_dir_ + "/data.csv";
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
    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    bool init_schema_ok = InitOptions(&convert_options);
    if (!init_schema_ok) {
        LOG(WARNING) << "fail to init convert options for table "
                     << table_name_;
        return false;
    }
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ok = fs_->OpenInputFile(path, &input);
    if (!ok.ok() || !input) {
        LOG(WARNING) << "fail to open file with path " << path << " and error "
                     << ok.message();
        return false;
    }
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    std::shared_ptr<arrow::csv::TableReader> reader;
    arrow::Status status = arrow::csv::TableReader::Make(
        arrow::default_memory_pool(), input, read_options, parse_options,
        convert_options, &reader);
    status = reader->Read(&table_);
    if (status.ok()) {
        LOG(INFO) << "read csv to table " << table_name_ << " with rows "
                  << table_->num_rows();
        return true;
    }
    LOG(WARNING) << "fail to read csv for table " << table_name_;
    return false;
}

bool CSVTableHandler::InitOptions(arrow::csv::ConvertOptions* options) {
    for (int32_t i = 0; i < schema_.size(); i++) {
        const type::ColumnDef& column = schema_.Get(i);
        switch (column.type()) {
            case type::kInt16: {
                std::shared_ptr<arrow::DataType> dt(new arrow::Int16Type());
                options->column_types.insert(std::make_pair(column.name(), dt));
                break;
            }
            case type::kInt32: {
                std::shared_ptr<arrow::DataType> dt(new arrow::Int32Type());
                options->column_types.insert(std::make_pair(column.name(), dt));
                break;
            }
            case type::kInt64: {
                std::shared_ptr<arrow::DataType> dt(new arrow::Int64Type());
                options->column_types.insert(std::make_pair(column.name(), dt));
                break;
            }
            case type::kFloat: {
                std::shared_ptr<arrow::DataType> dt(new arrow::FloatType());
                options->column_types.insert(std::make_pair(column.name(), dt));
                break;
            }
            case type::kDouble: {
                std::shared_ptr<arrow::DataType> dt(new arrow::DoubleType());
                options->column_types.insert(std::make_pair(column.name(), dt));
                break;
            }
            case type::kDate: {
                std::shared_ptr<arrow::DataType> dt(new arrow::Date32Type());
                options->column_types.insert(std::make_pair(column.name(), dt));
                break;
            }
            case type::kTimestamp: {
                std::shared_ptr<arrow::DataType> dt(
                    new arrow::TimestampType(arrow::TimeUnit::SECOND));
                options->column_types.insert(std::make_pair(column.name(), dt));
                break;
            }
            case type::kVarchar: {
                std::shared_ptr<arrow::DataType> dt(new arrow::StringType());
                options->column_types.insert(std::make_pair(column.name(), dt));
                break;
            }
            default: {
                LOG(WARNING)
                    << "not supported type with column name " << column.name();
                return false;
            }
        }
    }
    return true;
}

bool CSVTableHandler::InitConfig() {
    SchemaParser parser;
    std::string schema_path = table_dir_ + "/schema";
    bool ok = parser.Parse(schema_path, &schema_);
    if (!ok) {
        return false;
    }
    IndexParser idx_parser;
    ok = idx_parser.Parse(table_dir_ + "/index", &index_list_);
    return ok;
}

bool CSVTableHandler::InitIndex() {
    for (int32_t i = 0; i < index_list_.size(); i++) {
        const type::IndexDef& index_def = index_list_.Get(i);
        IndexSt index_st;
        index_st.index = i;
        index_st.ts_pos = GetColumnIndex(index_def.second_key());
        index_st.name = index_def.name();
        index_st.keys.push_back(types_[index_def.first_keys(0)]);
        index_hint_.insert(std::make_pair(index_st.name, index_st));
        int64_t chunk_offset = 0;
        int64_t array_offset = 0;
        while (true) {
            if (table_->num_rows() <= 0 || table_->num_columns() <= 0) break;
            if (table_->column(0)->num_chunks() <= chunk_offset) break;
            if (table_->column(0)->chunk(chunk_offset)->length() <=
                array_offset)
                break;
            int32_t first_key_column_index =
                GetColumnIndex(index_def.first_keys(0));
            if (first_key_column_index < 0) {
                LOG(WARNING)
                    << "fail to find column " << index_def.first_keys(0);
                continue;
            }
            const type::ColumnDef& first_key_column =
                schema_.Get(first_key_column_index);
            if (first_key_column.type() != type::kVarchar) {
                LOG(WARNING) << "varchar type is required";
                continue;
            }

            int32_t second_key_column_index =
                GetColumnIndex(index_def.second_key());
            if (second_key_column_index < 0) {
                LOG(WARNING) << "fail to find second key column "
                             << index_def.second_key();
                continue;
            }

            const type::ColumnDef& second_key_column =
                schema_.Get(second_key_column_index);
            if (second_key_column.type() != type::kInt64) {
                LOG(WARNING) << "kint64 type is required";
                continue;
            }
            auto first_key_column_array =
                std::static_pointer_cast<arrow::StringArray>(
                    table_->column(first_key_column_index)
                        ->chunk(chunk_offset));
            auto string_view = first_key_column_array->GetView(array_offset);
            std::string first_key(string_view.data(), string_view.size());
            auto second_key_column_array =
                std::static_pointer_cast<arrow::Int64Array>(
                    table_->column(second_key_column_index)
                        ->chunk(chunk_offset));
            auto second_key_value =
                second_key_column_array->Value(array_offset);
            RowLocation location;
            location.chunk_offset = chunk_offset;
            location.array_offset = array_offset;
            if (index_datas_->find(index_def.name()) == index_datas_->end()) {
                index_datas_->insert(std::make_pair(
                    index_def.name(),
                    std::map<std::string, std::map<uint64_t, RowLocation>>()));
            }
            index_datas_->at(index_def.name())[first_key].insert(
                std::make_pair(second_key_value, location));
            if (table_->column(0)->chunk(chunk_offset)->length() <=
                array_offset + 1) {
                chunk_offset += 1;
                array_offset = 0;
            } else {
                array_offset += 1;
            }
            LOG(INFO) << "first key " << first_key << " with size "
                      << index_datas_->at(index_def.name())[first_key].size();
        }
    }
    return true;
}

int32_t CSVTableHandler::GetColumnIndex(const std::string& name) {
    auto it = types_.find(name);
    if (it != types_.end()) {
        return it->second.pos;
    }
    return -1;
}
std::unique_ptr<Iterator> CSVTableHandler::GetIterator() {
    std::unique_ptr<CSVTableIterator> it(new CSVTableIterator(table_, schema_));
    return std::move(it);
}

std::unique_ptr<WindowIterator> CSVTableHandler::GetWindowIterator(
    const std::string& idx_name) {
    LOG(INFO) << "new window iterator with index name " << idx_name;
    std::unique_ptr<CSVWindowIterator> csv_window_iterator(
        new CSVWindowIterator(table_, idx_name, index_datas_, schema_));
    return std::move(csv_window_iterator);
}
CSVCatalog::CSVCatalog(const std::string& root_dir)
    : root_dir_(root_dir),
      tables_(),
      dbs_(),
      fs_(new arrow::fs::LocalFileSystem()) {}

CSVCatalog::~CSVCatalog() {}

bool CSVCatalog::Init() {
    std::vector<std::string> db_names;
    bool ok = base::ListDir(root_dir_, db_names);
    if (!ok) {
        LOG(WARNING) << "fail to read root dir " << root_dir_;
        return false;
    }
    auto it = db_names.begin();
    for (; it != db_names.end(); ++it) {
        ok = InitDatabase(*it);
        if (!ok) {
            return false;
        }
    }
    return true;
}

bool CSVCatalog::InitDatabase(const std::string& db) {
    std::string db_path = root_dir_ + "/" + db;
    std::vector<std::string> table_names;
    bool ok = base::ListDir(db_path, table_names);
    if (!ok) {
        LOG(WARNING) << "fail to read db dir " << db_path;
        return false;
    }
    auto it = table_names.begin();
    for (; it != table_names.end(); ++it) {
        std::string table_name = *it;
        std::string table_dir = db_path + "/" + table_name;
        std::shared_ptr<CSVTableHandler> table_handler(
            new CSVTableHandler(table_dir, table_name, db, fs_));
        ok = table_handler->Init();
        if (!ok) {
            LOG(WARNING) << "load table " << table_name << " failed";
            return false;
        }
        LOG(INFO) << "load table " << table_name << " for db " << db << " done";
        tables_[db].insert(std::make_pair(table_name, table_handler));
    }
    return true;
}

std::shared_ptr<TableHandler> CSVCatalog::GetTable(const std::string& db,
                                                   const std::string& table) {
    return tables_[db][table];
}

std::shared_ptr<type::Database> CSVCatalog::GetDatabase(const std::string& db) {
}

}  // namespace vm
}  // namespace fesql
