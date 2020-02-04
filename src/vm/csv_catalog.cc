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
#include "glog/logging.h"
#include "boost/lexical_cast.hpp"
#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/predicate.hpp"
#include "arrow/csv/api.h"
#include "arrow/status.h"
#include "arrow/type.h"

namespace fesql {
namespace vm {

bool SchemaParser::Convert(const std::string& type, 
                          type::Type* db_type) {
    if (db_type == nullptr) return false;
    if (type == "bool") {
        *db_type = type::kBool;
    }else if (type == "int16") {
        *db_type = type::kInt16;
    }else if (type == "int32") {
        *db_type = type::kInt32;
    }else if (type == "int64") {
        *db_type = type::kInt64;
    }else if (type == "float") {
        *db_type = type::kFloat;
    }else if (type == "double") {
        *db_type = type::kDouble;
    }else if (type == "varchar") {
        *db_type = type::kVarchar;
    }else if (type == "timestamp") {
        *db_type = type::kTimestamp;
    }else if (type == "date") {
        *db_type = type::kDate;
    }else {
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
            LOG(INFO) << "add column " << column_def->name() << " with type " << type::Type_Name(column_def->type());
        }else {
            LOG(WARNING) << "invalid line " << line;
        }
    }
    ifs.close();
    return true;
}

CSVTableHandler::CSVTableHandler(const std::string& table_dir,
        const std::string& table_name,
        const std::string& db,
        std::shared_ptr<arrow::fs::FileSystem> fs):table_dir_(table_dir), table_name_(table_name),
    db_(db), schema_(),
table_(), fs_(fs){}

CSVTableHandler::~CSVTableHandler() {}

bool CSVTableHandler::Init() {
    bool ok = InitSchema();
    if (!ok) {
        LOG(WARNING) << "fail to parse schema for table " << table_dir_;
        return false;
    }
    ok = InitTable();
    return ok;
}

bool CSVTableHandler::InitTable() {
    std::string path = table_dir_ + "/data.csv";
    arrow::fs::FileStats stats;
    arrow::Status ok = fs_->GetTargetStats(path, &stats);

    if (!ok.ok()) {
        LOG(WARNING) << "fail to get path " << path << " stats " << " with error " << ok.message();
        return false;
    }

    if (!stats.IsFile()) {
        LOG(WARNING) << "file is required but it's dir with path " << path;
        return false;
    }
    auto convert_options = arrow::csv::ConvertOptions::Defaults();
    bool init_schema_ok = InitOptions(&convert_options);
    if (!init_schema_ok) {
        LOG(WARNING) << "fail to init convert options for table " << table_name_;
        return false;
    }
    std::shared_ptr<arrow::io::RandomAccessFile> input;
    ok = fs_->OpenInputFile(path, &input);
    if (!ok.ok() || !input) {
        LOG(WARNING) << "fail to open file with path " << path << " and error " << ok.message();
        return false;
    }
    auto read_options = arrow::csv::ReadOptions::Defaults();
    auto parse_options = arrow::csv::ParseOptions::Defaults();
    std::shared_ptr<arrow::csv::TableReader> reader;
    arrow::Status status = arrow::csv::TableReader::Make(arrow::default_memory_pool(), 
                                      input, read_options,
                                      parse_options, convert_options,
                                      &reader);
    status = reader->Read(&table_);
    if (status.ok()) {
        LOG(INFO) << "read csv to table " << table_name_ << " with rows " << table_->num_rows();
        return true;
    }
    LOG(WARNING) << "fail to read csv for table " << table_name_;
    return false;
}

bool CSVTableHandler::InitOptions(arrow::csv::ConvertOptions* options) {
    for (int32_t i = 0; i < schema_.size(); i++) {
        const type::ColumnDef& column = schema_.Get(i);
        switch(column.type()) {
            case type::kInt16:
                {
                    std::shared_ptr<arrow::DataType> dt(new arrow::Int16Type());
                    options->column_types.insert(std::make_pair(column.name(), dt));
                    break;
                }
            case type::kInt32:
                {
                    std::shared_ptr<arrow::DataType> dt(new arrow::Int32Type());
                    options->column_types.insert(std::make_pair(column.name(), dt));
                    break;
                }
            case type::kInt64:
                {
                    std::shared_ptr<arrow::DataType> dt(new arrow::Int64Type());
                    options->column_types.insert(std::make_pair(column.name(), dt));
                    break;
                }
            case type::kFloat:
                {
                    std::shared_ptr<arrow::DataType> dt(new arrow::FloatType());
                    options->column_types.insert(std::make_pair(column.name(), dt));
                    break;
                }
            case type::kDouble:
                {
                    std::shared_ptr<arrow::DataType> dt(new arrow::DoubleType());
                    options->column_types.insert(std::make_pair(column.name(), dt));
                    break;
                }
            case type::kDate:
                {
                    std::shared_ptr<arrow::DataType> dt(new arrow::Date32Type());
                    options->column_types.insert(std::make_pair(column.name(), dt));
                    break;
                }
            case type::kTimestamp:
                {
                    std::shared_ptr<arrow::DataType> dt(new arrow::TimestampType(arrow::TimeUnit::SECOND));
                    options->column_types.insert(std::make_pair(column.name(), dt));
                    break;
                }
            case type::kVarchar:
                {
                    std::shared_ptr<arrow::DataType> dt(new arrow::StringType());
                    options->column_types.insert(std::make_pair(column.name(), dt));
                    break;
                }
            default:
                {
                    LOG(WARNING) << "not supported type with column name " << column.name();
                    return false;
                }
        }
    }
    return true;
}

bool CSVTableHandler::InitSchema() {
    SchemaParser parser;
    std::string schema_path = table_dir_ + "/schema";
    bool ok = parser.Parse(schema_path, &schema_);
    return ok;
}

}  // namespace vm
}  // namespace fesql


