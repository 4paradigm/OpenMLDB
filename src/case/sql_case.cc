/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * sql_case.cc
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#include "case/sql_case.h"
#include "boost/algorithm/string.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/lexical_cast.hpp"
#include "codec/row_codec.h"
#include "glog/logging.h"
#include "string"
#include "vector"
#include "yaml-cpp/yaml.h"
namespace fesql {
namespace sqlcase {
using fesql::codec::Row;
bool SQLCase::TypeParse(const std::string& type_str, fesql::type::Type* type) {
    if (nullptr == type) {
        LOG(WARNING) << "Null Type Output";
        return false;
    }
    std::string lower_type_str = type_str;
    boost::to_lower(lower_type_str);
    if ("int16" == type_str || "i16" == type_str || "smallint" == type_str) {
        *type = type::kInt16;
    } else if ("int32" == type_str || "i32" == type_str || "int" == type_str) {
        *type = type::kInt32;
    } else if ("int64" == type_str || "i64" == type_str ||
               "bigint" == type_str) {
        *type = type::kInt64;
    } else if ("float" == type_str) {
        *type = type::kFloat;
    } else if ("double" == type_str) {
        *type = type::kDouble;
    } else if ("string" == type_str || "varchar" == type_str) {
        *type = type::kVarchar;
    } else if ("timestamp" == type_str) {
        *type = type::kTimestamp;
    } else {
        LOG(WARNING) << "Invalid Type";
        return false;
    }
    return true;
}
bool SQLCase::ExtractSchema(const std::string& schema_str,
                            type::TableDef& table) {  // NOLINT
    if (schema_str.empty()) {
        LOG(WARNING) << "Empty Schema String";
        return false;
    }
    std::vector<std::string> col_vec;
    boost::split(col_vec, schema_str, boost::is_any_of(",\n"),
                 boost::token_compress_on);
    if (col_vec.empty()) {
        LOG(WARNING) << "Invalid Schema Format";
        return false;
    }
    for (auto col : col_vec) {
        std::vector<std::string> name_type_vec;
        boost::split(name_type_vec, col, boost::is_any_of(":"),
                     boost::token_compress_on);
        if (2 != name_type_vec.size()) {
            LOG(WARNING) << "Invalid Schema Format:" << schema_str;
            return false;
        }
        ::fesql::type::ColumnDef* column = table.add_columns();
        boost::trim(name_type_vec[0]);
        boost::trim(name_type_vec[1]);

        column->set_name(name_type_vec[0]);
        fesql::type::Type type;
        if (!TypeParse(name_type_vec[1], &type)) {
            LOG(WARNING) << "Invalid Column Type";
            return false;
        }
        column->set_type(type);
    }
    return true;
}

bool SQLCase::AddInput(const TableInfo&table_data) {
    inputs_.push_back(table_data);
    return true;
}
bool SQLCase::ExtractInputData(std::vector<Row>& rows, int32_t input_idx) {
    if (inputs_[input_idx].data_.empty()) {
        LOG(WARNING) << "Empty Data String";
        return false;
    }
    type::TableDef table;
    if (!ExtractInputTableDef(table)) {
        LOG(WARNING) << "Invalid Schema";
        return false;
    }
    return ExtractRows(table.columns(), inputs_[input_idx].data_, rows);
}

bool SQLCase::ExtractOutputData(std::vector<Row>& rows) {
    if (output_.data_.empty()) {
        LOG(WARNING) << "Empty Data String";
        return false;
    }
    type::TableDef table;
    if (!ExtractOutputSchema(table)) {
        LOG(WARNING) << "Invalid Schema";
        return false;
    }
    return ExtractRows(table.columns(), output_.data_, rows);
}
bool SQLCase::ExtractRow(const vm::Schema& schema, const std::string& row_str,
                         int8_t** out_ptr, int32_t* out_size) {
    std::vector<std::string> item_vec;
    boost::split(item_vec, row_str, boost::is_any_of(","),
                 boost::token_compress_on);
    if (item_vec.size() != schema.size()) {
        LOG(WARNING) << "Row doesn't match with schema";
        return false;
    }
    int str_size = 0;
    for (size_t i = 0; i < item_vec.size(); i++) {
        boost::trim(item_vec[i]);
        auto column = schema.Get(i);
        if (type::kVarchar == column.type()) {
            str_size += strlen(item_vec[i].c_str());
        }
    }
    codec::RowBuilder rb(schema);
    uint32_t row_size = rb.CalTotalLength(str_size);
    int8_t* ptr = static_cast<int8_t*>(malloc(row_size));
    rb.SetBuffer(ptr, row_size);
    auto it = schema.begin();
    uint32_t index = 0;
    for (; it != schema.end(); ++it) {
        if (index >= item_vec.size()) {
            LOG(WARNING) << "Row doesn't match with schema";
            return false;
        }
        bool ok = false;
        switch (it->type()) {
            case type::kInt16: {
                if (!rb.AppendInt16(
                        boost::lexical_cast<int16_t>(item_vec[index]))) {
                    LOG(WARNING) << "Fail Append Column " << index;
                    return false;
                }
                break;
            }
            case type::kInt32: {
                if (!rb.AppendInt32(
                        boost::lexical_cast<int32_t>(item_vec[index]))) {
                    LOG(WARNING) << "Fail Append Column " << index;
                    return false;
                }
                break;
            }
            case type::kInt64: {
                if (!rb.AppendInt64(
                        boost::lexical_cast<int64_t>(item_vec[index]))) {
                    LOG(WARNING) << "Fail Append Column " << index;
                    return false;
                }
                break;
            }
            case type::kFloat: {
                if (!rb.AppendFloat(
                        boost::lexical_cast<float>(item_vec[index]))) {
                    LOG(WARNING) << "Fail Append Column " << index;
                    return false;
                }
                break;
            }
            case type::kDouble: {
                double d = boost::lexical_cast<double>(item_vec[index]);
                if (!rb.AppendDouble(d)) {
                    LOG(WARNING) << "Fail Append Column " << index;
                    return false;
                }
                break;
            }
            case type::kVarchar: {
                std::string str =
                    boost::lexical_cast<std::string>(item_vec[index]);
                if (!rb.AppendString(str.c_str(), strlen(str.c_str()))) {
                    LOG(WARNING) << "Fail Append Column " << index;
                    return false;
                }
                break;
            }
            case type::kTimestamp: {
                if (!rb.AppendTimestamp(
                        boost::lexical_cast<int64_t>(item_vec[index]))) {
                    return false;
                }
                break;
            }
            default: {
                LOG(WARNING) << "Invalid Column Type";
                return false;
            }
        }
        index++;
    }
    *out_ptr = ptr;
    *out_size = row_size;
    return true;
}
bool SQLCase::ExtractRows(const vm::Schema& schema, const std::string& data_str,
                          std::vector<fesql::codec::Row>& rows) {
    std::vector<std::string> row_vec;
    boost::split(row_vec, data_str, boost::is_any_of("\n"),
                 boost::token_compress_on);
    if (row_vec.empty()) {
        LOG(WARNING) << "Invalid Data Format";
        return false;
    }

    for (auto row_str : row_vec) {
        int8_t* row_ptr = nullptr;
        int32_t row_size = 0;
        if (!ExtractRow(schema, row_str, &row_ptr, &row_size)) {
            return false;
        }
        rows.push_back(Row(row_ptr, row_size));
    }
    return true;
}
bool SQLCase::ExtractInputTableDef(type::TableDef& table, int32_t input_idx) {
    if (!ExtractSchema(inputs_[input_idx].schema_, table)) {
        return false;
    }
    table.set_catalog(inputs_[input_idx].db_);
    table.set_name(inputs_[input_idx].name_);
}
bool SQLCase::ExtractOutputSchema(type::TableDef& table) {
    return ExtractSchema(output_.schema_, table);
}
bool SQLCase::CreateSQLCasesFromYaml(const std::string& yaml_path,
                                     std::vector<SQLCase>& sql_cases) {
    LOG(INFO) << "SQL Cases Path: " << yaml_path;
    if (!boost::filesystem::is_regular_file(yaml_path)) {
        LOG(WARNING) << yaml_path << ": No such file";
        return false;
    }
    YAML::Node config = YAML::LoadFile(yaml_path);
    if (config["SQLCases"]) {
        auto sql_cases_node = config["SQLCases"];

        for (auto case_iter = sql_cases_node.begin();
             case_iter != sql_cases_node.end(); case_iter++) {
            SQLCase sql_case;
            auto sql_case_node = *case_iter;
            if (sql_case_node["desc"]) {
                sql_case.desc_ = sql_case_node["desc"].as<std::string>();
            } else {
                sql_case.id_ = -1;
            }
            if (sql_case_node["id"]) {
                sql_case.id_ = sql_case_node["id"].as<int32_t>();
            } else {
                sql_case.id_ = -1;
            }

            if (sql_case_node["sql"]) {
                sql_case.sql_str_ = sql_case_node["sql"].as<std::string>();
            }

            if (sql_case_node["inputs"]) {
                auto inputs = sql_case_node["inputs"];
                for (auto iter = inputs.begin(); iter != inputs.end(); iter++) {
                    SQLCase::TableInfo table;
                    auto schema_data = *iter;
                    if (schema_data["db"]) {
                        table.db_ = schema_data["db"].as<std::string>();
                    }
                    if (schema_data["name"]) {
                        table.name_ = schema_data["name"].as<std::string>();
                    }
                    if (schema_data["schema"]) {
                        table.schema_ = schema_data["schema"].as<std::string>();
                    }
                    if (schema_data["data"]) {
                        table.data_ = schema_data["data"].as<std::string>();
                        boost::trim(table.data_);
                    }
                    sql_case.inputs_.push_back(table);
                }
            }

            if (sql_case_node["output"]) {
                if (sql_case_node["output"]["schema"]) {
                    sql_case.output_.schema_ =
                        sql_case_node["output"]["schema"].as<std::string>();
                }
                if (sql_case_node["output"]["data"]) {
                    sql_case.output_.data_ =
                        sql_case_node["output"]["data"].as<std::string>();
                    boost::trim(sql_case.output_.data_);
                }
            }
            sql_cases.push_back(sql_case);
        }

    } else {
        LOG(WARNING) << "Invalid SQLCase";
        return false;
    }
    return true;
}
std::string FindFesqlDirPath() {
    boost::filesystem::path current_path(boost::filesystem::current_path());
    std::cout << "Current path is : " << current_path << std::endl;

    boost::filesystem::path fesql_path;

    while (current_path.has_parent_path()) {
        current_path = current_path.parent_path();
        if (current_path.filename().string() == "fesql") {
            break;
        }
    }

    if (current_path.filename().string() == "fesql") {
        LOG(INFO) << "Fesql Dir Path is : " << current_path.string()
                  << std::endl;
        return current_path.string();
    }
    return std::string();
}

}  // namespace sqlcase
}  // namespace fesql
