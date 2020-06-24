/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * sql_case.cc
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#include "case/sql_case.h"
#include <set>
#include <string>
#include <vector>
#include "boost/algorithm/string.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/lexical_cast.hpp"
#include "codec/fe_row_codec.h"
#include "glog/logging.h"
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
    } else if ("date" == type_str) {
        *type = type::kDate;
    } else if ("bool" == type_str) {
        *type = type::kBool;
    } else {
        LOG(WARNING) << "Invalid Type";
        return false;
    }
    return true;
}

const std::string SQLCase::TypeString(fesql::type::Type type) {
    switch (type) {
        case type::kInt16:
            return "smallint";
        case type::kInt32:
            return "int";
        case type::kInt64:
            return "bigint";
        case type::kFloat:
            return "float";
        case type::kDouble:
            return "double";
        case type::kVarchar:
            return "string";
        case type::kTimestamp:
            return "timestamp";
        case type::kDate:
            return "date";
        default: {
            return "";
        }
    }
}
bool SQLCase::ExtractTableDef(const std::vector<std::string>& columns,
                              const std::vector<std::string>& indexs,
                              type::TableDef& table) {
    if (!ExtractSchema(columns, table)) {
        return false;
    }
    if (indexs.empty()) {
        return true;
    }
    return ExtractIndex(indexs, table);
}
bool SQLCase::ExtractTableDef(const std::string& schema_str,
                              const std::string& index_str,
                              type::TableDef& table) {
    if (!ExtractSchema(schema_str, table)) {
        return false;
    }
    if (index_str.empty()) {
        return true;
    }
    return ExtractIndex(index_str, table);
}
bool SQLCase::ExtractIndex(const std::string& index_str,
                           type::TableDef& table) {  // NOLINT
    if (index_str.empty()) {
        LOG(WARNING) << "Empty Index String";
        return false;
    }
    std::vector<std::string> index_vec;
    boost::split(index_vec, index_str, boost::is_any_of(",\n"),
                 boost::token_compress_on);
    if (!ExtractIndex(index_vec, table)) {
        LOG(WARNING) << "Fail extract index: " << index_str;
        return false;
    }
    return true;
}
bool SQLCase::ExtractIndex(const std::vector<std::string>& indexs,
                           type::TableDef& table) {  // NOLINT
    if (indexs.empty()) {
        LOG(WARNING) << "Invalid Schema Format";
        return false;
    }
    auto index_vec = indexs;
    for (auto index : index_vec) {
        boost::trim(index);
        if (index.empty()) {
            LOG(WARNING) << "Index String Empty";
            return false;
        }
        std::vector<std::string> name_keys_order;
        boost::split(name_keys_order, index, boost::is_any_of(":"),
                     boost::token_compress_on);
        if (2 > name_keys_order.size()) {
            LOG(WARNING) << "Invalid Index Format:" << index;
            return false;
        }
        ::fesql::type::IndexDef* index_def = table.add_indexes();
        boost::trim(name_keys_order[0]);
        index_def->set_name(name_keys_order[0]);

        std::vector<std::string> keys;
        boost::trim(name_keys_order[1]);
        boost::split(keys, name_keys_order[1], boost::is_any_of("|"),
                     boost::token_compress_on);
        boost::trim(name_keys_order[1]);

        for (auto key : keys) {
            index_def->add_first_keys(key);
        }

        if (3 == name_keys_order.size()) {
            boost::trim(name_keys_order[2]);
            index_def->set_second_key(name_keys_order[2]);
        }
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
    if (!ExtractSchema(col_vec, table)) {
        LOG(WARNING) << "Invalid Schema Format:" << schema_str;
        return false;
    }
    return true;
}
bool SQLCase::ExtractSchema(const std::vector<std::string>& columns,
                            type::TableDef& table) {  // NOLINT
    if (columns.empty()) {
        LOG(WARNING) << "Invalid Schema Format";
        return false;
    }
    for (auto col : columns) {
        boost::trim(col);
        std::vector<std::string> name_type_vec;
        boost::split(name_type_vec, col, boost::is_any_of(": "),
                     boost::token_compress_on);
        if (2 != name_type_vec.size()) {
            LOG(WARNING) << "Invalid Schema Format:"
                         << " Invalid Column " << col;
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

bool SQLCase::BuildCreateSQLFromSchema(const type::TableDef& table,
                                       std::string* create_sql) {
    std::string sql = "CREATE TABLE " + table.name() + "(\n";
    for (int i = 0; i < table.columns_size(); i++) {
        auto column = table.columns(i);
        sql.append(column.name()).append(" ").append(TypeString(column.type()));

        if (column.is_not_null()) {
            sql.append(" NOT NULL");
        }
        sql.append(",\n");
    }

    for (int i = 0; i < table.indexes_size(); i++) {
        auto index = table.indexes(i);
        sql.append("index(");

        sql.append("key=(");
        for (int k = 0; k < index.first_keys_size(); k++) {
            if (k > 0) {
                sql.append(",");
            }
            sql.append(index.first_keys(k));
        }
        sql.append(")");
        // end key

        sql.append(", ts=").append(index.second_key());

        if (index.ttl_size() > 0) {
            sql.append(", ttl=").append(std::to_string(index.ttl(0)));
            switch (index.ttl_type()) {
                case type::kTTLCountLive: {
                    sql.append(", ttl_type=latest");
                    break;
                }
                case type::kTTLTimeLive: {
                    sql.append(", ttl_type=absolute");
                    break;
                }
                case type::kTTLNone: {
                    sql.append("");
                    break;
                }
                default: {
                    LOG(WARNING) << "Unrecognized ttl type";
                }
            }
        }

        sql.append(")\n");
        // end each index
    }
    sql.append(")");
    *create_sql = sql;
    return true;
}
bool SQLCase::AddInput(const TableInfo& table_data) {
    inputs_.push_back(table_data);
    return true;
}
bool SQLCase::ExtractInputData(std::vector<Row>& rows, int32_t input_idx) {
    if (inputs_[input_idx].data_.empty() && inputs_[input_idx].rows_.empty()) {
        LOG(WARNING) << "Empty Data String";
        return false;
    }
    type::TableDef table;
    if (!ExtractInputTableDef(table, input_idx)) {
        LOG(WARNING) << "Invalid Schema";
        return false;
    }

    if (!inputs_[input_idx].data_.empty()) {
        if (!ExtractRows(table.columns(), inputs_[input_idx].data_, rows)) {
            return false;
        }
    } else if (!inputs_[input_idx].columns_.empty()) {
        if (!ExtractRows(table.columns(), inputs_[input_idx].rows_, rows)) {
            return false;
        }
    } else {
        return false;
    }
    return true;
}

bool SQLCase::ExtractOutputData(std::vector<Row>& rows) {
    if (expect_.data_.empty() && expect_.rows_.empty()) {
        LOG(WARNING) << "Empty Data";
        return false;
    }
    type::TableDef table;
    if (!ExtractOutputSchema(table)) {
        LOG(WARNING) << "Invalid Schema";
        return false;
    }

    if (!expect_.data_.empty()) {
        if (!ExtractRows(table.columns(), expect_.data_, rows)) {
            return false;
        }
    } else if (!expect_.rows_.empty()) {
        if (!ExtractRows(table.columns(), expect_.rows_, rows)) {
            return false;
        }
    }
    return true;
}
bool SQLCase::BuildInsertSQLFromRow(const type::TableDef& table,
                                    const std::string& row_str,
                                    std::string* insert_sql) {
    std::string sql = "";
    sql.append("Insert into ").append(table.name()).append(" values(");
    auto schema = table.columns();
    std::vector<std::string> item_vec;
    boost::split(item_vec, row_str, boost::is_any_of(","),
                 boost::token_compress_on);
    if (item_vec.size() != static_cast<size_t>(schema.size())) {
        LOG(WARNING) << "Invalid Row: Row doesn't match with schema : "
                     << row_str;
        return false;
    }

    auto it = schema.begin();
    uint32_t index = 0;
    for (; it != schema.end(); ++it) {
        if (index >= item_vec.size()) {
            LOG(WARNING) << "Invalid Row: Row doesn't match with schema";
            return false;
        }
        if (index > 0) {
            sql.append(", ");
        }
        boost::trim(item_vec[index]);
        switch (it->type()) {
            case type::kInt16:
            case type::kInt32:
            case type::kInt64:
            case type::kFloat:
            case type::kDouble:
            case type::kTimestamp: {
                sql.append(item_vec[index]);
                break;
            }
            case type::kVarchar: {
                sql.append("'").append(item_vec[index]).append("'");
                break;
            }
            default: {
                LOG(WARNING) << "Invalid Column Type";
                return false;
            }
        }
        index++;
    }
    sql.append(")");
    *insert_sql = sql;
    return true;
}
bool SQLCase::ExtractRow(const vm::Schema& schema, const std::string& row_str,
                         int8_t** out_ptr, int32_t* out_size) {
    std::vector<std::string> item_vec;
    boost::split(item_vec, row_str, boost::is_any_of(","),
                 boost::token_compress_on);
    if (!ExtractRow(schema, item_vec, out_ptr, out_size)) {
        LOG(WARNING) << "Fail to extract row: " << row_str;
        return false;
    }
    return true;
}
bool SQLCase::ExtractRow(const vm::Schema& schema,
                         const std::vector<std::string>& row, int8_t** out_ptr,
                         int32_t* out_size) {
    if (row.size() != static_cast<size_t>(schema.size())) {
        LOG(WARNING) << "Invalid Row: Row doesn't match with schema";
        return false;
    }
    auto item_vec = row;
    int str_size = 0;
    for (size_t i = 0; i < item_vec.size(); i++) {
        boost::trim(item_vec[i]);
        auto column = schema.Get(i);
        if (type::kVarchar == column.type()) {
            if (item_vec[i] != "NULL" && item_vec[i] != "null") {
                str_size += strlen(item_vec[i].c_str());
            }
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
            LOG(WARNING) << "Invalid Row: Row doesn't match with schema";
            return false;
        }
        if (item_vec[index] == "NULL" || item_vec[index] == "null") {
            index++;
            rb.AppendNULL();
            continue;
        }
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
            case type::kBool: {
                bool b;
                std::istringstream ss(item_vec[index]);
                ss >> std::boolalpha >> b;
                if (!rb.AppendBool(b)) {
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
            case type::kDate: {
                std::vector<std::string> date_strs;
                boost::split(date_strs, item_vec[index], boost::is_any_of("-"),
                             boost::token_compress_on);

                if (!rb.AppendDate(
                        boost::lexical_cast<int32_t>(date_strs[0]),
                        boost::lexical_cast<int32_t>(date_strs[1]),
                        boost::lexical_cast<int32_t>(date_strs[2]))) {
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
bool SQLCase::ExtractRows(const vm::Schema& schema,
                          const std::vector<std::vector<std::string>>& row_vec,
                          std::vector<fesql::codec::Row>& rows) {
    if (row_vec.empty()) {
        LOG(WARNING) << "Invalid Data Format";
        return false;
    }

    for (auto row_item_vec : row_vec) {
        int8_t* row_ptr = nullptr;
        int32_t row_size = 0;
        if (!ExtractRow(schema, row_item_vec, &row_ptr, &row_size)) {
            return false;
        }
        rows.push_back(Row(base::RefCountedSlice::Create(row_ptr, row_size)));
    }
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
        rows.push_back(Row(base::RefCountedSlice::Create(row_ptr, row_size)));
    }
    return true;
}
bool SQLCase::ExtractInputTableDef(type::TableDef& table, int32_t input_idx) {
    if (!inputs_[input_idx].schema_.empty()) {
        if (!ExtractTableDef(inputs_[input_idx].schema_,
                             inputs_[input_idx].index_, table)) {
            return false;
        }
    } else if (!inputs_[input_idx].columns_.empty()) {
        if (!ExtractTableDef(inputs_[input_idx].columns_,
                             inputs_[input_idx].indexs_, table)) {
            return false;
        }
    }

    table.set_catalog(db_);
    table.set_name(inputs_[input_idx].name_);
    return true;
}
bool SQLCase::ExtractOutputSchema(type::TableDef& table) {
    if (!expect_.schema_.empty()) {
        return ExtractSchema(expect_.schema_, table);
    } else if (!expect_.columns_.empty()) {
        return ExtractSchema(expect_.columns_, table);
    } else {
        LOG(WARNING)
            << "Fail to extract output schema: schema or columns is empty";
        return false;
    }
}
std::ostream& operator<<(std::ostream& output, const SQLCase& thiz) {
    output << "Case ID: " << thiz.id() << ", Desc:" << thiz.desc();
    return output;
}
bool SQLCase::CreateStringListFromYamlNode(const YAML::Node& node,
                                           std::vector<std::string>& rows) {
    for (int i = 0; i < node.size(); i++) {
        rows.push_back(node[i].as<std::string>());
    }
    return true;
}
bool SQLCase::CreateRowsFromYamlNode(
    const YAML::Node& node, std::vector<std::vector<std::string>>& rows) {
    for (int i = 0; i < node.size(); ++i) {
        std::vector<std::string> row;
        if (!CreateStringListFromYamlNode(node[i], row)) {
            LOG(WARNING) << "Fail create rows from yaml node";
            return false;
        }
        rows.push_back(row);
    }
    return true;
}
bool SQLCase::CreateTableInfoFromYamlNode(const YAML::Node& schema_data,
                                          SQLCase::TableInfo* table) {
    if (schema_data["name"]) {
        table->name_ = schema_data["name"].as<std::string>();
        boost::trim(table->name_);
    }
    if (schema_data["schema"]) {
        table->schema_ = schema_data["schema"].as<std::string>();
        boost::trim(table->schema_);
    }
    if (schema_data["index"]) {
        table->index_ = schema_data["index"].as<std::string>();
        boost::trim(table->index_);
    }
    if (schema_data["indexs"]) {
        table->indexs_.clear();
        if (!CreateStringListFromYamlNode(schema_data["indexs"],
                                          table->indexs_)) {
            LOG(WARNING) << "Fail to parse indexs";
            return false;
        }
    }

    if (schema_data["order"]) {
        table->order_ = schema_data["order"].as<std::string>();
        boost::trim(table->order_);
    }

    if (schema_data["data"]) {
        table->data_ = schema_data["data"].as<std::string>();
        boost::trim(table->data_);
    }

    if (schema_data["rows"]) {
        table->rows_.clear();
        if (!CreateRowsFromYamlNode(schema_data["rows"], table->rows_)) {
            LOG(WARNING) << "Fail to parse rows";
            return false;
        }
    }
    if (schema_data["columns"]) {
        table->columns_.clear();
        if (!CreateStringListFromYamlNode(schema_data["columns"],
                                          table->columns_)) {
            LOG(WARNING) << "Fail to parse columns";
            return false;
        }
    }
    return true;
}
bool SQLCase::CreateExpectFromYamlNode(const YAML::Node& schema_data,
                                       SQLCase::ExpectInfo* expect) {
    if (schema_data["schema"]) {
        expect->schema_ = schema_data["schema"].as<std::string>();
        boost::trim(expect->schema_);
    }
    if (schema_data["order"]) {
        expect->order_ = schema_data["order"].as<std::string>();
        boost::trim(expect->order_);
    }

    if (schema_data["data"]) {
        expect->data_ = schema_data["data"].as<std::string>();
        boost::trim(expect->data_);
    }

    if (schema_data["count"]) {
        expect->count_ = schema_data["count"].as<int64_t>();
        boost::trim(expect->data_);
    }

    if (schema_data["result"]) {
        expect->rows_.clear();
        if (!CreateRowsFromYamlNode(schema_data["result"], expect->rows_)) {
            LOG(WARNING) << "Fail to parse rows";
            return false;
        }
    } else if (schema_data["rows"]) {
        expect->rows_.clear();
        if (!CreateRowsFromYamlNode(schema_data["rows"], expect->rows_)) {
            LOG(WARNING) << "Fail to parse rows";
            return false;
        }
    }
    if (schema_data["columns"]) {
        expect->columns_.clear();
        if (!CreateStringListFromYamlNode(schema_data["columns"],
                                          expect->columns_)) {
            LOG(WARNING) << "Fail to parse columns";
            return false;
        }
    }
    return true;
}
bool SQLCase::CreateSQLCasesFromYaml(const std::string& yaml_path,
                                     std::vector<SQLCase>& sql_cases,
                                     const std::string filter_mode) {
    std::vector<std::string> filter_modes;
    if (filter_mode.empty()) {
        return CreateSQLCasesFromYaml(yaml_path, sql_cases, filter_modes);
    } else {
        filter_modes.push_back(filter_mode);
        return CreateSQLCasesFromYaml(yaml_path, sql_cases, filter_modes);
    }
}

bool SQLCase::CreateTableInfoFromYaml(const std::string& yaml_path,
                                      TableInfo* table_info) {
    LOG(INFO) << "Resource path: " << yaml_path;
    if (!boost::filesystem::is_regular_file(yaml_path)) {
        LOG(WARNING) << yaml_path << ": No such file";
        return false;
    }
    YAML::Node table_config = YAML::LoadFile(yaml_path);
    if (table_config["table"]) {
        if (!CreateTableInfoFromYamlNode(table_config["table"], table_info)) {
            return false;
        }
    } else {
        LOG(WARNING) << "SQL Input Resource is invalid";
        return false;
    }
    return true;
}

bool SQLCase::LoadSchemaAndRowsFromYaml(const std::string& resource_path,
                                        type::TableDef& table,
                                        std::vector<fesql::codec::Row>& rows) {
    TableInfo table_info;
    if (!CreateTableInfoFromYaml(resource_path, &table_info)) {
        return false;
    }
    if (!SQLCase::ExtractTableDef(table_info.schema_, table_info.index_,
                                  table)) {
        return false;
    }
    table.set_name(table_info.name_);
    if (!SQLCase::ExtractRows(table.columns(), table_info.data_, rows)) {
        return false;
    }
    return true;
}
bool SQLCase::CreateSQLCasesFromYaml(
    const std::string& yaml_path, std::vector<SQLCase>& sql_cases,
    const std::vector<std::string>& filter_modes) {
    LOG(INFO) << "SQL Cases Path: " << yaml_path;
    if (!boost::filesystem::is_regular_file(yaml_path)) {
        LOG(WARNING) << yaml_path << ": No such file";
        return false;
    }
    YAML::Node config = YAML::LoadFile(yaml_path);
    std::string global_db = "";
    if (config["db"]) {
        global_db = config["db"].as<std::string>();
    }

    std::vector<std::string> debugs_vec;
    if (config["debugs"]) {
        if (!CreateStringListFromYamlNode(config["debugs"], debugs_vec)) {
            LOG(WARNING) << "Fail to parse debugs";
            return false;
        }
    }
    std::set<std::string> debugs(debugs_vec.begin(), debugs_vec.end());

    YAML::Node sql_cases_node;

    if (config["SQLCases"]) {
        sql_cases_node = config["SQLCases"];
    } else if (config["cases"]) {
        sql_cases_node = config["cases"];
    } else {
        LOG(WARNING) << "Fail to parse sql cases";
        return false;
    }
    for (auto case_iter = sql_cases_node.begin();
         case_iter != sql_cases_node.end(); case_iter++) {
        SQLCase sql_case;
        auto sql_case_node = *case_iter;
        if (sql_case_node["desc"]) {
            sql_case.desc_ = sql_case_node["desc"].as<std::string>();
            boost::trim(sql_case.desc_);
        } else {
            sql_case.desc_ = "";
        }

        if (!debugs.empty()) {
            if (debugs.find(sql_case.desc_) == debugs.end()) {
                continue;
            }
        }
        if (sql_case_node["id"]) {
            sql_case.id_ = sql_case_node["id"].as<int32_t>();
        } else {
            sql_case.id_ = -1;
        }
        if (sql_case_node["mode"]) {
            sql_case.mode_ = sql_case_node["mode"].as<std::string>();
            boost::trim(sql_case.mode_);
        } else {
            sql_case.mode_ = "batch";
        }

        if (sql_case_node["batch_plan"]) {
            sql_case.batch_plan_ =
                sql_case_node["batch_plan"].as<std::string>();
            boost::trim(sql_case.batch_plan_);
        }
        if (sql_case_node["request_plan"]) {
            sql_case.request_plan_ =
                sql_case_node["request_plan"].as<std::string>();
            boost::trim(sql_case.request_plan_);
        }
        if (!filter_modes.empty()) {
            bool need_filter = false;
            for (auto filter_mode : filter_modes) {
                if (boost::contains(sql_case.mode_, filter_mode)) {
                    need_filter = true;
                    break;
                }
            }

            if (need_filter) {
                LOG(INFO) << "SKIP SQL Case " << sql_case.desc();
                continue;
            }
        }

        if (sql_case_node["db"]) {
            sql_case.db_ = sql_case_node["db"].as<std::string>();
        } else {
            sql_case.db_ = global_db;
        }

        if (sql_case_node["create"]) {
            sql_case.create_str_ = sql_case_node["create"].as<std::string>();
            boost::trim(sql_case.create_str_);
        }

        if (sql_case_node["insert"]) {
            sql_case.insert_str_ = sql_case_node["insert"].as<std::string>();
            boost::trim(sql_case.insert_str_);
        }

        if (sql_case_node["sql"]) {
            sql_case.sql_str_ = sql_case_node["sql"].as<std::string>();
            boost::trim(sql_case.sql_str_);
        }
        if (sql_case_node["sqls"]) {
            sql_case.sql_strs_.clear();
            if (!CreateStringListFromYamlNode(sql_case_node["sqls"],
                                              sql_case.sql_strs_)) {
                LOG(WARNING) << "Fail to parse sqls";
                return false;
            }
        }

        if (sql_case_node["standard_sql"]) {
            sql_case.standard_sql_ = sql_case_node["standard_sql"].as<bool>();
        } else {
            sql_case.standard_sql_ = false;
        }

        if (sql_case_node["inputs"]) {
            auto inputs = sql_case_node["inputs"];
            for (auto iter = inputs.begin(); iter != inputs.end(); iter++) {
                SQLCase::TableInfo table;
                auto schema_data = *iter;

                if (schema_data["resource"]) {
                    std::string resource =
                        schema_data["resource"].as<std::string>();
                    boost::trim(resource);
                    std::string resource_path =
                        FindFesqlDirPath() + "/" + resource;
                    if (!CreateTableInfoFromYaml(resource_path, &table)) {
                        return false;
                    }
                } else {
                    if (!CreateTableInfoFromYamlNode(schema_data, &table)) {
                        return false;
                    }
                }
                sql_case.inputs_.push_back(table);
            }
        }

        if (sql_case_node["output"]) {
            auto schema_data = sql_case_node["output"];
            if (!CreateExpectFromYamlNode(schema_data, &sql_case.expect_)) {
                return false;
            }
        }
        if (sql_case_node["expect"]) {
            auto schema_data = sql_case_node["expect"];
            if (!CreateExpectFromYamlNode(schema_data, &sql_case.expect_)) {
                return false;
            }
        }
        sql_cases.push_back(sql_case);
    }
    return true;
}
std::string FindFesqlDirPath() {
    boost::filesystem::path current_path(boost::filesystem::current_path());
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
