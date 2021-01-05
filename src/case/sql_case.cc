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
        LOG(WARNING) << "Invalid Type: " << type_str;
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
        case type::kBool:
            return "bool";
        default: {
            return "unknow";
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

        if (3 <= name_keys_order.size()) {
            boost::trim(name_keys_order[2]);
            index_def->set_second_key(name_keys_order[2]);
        }

        if (4 <= name_keys_order.size()) {
            boost::trim(name_keys_order[3]);
            index_def->add_ttl(
                boost::lexical_cast<int64_t>(name_keys_order[3]));
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
        boost::replace_last(col, " ", ":");
        std::vector<std::string> name_type_vec;
        boost::split(name_type_vec, col, boost::is_any_of(":"),
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
        column->set_is_not_null(false);
    }
    return true;
}
bool SQLCase::BuildCreateSQLFromSchema(const type::TableDef& table,
                                       std::string* create_sql,
                                       bool isGenerateIndex,
                                       int partition_num) {
    std::string sql = "CREATE TABLE " + table.name() + "(\n";
    for (int i = 0; i < table.columns_size(); i++) {
        auto column = table.columns(i);
        sql.append(column.name()).append(" ").append(TypeString(column.type()));

        if (column.is_not_null()) {
            sql.append(" NOT NULL");
        }
        if (isGenerateIndex || i < table.columns_size() - 1) {
            sql.append(",\n");
        }
    }

    if (!isGenerateIndex) {
        sql.append(");");
        *create_sql = sql;
        return true;
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
        if (!index.second_key().empty()) {
            sql.append(", ts=").append(index.second_key());
        }

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

        if (i < (table.indexes_size() - 1)) {
            sql.append("),\n");
        } else {
            sql.append(")\n");
        }
        // end each index
    }
    if (1 != partition_num) {
        sql.append(") partitionnum=");
        sql.append(std::to_string(partition_num));
        sql.append(";");
    } else {
        sql.append(");");
    }
    *create_sql = sql;
    return true;
}
bool SQLCase::AddInput(const TableInfo& table_data) {
    inputs_.push_back(table_data);
    return true;
}
bool SQLCase::ExtractInputData(std::vector<Row>& rows,
                               int32_t input_idx) const {
    return ExtractInputData(inputs_[input_idx], rows);
}
bool SQLCase::ExtractInputData(const TableInfo& input,
                               std::vector<Row>& rows) const {
    if (input.data_.empty() && input.rows_.empty()) {
        LOG(WARNING) << "Empty Data String";
        return false;
    }
    type::TableDef table;
    if (!ExtractInputTableDef(input, table)) {
        LOG(WARNING) << "Invalid Schema";
        return false;
    }

    if (!input.data_.empty()) {
        if (!ExtractRows(table.columns(), input.data_, rows)) {
            return false;
        }
    } else if (!input.columns_.empty()) {
        if (!ExtractRows(table.columns(), input.rows_, rows)) {
            return false;
        }
    } else {
        return false;
    }
    return true;
}

bool SQLCase::ExtractOutputData(std::vector<Row>& rows) const {
    if (expect_.data_.empty() && expect_.rows_.empty()) {
        LOG(WARNING) << "ExtractOutputData Fail: Empty Data";
        return false;
    }
    type::TableDef table;
    if (!ExtractOutputSchema(table)) {
        LOG(WARNING) << "ExtractOutputData Fail: Invalid Schema";
        return false;
    }

    if (!expect_.data_.empty()) {
        if (!ExtractRows(table.columns(), expect_.data_, rows)) {
            LOG(WARNING) << "ExtractOutputData Fail";
            return false;
        }
    } else if (!expect_.rows_.empty()) {
        if (!ExtractRows(table.columns(), expect_.rows_, rows)) {
            LOG(WARNING) << "ExtractOutputData Fail";
            return false;
        }
    }
    return true;
}
bool SQLCase::BuildInsertSQLFromRows(
    const type::TableDef& table,
    const std::vector<std::vector<std::string>>& rows,
    std::string* insert_sql) {
    std::string sql = "";
    sql.append("Insert into ").append(table.name()).append(" values");

    size_t i = 0;
    for (auto item_vec : rows) {
        std::string values = "";
        if (!BuildInsertValueStringFromRow(table, item_vec, &values)) {
            return false;
        }
        sql.append("\n").append(values);
        i++;
        if (i < rows.size()) {
            sql.append(",");
        } else {
            sql.append(";");
        }
    }
    *insert_sql = sql;
    return true;
}
bool SQLCase::BuildInsertSQLFromData(const type::TableDef& table,
                                     std::string data,
                                     std::string* insert_sql) {
    boost::trim(data);
    std::vector<std::string> row_vec;
    boost::split(row_vec, data, boost::is_any_of("\n"),
                 boost::token_compress_on);
    std::vector<std::vector<std::string>> rows;
    for (auto row_str : row_vec) {
        std::vector<std::string> item_vec;
        boost::split(item_vec, row_str, boost::is_any_of(","),
                     boost::token_compress_on);
        rows.push_back(item_vec);
    }
    BuildInsertSQLFromRows(table, rows, insert_sql);
    return true;
}

bool SQLCase::BuildInsertValueStringFromRow(
    const type::TableDef& table, const std::vector<std::string>& item_vec,
    std::string* values) {
    std::string sql = "(";
    auto schema = table.columns();
    if (item_vec.size() != static_cast<size_t>(schema.size())) {
        LOG(WARNING) << "Invalid Row: Row doesn't match with schema : exp "
                     << schema.size() << " but " << item_vec.size();
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
        auto item = item_vec[index];
        boost::trim(item);
        if (item == "null" || item == "NULL") {
            sql.append("null");
            index++;
            continue;
        }
        switch (it->type()) {
            case type::kBool:
            case type::kInt16:
            case type::kInt32:
            case type::kInt64:
            case type::kFloat:
            case type::kDouble:
            case type::kTimestamp: {
                sql.append(item);
                break;
            }
            case type::kDate:
            case type::kVarchar: {
                sql.append("'").append(item).append("'");
                break;
            }
            default: {
                LOG(WARNING)
                    << "Invalid Column Type " << TypeString(it->type());
                return false;
            }
        }
        index++;
    }
    sql.append(")");
    *values = sql;
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
        LOG(WARNING) << "Invalid Row: Row doesn't match with schema: exp size "
                     << schema.size() << " but real size " << row.size();
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
const std::string SQLCase::case_name() const {
    std::string name = id_ + "_" + desc_;
    boost::replace_all(name, " ", "_");
    return name;
}
bool SQLCase::ExtractInputTableDef(type::TableDef& table,
                                   int32_t input_idx) const {
    return ExtractInputTableDef(inputs_[input_idx], table);
}
bool SQLCase::ExtractInputTableDef(const TableInfo& input,
                                   type::TableDef& table) const {
    if (!input.schema_.empty()) {
        if (!ExtractTableDef(input.schema_, input.index_, table)) {
            return false;
        }
    } else if (!input.columns_.empty()) {
        if (!ExtractTableDef(input.columns_, input.indexs_, table)) {
            return false;
        }
    }

    table.set_catalog(db_);
    table.set_name(input.name_);
    return true;
}

// Build Create SQL
// schema + index --> create sql
// columns + indexs --> create sql
bool SQLCase::BuildCreateSQLFromInput(int32_t input_idx, std::string* sql,
                                      int partition_num) const {
    if (!inputs_[input_idx].create_.empty()) {
        *sql = inputs_[input_idx].create_;
        return true;
    }
    type::TableDef table;
    if (!ExtractInputTableDef(table, input_idx)) {
        LOG(WARNING) << "Fail to extract table schema";
        return false;
    }
    if (!BuildCreateSQLFromSchema(table, sql, true, partition_num)) {
        LOG(WARNING) << "Fail to build create sql string";
        return false;
    }
    return true;
}

bool SQLCase::BuildInsertSQLListFromInput(
    int32_t input_idx, std::vector<std::string>* sql_list) const {
    if (!inputs_[input_idx].insert_.empty()) {
        sql_list->push_back(inputs_[input_idx].insert_);
        return true;
    }
    type::TableDef table;
    if (!ExtractInputTableDef(table, input_idx)) {
        LOG(WARNING) << "Fail to extract table schema";
        return false;
    }

    if (!inputs_[input_idx].data_.empty()) {
        auto data = inputs_[input_idx].data_;
        boost::trim(data);
        std::vector<std::string> row_vec;
        boost::split(row_vec, data, boost::is_any_of("\n"),
                     boost::token_compress_on);
        for (auto row_str : row_vec) {
            std::vector<std::vector<std::string>> rows;
            std::vector<std::string> item_vec;
            boost::split(item_vec, row_str, boost::is_any_of(","),
                         boost::token_compress_on);
            rows.push_back(item_vec);
            std::string insert_sql;
            if (!BuildInsertSQLFromRows(table, rows, &insert_sql)) {
                LOG(WARNING) << "Fail to build insert sql from rows";
                return false;
            }
            sql_list->push_back(insert_sql);
        }

    } else if (!inputs_[input_idx].rows_.empty()) {
        for (auto row : inputs_[input_idx].rows_) {
            std::vector<std::vector<std::string>> rows;
            rows.push_back(row);
            std::string insert_sql;
            if (!BuildInsertSQLFromRows(table, rows, &insert_sql)) {
                LOG(WARNING) << "Fail to build insert sql from rows";
                return false;
            }
            sql_list->push_back(insert_sql);
        }
    }
    return true;
}
bool SQLCase::BuildInsertSQLFromInput(int32_t input_idx,
                                      std::string* sql) const {
    if (!inputs_[input_idx].insert_.empty()) {
        *sql = inputs_[input_idx].insert_;
        return true;
    }
    type::TableDef table;
    if (!ExtractInputTableDef(table, input_idx)) {
        LOG(WARNING) << "Fail to extract table schema";
        return false;
    }

    if (!inputs_[input_idx].data_.empty()) {
        if (!BuildInsertSQLFromData(table, inputs_[input_idx].data_, sql)) {
            LOG(WARNING) << "Fail to build create sql string";
            return false;
        }
    } else if (!inputs_[input_idx].rows_.empty()) {
        if (!BuildInsertSQLFromRows(table, inputs_[input_idx].rows_, sql)) {
            LOG(WARNING) << "Fail to build create sql string";
            return false;
        }
    }
    return true;
}
bool SQLCase::ExtractOutputSchema(type::TableDef& table) const {
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
    for (size_t i = 0; i < node.size(); i++) {
        if (node[i].IsNull()) {
            rows.push_back("null");
        } else {
            rows.push_back(node[i].as<std::string>());
        }
    }
    return true;
}
bool SQLCase::CreateRowsFromYamlNode(
    const YAML::Node& node, std::vector<std::vector<std::string>>& rows) {
    for (size_t i = 0; i < node.size(); ++i) {
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

    if (schema_data["repeat"]) {
        table->repeat_ = schema_data["repeat"].as<int64_t>();
    }
    if (schema_data["repeat_tag"]) {
        table->repeat_tag_ = schema_data["repeat_tag"].as<std::string>();
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

    if (schema_data["create"]) {
        table->create_ = schema_data["create"].as<std::string>();
        boost::trim(table->create_);
    }

    if (schema_data["insert"]) {
        table->insert_ = schema_data["insert"].as<std::string>();
        boost::trim(table->insert_);
    }

    if (schema_data["common_column_indices"]) {
        auto data = schema_data["common_column_indices"];
        std::vector<std::string> idxs;
        if (!CreateStringListFromYamlNode(data, idxs)) {
            return false;
        }
        for (auto str : idxs) {
            table->common_column_indices_.insert(
                boost::lexical_cast<size_t>(str));
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
    if (schema_data["success"]) {
        expect->success_ = schema_data["success"].as<bool>();
    } else {
        expect->success_ = true;
    }
    if (schema_data["common_column_indices"]) {
        auto data = schema_data["common_column_indices"];
        std::vector<std::string> idxs;
        if (!CreateStringListFromYamlNode(data, idxs)) {
            return false;
        }
        for (auto str : idxs) {
            expect->common_column_indices_.insert(
                boost::lexical_cast<size_t>(str));
        }
    }
    return true;
}
bool SQLCase::CreateSQLCasesFromYaml(const std::string& cases_dir,
                                     const std::string& yaml_path,
                                     std::vector<SQLCase>& sql_cases,
                                     const std::string filter_mode) {
    std::vector<std::string> filter_modes;
    if (filter_mode.empty()) {
        return CreateSQLCasesFromYaml(cases_dir, yaml_path, sql_cases,
                                      filter_modes);
    } else {
        filter_modes.push_back(filter_mode);
        return CreateSQLCasesFromYaml(cases_dir, yaml_path, sql_cases,
                                      filter_modes);
    }
}

bool SQLCase::CreateTableInfoFromYaml(const std::string& cases_dir,
                                      const std::string& yaml_path,
                                      TableInfo* table_info) {
    std::string resouces_path;
    if (cases_dir != "") {
        resouces_path = cases_dir + "/" + yaml_path;
    } else {
        resouces_path = yaml_path;
    }
    if (!boost::filesystem::is_regular_file(resouces_path)) {
        LOG(WARNING) << resouces_path << ": No such file";
        return false;
    }
    YAML::Node table_config = YAML::LoadFile(resouces_path);
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

bool SQLCase::LoadSchemaAndRowsFromYaml(const std::string& cases_dir,
                                        const std::string& resource_path,
                                        type::TableDef& table,
                                        std::vector<fesql::codec::Row>& rows) {
    TableInfo table_info;
    if (!CreateTableInfoFromYaml(cases_dir, resource_path, &table_info)) {
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
    const std::string& cases_dir, const std::string& yaml_path,
    std::vector<SQLCase>& sql_cases,
    const std::vector<std::string>& filter_modes) {
    std::string sql_case_path;
    if (cases_dir != "") {
        sql_case_path = cases_dir + "/" + yaml_path;
    } else {
        sql_case_path = yaml_path;
    }
    if (IS_DEBUG()) {
        DLOG(INFO) << "SQL Cases Path: " << sql_case_path;
    }
    if (!boost::filesystem::is_regular_file(sql_case_path)) {
        LOG(WARNING) << sql_case_path << ": No such file";
        return false;
    }
    YAML::Node config = YAML::LoadFile(sql_case_path);
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
        YAML::Node& sql_case_node = sql_case.raw_node_;
        sql_case_node = (YAML::Node)(*case_iter);

        if (sql_case_node["id"]) {
            sql_case.id_ = sql_case_node["id"].as<std::string>();
        } else {
            sql_case.id_ = "-1";
        }

        if (sql_case_node["desc"]) {
            sql_case.desc_ = sql_case_node["desc"].as<std::string>();
            boost::trim(sql_case.desc_);
        } else {
            sql_case.desc_ = "";
        }

        if (!debugs.empty()) {
            if (debugs.find(sql_case.desc_) == debugs.end()) {
                continue;
            } else {
                sql_case.debug_ = true;
            }
        }
        if (sql_case_node["mode"]) {
            sql_case.mode_ = sql_case_node["mode"].as<std::string>();
            boost::trim(sql_case.mode_);
        } else {
            sql_case.mode_ = "batch";
        }

        if (sql_case_node["debug"]) {
            sql_case.debug_ = sql_case_node["debug"].as<bool>();
        } else {
            sql_case.debug_ = false;
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
        if (sql_case_node["tags"]) {
            if (!CreateStringListFromYamlNode(sql_case_node["tags"],
                                              sql_case.tags_)) {
                LOG(WARNING) << "Fail to parse tags";
                return false;
            }
            std::set<std::string> tags(sql_case.tags_.begin(),
                                       sql_case.tags_.end());

            if (tags.find("todo") != tags.cend()) {
                continue;
            }

            if (tags.find("TODO") != tags.cend()) {
                continue;
            }
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
        if (sql_case_node["cluster_request_plan"]) {
            sql_case.cluster_request_plan_ =
                sql_case_node["cluster_request_plan"].as<std::string>();
            boost::trim(sql_case.cluster_request_plan_);
        }
        if (sql_case_node["db"]) {
            sql_case.db_ = sql_case_node["db"].as<std::string>();
        } else {
            sql_case.db_ = global_db;
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
        if (sql_case_node["batch_request_optimized"]) {
            sql_case.batch_request_optimized_ =
                sql_case_node["batch_request_optimized"].as<bool>();
        } else {
            sql_case.batch_request_optimized_ = true;
        }

        if (sql_case_node["standard_sql_compatible"]) {
            sql_case.standard_sql_compatible_ =
                sql_case_node["standard_sql_compatible"].as<bool>();
        } else {
            sql_case.standard_sql_compatible_ = true;
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
                    if (!CreateTableInfoFromYaml(cases_dir, resource, &table)) {
                        return false;
                    }
                }
                if (!CreateTableInfoFromYamlNode(schema_data, &table)) {
                    return false;
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
        if (sql_case_node["batch_request"]) {
            if (!CreateTableInfoFromYamlNode(sql_case_node["batch_request"],
                                             &sql_case.batch_request_)) {
                return false;
            }
        }
        sql_cases.push_back(sql_case);
    }
    return true;
}
fesql::sqlcase::SQLCase SQLCase::LoadSQLCaseWithID(const std::string& dir_path,
                                                   const std::string& yaml_path,
                                                   const std::string& case_id) {
    std::vector<SQLCase> cases;
    LOG(INFO) << "BENCHMARK LOAD SQL CASE";
    SQLCase::CreateSQLCasesFromYaml(dir_path, yaml_path, cases);

    for (const auto& sql_case : cases) {
        if (sql_case.id() == case_id) {
            return sql_case;
        }
    }
    return SQLCase();
}
std::string FindFesqlDirPath() {
    boost::filesystem::path current_path(boost::filesystem::current_path());
    boost::filesystem::path fesql_path;
    bool find_fesql_dir = false;

    while (current_path.has_parent_path()) {
        current_path = current_path.parent_path();
        if (current_path.filename().string() == "fesql") {
            fesql_path = current_path;
            find_fesql_dir = true;
            break;
        }
        boost::filesystem::directory_iterator endIter;
        for (boost::filesystem::directory_iterator iter(current_path);
             iter != endIter; iter++) {
            if (boost::filesystem::is_directory(*iter) &&
                iter->path().filename() == "fesql") {
                fesql_path = iter->path();
                find_fesql_dir = true;
                break;
            }
        }
        if (find_fesql_dir) {
            break;
        }
    }

    if (find_fesql_dir) {
        return fesql_path.string();
    }
    return std::string();
}

bool SQLCase::BuildCreateSpSQLFromInput(int32_t input_idx,
                                        const std::string& select_sql,
                                        const std::set<size_t>& common_idx,
                                        std::string* create_sp_sql) {
    type::TableDef table;
    if (!ExtractInputTableDef(table, input_idx)) {
        LOG(WARNING) << "Fail to extract table schema";
        return false;
    }
    if (!BuildCreateSpSQLFromSchema(table, select_sql, common_idx,
                                    create_sp_sql)) {
        LOG(WARNING) << "Fail to build create sql string";
        return false;
    }
    return true;
}

bool SQLCase::BuildCreateSpSQLFromSchema(const type::TableDef& table,
                                         const std::string& select_sql,
                                         const std::set<size_t>& common_idx,
                                         std::string* create_sql) {
    std::string sql = "CREATE Procedure " + sp_name_ + "(\n";
    for (int i = 0; i < table.columns_size(); i++) {
        auto column = table.columns(i);
        if (!common_idx.empty() && common_idx.count(i)) {
            sql.append("const ");
        }
        sql.append(column.name()).append(" ").append(TypeString(column.type()));
        if (i < table.columns_size() - 1) {
            sql.append(",\n");
        }
    }
    sql.append(")\n");
    sql.append("BEGIN\n");
    sql.append(select_sql);
    sql.append("\n");
    sql.append("END;");
    *create_sql = sql;
    return true;
}

}  // namespace sqlcase
}  // namespace fesql
