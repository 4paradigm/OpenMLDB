/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "case/sql_case.h"

#include <filesystem>
#include <fstream>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "absl/cleanup/cleanup.h"
#include "absl/strings/ascii.h"
#include "absl/strings/substitute.h"
#include "absl/synchronization/mutex.h"
#include "boost/algorithm/string.hpp"
#include "boost/filesystem/operations.hpp"
#include "boost/lexical_cast.hpp"
#include "boost/regex.hpp"
#include "codec/fe_row_codec.h"
#include "glog/logging.h"
#include "node/sql_node.h"
#include "yaml-cpp/yaml.h"

namespace hybridse {
namespace sqlcase {
using hybridse::codec::Row;

static absl::Mutex mtx;
// working directory, where the yaml file lives
static std::filesystem::path working_dir;

bool SqlCase::TTLParse(const std::string& org_type_str,
                       std::vector<int64_t>& ttls) {
    std::string type_str = org_type_str;
    boost::to_lower(type_str);
    if (type_str.empty()) {
        LOG(WARNING) << "Empty TTL String";
        return false;
    }
    std::vector<std::string> ttlstrings;
    boost::trim(type_str);
    boost::split(ttlstrings, type_str, boost::is_any_of("|"),
                 boost::token_compress_on);

    for (std::string ttlstr : ttlstrings) {
        char unit = ttlstr[ttlstr.size() - 1];
        if ('d' == unit) {
            ttls.push_back(boost::lexical_cast<int64_t>(
                               ttlstr.substr(0, ttlstr.size() - 1)) *
                           24 * 60);
        } else if ('h' == unit) {
            ttls.push_back(boost::lexical_cast<int64_t>(
                               ttlstr.substr(0, ttlstr.size() - 1)) *
                           60);
        } else if ('m' == unit) {
            ttls.push_back(boost::lexical_cast<int64_t>(
                ttlstr.substr(0, ttlstr.size() - 1)));
        } else {
            ttls.push_back(
                boost::lexical_cast<int64_t>(ttlstr.substr(0, ttlstr.size())));
        }
    }
    return true;
}
bool SqlCase::TTLTypeParse(const std::string& org_type_str,
                           ::hybridse::type::TTLType* type) {
    if (nullptr == type) {
        LOG(WARNING) << "Null TTL Type Output";
        return false;
    }
    std::string type_str = org_type_str;
    boost::to_lower(type_str);
    if ("absolute" == type_str) {
        *type = hybridse::type::TTLType::kTTLTimeLive;
    } else if ("latest" == type_str) {
        *type = hybridse::type::TTLType::kTTLCountLive;
    } else if ("absorlat" == type_str) {
        *type = hybridse::type::TTLType::kTTLTimeLiveOrCountLive;
    } else if ("absandlat" == type_str) {
        *type = hybridse::type::TTLType::kTTLTimeLiveAndCountLive;
    } else {
        LOG(WARNING) << "Invalid TTLType: " << type_str;
        return false;
    }
    return true;
}
bool SqlCase::TypeParse(const std::string& org_type_str,
                        hybridse::type::Type* type) {
    if (nullptr == type) {
        LOG(WARNING) << "Null Type Output";
        return false;
    }
    std::string type_str = org_type_str;
    boost::to_lower(type_str);
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

const std::string SqlCase::TypeString(hybridse::type::Type type) {
    return node::TypeName(type);
}
bool SqlCase::ExtractTableDef(const std::vector<std::string>& columns,
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
bool SqlCase::ExtractTableDef(const std::string& schema_str,
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
bool SqlCase::ExtractIndex(const std::string& index_str,
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
bool SqlCase::ExtractIndex(const std::vector<std::string>& indexs,
                           type::TableDef& table) {  // NOLINT
    if (indexs.empty()) {
        LOG(WARNING) << "Invalid Schema Format";
        return false;
    }
    auto index_vec = indexs;
    std::set<std::string> index_names;
    try {
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
            ::hybridse::type::IndexDef* index_def = table.add_indexes();
            boost::trim(name_keys_order[0]);
            if (index_names.find(name_keys_order[0]) != index_names.end()) {
                LOG(WARNING) << "Invalid Index: index name "
                             << name_keys_order[0] << " duplicate";
                return false;
            }
            index_def->set_name(name_keys_order[0]);
            index_names.insert(name_keys_order[0]);

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
                if (!name_keys_order[2].empty() &&
                    name_keys_order[2] != "null") {
                    index_def->set_second_key(name_keys_order[2]);
                }
            }

            if (4 <= name_keys_order.size()) {
                boost::trim(name_keys_order[3]);
                std::vector<int64_t> ttls;
                if (!TTLParse(name_keys_order[3], ttls)) {
                    return false;
                }
                for (int64_t ttl : ttls) {
                    index_def->add_ttl(ttl);
                }
            }
            if (5 <= name_keys_order.size()) {
                boost::trim(name_keys_order[4]);
                ::hybridse::type::TTLType ttl_type;
                if (!TTLTypeParse(name_keys_order[4], &ttl_type)) {
                    return false;
                }
                index_def->set_ttl_type(ttl_type);
            }
        }
    } catch (const std::exception& ex) {
        LOG(WARNING) << "Fail to ExtractIndex: " << ex.what();
        return false;
    }

    return true;
}

bool SqlCase::ExtractSchema(const std::string& schema_str,
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
bool SqlCase::ExtractSchema(const std::vector<std::string>& columns,
                            type::TableDef& table) {  // NOLINT
    if (columns.empty()) {
        LOG(WARNING) << "Invalid Schema Format";
        return false;
    }
    try {
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
            ::hybridse::type::ColumnDef* column = table.add_columns();
            boost::trim(name_type_vec[0]);
            boost::trim(name_type_vec[1]);

            column->set_name(name_type_vec[0]);
            hybridse::type::Type type;
            if (!TypeParse(name_type_vec[1], &type)) {
                LOG(WARNING) << "Invalid Column Type";
                return false;
            }
            column->set_type(type);
            column->set_is_not_null(false);
        }
    } catch (const std::exception& ex) {
        LOG(WARNING) << "Fail to ExtractSchema: " << ex.what();
        return false;
    }
    return true;
}
bool SqlCase::BuildCreateSqlFromSchema(const type::TableDef& table,
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
            switch (index.ttl_type()) {
                case type::kTTLCountLive: {
                    sql.append(", ttl=").append(std::to_string(index.ttl(0)));
                    sql.append(", ttl_type=latest");
                    break;
                }
                case type::kTTLTimeLive: {
                    sql.append(", ttl=").append(std::to_string(index.ttl(0)));
                    sql.append("m, ttl_type=absolute");
                    break;
                }
                case type::kTTLTimeLiveAndCountLive: {
                    sql.append(", ttl=(")
                        .append(std::to_string(index.ttl(0)))
                        .append("m,")
                        .append(std::to_string(index.ttl(1)))
                        .append(")")
                        .append(", ttl_type=absandlat");
                    break;
                }
                case type::kTTLTimeLiveOrCountLive: {
                    sql.append(", ttl=(")
                        .append(std::to_string(index.ttl(0)))
                        .append("m,")
                        .append(std::to_string(index.ttl(1)))
                        .append(")")
                        .append(", ttl_type=absorlat");
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
    if (partition_num != 0) {
        // partition_num = 0 -> unset, respect the cluster environment
        if (1 != partition_num) {
            sql.append(") options(partitionnum=");
            sql.append(std::to_string(partition_num));
            sql.append(");");
        } else {
            sql.append(") options(partitionnum=1, replicanum=1);");
        }
    } else {
        sql.append(");");
    }
    *create_sql = sql;
    return true;
}
bool SqlCase::AddInput(const TableInfo& table_data) {
    inputs_.push_back(table_data);
    return true;
}
bool SqlCase::ExtractInputData(std::vector<Row>& rows,
                               int32_t input_idx) const {
    return ExtractInputData(inputs_[input_idx], rows);
}
bool SqlCase::ExtractInputData(const TableInfo& input,
                               std::vector<Row>& rows) const {
    try {
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
    } catch (const std::exception& ex) {
        LOG(WARNING) << "Fail to ExtractInput Data: " << ex.what();
        return false;
    }
    return true;
}

bool SqlCase::ExtractOutputData(std::vector<Row>& rows) const {
    if (expect_.data_.empty() && expect_.rows_.empty()) {
        return true;
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
bool SqlCase::BuildInsertSqlFromRows(
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
bool SqlCase::BuildInsertSqlFromData(const type::TableDef& table,
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
    BuildInsertSqlFromRows(table, rows, insert_sql);
    return true;
}

bool SqlCase::BuildInsertValueStringFromRow(
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
bool SqlCase::ExtractRow(const vm::Schema& schema, const std::string& row_str,
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
bool SqlCase::ExtractRow(const vm::Schema& schema,
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

    try {
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
                    boost::split(date_strs, item_vec[index],
                                 boost::is_any_of("-"),
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
    } catch (const std::exception& ex) {
        LOG(WARNING) << "Fail to ExtractSchema: " << ex.what();
        return false;
    }
    *out_ptr = ptr;
    *out_size = row_size;
    return true;
}
bool SqlCase::ExtractRows(const vm::Schema& schema,
                          const std::vector<std::vector<std::string>>& row_vec,
                          std::vector<hybridse::codec::Row>& rows) {
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
bool SqlCase::ExtractRows(const vm::Schema& schema, const std::string& data_str,
                          std::vector<hybridse::codec::Row>& rows) {
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
const std::string SqlCase::case_name() const {
    std::string name = id_ + "_" + desc_;
    boost::replace_all(name, " ", "_");
    return name;
}
bool SqlCase::ExtractInputTableDef(type::TableDef& table,
                                   int32_t input_idx) const {
    return ExtractInputTableDef(inputs_[input_idx], table);
}
bool SqlCase::ExtractInputTableDef(const TableInfo& input,
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
    if (input.db_.empty()) {
        table.set_catalog(db_);
    } else {
        table.set_catalog(input.db_);
    }
    table.set_name(input.name_);
    return true;
}

// Build Create SQL
// schema + index --> create sql
// columns + indexs --> create sql
bool SqlCase::BuildCreateSqlFromInput(int32_t input_idx, std::string* sql,
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
    if (table.columns_size() == 0) {
        LOG(WARNING) << "Do not build create sql from empty columns table";
        return false;
    }
    if (!BuildCreateSqlFromSchema(table, sql, true, partition_num)) {
        LOG(WARNING) << "Fail to build create sql string";
        return false;
    }
    return true;
}

bool SqlCase::BuildInsertSqlListFromInput(
    int32_t input_idx, std::vector<std::string>* sql_list) const {
    if (!inputs_[input_idx].insert_.empty()) {
        sql_list->push_back(inputs_[input_idx].insert_);
        return true;
    }

    if (!inputs_[input_idx].inserts_.empty()) {
        *sql_list = inputs_[input_idx].inserts_;
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
            if (!BuildInsertSqlFromRows(table, rows, &insert_sql)) {
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
            if (!BuildInsertSqlFromRows(table, rows, &insert_sql)) {
                LOG(WARNING) << "Fail to build insert sql from rows";
                return false;
            }
            sql_list->push_back(insert_sql);
        }
    }
    return true;
}
bool SqlCase::BuildInsertSqlFromInput(int32_t input_idx,
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
        if (!BuildInsertSqlFromData(table, inputs_[input_idx].data_, sql)) {
            LOG(WARNING) << "Fail to build create sql string";
            return false;
        }
    } else if (!inputs_[input_idx].rows_.empty()) {
        if (!BuildInsertSqlFromRows(table, inputs_[input_idx].rows_, sql)) {
            LOG(WARNING) << "Fail to build create sql string";
            return false;
        }
    }
    return true;
}
bool SqlCase::ExtractOutputSchema(type::TableDef& table) const {
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
const vm::Schema SqlCase::ExtractParameterTypes() const {
    vm::Schema emtpy_schema;
    type::TableDef parameter_schema;
    if (!ExtractInputTableDef(parameters(), parameter_schema)) {
        return emtpy_schema;
    }
    return parameter_schema.columns();
}
std::ostream& operator<<(std::ostream& output, const SqlCase& thiz) {
    output << "Case ID: " << thiz.id() << ", Desc:" << thiz.desc();
    return output;
}
bool SqlCase::CreateStringListFromYamlNode(const YAML::Node& node,
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
bool SqlCase::CreateRowsFromYamlNode(
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
bool SqlCase::CreateTableInfoFromYamlNode(const YAML::Node& schema_data,
                                          SqlCase::TableInfo* table) {
    if (schema_data["db"]) {
        table->db_ = schema_data["db"].as<std::string>();
        boost::trim(table->db_);
    }
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
        if (schema_data["data"].IsMap()) {
            // csv format only
            table->csv_data_file_ = absl::StripAsciiWhitespace(schema_data["data"]["file"].as<std::string>());
            if (table->csv_data_file_.empty()) {
                LOG(ERROR) << "table csv data file name is empty";
                return false;
            }
            std::fstream f;
            if (table->csv_data_file_.front() == '/') {
                f.open(table->csv_data_file_, std::ios::in);
            } else {
                f.open(working_dir / table->csv_data_file_, std::ios::in);
            }

            if (f.is_open()) {
                absl::Cleanup clean = [&f]() {
                    f.close();
                };
                std::stringstream ss;
                ss << f.rdbuf();
                table->data_ = ss.str();
                boost::trim(table->data_);
            } else {
                LOG(ERROR) << "file " << table->csv_data_file_ << " not open";
                return false;
            }
        } else if (schema_data["data"].IsScalar()) {
            table->data_ = schema_data["data"].as<std::string>();
            boost::trim(table->data_);
        } else {
            LOG(ERROR) << "cases[*].inputs[*].data is not a acceptable type: " << schema_data["data"];
            return false;
        }
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

    if (schema_data["inserts"]) {
        auto data = schema_data["inserts"];
        if (!CreateStringListFromYamlNode(data, table->inserts_)) {
            return false;
        }
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
bool SqlCase::CreateExpectFromYamlNode(const YAML::Node& schema_data,
                                       const YAML::Node& expect_provider,
                                       SqlCase::ExpectInfo* expect) {
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

    if (schema_data["node_tree_str"]) {
        auto tree = schema_data["node_tree_str"].as<std::string>();
        boost::trim(tree);
        if (!tree.empty()) {
            expect->node_tree_str_ = tree;
        }
    }

    if (schema_data["plan_tree_str"]) {
        const auto& tree = schema_data["plan_tree_str"].as<std::string>();
        absl::string_view sv = absl::StripAsciiWhitespace(tree);
        if (!sv.empty()) {
            expect->plan_tree_str_ = sv;
        }
    }

    YAML::Node rows_node;
    if (expect_provider["rows"]) {
        rows_node = expect_provider["rows"];
    } else if (schema_data["result"]) {
        rows_node = schema_data["result"];
    } else if (schema_data["rows"]) {
        rows_node = schema_data["rows"];
    }
    if (rows_node) {
        expect->rows_.clear();
        if (!CreateRowsFromYamlNode(rows_node, expect->rows_)) {
            LOG(WARNING) << "Fail to parse rows";
            return false;
        }
    }

    YAML::Node columns_node;
    if (expect_provider["columns"]) {
        columns_node = expect_provider["columns"];
    } else if (schema_data["columns"]) {
        columns_node = schema_data["columns"];
    }
    if (columns_node) {
        expect->columns_.clear();
        if (!CreateStringListFromYamlNode(columns_node, expect->columns_)) {
            LOG(WARNING) << "Fail to parse columns";
            return false;
        }
    }
    if (schema_data["success"]) {
        expect->success_ = schema_data["success"].as<bool>();
    } else {
        expect->success_ = true;
    }
    if (schema_data["code"]) {
        expect->code_ = schema_data["code"].as<int>();
    } else {
        expect->code_ = -1;
    }
    if (schema_data["msg"]) {
        expect->msg_ = schema_data["msg"].as<std::string>();
    } else {
        expect->msg_ = "";
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
bool SqlCase::CreateSqlCasesFromYaml(const std::string& cases_dir,
                                     const std::string& yaml_path,
                                     std::vector<SqlCase>& sql_case_ptr,
                                     const std::string filter_mode) {
    std::vector<std::string> filter_modes;
    if (filter_mode.empty()) {
        return CreateSqlCasesFromYaml(cases_dir, yaml_path, sql_case_ptr,
                                      filter_modes);
    } else {
        filter_modes.push_back(filter_mode);
        return CreateSqlCasesFromYaml(cases_dir, yaml_path, sql_case_ptr,
                                      filter_modes);
    }
}

bool SqlCase::CreateTableInfoFromYaml(const std::string& cases_dir,
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

bool SqlCase::LoadSchemaAndRowsFromYaml(
    const std::string& cases_dir, const std::string& resource_path,
    type::TableDef& table, std::vector<hybridse::codec::Row>& rows) {
    TableInfo table_info;
    if (!CreateTableInfoFromYaml(cases_dir, resource_path, &table_info)) {
        return false;
    }
    if (!SqlCase::ExtractTableDef(table_info.schema_, table_info.index_,
                                  table)) {
        return false;
    }
    table.set_name(table_info.name_);
    if (!SqlCase::ExtractRows(table.columns(), table_info.data_, rows)) {
        return false;
    }
    return true;
}

static bool ParseSqlCaseNode(const YAML::Node& sql_case_node,
                             const YAML::Node& expect_provider,
                             const std::string& global_db,
                             const std::string& cases_dir,
                             const std::set<std::string>& debugs,
                             const std::vector<std::string>& filter_modes,
                             SqlCase* sql_case_ptr, bool* is_skip) {
    *is_skip = false;
    SqlCase& sql_case = *sql_case_ptr;
    sql_case.raw_node_ = YAML::Node(sql_case_node);
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

    if (sql_case_node["mode"]) {
        sql_case.mode_ = sql_case_node["mode"].as<std::string>();
        boost::trim(sql_case.mode_);
    } else {
        sql_case.mode_ = "batch";
    }
    if (sql_case_node["level"]) {
        sql_case.level_ = sql_case_node["level"].as<int>();
    }
    if (sql_case_node["debug"]) {
        sql_case.debug_ = sql_case_node["debug"].as<bool>();
    }
    if (sql_case_node["deployable"]) {
        sql_case.deployable_ = sql_case_node["deployable"].as<bool>();
    }
    if (sql_case_node["deployment"]) {
        auto& dep = sql_case_node["deployment"];
        if (dep["name"]) {
            sql_case.deployment_.name_ = dep["name"].as<std::string>();
        }
    }
    if (sql_case_node["tags"]) {
        if (!SqlCase::CreateStringListFromYamlNode(sql_case_node["tags"],
                                                   sql_case.tags_)) {
            LOG(WARNING) << "Fail to parse tags";
            return false;
        }
        std::set<std::string> tags(sql_case.tags_.begin(),
                                   sql_case.tags_.end());

        if (tags.find("todo") != tags.cend()) {
            LOG(INFO) << "SKIP TODO SQL Case " << sql_case.desc();
            *is_skip = true;
        }

        if (tags.find("TODO") != tags.cend()) {
            if (SqlCase::IsDebug()) {
                DLOG(INFO) << "SKIP TODO SQL Case " << sql_case.desc();
            }
            *is_skip = true;
        }
    }

    if (sql_case_node["batch_plan"]) {
        sql_case.batch_plan_ = sql_case_node["batch_plan"].as<std::string>();
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
        auto& sql_node = sql_case_node["sql"];
        if (sql_node.IsScalar()) {
            sql_case.sql_str_ = sql_case_node["sql"].as<std::string>();
            boost::trim(sql_case.sql_str_);
        } else if (sql_node.IsMap()) {
            if (sql_node["file"].IsScalar()) {
                std::string file_path = sql_node["file"].as<std::string>();
                if (file_path.empty()) {
                    LOG(ERROR) << "file path to sql is empty";
                    return false;
                }
                if (file_path.front() != '/') {
                    file_path = working_dir / file_path;
                }
                std::ifstream ifs(file_path);
                sql_case.sql_str_.assign((std::istreambuf_iterator<char>(ifs)), std::istreambuf_iterator<char>());
            } else {
                return false;
            }
        } else {
            return false;
        }
    }
    if (sql_case_node["sqls"]) {
        sql_case.sql_strs_.clear();
        if (!SqlCase::CreateStringListFromYamlNode(sql_case_node["sqls"],
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
            SqlCase::TableInfo table;
            auto schema_data = *iter;

            if (schema_data["resource"]) {
                std::string resource =
                    schema_data["resource"].as<std::string>();
                boost::trim(resource);
                if (!SqlCase::CreateTableInfoFromYaml(cases_dir, resource,
                                                      &table)) {
                    return false;
                }
            }
            if (!SqlCase::CreateTableInfoFromYamlNode(schema_data, &table)) {
                return false;
            }
            sql_case.inputs_.push_back(table);
        }
    }

    YAML::Node expect_node;
    if (sql_case_node["output"]) {
        expect_node = sql_case_node["output"];
    } else if (sql_case_node["expect"]) {
        expect_node = sql_case_node["expect"];
    }

    if (expect_node) {
        if (!SqlCase::CreateExpectFromYamlNode(expect_node, expect_provider,
                                               &sql_case.expect_)) {
            return false;
        }
    } else if (expect_provider) {
        if (!SqlCase::CreateExpectFromYamlNode(expect_provider, YAML::Node(),
                                               &sql_case.expect_)) {
            return false;
        }
    }
    // parse expect info for "output","expect", "expectProvider"
    if (expect_node.IsDefined() || expect_provider.IsDefined()) {
        if (!SqlCase::CreateExpectFromYamlNode(expect_node, expect_provider,
                                               &sql_case.expect_)) {
            return false;
        }
    }
    if (sql_case_node["batch_request"]) {
        if (!SqlCase::CreateTableInfoFromYamlNode(
                sql_case_node["batch_request"], &sql_case.batch_request_)) {
            return false;
        }
    }

    if (sql_case_node["parameters"]) {
        if (!SqlCase::CreateTableInfoFromYamlNode(
            sql_case_node["parameters"], &sql_case.parameters_)) {
            return false;
        }
    }

    if (!debugs.empty()) {
        if (debugs.find(sql_case.desc_) == debugs.end()) {
            *is_skip = true;
        }
        sql_case.debug_ = true;
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
            *is_skip = true;
        }
    }
    return true;
}

/// \brief DoExpandProviderCase()
///
/// \param[in] idx current        processing level of provider_contents
/// \param[in] sql_case_node      original sql case node
/// \param[in] provider_contents  extracted dataProvider node from sql_case_node
/// \param[in] expect_element     extracted (n-idx) level expectProvider node from sql_case_node
/// \param[in] global_db          global db
/// \param[in] cases_dir          case directory
/// \param[in] debugs             debug
/// \param[in] filter_modes       case filter modes
/// \param[in] choice_idxs        helper state vector store current choices in provider_contents
/// \param[out] outputs           extracted sql cases
///
/// Returns bool: ___
static bool DoExpandProviderCase(const size_t idx, const YAML::Node& sql_case_node,
                                 const std::vector<std::vector<std::string>>& provider_contents,
                                 const YAML::Node& expect_element, const std::string& global_db,
                                 const std::string& cases_dir, const std::set<std::string>& debugs,
                                 const std::vector<std::string>& filter_modes, std::vector<size_t>* choice_idxs,
                                 std::vector<SqlCase>* outputs) {
    if (idx == provider_contents.size()) {
        // reach the end of one choice chain
        SqlCase sql_case;
        bool is_skip = false;
        bool parse_success = ParseSqlCaseNode(sql_case_node, expect_element, global_db, cases_dir, debugs, filter_modes,
                                              &sql_case, &is_skip);
        if (!parse_success) {
            LOG(WARNING) << "Parse sql node failed";
            return false;
        }
        std::string sql = sql_case.sql_str_;
        for (size_t i = 0; i < choice_idxs->size(); ++i) {
            std::string repl = provider_contents[i][choice_idxs->at(i)];
            std::string ph = "d\\[";
            ph.append(std::to_string(i));
            ph.append("\\]");
            sql = boost::regex_replace(sql, boost::regex(ph), repl);
        }
        sql_case.sql_str_ = sql;
        if (!is_skip) {
            outputs->push_back(sql_case);
        }
        return true;
    }

    auto current_choices_num = provider_contents.at(idx).size();
    for (size_t i = 0; i < current_choices_num; ++i) {
        choice_idxs->at(idx) = i;

        YAML::Node child_expect_element;

        size_t expect_idx = choice_idxs->at(idx);
        // support two schema to find expect node
        //  - expect_element["n"]: yaml object in expect_element whose key is string(choice_idxs[idx]),
        //    expect_element is a map
        //  - expect_element[num]: choice_idxs[idx] th element of expect_element, expect_element is a sequence
        if (expect_element.IsMap()) {
            if (expect_element[std::to_string(expect_idx)]) {
                child_expect_element = expect_element[std::to_string(expect_idx)];
            } else {
                //  when expect_element is map, pass parent expect to next call, so make it possible to use single
                //  expect result for all related test cases
                //  e.g. the rows:-["r1"] become the expect result for both "1 & 'a'" and "1 & 'b'"
                //   dataProvider:
                //     - [1, 2]
                //     - ['a', 'b']
                //   expectProvider:
                //     0:
                //       rows:
                //         - ['r1']
                //     1:
                //       0:
                //         rows:
                //           - ['r3']
                //       1:
                //         rows:
                //           - ['r4']
                child_expect_element = expect_element;
            }
        } else if (expect_element.IsSequence() && expect_element.size() > expect_idx && expect_element[expect_idx]) {
            child_expect_element = expect_element[expect_idx];
        }

        if (!DoExpandProviderCase(idx + 1, sql_case_node, provider_contents, child_expect_element, global_db, cases_dir,
                                  debugs, filter_modes, choice_idxs, outputs)) {
            return false;
        }
    }
    return true;
}

/// expand sql case from 'dataProvider' section
/// a sql case is generated if
/// - successful sql_str replace occur by one permutation from dataProvider
/// - optionally, a corresponding expect result found from 'expectProvider'
///
/// by convention, a n x m dataProvider's (i, j, k, ...) permutation, it's expect
///  result can be found at (i, j, k, ...) th element at expectProvider
static bool ExpandProviderCase(const YAML::Node& sql_case_node, const std::string& global_db,
                               const std::string& cases_dir, const std::set<std::string>& debugs,
                               const std::vector<std::string>& filter_modes, std::vector<SqlCase>* outputs) {
    YAML::Node providers = sql_case_node["dataProvider"];
    // create data provider as two dimension matrix
    std::vector<std::vector<std::string>> provider_contents;
    for (auto provider_iter = providers.begin(); provider_iter != providers.end(); provider_iter++) {
        YAML::Node cur = YAML::Node(*provider_iter);
        std::vector<std::string> choices;
        for (auto choice_iter = cur.begin(); choice_iter != cur.end(); ++choice_iter) {
            choices.push_back(choice_iter->as<std::string>());
        }
        provider_contents.push_back(choices);
    }
    if (provider_contents.empty()) {
        LOG(WARNING) << "Empty provider";
        return false;
    }

    // choice_idxs is a state vector remember choices before idx
    std::vector<size_t> choice_idxs(provider_contents.size());
    if (sql_case_node["expectProvider"]) {
        return DoExpandProviderCase(0, sql_case_node, provider_contents, sql_case_node["expectProvider"], global_db,
                                    cases_dir, debugs, filter_modes, &choice_idxs, outputs);
    } else {
        return DoExpandProviderCase(0, sql_case_node, provider_contents, YAML::Node(), global_db, cases_dir, debugs,
                                    filter_modes, &choice_idxs, outputs);
    }
}

bool SqlCase::CreateSqlCasesFromYaml(
    const std::string& cases_dir, const std::string& yaml_path,
    std::vector<SqlCase>& sql_case_ptr,
    const std::vector<std::string>& filter_modes) {
    std::string sql_case_path = yaml_path;
    // 1. yaml_path starts from '/', respect it as absolute path
    // 2. otherwise, prepend with cases_dir
    if (yaml_path.front() != '/' && cases_dir != "") {
        sql_case_path = cases_dir + "/" + yaml_path;
    }
    std::filesystem::path p(sql_case_path);
    absl::MutexLock lock(&mtx);
    working_dir = p.parent_path();

    if (IsDebug()) {
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
    if (config["cases"]) {
        sql_cases_node = config["cases"];
    } else {
        LOG(WARNING) << "Fail to parse sql cases: " << yaml_path;
        return false;
    }
    for (auto case_iter = sql_cases_node.begin();
         case_iter != sql_cases_node.end(); case_iter++) {
        YAML::Node sql_case_node = YAML::Node(*case_iter);
        if (sql_case_node["dataProvider"]) {
            bool expand_ok =
                ExpandProviderCase(sql_case_node, global_db, cases_dir, debugs,
                                   filter_modes, &sql_case_ptr);
            if (!expand_ok) {
                LOG(WARNING) << "Expand provider case failed in: " << yaml_path;
                return false;
            }
            continue;
        }
        SqlCase sql_case;
        bool is_skip = false;
        bool parse_success =
            ParseSqlCaseNode(sql_case_node, YAML::Node(), global_db, cases_dir,
                             debugs, filter_modes, &sql_case, &is_skip);
        if (!parse_success) {
            LOG(WARNING) << "Parse sql case failed in: " << yaml_path;
            return false;
        } else if (is_skip) {
            continue;
        }
        sql_case_ptr.push_back(sql_case);
    }
    return true;
}

hybridse::sqlcase::SqlCase SqlCase::LoadSqlCaseWithID(
    const std::string& dir_path, const std::string& yaml_path,
    const std::string& case_id) {
    std::vector<SqlCase> cases;
    LOG(INFO) << "BENCHMARK LOAD SQL CASE";
    SqlCase::CreateSqlCasesFromYaml(dir_path, yaml_path, cases);

    for (const auto& sql_case : cases) {
        if (sql_case.id() == case_id) {
            return sql_case;
        }
    }
    return SqlCase();
}
std::string FindSqlCaseBaseDirPath() {
    return SqlCase::SqlCaseBaseDir();
}
void InitCases(std::string yaml_path, std::vector<SqlCase>& cases) {  // NOLINT
    SqlCase::CreateSqlCasesFromYaml(hybridse::sqlcase::FindSqlCaseBaseDirPath(), yaml_path, cases);
}
std::vector<SqlCase> InitCases(std::string yaml_path) {
    std::vector<SqlCase> cases;
    InitCases(yaml_path, cases);
    return cases;
}
std::vector<SqlCase> InitCases(std::string yaml_path, std::vector<std::string> filters) {
    std::vector<SqlCase> cases;
    InitCases(yaml_path, cases, filters);
    return cases;
}

void InitCases(std::string yaml_path, std::vector<SqlCase>& cases,  // NOLINT
               const std::vector<std::string>& filters) {
    SqlCase::CreateSqlCasesFromYaml(hybridse::sqlcase::FindSqlCaseBaseDirPath(), yaml_path, cases, filters);
}
absl::StatusOr<std::string> SqlCase::BuildCreateSpSqlFromInput(int32_t input_idx,
                                        absl::string_view select_sql,
                                        const std::set<size_t>& common_idx) {
    type::TableDef table;
    if (!ExtractInputTableDef(table, input_idx)) {
        return absl::FailedPreconditionError("Fail to extract table schema");
    }

    return BuildCreateSpSqlFromSchema(table, select_sql, common_idx);
}

absl::StatusOr<std::string> SqlCase::BuildCreateSpSqlFromSchema(const type::TableDef& table,
                                                                absl::string_view select_sql,
                                                                const std::set<size_t>& common_idx) {
    auto sql_view = absl::StripAsciiWhitespace(select_sql);
    std::string query_stmt(sql_view);
    if (query_stmt.back() != ';') {
        absl::StrAppend(&query_stmt, ";");
    }

    std::string sql = absl::Substitute("CREATE PROCEDURE $0 (\n", sp_name_);
    for (int i = 0; i < table.columns_size(); i++) {
        auto column = table.columns(i);
        if (!common_idx.empty() && common_idx.count(i)) {
            absl::StrAppend(&sql, "const ");
        }
        absl::SubstituteAndAppend(&sql, "$0 $1", column.name(), TypeString(column.type()));
        if (i < table.columns_size() - 1) {
            absl::StrAppend(&sql, ",\n");
        }
    }
    absl::SubstituteAndAppend(&sql, ")\nBEGIN\n$0\nEND;", query_stmt);
    return sql;
}

std::set<std::string> SqlCase::HYBRIDSE_LEVEL() {
    const char* env_name = "HYBRIDSE_LEVEL";
    char* value = getenv(env_name);
    if (value != nullptr) {
        try {
            std::set<std::string> item_vec;
            boost::split(item_vec, value, boost::is_any_of(","),
                         boost::token_compress_on);
            return item_vec;
        } catch (const std::exception& ex) {
            LOG(WARNING) << "Fail to parser hybridse level: " << ex.what();
            return std::set<std::string>({"0"});
        }
    } else {
        return std::set<std::string>({"0"});
    }
}

std::string SqlCase::SqlCaseBaseDir() {
    char* value = getenv("SQL_CASE_BASE_DIR");
    if (value != nullptr) {
        return std::string(value);
    }
    value = getenv("YAML_CASE_BASE_DIR");
    if (value != nullptr) {
        return std::string(value);
    }
    return "";
}
}  // namespace sqlcase
}  // namespace hybridse
