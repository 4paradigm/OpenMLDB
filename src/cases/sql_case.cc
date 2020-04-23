/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * sql_case.cc
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#include "cases/sql_case.h"
#include "boost/algorithm/string.hpp"
#include "boost/lexical_cast.hpp"
#include "codec/row_codec.h"
#include "glog/logging.h"
#include "string"
#include "vector"
namespace fesql {
namespace cases {
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
            LOG(WARNING) << "Invalid Schema Format";
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

bool SQLCase::ExtractData(std::vector<Row>& rows) {
    if (data_str.empty()) {
        LOG(WARNING) << "Empty Data String";
        return false;
    }
    type::TableDef table;
    if (!ExtractDataSchema(table)) {
        LOG(WARNING) << "Invalid Schema";
        return false;
    }
    return ExtractRows(table.columns(), data_str, rows);
}

bool SQLCase::ExtractExpResult(std::vector<Row>& rows) {
    if (expect_str_.empty()) {
        LOG(WARNING) << "Empty Data String";
        return false;
    }
    type::TableDef table;
    if (!ExtractExpSchema(table)) {
        LOG(WARNING) << "Invalid Schema";
        return false;
    }
    return ExtractRows(table.columns(), expect_str_, rows);
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
bool SQLCase::ExtractDataSchema(type::TableDef& table) {
    return ExtractSchema(data_schema_str_, table);
}
bool SQLCase::ExtractExpSchema(type::TableDef& table) {
    return ExtractSchema(expect_schema_str_, table);
}

}  // namespace cases
}  // namespace fesql
