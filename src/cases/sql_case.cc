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
#include "glog/logging.h"
#include "string"
#include "vector"
namespace fesql {
namespace cases {

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
    } else if ("string" == type_str) {
        *type = type::kVarchar;
    } else {
        LOG(WARNING) << "Invalid Type";
        return false;
    }
    return true;
}
bool SQLCase::ExtractSchema(type::TableDef& table) {  // NOLINT
    if (schema_str_.empty()) {
        LOG(WARNING) << "Empty Schema String";
        return false;
    }
    std::vector<std::string> col_vec;
    boost::split(col_vec, schema_str_, boost::is_any_of(","),
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
}  // namespace cases
}  // namespace fesql
