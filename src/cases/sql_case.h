/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * case.h
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CASES_SQL_CASE_H_
#define SRC_CASES_SQL_CASE_H_
#include <parser/parser.h>
#include <vm/catalog.h>
#include <string>
#include "proto/type.pb.h"
namespace fesql {
namespace cases {
class SQLCaseBuilder;

class SQLCase {
 public:
    bool TypeParse(const std::string& type_str, fesql::type::Type *type);
    // extract schema from schema string
    // name:type|name:type|name:type|
    bool ExtractSchema(type::TableDef& table);  // NOLINT
    std::string sql_str_;
    std::string table_name_;
    std::string schema_str_;
    std::string input_str_;
    std::string expect_str_;
    friend SQLCaseBuilder;

 private:
};

}  // namespace cases
}  // namespace fesql
#endif  // SRC_CASES_SQL_CASE_H_
