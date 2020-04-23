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
#include "codec/row_codec.h"
#include "proto/type.pb.h"
namespace fesql {
namespace cases {
class SQLCaseBuilder;

class SQLCase {
 public:
    SQLCase() {}
    virtual ~SQLCase() {}
    // extract schema from schema string
    // name:type|name:type|name:type|
    bool ExtractDataSchema(type::TableDef& table);                // NOLINT
    bool ExtractExpSchema(type::TableDef& table);                 // NOLINT
    bool ExtractData(std::vector<fesql::codec::Row>& rows);       // NOLINT
    bool ExtractExpResult(std::vector<fesql::codec::Row>& rows);  // NOLINT

    static bool TypeParse(const std::string& row_str, fesql::type::Type* type);
    static bool ExtractSchema(const std::string& schema_str,
                              type::TableDef& table);  // NOLINT
    static bool ExtractRows(const vm::Schema& schema,
                            const std::string& data_str,
                            std::vector<fesql::codec::Row>& rows);  // NOLINT
    static bool ExtractRow(const vm::Schema& schema, const std::string& row_str,
                           int8_t** out_ptr, int32_t* out_size);
    std::string sql_str_;
    std::string table_name_;
    std::string data_schema_str_;
    std::string data_str;
    std::string expect_schema_str_;
    std::string expect_str_;
    friend SQLCaseBuilder;

 private:
    type::TableDef table_;

 private:
};

}  // namespace cases
}  // namespace fesql
#endif  // SRC_CASES_SQL_CASE_H_
