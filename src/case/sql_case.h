/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * case.h
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CASE_SQL_CASE_H_
#define SRC_CASE_SQL_CASE_H_
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
    struct TableData {
        std::string name_;
        std::string schema_;
        std::string data_;
    };
    SQLCase() {}
    virtual ~SQLCase() {}
    // extract schema from schema string
    // name:type|name:type|name:type|
    bool ExtractInputSchema(type::TableDef& table,
                            int32_t input_idx = 0);   // NOLINT
    bool ExtractOutputSchema(type::TableDef& table);  // NOLINT
    bool ExtractInputData(std::vector<fesql::codec::Row>& rows,
                          int32_t input_idx = 0);                  // NOLINT
    bool ExtractOutputData(std::vector<fesql::codec::Row>& rows);  // NOLINT

    bool AddInput(const std::string& name, const std::string& schema,
                  const std::string& data);
    static bool TypeParse(const std::string& row_str, fesql::type::Type* type);
    static bool ExtractSchema(const std::string& schema_str,
                              type::TableDef& table);  // NOLINT
    static bool ExtractRows(const vm::Schema& schema,
                            const std::string& data_str,
                            std::vector<fesql::codec::Row>& rows);  // NOLINT
    static bool ExtractRow(const vm::Schema& schema, const std::string& row_str,
                           int8_t** out_ptr, int32_t* out_size);
    static bool CreateSQLCaseFromYaml(const std::string& yaml_path,
                                      SQLCase* sql_case_ptr);
    static std::string FindFesqlDirPath();
    int32_t id_;
    std::string sql_str_;
    std::string table_name_;
    std::vector<TableData> inputs_;
    TableData output_;
    friend SQLCaseBuilder;

 private:
    type::TableDef table_;

 private:
};

}  // namespace cases
}  // namespace fesql
#endif  // SRC_CASE_SQL_CASE_H_
