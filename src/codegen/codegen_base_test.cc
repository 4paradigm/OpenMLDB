/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * codegen_base_test.cc
 *
 * Author: chenjing
 * Date: 2020/2/14
 *--------------------------------------------------------------------------
 **/

#include "codegen/codegen_base_test.h"
#include <proto/fe_type.pb.h>
#include "case/sql_case.h"
namespace fesql {
namespace codegen {

using fesql::codec::ArrayListV;
using fesql::codec::Row;
using fesql::sqlcase::SQLCase;

bool BuildWindowFromResource(const std::string& resource_path,
                             ::fesql::type::TableDef& table_def,  // NOLINT
                             std::vector<Row>& rows,              // NOLINT
                             int8_t** buf) {
    if (!SQLCase::LoadSchemaAndRowsFromYaml(fesql::sqlcase::FindFesqlDirPath(),
                                            resource_path, table_def, rows)) {
        return false;
    }
    ArrayListV<Row>* w = new ArrayListV<Row>(&rows);
    *buf = reinterpret_cast<int8_t*>(w);
    return true;
}
bool BuildWindow(::fesql::type::TableDef& table_def,  // NOLINT
                 std::vector<Row>& rows,              // NOLINT
                 int8_t** buf) {
    if (!SQLCase::LoadSchemaAndRowsFromYaml(
            fesql::sqlcase::FindFesqlDirPath(),
            "cases/resource/codegen_t1_rows.yaml", table_def, rows)) {
        return false;
    }
    ArrayListV<Row>* w = new ArrayListV<Row>(&rows);
    *buf = reinterpret_cast<int8_t*>(w);
    return true;
}
bool BuildWindow2(::fesql::type::TableDef& table_def,  // NOLINT
                  std::vector<Row>& rows,              // NOLINT
                  int8_t** buf) {
    if (!SQLCase::LoadSchemaAndRowsFromYaml(
            fesql::sqlcase::FindFesqlDirPath(),
            "cases/resource/codegen_t2_rows.yaml", table_def, rows)) {
        return false;
    }
    ArrayListV<Row>* w = new ArrayListV<Row>(&rows);
    *buf = reinterpret_cast<int8_t*>(w);
    return true;
}
bool BuildT1Buf(type::TableDef& table_def, int8_t** buf,  // NOLINT
                uint32_t* size) {
    std::vector<Row> rows;
    if (!SQLCase::LoadSchemaAndRowsFromYaml(
            fesql::sqlcase::FindFesqlDirPath(),
                "cases/resource/codegen_t1_one_row.yaml",
            table_def, rows)) {
        return false;
    }
    *buf = rows[0].buf();
    *size = rows[0].size();
    return true;
}
bool BuildT2Buf(type::TableDef& table_def, int8_t** buf,  // NOLINT
                uint32_t* size) {
    std::vector<Row> rows;

    if (!SQLCase::LoadSchemaAndRowsFromYaml(
            fesql::sqlcase::FindFesqlDirPath(),
                "cases/resource/codegen_t2_one_row.yaml",
            table_def, rows)) {
        return false;
    }
    *buf = rows[0].buf();
    *size = rows[0].size();
    return true;
}

}  // namespace codegen
}  // namespace fesql
