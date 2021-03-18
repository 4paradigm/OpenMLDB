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


#ifndef SRC_CODEGEN_CODEGEN_BASE_TEST_H_
#define SRC_CODEGEN_CODEGEN_BASE_TEST_H_

#include <cstdint>
#include <string>
#include <vector>
#include "codec/list_iterator_codec.h"

namespace hybridse {
namespace codegen {
using hybridse::codec::ArrayListV;
using hybridse::codec::Row;
using hybridse::sqlcase::SQLCase;

bool BuildWindowFromResource(const std::string& resource_path,
                             ::hybridse::type::TableDef& table_def,  // NOLINT
                             std::vector<Row>& rows,              // NOLINT
                             int8_t** buf) {
    if (!SQLCase::LoadSchemaAndRowsFromYaml(hybridse::sqlcase::FindFesqlDirPath(),
                                            resource_path, table_def, rows)) {
        return false;
    }
    ArrayListV<Row>* w = new ArrayListV<Row>(&rows);
    *buf = reinterpret_cast<int8_t*>(w);
    return true;
}
bool BuildWindow(::hybridse::type::TableDef& table_def,  // NOLINT
                 std::vector<Row>& rows,              // NOLINT
                 int8_t** buf) {
    if (!SQLCase::LoadSchemaAndRowsFromYaml(
            hybridse::sqlcase::FindFesqlDirPath(),
            "cases/resource/codegen_t1_rows.yaml", table_def, rows)) {
        return false;
    }
    ArrayListV<Row>* w = new ArrayListV<Row>(&rows);
    *buf = reinterpret_cast<int8_t*>(w);
    return true;
}
bool BuildWindow2(::hybridse::type::TableDef& table_def,  // NOLINT
                  std::vector<Row>& rows,              // NOLINT
                  int8_t** buf) {
    if (!SQLCase::LoadSchemaAndRowsFromYaml(
            hybridse::sqlcase::FindFesqlDirPath(),
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
            hybridse::sqlcase::FindFesqlDirPath(),
            "cases/resource/codegen_t1_one_row.yaml", table_def, rows)) {
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
            hybridse::sqlcase::FindFesqlDirPath(),
            "cases/resource/codegen_t2_one_row.yaml", table_def, rows)) {
        return false;
    }
    *buf = rows[0].buf();
    *size = rows[0].size();
    return true;
}

}  // namespace codegen
}  // namespace hybridse

#endif  // SRC_CODEGEN_CODEGEN_BASE_TEST_H_
