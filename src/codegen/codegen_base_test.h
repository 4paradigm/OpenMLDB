/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * codegen_base_test.h
 *
 * Author: chenjing
 * Date: 2020/2/14
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_CODEGEN_BASE_TEST_H_
#define SRC_CODEGEN_CODEGEN_BASE_TEST_H_

#include <cstdint>
#include <string>
#include <vector>
#include "codec/list_iterator_codec.h"

namespace fesql {
namespace codegen {
using fesql::codec::Row;

bool BuildWindowFromResource(const std::string& resource_path,
                             ::fesql::type::TableDef& table_def,  // NOLINT
                             std::vector<Row>& rows,              // NOLINT
                             int8_t** buf);
bool BuildWindow(::fesql::type::TableDef& table_def,  // NOLINT
                 std::vector<Row>& rows,              // NOLINT
                 int8_t** buf);
bool BuildWindow2(::fesql::type::TableDef& table_def,  // NOLINT
                  std::vector<Row>& rows,              // NOLINT
                  int8_t** buf);
bool BuildT1Buf(type::TableDef& table_def, int8_t** buf,  // NOLINT
                uint32_t* size);                          // NOLINT
bool BuildT2Buf(type::TableDef& table_def, int8_t** buf,  // NOLINT
                uint32_t* size);                          // NOLINT
}  // namespace codegen
}  // namespace fesql

#endif  // SRC_CODEGEN_CODEGEN_BASE_TEST_H_
