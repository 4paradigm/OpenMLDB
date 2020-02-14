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

#include <storage/window.h>
#include <cstdint>
#include <vector>
namespace fesql {
namespace codegen {
void BuildBuf(int8_t** buf, uint32_t* size);
void BuildWindow(std::vector<fesql::storage::Row>& rows,  // NOLINT
                 int8_t** buf);
void BuildWindow2(std::vector<fesql::storage::Row>& rows,  // NOLINT
                 int8_t** buf);
}  // namespace codegen
}  // namespace fesql

#endif  // SRC_CODEGEN_CODEGEN_BASE_TEST_H_
