/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf.h"
#include <stdint.h>
#include "base/window.cc"
namespace fesql {
namespace udf {
using base::ColumnIteratorImpl;

int32_t fesql_get5() {
    return 5;
}
int32_t fesql_sum(int8_t *input) {
    ColumnIteratorImpl<int32_t> *col = (ColumnIteratorImpl<int32_t> *)(input);
    int32_t result = 0;

    while (col->Valid()) {
        result += col->Next();
    }
    return result;
}
}  // namespace udf
}  // namespace fesql
