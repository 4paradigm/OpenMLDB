/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf.h
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_UDF_H
#define SRC_CODEGEN_UDF_H
#include "base/window.cc"
namespace fesql {
namespace codegen {

using base::ColumnIteratorImpl;
using base::WindowIteratorImpl;


bool CreateColumnIterator(int8_t *input, uint32_t offset,
                          ::fesql::type::Type type, int8_t **output) {
    if (nullptr == input || nullptr == output) {
        return false;
    }
    WindowIteratorImpl *w = (WindowIteratorImpl *)input;
    switch (type) {
        case fesql::type::kInt32: {
            ColumnIteratorImpl<int> *impl =
                new ColumnIteratorImpl<int>(*w, offset);
            *output = (int8_t *)impl;
            break;
        }
        case fesql::type::kInt16: {
            ColumnIteratorImpl<int16_t> *impl =
                new ColumnIteratorImpl<int16_t>(*w, offset);
            *output = (int8_t *)impl;
            break;
        }
        case fesql::type::kInt64: {
            ColumnIteratorImpl<int64_t> *impl =
                new ColumnIteratorImpl<int64_t>(*w, offset);
            *output = (int8_t *)impl;
            break;
        }
        default: {
            return false;
        }
            // TODO(chenjing): handle more type
    }
    return true;
}


}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_UDF_H
