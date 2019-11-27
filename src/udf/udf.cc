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
using base::Row;
using base::WindowIteratorImpl;
template <class V>
int32_t current_time() {
    return 5;
}

template <class V>
inline V inc(V i) {
    return i + 1;
}
int32_t inc_int32(int32_t i) { return inc<int32_t>(i); }

template <class V>
V sum(int8_t *input) {
    ColumnIteratorImpl<V> *col = (ColumnIteratorImpl<V> *)(input);
    V result = 0;

    while (col->Valid()) {
        result += col->Next();
    }
    return result;
}

int32_t sum_int32(int8_t *input) { return sum<int32_t>(input); }

int64_t sum_int64(int8_t *input) { return sum<int64_t>(input); }

int8_t *col(int8_t *input, uint32_t offset, uint32_t type_id) {
    fesql::type::Type type = static_cast<fesql::type::Type>(type_id);
    if (nullptr == input) {
        return nullptr;
    }
    WindowIteratorImpl *w = (WindowIteratorImpl *)input;
    switch (type) {
        case fesql::type::kInt32: {
            ColumnIteratorImpl<int> *impl =
                new ColumnIteratorImpl<int>(*w, offset);
            return (int8_t *)impl;
        }
        case fesql::type::kInt16: {
            ColumnIteratorImpl<int16_t> *impl =
                new ColumnIteratorImpl<int16_t>(*w, offset);
            return (int8_t *)impl;
        }
        case fesql::type::kInt64: {
            ColumnIteratorImpl<int64_t> *impl =
                new ColumnIteratorImpl<int64_t>(*w, offset);
            return (int8_t *)impl;
        }
        default: {
            return nullptr;
        }
            // TODO(chenjing): handle more type
    }
    return nullptr;
}

}  // namespace udf
}  // namespace fesql
