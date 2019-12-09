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
#include "proto/type.pb.h"
#include "storage/type_ir_builder.h"
#include "storage/window.h"
namespace fesql {
namespace udf {
using fesql::storage::ColumnIteratorImpl;
using fesql::storage::ColumnStringIteratorImpl;
using fesql::storage::Row;
using fesql::storage::WindowIteratorImpl;
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
    V result = 0;
    if (nullptr == input) {
        return result;
    }
    ::fesql::storage::ListRef *list_ref =
        reinterpret_cast<::fesql::storage::ListRef *>(input);
    if (nullptr == list_ref->iterator) {
        return result;
    }
    ColumnIteratorImpl<V> *col = (ColumnIteratorImpl<V> *)(list_ref->iterator);
    while (col->Valid()) {
        result += col->Next();
    }
    return result;
}

int16_t sum_int16(int8_t *input) { return sum<int16_t>(input); }
int32_t sum_int32(int8_t *input) { return sum<int32_t>(input); }
int64_t sum_int64(int8_t *input) { return sum<int64_t>(input); }
float sum_float(int8_t *input) { return sum<float>(input); }
double sum_double(int8_t *input) { return sum<double>(input); }
}  // namespace udf
}  // namespace fesql
