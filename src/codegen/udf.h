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
namespace fesql {
namespace udf {

template <T>
T sum(::fesql::udf::Iterator<T> &iter, T default_value) {
    T result = 0.0;
    while (iter.Valid()) {
        result += iter.Next();
    }
    return result;
}
}  // namespace udf
}  // namespace fesql
#endif  // SRC_CODEGEN_UDF_H
