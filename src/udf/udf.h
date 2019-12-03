/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf.h
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_UDF_UDF_H_
#define SRC_UDF_UDF_H_
#include <stdint.h>
#include "proto/type.pb.h"
namespace fesql {
namespace udf {
#ifdef __cplusplus
extern "C" {
#endif
int32_t inc_int32(int32_t i);
int64_t sum_int64(int8_t *input);
int32_t sum_int32(int8_t *input);
// int16_t sum_int16(int8_t *input);
// float sum_float(int8_t *input);
// double sum_double(int8_t *input);
int8_t *col(int8_t *input, int32_t offset, int32_t type_id);

#ifdef __cplusplus
}
#endif

}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_UDF_H_
