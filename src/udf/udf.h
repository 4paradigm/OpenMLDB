/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf.h
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_UDF_UDF_H
#define SRC_UDF_UDF_H
#include <stdint.h>
namespace fesql {
namespace udf {
#ifdef __cplusplus
extern "C"{
#endif
int32_t fesql_get5();
int32_t fesql_sum(int8_t* input);
}

#ifdef __cplusplus
}
#endif

}

#endif  // SRC_UDF_UDF_H
