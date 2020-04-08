/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_bm_case.h
 *
 * Author: chenjing
 * Date: 2020/4/8
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BM_UDF_BM_CASE_H_
#define SRC_BM_UDF_BM_CASE_H_
#include "benchmark/benchmark.h"
namespace fesql {
namespace bm {
enum MODE { BENCHMARK, TEST };
void SumCol1(benchmark::State* state, MODE mode,int64_t data_size);  // NOLINT
}
}  // namespace fesql
#endif  // SRC_BM_UDF_BM_CASE_H_
