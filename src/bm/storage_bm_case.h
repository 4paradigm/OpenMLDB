/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * storage_bm_case.h
 *
 * Author: chenjing
 * Date: 2020/4/8
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BM_STORAGE_BM_CASE_H_
#define SRC_BM_STORAGE_BM_CASE_H_
#include <string>
#include "benchmark/benchmark.h"
namespace fesql {
namespace bm {
enum MODE { BENCHMARK, TEST };
void ArrayListIterate(benchmark::State* state, MODE mode, int64_t data_size);
}  // namespace bm
}  // namespace fesql
#endif  // SRC_BM_STORAGE_BM_CASE_H_
