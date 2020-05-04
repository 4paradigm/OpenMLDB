/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * runner_bm_case.h
 *
 * Author: chenjing
 * Date: 2020/4/9
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BM_RUNNER_BM_CASE_H_
#define SRC_BM_RUNNER_BM_CASE_H_
#include "benchmark/benchmark.h"
namespace fesql {
namespace bm {
enum MODE { BENCHMARK, TEST };
void WindowSumFeature1_Aggregation(benchmark::State* state, MODE mode,
                                   int64_t limit_cnt,
                                   int64_t size);  // NOLINT
void WindowSumFeature1_RequestUnion(benchmark::State* state, MODE mode,
                                    int64_t limit_cnt,
                                    int64_t size);  // NOLINT
}  // namespace bm
}  // namespace fesql
#endif  // SRC_BM_RUNNER_BM_CASE_H_
