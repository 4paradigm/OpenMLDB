/*
 * runner_bm_case.h
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
