/*
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

#ifndef EXAMPLES_TOYDB_SRC_BM_FESQL_CLIENT_BM_CASE_H_
#define EXAMPLES_TOYDB_SRC_BM_FESQL_CLIENT_BM_CASE_H_
#include <stdio.h>
#include <stdlib.h>
#include "benchmark/benchmark.h"
#include "testing/toydb_engine_test_base.h"
namespace fesql {
namespace bm {
enum MODE { BENCHMARK, TEST };
void SIMPLE_CASE1_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t group_size,
                        int64_t max_window_size);
void WINDOW_CASE1_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t group_size,
                        int64_t max_window_size);
void WINDOW_CASE2_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t group_size,
                        int64_t max_window_size);
void WINDOW_CASE3_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t group_size,
                        int64_t max_window_size);
void WINDOW_CASE0_QUERY(benchmark::State *state_ptr, MODE mode,
                        bool is_batch_mode, int64_t group_size,
                        int64_t max_window_size);

void GROUPBY_CASE0_QUERY(benchmark::State *state_ptr, MODE mode,
                         bool is_batch_mode, int64_t group_size,
                         int64_t max_window_size);

}  // namespace bm
}  // namespace fesql
#endif  // EXAMPLES_TOYDB_SRC_BM_FESQL_CLIENT_BM_CASE_H_
