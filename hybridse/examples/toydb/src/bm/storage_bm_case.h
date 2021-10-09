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

#ifndef HYBRIDSE_EXAMPLES_TOYDB_SRC_BM_STORAGE_BM_CASE_H_
#define HYBRIDSE_EXAMPLES_TOYDB_SRC_BM_STORAGE_BM_CASE_H_
#include <string>
#include "benchmark/benchmark.h"
#include "testing/toydb_engine_test_base.h"
#include "vm/mem_catalog.h"
namespace hybridse {
namespace bm {
enum MODE { BENCHMARK, TEST };
void MemTableIterate(benchmark::State* state, MODE mode, int64_t data_size);
void RequestUnionTableIterate(benchmark::State* state, MODE mode,
                              int64_t data_size);
void MemSegmentIterate(benchmark::State* state, MODE mode, int64_t data_size);
void TabletFullIterate(benchmark::State* state, MODE mode, int64_t data_size);
void TabletWindowIterate(benchmark::State* state, MODE mode, int64_t data_size);
void ArrayListIterate(benchmark::State* state, MODE mode, int64_t data_size);
}  // namespace bm
}  // namespace hybridse
#endif  // HYBRIDSE_EXAMPLES_TOYDB_SRC_BM_STORAGE_BM_CASE_H_
