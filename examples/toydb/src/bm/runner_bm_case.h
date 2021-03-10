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

#ifndef EXAMPLES_TOYDB_SRC_BM_RUNNER_BM_CASE_H_
#define EXAMPLES_TOYDB_SRC_BM_RUNNER_BM_CASE_H_
#include <string>
#include "benchmark/benchmark.h"
#include "testing/toydb_engine_test_base.h"
#include "vm/engine.h"
namespace fesql {
namespace bm {
using vm::Runner;
using vm::RunSession;
using vm::DataHandler;
using vm::TableHandler;
enum MODE { BENCHMARK, TEST };
static Runner* GetRunner(Runner* root, int id);
static bool RunnerRun(
    RunSession* session, Runner* runner,
    std::shared_ptr<TableHandler> table_handler, int64_t limit_cnt,
    std::vector<std::shared_ptr<DataHandler>>& result);  // NOLINT
static void RequestUnionRunnerCase(const std::string& sql, int runner_id,
                                   benchmark::State* state, MODE mode,
                                   int64_t limit_cnt, int64_t size);

void AggRunnerCase(const std::string sql, int runner_id,
                   benchmark::State* state, MODE mode, int64_t limit_cnt,
                   int64_t data_size);
void WindowSumFeature1_Aggregation(benchmark::State* state, MODE mode,
                                   int64_t limit_cnt,
                                   int64_t size);  // NOLINT
void WindowSumFeature1_RequestUnion(benchmark::State* state, MODE mode,
                                    int64_t limit_cnt,
                                    int64_t size);  // NOLINT
}  // namespace bm
}  // namespace fesql
#endif  // EXAMPLES_TOYDB_SRC_BM_RUNNER_BM_CASE_H_
