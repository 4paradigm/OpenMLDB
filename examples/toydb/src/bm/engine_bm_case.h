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

#ifndef EXAMPLES_TOYDB_SRC_BM_ENGINE_BM_CASE_H_
#define EXAMPLES_TOYDB_SRC_BM_ENGINE_BM_CASE_H_
#include <string>
#include "benchmark/benchmark.h"
#include "case/sql_case.h"
#include "testing/toydb_engine_test_base.h"

namespace hybridse {
namespace bm {
enum MODE { BENCHMARK, TEST };
void EngineRequestModeSimpleQueryBM(const std::string& db,
                                    const std::string& request_table,
                                    const std::string& sql, int32_t limit_cnt,
                                    const std::string& resource_path,
                                    benchmark::State* state, MODE mode);
void EngineBatchModeSimpleQueryBM(const std::string& db, const std::string& sql,
                                  const std::string& resource_path,
                                  benchmark::State* state, MODE mode);
void EngineWindowSumFeature1(benchmark::State* state, MODE mode,
                             int64_t limit_cnt,
                             int64_t size);  // NOLINT
void EngineWindowSumFeature1ExcludeCurrentTime(benchmark::State* state,
                                               MODE mode, int64_t limit_cnt,
                                               int64_t size);  // NOLINT
void EngineWindowRowsSumFeature1(benchmark::State* state, MODE mode,
                                 int64_t limit_cnt,
                                 int64_t size);  // NOLINT

void EngineRunBatchWindowSumFeature1ExcludeCurrentTime(benchmark::State* state,
                                                       MODE mode,
                                                       int64_t limit_cnt,
                                                       int64_t size);  // NOLINT
void EngineRunBatchWindowSumFeature1(benchmark::State* state, MODE mode,
                                     int64_t limit_cnt,
                                     int64_t size);  // NOLINT
void EngineRunBatchWindowSumFeature5Window5(benchmark::State* state, MODE mode,
                                            int64_t limit_cnt,
                                            int64_t size);  // NOLINT
void EngineRunBatchWindowMultiAggWindow25Feature25(benchmark::State* state,
                                                   MODE mode, int64_t limit_cnt,
                                                   int64_t size);  // NOLINT
void EngineRunBatchWindowSumFeature5(benchmark::State* state, MODE mode,
                                     int64_t limit_cnt,
                                     int64_t size);  // NOLINT
void EngineRunBatchWindowSumFeature5ExcludeCurrentTime(benchmark::State* state,
                                                       MODE mode,
                                                       int64_t limit_cnt,
                                                       int64_t size);  // NOLINT
void EngineWindowSumFeature5(benchmark::State* state, MODE mode,
                             int64_t limit_cnt,
                             int64_t size);  // NOLINT
void EngineWindowSumFeature5ExcludeCurrentTime(benchmark::State* state,
                                               MODE mode, int64_t limit_cnt,
                                               int64_t size);  // NOLINT
void EngineWindowSumFeature5Window5(benchmark::State* state, MODE mode,
                                    int64_t limit_cnt,
                                    int64_t size);  // NOLINT

void EngineWindowTop1RatioFeature(benchmark::State* state, MODE mode,
                                  int64_t limit_cnt, int64_t size);

void EngineWindowDistinctCntFeature(benchmark::State* state, MODE mode,
                                    int64_t limit_cnt, int64_t size);

void MapTop1(benchmark::State* state, MODE mode, int64_t limit_cnt,
             int64_t size);
void EngineWindowMultiAggFeature5(benchmark::State* state, MODE mode,
                                  int64_t limit_cnt,
                                  int64_t size);  // NOLINT
void EngineWindowMultiAggWindow25Feature25(benchmark::State* state, MODE mode,
                                           int64_t limit_cnt,
                                           int64_t size);  // NOLINT

void EngineSimpleSelectDouble(benchmark::State* state, MODE mode);

void EngineSimpleSelectVarchar(benchmark::State* state, MODE mode);
void EngineSimpleSelectDate(benchmark::State* state, MODE mode);
void EngineSimpleSelectTimestamp(benchmark::State* state, MODE mode);

void EngineSimpleSelectInt32(benchmark::State* state, MODE mode);

void EngineSimpleUDF(benchmark::State* state, MODE mode);

void EngineRequestSimpleSelectDouble(benchmark::State* state, MODE mode);

void EngineRequestSimpleSelectVarchar(benchmark::State* state, MODE mode);

void EngineRequestSimpleSelectInt32(benchmark::State* state, MODE mode);

void EngineRequestSimpleUDF(benchmark::State* state, MODE mode);
void EngineRequestSimpleSelectTimestamp(benchmark::State* state, MODE mode);
void EngineRequestSimpleSelectDate(benchmark::State* state, MODE mode);

hybridse::sqlcase::SQLCase LoadSQLCaseWithID(const std::string& yaml,
                                          const std::string& case_id);
void EngineBenchmarkOnCase(const std::string& yaml_path,
                           const std::string& case_id,
                           vm::EngineMode engine_mode, benchmark::State* state);
void EngineBenchmarkOnCase(hybridse::sqlcase::SQLCase& sql_case,  // NOLINT
                           vm::EngineMode engine_mode, benchmark::State* state);

}  // namespace bm
}  // namespace hybridse
#endif  // EXAMPLES_TOYDB_SRC_BM_ENGINE_BM_CASE_H_
