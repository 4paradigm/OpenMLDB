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

#include "benchmark/benchmark.h"
#include "bm/engine_bm_case.h"

namespace hybridse {
namespace bm {
using namespace ::llvm;  // NOLINT

#define DEFINE_REQUEST_CASE(NAME, PATH, CASE_ID)                        \
    static void BM_Request_##NAME(benchmark::State& state) {            \
        EngineBenchmarkOnCase(PATH, CASE_ID, vm::kRequestMode, &state); \
    }                                                                   \
    BENCHMARK(BM_Request_##NAME);

#define DEFINE_REQUEST_WINDOW_CASE(NAME, PATH, CASE_ID)                   \
    static void BM_Request_##NAME(benchmark::State& state) {              \
        auto sql_case = hybridse::sqlcase::SqlCase::LoadSqlCaseWithID(    \
            hybridse::sqlcase::FindSqlCaseBaseDirPath(), PATH, CASE_ID);  \
        if (!hybridse::sqlcase::SqlCase::IsDebug()) {                     \
            sql_case.SqlCaseRepeatConfig("window_scale", state.range(0)); \
        }                                                                 \
        EngineBenchmarkOnCase(sql_case, vm::kRequestMode, &state);        \
    }                                                                     \
    BENCHMARK(BM_Request_##NAME)                                          \
        ->ArgNames({"window_scale"})                                      \
        ->Args({100})                                                     \
        ->Args({1000})                                                    \
        ->Args({2000});

const char* DEFAULT_YAML_PATH = "cases/benchmark/request_benchmark.yaml";
DEFINE_REQUEST_CASE(BM_SimpleLastJoin2Right, DEFAULT_YAML_PATH, "0");
DEFINE_REQUEST_CASE(BM_SimpleLastJoin4Right, DEFAULT_YAML_PATH, "1");
DEFINE_REQUEST_WINDOW_CASE(BM_SimpleWindowOutputLastJoinTable2,
                           DEFAULT_YAML_PATH, "2");
DEFINE_REQUEST_WINDOW_CASE(BM_SimpleWindowOutputLastJoinTable4,
                           DEFAULT_YAML_PATH, "3");
DEFINE_REQUEST_WINDOW_CASE(BM_LastJoin4WindowOutput, DEFAULT_YAML_PATH, "4");
DEFINE_REQUEST_WINDOW_CASE(BM_LastJoin8WindowOutput, DEFAULT_YAML_PATH, "5");

DEFINE_REQUEST_WINDOW_CASE(BM_MultipleUDAF,
                           "cases/benchmark/udaf_benchmark.yaml", "0");

}  // namespace bm
}  // namespace hybridse

BENCHMARK_MAIN();
