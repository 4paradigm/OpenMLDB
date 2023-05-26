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

#include <gflags/gflags.h>

#include "benchmark/benchmark.h"
#include "sdk/mini_cluster.h"
#include "sdk/mini_cluster_bm.h"
DECLARE_bool(enable_distsql);
DECLARE_bool(enable_localtablet);
::openmldb::sdk::MiniCluster* mc;
#define DEFINE_BATCH_REQUEST_CASE(NAME, PATH, CASE_ID)                         \
    static void BM_BatchRequest_##NAME(benchmark::State& state) {              \
        auto sql_case = LoadSQLCaseWithID(PATH, CASE_ID);                      \
        sql_case.batch_request_optimized_ = state.range(0) == 1;               \
        if (!hybridse::sqlcase::SqlCase::IsDebug()) {                          \
            sql_case.SqlCaseRepeatConfig("window_scale", state.range(1));      \
        }                                                                      \
        if (!hybridse::sqlcase::SqlCase::IsDebug()) {                          \
            sql_case.SqlCaseRepeatConfig("batch_scale", state.range(2));       \
        }                                                                      \
                                                                               \
        MiniBenchmarkOnCase(sql_case, kBatchRequestMode, mc, &state);          \
    }                                                                          \
    BENCHMARK(BM_BatchRequest_##NAME)                                          \
        ->Unit(benchmark::kMicrosecond)                                        \
        ->ArgNames({"batch_request_optimized", "window_scale", "batch_scale"}) \
        ->Args({0, 10, 10})                                                    \
        ->Args({1, 10, 10})                                                    \
        ->Args({0, 1000, 10})                                                  \
        ->Args({1, 1000, 10})                                                  \
        ->Args({0, 10, 100})                                                   \
        ->Args({1, 10, 100})                                                   \
        ->Args({0, 1000, 100})                                                 \
        ->Args({1, 1000, 100});
const char* DEFAULT_YAML_PATH = "cases/benchmark/batch_request_benchmark.yaml";

DEFINE_BATCH_REQUEST_CASE(TwoWindow, DEFAULT_YAML_PATH, "0");
DEFINE_BATCH_REQUEST_CASE(CommonWindow, DEFAULT_YAML_PATH, "1");

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::base::SetupGlog(true);
    ::hybridse::vm::Engine::InitializeGlobalLLVM();
    FLAGS_enable_distsql = hybridse::sqlcase::SqlCase::IsCluster();
    FLAGS_enable_localtablet = !hybridse::sqlcase::SqlCase::IsDisableLocalTablet();
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::openmldb::sdk::MiniCluster mini_cluster(6181);
    mc = &mini_cluster;
    if (!hybridse::sqlcase::SqlCase::IsCluster()) {
        mini_cluster.SetUp(1);
    } else {
        mini_cluster.SetUp();
    }
    sleep(2);
    ::benchmark::RunSpecifiedBenchmarks();
    mini_cluster.Close();
}
