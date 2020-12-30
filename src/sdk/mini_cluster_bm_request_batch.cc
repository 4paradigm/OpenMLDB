/*
 * sql_sdk_test.cc
 * Copyright (C) 4paradigm.com 2020 chenjing <chenjing@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
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
::rtidb::sdk::MiniCluster* mc;
#define DEFINE_BATCH_REQUEST_CASE(NAME, PATH, CASE_ID)                     \
    static void BM_BatchRequest_##NAME(benchmark::State& state) {          \
        MiniBenchmarkOnCase(PATH, CASE_ID, kBatchRequestMode, mc, &state); \
    }                                                                      \
    BENCHMARK(BM_BatchRequest_##NAME)->Args({0})->Args({1});

const char* DEFAULT_YAML_PATH = "/cases/benchmark/batch_request_benchmark.yaml";

DEFINE_BATCH_REQUEST_CASE(TwoWindow, DEFAULT_YAML_PATH, "0");
DEFINE_BATCH_REQUEST_CASE(SmallCommonWindow, DEFAULT_YAML_PATH, "1");
DEFINE_BATCH_REQUEST_CASE(LargeCommonWindow, DEFAULT_YAML_PATH, "2");

int main(int argc, char** argv) {
    ::fesql::vm::Engine::InitializeGlobalLLVM();
    FLAGS_enable_distsql = fesql::sqlcase::SQLCase::IS_CLUSTER();
    FLAGS_enable_localtablet = !fesql::sqlcase::SQLCase::IS_DISABLE_LOCALTABLET();
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
    ::rtidb::sdk::MiniCluster mini_cluster(6181);
    mc = &mini_cluster;
    if (!fesql::sqlcase::SQLCase::IS_CLUSTER()) {
        mini_cluster.SetUp(1);
    } else {
        mini_cluster.SetUp();
    }
    sleep(2);
    ::benchmark::RunSpecifiedBenchmarks();
    mini_cluster.Close();
}
