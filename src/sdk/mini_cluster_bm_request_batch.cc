//
// Created by Chenjing on 2020-12-28.
//

#include "benchmark/benchmark.h"
#include "sdk/mini_cluster_bm.h"
#define DEFINE_BATCH_REQUEST_CASE(NAME, PATH, CASE_ID)                     \
    static void BM_BatchRequest_##NAME(benchmark::State& state) {          \
        MiniBenchmarkOnCase(PATH, CASE_ID, kBatchRequestMode, &state); \
    }                                                                      \
    BENCHMARK(BM_BatchRequest_##NAME)->Args({0})->Args({1});

const char* DEFAULT_YAML_PATH = "/cases/benchmark/batch_request_benchmark.yaml";

DEFINE_BATCH_REQUEST_CASE(TwoWindow, DEFAULT_YAML_PATH, "0");
DEFINE_BATCH_REQUEST_CASE(SmallCommonWindow, DEFAULT_YAML_PATH, "1");
DEFINE_BATCH_REQUEST_CASE(LargeCommonWindow, DEFAULT_YAML_PATH, "2");


BENCHMARK_MAIN();