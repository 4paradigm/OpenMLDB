/*
 * batch_request_bm.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#include "benchmark/benchmark.h"
#include "bm/engine_bm_case.h"

namespace fesql {
namespace bm {
using namespace ::llvm;  // NOLINT

#define DEFINE_BATCH_REQUEST_CASE(NAME, PATH, CASE_ID)                       \
    static void BM_BatchRequest_##NAME(benchmark::State& state) {            \
        EngineBenchmarkOnCase(PATH, CASE_ID, vm::kBatchRequestMode, &state); \
    }                                                                        \
    BENCHMARK(BM_BatchRequest_##NAME)->Args({0})->Args({1});

const char* DEFAULT_YAML_PATH = "/cases/benchmark/batch_request_benchmark.yaml";

DEFINE_BATCH_REQUEST_CASE(TwoWindow, DEFAULT_YAML_PATH, "0");
DEFINE_BATCH_REQUEST_CASE(SmallCommonWindow, DEFAULT_YAML_PATH, "1");
DEFINE_BATCH_REQUEST_CASE(LargeCommonWindow, DEFAULT_YAML_PATH, "2");

}  // namespace bm
}  // namespace fesql

BENCHMARK_MAIN();
