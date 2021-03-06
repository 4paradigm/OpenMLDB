/*
 * Copyright (c) 2021 4Paradigm
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
#include "bm/runner_bm_case.h"
#include "llvm/Transforms/Scalar.h"

namespace fesql {
namespace bm {
using namespace ::llvm;  // NOLINT

static void BM_WindowSumFeature1_RequestUnion(
    benchmark::State& state) {  // NOLINT
    WindowSumFeature1_RequestUnion(&state, BENCHMARK, state.range(0),
                                   state.range(1));
}

static void BM_WindowSumFeature1_Aggregation(
    benchmark::State& state) {  // NOLINT
    WindowSumFeature1_Aggregation(&state, BENCHMARK, state.range(0),
                                  state.range(1));
}

// request engine window bm
BENCHMARK(BM_WindowSumFeature1_RequestUnion)
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000});
BENCHMARK(BM_WindowSumFeature1_Aggregation)
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000});
}  // namespace bm
}  // namespace fesql

BENCHMARK_MAIN();
