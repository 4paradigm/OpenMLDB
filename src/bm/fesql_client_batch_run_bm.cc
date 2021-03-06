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
#include "bm/fesql_client_bm_case.h"
#include "glog/logging.h"
#include "llvm/Support/TargetSelect.h"

using namespace ::llvm;  // NOLINT
namespace fesql {
namespace bm {

static void BM_SIMPLE_QUERY(benchmark::State &state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    SIMPLE_CASE1_QUERY(&state, BENCHMARK, true, state.range(0), state.range(1));
}

static void BM_WINDOW_CASE0_QUERY(benchmark::State &state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::fesql::bm::WINDOW_CASE0_QUERY(&state, BENCHMARK, true, state.range(0),
                                    state.range(1));
}
static void BM_WINDOW_CASE1_QUERY(benchmark::State &state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::fesql::bm::WINDOW_CASE1_QUERY(&state, BENCHMARK, true, state.range(0),
                                    state.range(1));
}

static void BM_WINDOW_CASE2_QUERY(benchmark::State &state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::fesql::bm::WINDOW_CASE2_QUERY(&state, BENCHMARK, true, state.range(0),
                                    state.range(1));
}
static void BM_WINDOW_CASE3_QUERY(benchmark::State &state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::fesql::bm::WINDOW_CASE3_QUERY(&state, BENCHMARK, true, state.range(0),
                                    state.range(1));
}

static void BM_GROUPBY_CASE0_QUERY(benchmark::State &state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::fesql::bm::GROUPBY_CASE0_QUERY(&state, BENCHMARK, true, state.range(0),
                                     state.range(1));
}

BENCHMARK(BM_GROUPBY_CASE0_QUERY)
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({10, 10})
    ->Args({10, 100})
    ->Args({10, 1000});

BENCHMARK(BM_SIMPLE_QUERY)
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000});
BENCHMARK(BM_WINDOW_CASE0_QUERY)
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({10, 10})
    ->Args({10, 100})
    ->Args({10, 1000});
BENCHMARK(BM_WINDOW_CASE1_QUERY)
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({10, 10})
    ->Args({10, 100})
    ->Args({10, 1000});
BENCHMARK(BM_WINDOW_CASE2_QUERY)
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({10, 10})
    ->Args({10, 100})
    ->Args({10, 1000});
BENCHMARK(BM_WINDOW_CASE3_QUERY)
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({10, 10})
    ->Args({10, 100})
    ->Args({10, 1000});

}  // namespace bm
};  // namespace fesql

BENCHMARK_MAIN();
