/*
 * Copyright 2021 4Paradigm
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
#include "llvm/Transforms/Scalar.h"

namespace fesql {
namespace bm {
using namespace ::llvm;  // NOLINT

static void BM_EngineRequestSimpleSelectVarchar(
    benchmark::State& state) {  // NOLINT
    EngineRequestSimpleSelectVarchar(&state, BENCHMARK);
}
static void BM_EngineRequestSimpleSelectDouble(
    benchmark::State& state) {  // NOLINT
    EngineRequestSimpleSelectDouble(&state, BENCHMARK);
}
static void BM_EngineRequestSimpleSelectInt32(
    benchmark::State& state) {  // NOLINT
    EngineRequestSimpleSelectInt32(&state, BENCHMARK);
}

static void BM_EngineRequestSimpleSelectTimestamp(
    benchmark::State& state) {  // NOLINT
    EngineRequestSimpleSelectTimestamp(&state, BENCHMARK);
}

static void BM_EngineRequestSimpleSelectDate(
    benchmark::State& state) {  // NOLINT
    EngineRequestSimpleSelectDate(&state, BENCHMARK);
}
static void BM_EngineRequestSimpleUDF(benchmark::State& state) {  // NOLINT
    EngineRequestSimpleUDF(&state, BENCHMARK);
}

static void BM_EngineWindowSumFeature1(benchmark::State& state) {  // NOLINT
    EngineWindowSumFeature1(&state, BENCHMARK, state.range(0), state.range(1));
}

static void BM_EngineWindowSumFeature5(benchmark::State& state) {  // NOLINT
    EngineWindowSumFeature5(&state, BENCHMARK, state.range(0), state.range(1));
}

static void BM_EngineWindowSumFeature1ExcludeCurrentTime(
    benchmark::State& state) {  // NOLINT
    EngineWindowSumFeature1ExcludeCurrentTime(&state, BENCHMARK, state.range(0),
                                              state.range(1));
}

static void BM_EngineWindowSumFeature5ExcludeCurrentTime(
    benchmark::State& state) {  // NOLINT
    EngineWindowSumFeature5ExcludeCurrentTime(&state, BENCHMARK, state.range(0),
                                              state.range(1));
}

static void BM_EngineWindowSumFeature5Window5(
    benchmark::State& state) {  // NOLINT
    EngineWindowSumFeature5Window5(&state, BENCHMARK, state.range(0),
                                   state.range(1));
}
static void BM_EngineWindowDistinctCntFeature(
    benchmark::State& state) {  // NOLINT
    EngineWindowDistinctCntFeature(&state, BENCHMARK, state.range(0),
                                   state.range(1));
}

static void BM_EngineWindowTop1RatioFeature(
    benchmark::State& state) {  // NOLINT
    EngineWindowTop1RatioFeature(&state, BENCHMARK, state.range(0),
                                 state.range(1));
}

static void BM_MapTop(benchmark::State& state) {  // NOLINT
    MapTop1(&state, BENCHMARK, state.range(0), state.range(1));
}

static void BM_EngineWindowMultiAggFeature5(
    benchmark::State& state) {  // NOLINT
    EngineWindowMultiAggFeature5(&state, BENCHMARK, state.range(0),
                                 state.range(1));
}
static void BM_EngineWindowMultiAggWindow25Feature25(
    benchmark::State& state) {  // NOLINT
    EngineWindowMultiAggWindow25Feature25(&state, BENCHMARK, state.range(0),
                                          state.range(1));
}
static void BM_EngineRunBatchWindowMultiAggWindow25Feature25(
    benchmark::State& state) {  // NOLINT
    EngineRunBatchWindowMultiAggWindow25Feature25(
        &state, BENCHMARK, state.range(0), state.range(1));
}

static void BM_EngineSimpleSelectVarchar(benchmark::State& state) {  // NOLINT
    EngineSimpleSelectVarchar(&state, BENCHMARK);
}
static void BM_EngineSimpleSelectDouble(benchmark::State& state) {  // NOLINT
    EngineSimpleSelectDouble(&state, BENCHMARK);
}
static void BM_EngineSimpleSelectInt32(benchmark::State& state) {  // NOLINT
    EngineSimpleSelectInt32(&state, BENCHMARK);
}
static void BM_EngineSimpleSelectTimestamp(benchmark::State& state) {  // NOLINT
    EngineSimpleSelectTimestamp(&state, BENCHMARK);
}
static void BM_EngineSimpleSelectDate(benchmark::State& state) {  // NOLINT
    EngineSimpleSelectDate(&state, BENCHMARK);
}
static void BM_EngineSimpleUDF(benchmark::State& state) {  // NOLINT
    EngineSimpleUDF(&state, BENCHMARK);
}

static void BM_EngineRunBatchWindowSumFeature1(
    benchmark::State& state) {  // NOLINT
    EngineRunBatchWindowSumFeature1(&state, BENCHMARK, state.range(0),
                                    state.range(1));
}
static void BM_EngineRunBatchWindowSumFeature5(
    benchmark::State& state) {  // NOLINT
    EngineRunBatchWindowSumFeature5(&state, BENCHMARK, state.range(0),
                                    state.range(1));
}
static void BM_EngineRunBatchWindowSumFeature1ExcludeCurrentTime(
    benchmark::State& state) {  // NOLINT
    EngineRunBatchWindowSumFeature1ExcludeCurrentTime(
        &state, BENCHMARK, state.range(0), state.range(1));
}
static void BM_EngineRunBatchWindowSumFeature5ExcludeCurrentTime(
    benchmark::State& state) {  // NOLINT
    EngineRunBatchWindowSumFeature5ExcludeCurrentTime(
        &state, BENCHMARK, state.range(0), state.range(1));
}
static void BM_EngineRunBatchWindowSumFeature5Window5(
    benchmark::State& state) {  // NOLINT
    EngineRunBatchWindowSumFeature5Window5(&state, BENCHMARK, state.range(0),
                                           state.range(1));
}

// request engine simple bm
BENCHMARK(BM_EngineRequestSimpleSelectVarchar);
BENCHMARK(BM_EngineRequestSimpleSelectDouble);
BENCHMARK(BM_EngineRequestSimpleSelectInt32);
BENCHMARK(BM_EngineRequestSimpleSelectTimestamp);
BENCHMARK(BM_EngineRequestSimpleSelectDate);
// TODO(xxx): udf script fix
// BENCHMARK(BM_EngineRequestSimpleUDF);

// batch engine simple bm
BENCHMARK(BM_EngineSimpleSelectVarchar);
BENCHMARK(BM_EngineSimpleSelectDouble);
BENCHMARK(BM_EngineSimpleSelectInt32);
BENCHMARK(BM_EngineSimpleSelectTimestamp);
BENCHMARK(BM_EngineSimpleSelectDate);
// TODO(xxx): udf script fix
// BENCHMARK(BM_EngineSimpleUDF);

// request engine window bm
BENCHMARK(BM_EngineWindowSumFeature1)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});
BENCHMARK(BM_EngineWindowSumFeature5)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});

BENCHMARK(BM_MapTop)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 2000})
    ->Args({1, 10000});

BENCHMARK(BM_EngineWindowDistinctCntFeature)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 2000})
    ->Args({1, 10000});

BENCHMARK(BM_EngineWindowTop1RatioFeature)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 2000})
    ->Args({1, 10000});
BENCHMARK(BM_EngineWindowSumFeature5Window5)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});
BENCHMARK(BM_EngineWindowMultiAggFeature5)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});
BENCHMARK(BM_EngineWindowMultiAggWindow25Feature25)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});
// batch engine window bm
BENCHMARK(BM_EngineRunBatchWindowSumFeature1)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});
BENCHMARK(BM_EngineRunBatchWindowSumFeature5)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});
BENCHMARK(BM_EngineRunBatchWindowSumFeature5Window5)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});
BENCHMARK(BM_EngineRunBatchWindowMultiAggWindow25Feature25)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});

// batch engine window bm exclude current time
BENCHMARK(BM_EngineRunBatchWindowSumFeature1ExcludeCurrentTime)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});
BENCHMARK(BM_EngineRunBatchWindowSumFeature5ExcludeCurrentTime)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});
// request engine window bm exclude current time
BENCHMARK(BM_EngineWindowSumFeature1ExcludeCurrentTime)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});
BENCHMARK(BM_EngineWindowSumFeature5ExcludeCurrentTime)
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});

}  // namespace bm
}  // namespace fesql

BENCHMARK_MAIN();
