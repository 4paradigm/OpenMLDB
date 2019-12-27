/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * fesql_client_bm.cc
 *
 * Author: chenjing
 * Date: 2019/12/25
 *--------------------------------------------------------------------------
 **/

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
    SIMPLE_CASE1_QUERY(&state, BENCHMARK, false, state.range(0), state.range(1));
}

static void BM_WINDOW_CASE1_QUERY(benchmark::State &state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::fesql::bm::WINDOW_CASE1_QUERY(&state, BENCHMARK, false, state.range(0), state.range(1));
}

BENCHMARK(BM_SIMPLE_QUERY)->Arg(10)->Arg(100)->Arg(1000)->Arg(10000);
BENCHMARK(BM_WINDOW_CASE1_QUERY)
->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({10, 10})
    ->Args({10, 100})
    ->Args({10, 1000});


}  // namespace bm
};  // namespace fesql

BENCHMARK_MAIN();
