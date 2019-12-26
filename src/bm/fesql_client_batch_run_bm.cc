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
    int64_t record_size = state.range(0);
    SIMPLE_CASE1_QUERY(&state, BENCHMARK, true, record_size);
}

static void BM_WINDOW_CASE1_QUERY(benchmark::State &state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    int64_t record_size = state.range(0);
    ::fesql::bm::WINDOW_CASE1_QUERY(&state, BENCHMARK, true, record_size);
}

static void BM_WINDOW_CASE2_QUERY(benchmark::State &state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    int64_t record_size = state.range(0);
    ::fesql::bm::WINDOW_CASE2_QUERY(&state, BENCHMARK, true, record_size);
}
BENCHMARK(BM_SIMPLE_QUERY)->Arg(10)->Arg(100)->Arg(1000)->Arg(10000);
BENCHMARK(BM_WINDOW_CASE1_QUERY)
    ->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000);
BENCHMARK(BM_WINDOW_CASE2_QUERY)
->Arg(10)
    ->Arg(100)
    ->Arg(1000)
    ->Arg(10000);
}  // namespace bm
};  // namespace fesql

BENCHMARK_MAIN();
