/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * engine_bm_case.h
 *
 * Author: chenjing
 * Date: 2020/4/7
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_VM_ENGINE_BM_CASE_H_
#define SRC_VM_ENGINE_BM_CASE_H_
#include "benchmark/benchmark.h"
namespace fesql {
namespace vm {
enum MODE { BENCHMARK, TEST };
void EngineWindowSumFeature1(benchmark::State* state, MODE mode,
                             int64_t limit_cnt,
                             int64_t size);  // NOLINT

void EngineRunBatchWindowSumFeature1(benchmark::State* state, MODE mode,
                                     int64_t limit_cnt,
                                     int64_t size);  // NOLINT
void EngineRunBatchWindowSumFeature5(benchmark::State* state, MODE mode,
                                     int64_t limit_cnt,
                                     int64_t size);  // NOLINT

void EngineWindowSumFeature5(benchmark::State* state, MODE mode,
                             int64_t limit_cnt,
                             int64_t size);  // NOLINT

void EngineSimpleSelectDouble(benchmark::State* state, MODE mode);

void EngineSimpleSelectVarchar(benchmark::State* state, MODE mode);

void EngineSimpleSelectInt32(benchmark::State* state, MODE mode);

void EngineSimpleUDF(benchmark::State* state, MODE mode);

void EngineRequestSimpleSelectDouble(benchmark::State* state, MODE mode);

void EngineRequestSimpleSelectVarchar(benchmark::State* state, MODE mode);

void EngineRequestSimpleSelectInt32(benchmark::State* state, MODE mode);

void EngineRequestSimpleUDF(benchmark::State* state, MODE mode);
}  // namespace vm
}  // namespace fesql
#endif  // SRC_VM_ENGINE_BM_CASE_H_
