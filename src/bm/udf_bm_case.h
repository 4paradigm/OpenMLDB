/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_bm_case.h
 *
 * Author: chenjing
 * Date: 2020/4/8
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BM_UDF_BM_CASE_H_
#define SRC_BM_UDF_BM_CASE_H_
#include <string>
#include "benchmark/benchmark.h"
#include "vm/mem_catalog.h"
namespace fesql {
namespace bm {
enum MODE { BENCHMARK, TEST };
void SumMemTableCol(benchmark::State* state, MODE mode, int64_t data_size,
                    const std::string& col_name);
void SumRequestUnionTableCol(benchmark::State* state, MODE mode,
                             int64_t data_size, const std::string& col_name);
void SumArrayListCol(benchmark::State* state, MODE mode, int64_t data_size,
                     const std::string& col_name);
void TabletFullIterate(benchmark::State* state, MODE mode, int64_t data_size);
void TabletWindowIterate(benchmark::State* state, MODE mode, int64_t data_size);
void MemTableIterate(benchmark::State* state, MODE mode, int64_t data_size);
void RequestUnionTableIterate(benchmark::State* state, MODE mode,
                              int64_t data_size);
void MemSegmentIterate(benchmark::State* state, MODE mode, int64_t data_size);
void CopyMemTable(benchmark::State* state, MODE mode, int64_t data_size);
void CopyMemSegment(benchmark::State* state, MODE mode, int64_t data_size);
void CopyArrayList(benchmark::State* state, MODE mode, int64_t data_size);
// Time UDF
void CTimeDay(benchmark::State* state, MODE mode, const int32_t data_size);
void CTimeMonth(benchmark::State* state, MODE mode, const int32_t data_size);
void CTimeYear(benchmark::State* state, MODE mode, const int32_t data_size);
void TimestampToString(benchmark::State* state, MODE mode);
void TimestampFormat(benchmark::State* state, MODE mode);

void DateToString(benchmark::State* state, MODE mode);
void DateFormat(benchmark::State* state, MODE mode);
void ByteMemPoolAlloc1000(benchmark::State* state, MODE mode,
                          size_t request_size);
void NewFree1000(benchmark::State* state, MODE mode, size_t request_size);

int64_t RunHistoryWindowBuffer(const fesql::vm::WindowRange& window_range,
                               uint64_t data_size,
                               const bool exclude_current_time);
void HistoryWindowBuffer(benchmark::State* state, MODE mode, int64_t data_size);
void HistoryWindowBufferExcludeCurrentTime(benchmark::State* state, MODE mode,
                                           int64_t data_size);
void RequestUnionWindow(benchmark::State* state, MODE mode, int64_t data_size);
void RequestUnionWindowExcludeCurrentTime(benchmark::State* state, MODE mode,
                                          int64_t data_size);
}  // namespace bm
}  // namespace fesql
#endif  // SRC_BM_UDF_BM_CASE_H_
