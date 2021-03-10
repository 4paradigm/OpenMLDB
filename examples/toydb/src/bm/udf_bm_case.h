/*
 * Copyright 2021 4Paradigm
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

#ifndef EXAMPLES_TOYDB_SRC_BM_UDF_BM_CASE_H_
#define EXAMPLES_TOYDB_SRC_BM_UDF_BM_CASE_H_
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
#endif  // EXAMPLES_TOYDB_SRC_BM_UDF_BM_CASE_H_
