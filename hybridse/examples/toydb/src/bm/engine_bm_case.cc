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

#include "bm/engine_bm_case.h"
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "benchmark/benchmark.h"
#include "codec/type_codec.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"
#include "tablet/tablet_catalog.h"

namespace hybridse {
namespace bm {
using codec::Row;
using sqlcase::CaseDataMock;
using sqlcase::SqlCase;
using vm::BatchRunSession;
using vm::Engine;
using vm::RequestRunSession;

using namespace ::llvm;  // NOLINT

// Use const return to avoid some compiler bug
// in debug mode of benchmark
static const int64_t RunTableRequest(
    RequestRunSession& session,  // NOLINT
    std::shared_ptr<vm::TableHandler> table_handler, int64_t limit_cnt) {
    auto iter = table_handler->GetIterator();
    int64_t cnt = 0;
    while (cnt < limit_cnt && iter->Valid()) {
        cnt++;
        Row row;
        session.Run(iter->GetValue(), &row);
        iter->Next();
    }
    return cnt;
}

int32_t MapTopFn(int64_t limit) {
    std::string key = "key";
    double d[5];  // NOLINT
    codec::StringRef rkey(key.size(), key.c_str());
    for (int j = 0; j < 5; j++) {
        std::map<codec::StringRef, int32_t> state;
        for (uint32_t i = 0; i < limit; i++) {
            auto it = state.find(rkey);
            if (it == state.end()) {
                state.insert(it, {rkey, 1});
            } else {
                auto& single = it->second;
                single += 1;
            }
        }
        int max = 0;
        int size = 0;
        for (auto it = state.begin(); it != state.end(); ++it) {
            size += it->second;
            if (it->second > max) {
                max = it->second;
            }
        }
        d[j] = static_cast<double>(max) / size;
    }
    return 0;
}

void MapTop1(benchmark::State* state, MODE mode, int64_t limit_cnt,
             int64_t size) {
    for (auto _ : *state) {
        benchmark::DoNotOptimize(MapTopFn(size));
    }
}

static void EngineRequestMode(const std::string sql, MODE mode,
                              int64_t limit_cnt, int64_t size,
                              benchmark::State* state) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    // prepare data into table
    auto catalog = vm::BuildOnePkTableStorage(size);
    vm::EngineOptions options;
    if (hybridse::sqlcase::SqlCase::IsCluster()) {
        options.SetClusterOptimized(true);
    }
    Engine engine(catalog, options);
    RequestRunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);

    auto table = catalog->GetTable("db", "t1");
    if (!table) {
        LOG(WARNING) << "table not exist";
        return;
    }

    std::ostringstream plan_oss;
    session.GetCompileInfo()->DumpPhysicalPlan(plan_oss, "");

    LOG(INFO) << "physical plan:\n" << plan_oss.str() << std::endl;
    std::ostringstream runner_oss;
    session.GetCompileInfo()->DumpClusterJob(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    std::unique_ptr<codec::RowView> row_view = std::unique_ptr<codec::RowView>(
        new codec::RowView(session.GetSchema()));

    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    RunTableRequest(session, table, limit_cnt));
            }
            break;
        }
        case TEST: {
            ASSERT_EQ(limit_cnt, RunTableRequest(session, table, limit_cnt));
            break;
        }
    }
}

static void EngineBatchMode(const std::string sql, MODE mode, int64_t limit_cnt,
                            int64_t size, benchmark::State* state) {
    // prepare data into table
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    auto catalog = vm::BuildOnePkTableStorage(size);
    Engine engine(catalog);
    BatchRunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    std::ostringstream runner_oss;
    session.GetCompileInfo()->DumpClusterJob(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                std::vector<hybridse::codec::Row> outputs;
                benchmark::DoNotOptimize(session.Run(outputs));
            }
            break;
        }
        case TEST: {
            std::vector<hybridse::codec::Row> outputs;
            if (0 != session.Run(outputs)) {
                FAIL();
            }
            ASSERT_EQ(static_cast<uint64_t>(limit_cnt), outputs.size());
            break;
        }
    }
}
void EngineWindowSumFeature1ExcludeCurrentTime(benchmark::State* state,
                                               MODE mode, int64_t limit_cnt,
                                               int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW EXCLUDE CURRENT_TIME) limit " +
        std::to_string(limit_cnt) + ";";
    EngineRequestMode(sql, mode, limit_cnt, size, state);
}
void EngineWindowSumFeature1(benchmark::State* state, MODE mode,
                             int64_t limit_cnt, int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineRequestMode(sql, mode, limit_cnt, size, state);
}
void EngineWindowRowsSumFeature1(benchmark::State* state, MODE mode,
                                 int64_t limit_cnt, int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "40000 PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineRequestMode(sql, mode, limit_cnt, size, state);
}

void EngineRunBatchWindowSumFeature1(benchmark::State* state, MODE mode,
                                     int64_t limit_cnt,
                                     int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";

    EngineBatchMode(sql, mode, limit_cnt, size, state);
}
void EngineRunBatchWindowSumFeature5(benchmark::State* state, MODE mode,
                                     int64_t limit_cnt,
                                     int64_t size) {  // NOLINT
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineBatchMode(sql, mode, limit_cnt, size, state);
}

void EngineRunBatchWindowSumFeature1ExcludeCurrentTime(
    benchmark::State* state, MODE mode, int64_t limit_cnt,
    int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW EXCLUDE CURRENT_TIME) limit " +
        std::to_string(limit_cnt) + ";";

    EngineBatchMode(sql, mode, limit_cnt, size, state);
}
void EngineRunBatchWindowSumFeature5ExcludeCurrentTime(
    benchmark::State* state, MODE mode, int64_t limit_cnt,
    int64_t size) {  // NOLINT
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW EXCLUDE CURRENT_TIME) limit " +
        std::to_string(limit_cnt) + ";";
    EngineBatchMode(sql, mode, limit_cnt, size, state);
}
void EngineRunBatchWindowSumFeature5Window5(benchmark::State* state, MODE mode,
                                            int64_t limit_cnt,
                                            int64_t size) {  // NOLINT
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) as w2_col3_sum, "
        "sum(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "20d "
        "PRECEDING AND CURRENT ROW) as w3_col4_sum, "
        "sum(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w4_col2_sum, "
        "sum(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN 5d "
        "PRECEDING AND CURRENT ROW) as w5_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineBatchMode(sql, mode, limit_cnt, size, state);
}
void EngineRunBatchWindowMultiAggWindow25Feature25(benchmark::State* state,
                                                   MODE mode, int64_t limit_cnt,
                                                   int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        30d PRECEDING AND CURRENT ROW)  as w1_col3_sum, "
        "sum(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        30d PRECEDING AND CURRENT ROW)  as w1_col4_sum, "
        "sum(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        30d PRECEDING AND CURRENT ROW)  as w1_col2_sum, "
        "sum(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        30d PRECEDING AND CURRENT ROW)  as w1_col5_sum, "

        "count(col1) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        20d PRECEDING AND CURRENT ROW)  as w1_col1_cnt, "
        "count(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        20d PRECEDING AND CURRENT ROW) as w1_col3_cnt, "
        "count(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "       20d PRECEDING AND CURRENT ROW) as w1_col4_cnt, "
        "count(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        20d PRECEDING AND CURRENT ROW) as w1_col2_cnt, "
        "count(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        20d PRECEDING AND CURRENT ROW) as w1_col5_cnt, "

        "avg(col1) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "15d "
        "PRECEDING AND CURRENT ROW) as w1_col1_avg, "
        "avg(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "15d "
        "PRECEDING AND CURRENT ROW) as w1_col3_avg, "
        "avg(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "15d "
        "PRECEDING AND CURRENT ROW) as w1_col4_avg, "
        "avg(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "15d "
        "PRECEDING AND CURRENT ROW) as w1_col2_avg, "
        "avg(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "15d "
        "PRECEDING AND CURRENT ROW) as w1_col5_avg, "

        "min(col1) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col1_min, "
        "min(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col3_min, "
        "min(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col4_min, "
        "min(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col2_min, "
        "min(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col5_min, "

        "max(col1) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col1_max, "
        "max(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col3_max, "
        "max(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col4_max, "
        "max(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col2_max, "
        "max(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col5_max "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineBatchMode(sql, mode, limit_cnt, size, state);
}

void EngineWindowSumFeature5ExcludeCurrentTime(benchmark::State* state,
                                               MODE mode, int64_t limit_cnt,
                                               int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW EXCLUDE CURRENT_TIME) limit " +
        std::to_string(limit_cnt) + ";";
    EngineRequestMode(sql, mode, limit_cnt, size, state);
}

void EngineWindowSumFeature5(benchmark::State* state, MODE mode,
                             int64_t limit_cnt,
                             int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineRequestMode(sql, mode, limit_cnt, size, state);
}

void EngineWindowDistinctCntFeature(benchmark::State* state, MODE mode,
                                    int64_t limit_cnt,
                                    int64_t size) {  // NOLINT
    const std::string sql =
        "SELECT "
        "distinct_count(col6) OVER  w1  as top1, "
        "distinct_count(col6) OVER  w1  as top2, "
        "distinct_count(col6) OVER  w1  as top3, "
        "distinct_count(col6) OVER  w1  as top4, "
        "distinct_count(col6) OVER  w1  as top5 "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineRequestMode(sql, mode, limit_cnt, size, state);
}

void EngineWindowTop1RatioFeature(benchmark::State* state, MODE mode,
                                  int64_t limit_cnt,
                                  int64_t size) {  // NOLINT
    const std::string sql =
        "SELECT "
        "fz_top1_ratio(col6) OVER  w1  as top1, "
        "fz_top1_ratio(col6) OVER  w1  as top2, "
        "fz_top1_ratio(col6) OVER  w1  as top3, "
        "fz_top1_ratio(col6) OVER  w1  as top4, "
        "fz_top1_ratio(col6) OVER  w1  as top5 "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineRequestMode(sql, mode, limit_cnt, size, state);
}

void EngineWindowSumFeature5Window5(benchmark::State* state, MODE mode,
                                    int64_t limit_cnt,
                                    int64_t size) {  // NOLINT
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) as w2_col3_sum, "
        "sum(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "20d "
        "PRECEDING AND CURRENT ROW) as w3_col4_sum, "
        "sum(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w4_col2_sum, "
        "sum(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN 5d "
        "PRECEDING AND CURRENT ROW) as w5_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineRequestMode(sql, mode, limit_cnt, size, state);
}
void EngineWindowMultiAggFeature5(benchmark::State* state, MODE mode,
                                  int64_t limit_cnt,
                                  int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum, "

        "count(col1) OVER w1 as w1_col1_cnt, "
        "count(col3) OVER w1 as w1_col3_cnt, "
        "count(col4) OVER w1 as w1_col4_cnt, "
        "count(col2) OVER w1 as w1_col2_cnt, "
        "count(col5) OVER w1 as w1_col5_cnt, "

        "avg(col1) OVER w1 as w1_col1_avg, "
        "avg(col3) OVER w1 as w1_col3_avg, "
        "avg(col4) OVER w1 as w1_col4_avg, "
        "avg(col2) OVER w1 as w1_col2_avg, "
        "avg(col5) OVER w1 as w1_col5_avg, "

        "min(col1) OVER w1 as w1_col1_min, "
        "min(col3) OVER w1 as w1_col3_min, "
        "min(col4) OVER w1 as w1_col4_min, "
        "min(col2) OVER w1 as w1_col2_min, "
        "min(col5) OVER w1 as w1_col5_min, "

        "max(col1) OVER w1 as w1_col1_max, "
        "max(col3) OVER w1 as w1_col3_max, "
        "max(col4) OVER w1 as w1_col4_max, "
        "max(col2) OVER w1 as w1_col2_max, "
        "max(col5) OVER w1 as w1_col5_max "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineRequestMode(sql, mode, limit_cnt, size, state);
}

void EngineWindowMultiAggWindow25Feature25(benchmark::State* state, MODE mode,
                                           int64_t limit_cnt,
                                           int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        30d PRECEDING AND CURRENT ROW)  as w1_col3_sum, "
        "sum(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        30d PRECEDING AND CURRENT ROW)  as w1_col4_sum, "
        "sum(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        30d PRECEDING AND CURRENT ROW)  as w1_col2_sum, "
        "sum(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        30d PRECEDING AND CURRENT ROW)  as w1_col5_sum, "

        "count(col1) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        20d PRECEDING AND CURRENT ROW)  as w1_col1_cnt, "
        "count(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        20d PRECEDING AND CURRENT ROW) as w1_col3_cnt, "
        "count(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "       20d PRECEDING AND CURRENT ROW) as w1_col4_cnt, "
        "count(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        20d PRECEDING AND CURRENT ROW) as w1_col2_cnt, "
        "count(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "        20d PRECEDING AND CURRENT ROW) as w1_col5_cnt, "

        "avg(col1) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "15d "
        "PRECEDING AND CURRENT ROW) as w1_col1_avg, "
        "avg(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "15d "
        "PRECEDING AND CURRENT ROW) as w1_col3_avg, "
        "avg(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "15d "
        "PRECEDING AND CURRENT ROW) as w1_col4_avg, "
        "avg(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "15d "
        "PRECEDING AND CURRENT ROW) as w1_col2_avg, "
        "avg(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "15d "
        "PRECEDING AND CURRENT ROW) as w1_col5_avg, "

        "min(col1) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col1_min, "
        "min(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col3_min, "
        "min(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col4_min, "
        "min(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col2_min, "
        "min(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col5_min, "

        "max(col1) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col1_max, "
        "max(col3) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col3_max, "
        "max(col4) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col4_max, "
        "max(col2) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col2_max, "
        "max(col5) OVER (PARTITION BY col0 ORDER BY col5 ROWS_RANGE BETWEEN "
        "10d "
        "PRECEDING AND CURRENT ROW) as w1_col5_max "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS_RANGE "
        "BETWEEN "
        "30d PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineRequestMode(sql, mode, limit_cnt, size, state);
}
void EngineSimpleSelectDouble(benchmark::State* state, MODE mode) {  // NOLINT
    const std::string sql = "SELECT col4 FROM t1 limit 2;";
    const std::string resource =
        "cases/resource/benchmark_t1_basic_one_row.yaml";
    EngineBatchModeSimpleQueryBM("db", sql, resource, state, mode);
}

void EngineSimpleSelectVarchar(benchmark::State* state, MODE mode) {  // NOLINT
    const std::string sql = "SELECT col6 FROM t1 limit 1;";
    const std::string resource =
        "cases/resource/benchmark_t1_basic_one_row.yaml";
    EngineBatchModeSimpleQueryBM("db", sql, resource, state, mode);
}

void EngineSimpleSelectTimestamp(benchmark::State* state,
                                 MODE mode) {  // NOLINT
    const std::string sql = "SELECT std_ts FROM t1 limit 1;";
    const std::string resource =
        "cases/resource/benchmark_t1_with_time_one_row.yaml";
    EngineBatchModeSimpleQueryBM("db", sql, resource, state, mode);
}

void EngineSimpleSelectDate(benchmark::State* state, MODE mode) {  // NOLINT
    const std::string sql = "SELECT std_date FROM t1 limit 1;";
    const std::string resource =
        "cases/resource/benchmark_t1_with_time_one_row.yaml";
    EngineBatchModeSimpleQueryBM("db", sql, resource, state, mode);
}
void EngineSimpleSelectInt32(benchmark::State* state, MODE mode) {  // NOLINT
    const std::string sql = "SELECT col1 FROM t1 limit 1;";
    const std::string resource =
        "cases/resource/benchmark_t1_with_time_one_row.yaml";
    EngineBatchModeSimpleQueryBM("db", sql, resource, state, mode);
}

void EngineSimpleUDF(benchmark::State* state, MODE mode) {  // NOLINT
    const std::string sql =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend\n%%sql\nSELECT test(col1,col1) FROM t1 limit 1;";
    const std::string resource =
        "cases/resource/benchmark_t1_basic_one_row.yaml";
    EngineBatchModeSimpleQueryBM("db", sql, resource, state, mode);
}

void EngineRequestModeSimpleQueryBM(const std::string& db,
                                    const std::string& query_table,
                                    const std::string& sql, int32_t limit_cnt,
                                    const std::string& resource_path,
                                    benchmark::State* state, MODE mode) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    type::TableDef table_def;
    int8_t* ptr = NULL;
    uint32_t size = 0;
    std::vector<Row> rows;
    CaseDataMock::LoadResource(resource_path, table_def, rows);
    ptr = rows[0].buf();
    size = static_cast<uint32_t>(rows[0].size());
    table_def.set_catalog(db);

    std::shared_ptr<::hybridse::storage::Table> table(
        new ::hybridse::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    table->Put(reinterpret_cast<char*>(ptr), size);
    table->Put(reinterpret_cast<char*>(ptr), size);
    delete ptr;
    auto catalog = vm::BuildCommonCatalog(table_def, table);

    Engine engine(catalog);
    RequestRunSession session;
    base::Status query_status;
    engine.Get(sql, db, session, query_status);
    LOG(INFO) << query_status;
    std::ostringstream runner_oss;
    session.GetCompileInfo()->DumpClusterJob(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    auto table_handler = catalog->GetTable(db, query_table);
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    RunTableRequest(session, table_handler, limit_cnt));
            }
            break;
        }
        case TEST: {
            session.EnableDebug();
            ASSERT_EQ(limit_cnt,
                      RunTableRequest(session, table_handler, limit_cnt));
            break;
        }
    }
}

void EngineBatchModeSimpleQueryBM(const std::string& db, const std::string& sql,
                                  const std::string& resource_path,
                                  benchmark::State* state, MODE mode) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    type::TableDef table_def;
    int8_t* ptr = NULL;
    uint32_t size = 0;
    std::vector<Row> rows;
    hybridse::sqlcase::CaseDataMock::LoadResource(resource_path, table_def,
                                                  rows);
    ptr = rows[0].buf();
    size = static_cast<uint32_t>(rows[0].size());
    table_def.set_catalog(db);

    std::shared_ptr<::hybridse::storage::Table> table(
        new ::hybridse::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    table->Put(reinterpret_cast<char*>(ptr), size);
    table->Put(reinterpret_cast<char*>(ptr), size);
    delete ptr;
    auto catalog = vm::BuildCommonCatalog(table_def, table);

    Engine engine(catalog);
    BatchRunSession session;
    base::Status query_status;
    engine.Get(sql, db, session, query_status);
    std::ostringstream runner_oss;
    session.GetCompileInfo()->DumpClusterJob(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                // use const value to avoid compiler bug for some version
                vector<hybrise::codec::Row> outputs;
                benchmark::DoNotOptimize(
                    static_cast<
                        const std::shared_ptr<hybridse::vm::DataHandler>>(
                        session.Run(outputs)));
            }
            break;
        }
        case TEST: {
            session.EnableDebug();
            vector<hybrise::codec::Row> outputs;

            if (0 != session.Run(outputs)) {
                FAIL();
            }
            ASSERT_GT(outputs.size(), 0u);
            break;
        }
    }
}
void EngineRequestSimpleSelectDouble(benchmark::State* state, MODE mode) {
    const std::string sql = "SELECT col4 FROM t1 limit 2;";
    const std::string resource =
        "cases/resource/benchmark_t1_basic_one_row.yaml";
    EngineRequestModeSimpleQueryBM("db", "t1", sql, 2, resource, state, mode);
}
void EngineRequestSimpleSelectVarchar(benchmark::State* state,
                                      MODE mode) {  // NOLINT
    const std::string sql = "SELECT col6 FROM t1 limit 1;";
    const std::string resource =
        "cases/resource/benchmark_t1_basic_one_row.yaml";
    EngineRequestModeSimpleQueryBM("db", "t1", sql, 1, resource, state, mode);
}

void EngineRequestSimpleSelectInt32(benchmark::State* state,
                                    MODE mode) {  // NOLINT
    const std::string sql = "SELECT col1 FROM t1 limit 1;";
    const std::string resource =
        "cases/resource/benchmark_t1_basic_one_row.yaml";
    EngineRequestModeSimpleQueryBM("db", "t1", sql, 1, resource, state, mode);
}

void EngineRequestSimpleUDF(benchmark::State* state, MODE mode) {  // NOLINT
    const std::string sql =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend\n%%sql\nSELECT test(col1,col1) FROM t1 limit 1;";
    const std::string resource =
        "cases/resource/benchmark_t1_basic_one_row.yaml";
    EngineRequestModeSimpleQueryBM("db", "t1", sql, 1, resource, state, mode);
}

void EngineRequestSimpleSelectTimestamp(benchmark::State* state,
                                        MODE mode) {  // NOLINT
    const std::string sql = "SELECT std_ts FROM t1 limit 1;";
    const std::string resource =
        "cases/resource/benchmark_t1_with_time_one_row.yaml";
    EngineRequestModeSimpleQueryBM("db", "t1", sql, 1, resource, state, mode);
}

void EngineRequestSimpleSelectDate(benchmark::State* state,
                                   MODE mode) {  // NOLINT
    const std::string sql = "SELECT std_date FROM t1 limit 1;";
    const std::string resource =
        "cases/resource/benchmark_t1_with_time_one_row.yaml";
    EngineRequestModeSimpleQueryBM("db", "t1", sql, 1, resource, state, mode);
}
hybridse::sqlcase::SqlCase LoadSqlCaseWithID(const std::string& yaml,
                                             const std::string& case_id) {
    return hybridse::sqlcase::SqlCase::LoadSqlCaseWithID(
        hybridse::sqlcase::FindSqlCaseBaseDirPath(), yaml, case_id);
}
void EngineBenchmarkOnCase(const std::string& yaml_path,
                           const std::string& case_id,
                           vm::EngineMode engine_mode,
                           benchmark::State* state) {
    SqlCase target_case = hybridse::sqlcase::SqlCase::LoadSqlCaseWithID(
        hybridse::sqlcase::FindSqlCaseBaseDirPath(), yaml_path, case_id);
    if (target_case.id() != case_id) {
        LOG(WARNING) << "Fail to find case #" << case_id << " in " << yaml_path;
        state->SkipWithError("BENCHMARK CASE LOAD FAIL: fail to find case");
        return;
    }
    EngineBenchmarkOnCase(target_case, engine_mode, state);
}
void EngineBenchmarkOnCase(hybridse::sqlcase::SqlCase& sql_case,  // NOLINT
                           vm::EngineMode engine_mode,
                           benchmark::State* state) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();

    LOG(INFO) << "BENCHMARK INIT Engine Runner";

    vm::EngineOptions engine_options;
    if (engine_mode == vm::kBatchRequestMode) {
        engine_options.SetBatchRequestOptimized(
            sql_case.batch_request_optimized_);
    }
    if (hybridse::sqlcase::SqlCase::IsCluster()) {
        engine_options.SetClusterOptimized(true);
    } else {
        engine_options.SetClusterOptimized(false);
    }
    if (hybridse::sqlcase::SqlCase::IsDisableExprOpt()) {
        engine_options.SetEnableExprOptimize(false);
    } else {
        engine_options.SetEnableExprOptimize(true);
    }
    std::unique_ptr<vm::EngineTestRunner> engine_runner;
    if (engine_mode == vm::kBatchMode) {
        engine_runner = std::unique_ptr<vm::BatchEngineTestRunner>(
            new vm::ToydbBatchEngineTestRunner(sql_case, engine_options));
    } else if (engine_mode == vm::kRequestMode) {
        engine_runner = std::unique_ptr<vm::RequestEngineTestRunner>(
            new vm::ToydbRequestEngineTestRunner(sql_case, engine_options));
    } else {
        engine_runner = std::unique_ptr<vm::BatchRequestEngineTestRunner>(
            new vm::ToydbBatchRequestEngineTestRunner(
                sql_case, engine_options,
                sql_case.batch_request().common_column_indices_));
    }
    if (SqlCase::IsDebug()) {
        LOG(INFO) << "BENCHMARK CASE TEST: BEGIN";
        for (auto _ : *state) {
            engine_runner->RunCheck();
            if (engine_runner->return_code() == ENGINE_TEST_RET_SUCCESS) {
                state->SkipWithError("BENCHMARK CASE TEST: OK");
            } else {
                state->SkipWithError("BENCHMARK CASE TEST: FAIL");
            }
            break;
        }
        return;
    }

    if (!engine_runner->InitEngineCatalog()) {
        LOG(ERROR) << "Engine Test Init Catalog Error";
        return;
    }
    base::Status status = engine_runner->Compile();
    if (!status.isOK()) {
        LOG(WARNING) << "Compile error: " << status;
        return;
    }
    status = engine_runner->PrepareData();
    if (!status.isOK()) {
        LOG(WARNING) << "Prepare data error: " << status;
        return;
    }
    std::vector<Row> output_rows;
    for (auto _ : *state) {
        output_rows.clear();
        benchmark::DoNotOptimize(static_cast<const base::Status>(
            engine_runner->Compute(&output_rows)));
    }
}

}  // namespace bm
}  // namespace hybridse
