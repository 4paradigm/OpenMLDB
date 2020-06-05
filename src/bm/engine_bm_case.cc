/*
 * engine_bm_case.cc
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

#include "bm/engine_bm_case.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "benchmark/benchmark.h"
#include "bm/base_bm.h"
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
#include "parser/parser.h"
#include "tablet/tablet_catalog.h"
#include "vm/engine.h"

namespace fesql {
namespace bm {
using codec::Row;
using vm::BatchRunSession;
using vm::Engine;
using vm::RequestRunSession;

using namespace ::llvm;  // NOLINT

static int64_t RunTableRequest(RequestRunSession& session,  // NOLINT
                               std::shared_ptr<vm::TableHandler> table_handler,
                               int64_t limit_cnt) {
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
static void EngineRequestMode(const std::string sql, MODE mode,
                              int64_t limit_cnt, int64_t size,
                              benchmark::State* state) {
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    // prepare data into table
    auto catalog = BuildOnePkTableStorage(size);
    Engine engine(catalog);
    RequestRunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);

    auto table = catalog->GetTable("db", "t1");
    if (!table) {
        LOG(WARNING) << "table not exist";
        return;
    }

    std::ostringstream plan_oss;
    session.GetPhysicalPlan()->Print(plan_oss, "");
    LOG(INFO) << "physical plan:\n" << plan_oss.str() << std::endl;
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
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
    auto catalog = BuildOnePkTableStorage(size);
    Engine engine(catalog);
    BatchRunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                std::shared_ptr<vm::TableHandler> res;
                benchmark::DoNotOptimize(res = session.Run());
            }
            break;
        }
        case TEST: {
            auto res = session.Run();
            if (!res) {
                FAIL();
            }
            ASSERT_EQ(static_cast<uint64_t>(limit_cnt), res->GetCount());
            break;
        }
    }
}

static int64_t RunTableRequest(RequestRunSession& session,  // NOLINT
                               std::shared_ptr<vm::TableHandler> table_handler,
                               int64_t limit_cnt);
void EngineWindowSumFeature1(benchmark::State* state, MODE mode,
                             int64_t limit_cnt, int64_t size) {  // NOLINT
    // prepare data into table
    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
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
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS BETWEEN "
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
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    EngineBatchMode(sql, mode, limit_cnt, size, state);
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
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
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
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
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
    LoadResource(resource_path, table_def, rows);
    ptr = rows[0].buf();
    size = static_cast<uint32_t>(rows[0].size());
    table_def.set_catalog(db);

    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    table->Put(reinterpret_cast<char*>(ptr), size);
    table->Put(reinterpret_cast<char*>(ptr), size);
    delete ptr;
    auto catalog = BuildCommonCatalog(table_def, table);

    Engine engine(catalog);
    RequestRunSession session;
    base::Status query_status;
    engine.Get(sql, db, session, query_status);
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
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
    LoadResource(resource_path, table_def, rows);
    ptr = rows[0].buf();
    size = static_cast<uint32_t>(rows[0].size());
    table_def.set_catalog(db);

    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    table->Put(reinterpret_cast<char*>(ptr), size);
    table->Put(reinterpret_cast<char*>(ptr), size);
    delete ptr;
    auto catalog = BuildCommonCatalog(table_def, table);

    Engine engine(catalog);
    BatchRunSession session;
    base::Status query_status;
    engine.Get(sql, db, session, query_status);
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(session.Run());
            }
            break;
        }
        case TEST: {
            session.EnableDebug();
            auto res = session.Run();
            if (!res) {
                FAIL();
            }
            ASSERT_GT(res->GetCount(), 0u);
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
}  // namespace bm
}  // namespace fesql
