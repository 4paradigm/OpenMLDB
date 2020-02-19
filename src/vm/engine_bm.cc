/*
 * engine_mk.cc
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
#include "plan/planner.h"
#include "vm/engine.h"
#include "vm/test_base.h"
#include "tablet/tablet_catalog.h"

namespace fesql {
namespace vm {
using namespace ::llvm;  // NOLINT

static void BuildTableDef(::fesql::type::TableDef& table) {  // NOLINT
    table.set_name("t1");
    table.set_catalog("db");
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col0");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }
    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col5");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }
}

static void BuildBuf(int8_t** buf, uint32_t* size,
                     ::fesql::type::TableDef& table) {  // NOLINT
    BuildTableDef(table);
    ::fesql::type::IndexDef* index = table.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col6");
    index->set_second_key("col5");
    storage::RowBuilder builder(table.columns());
    uint32_t total_size = builder.CalTotalLength(2);
    int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
    builder.SetBuffer(ptr, total_size);
    builder.AppendString("0", 1);
    builder.AppendInt32(32);
    builder.AppendInt16(16);
    builder.AppendFloat(2.1f);
    builder.AppendDouble(3.1);
    builder.AppendInt64(64);
    builder.AppendString("1", 1);
    *buf = ptr;
    *size = total_size;
}

static std::shared_ptr<tablet::TabletCatalog> Data_WindowCase1(int32_t data_size) {
    DLOG(INFO) << "insert window data";
    type::TableDef table_def; 
    BuildTableDef(table_def);
    // Build index
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col0");
    index->set_second_key("col5");

    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));

    table->Init();

    auto catalog = BuildCommonCatalog(table_def, table);

    ::fesql::bm::Repeater<std::string> col0(
        std::vector<std::string>({"hello"}));
    ::fesql::bm::IntRepeater<int32_t> col1;
    col1.Range(1, 100, 1);
    ::fesql::bm::IntRepeater<int16_t> col2;
    col2.Range(1u, 100u, 2);
    ::fesql::bm::RealRepeater<float> col3;
    col3.Range(1.0, 100.0, 3.0f);
    ::fesql::bm::RealRepeater<double> col4;
    col4.Range(100.0, 10000.0, 10.0);
    ::fesql::bm::IntRepeater<int64_t> col5;
    col5.Range(1576571615000 - 100000000, 1576571615000, 1000);
    ::fesql::bm::Repeater<std::string> col6({"astring", "bstring", "cstring",
                                             "dstring", "estring", "fstring",
                                             "gstring", "hstring"});

    for (int i = 0; i < data_size; ++i) {
        std::string str1 = col0.GetValue();
        std::string str2 = col6.GetValue();
        storage::RowBuilder builder(table_def.columns());
        uint32_t total_size = builder.CalTotalLength(str1.size() + str2.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString(str1.c_str(), str1.size());
        builder.AppendInt32(col1.GetValue());
        builder.AppendInt16(col2.GetValue());
        builder.AppendFloat(col3.GetValue());
        builder.AppendDouble(col4.GetValue());
        builder.AppendInt64(col5.GetValue());
        builder.AppendString(str2.c_str(), str2.size());
        table->Put(reinterpret_cast<char*>(ptr), total_size);
        free(ptr);
    }
    return catalog;
}

static void BM_EngineSimpleSelectDouble(benchmark::State& state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    type::TableDef table_def;
    int8_t* ptr = NULL;
    uint32_t size = 0;
    BuildBuf(&ptr, &size, table_def);
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    table->Put(reinterpret_cast<char*>(ptr), size);
    table->Put(reinterpret_cast<char*>(ptr), size);
    delete ptr;
    auto catalog = BuildCommonCatalog(table_def, table);
    const std::string sql = "SELECT col4 FROM t1 limit 2;";
    Engine engine(catalog);
    RunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    for (auto _ : state) {
        std::vector<int8_t*> output(2);
        benchmark::DoNotOptimize(session.Run(output, 2));
        for (int8_t* row : output) {
            free(row);
        }
    }
}

static void BM_EngineSimpleSelectVarchar(benchmark::State& state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    type::TableDef table_def;
    int8_t* ptr = NULL;
    uint32_t size = 0;
    BuildBuf(&ptr, &size, table_def);
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    table->Put(reinterpret_cast<char*>(ptr), size);
    table->Put(reinterpret_cast<char*>(ptr), size);
    delete ptr;
    auto catalog = BuildCommonCatalog(table_def, table);
    const std::string sql = "SELECT col6 FROM t1 limit 1;";
    Engine engine(catalog);
    RunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    for (auto _ : state) {
        std::vector<int8_t*> output(2);
        benchmark::DoNotOptimize(session.Run(output, 2));
        for (int8_t* row : output) {
            free(row);
        }
    }
}

static void BM_EngineSimpleSelectInt32(benchmark::State& state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    type::TableDef table_def;
    int8_t* ptr = NULL;
    uint32_t size = 0;
    BuildBuf(&ptr, &size, table_def);
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    table->Put(reinterpret_cast<char*>(ptr), size);
    table->Put(reinterpret_cast<char*>(ptr), size);
    delete ptr;
    auto catalog = BuildCommonCatalog(table_def, table);
    const std::string sql = "SELECT col1 FROM t1 limit 1;";
    Engine engine(catalog);
    RunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    for (auto _ : state) {
        std::vector<int8_t*> output(2);
        benchmark::DoNotOptimize(session.Run(output, 2));
        for (int8_t* row : output) {
            free(row);
        }
    }
}

static void BM_EngineSimpleUDF(benchmark::State& state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    type::TableDef table_def;
    int8_t* ptr = NULL;
    uint32_t size = 0;
    BuildBuf(&ptr, &size, table_def);
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    table->Put(reinterpret_cast<char*>(ptr), size);
    table->Put(reinterpret_cast<char*>(ptr), size);
    delete ptr;
    auto catalog = BuildCommonCatalog(table_def, table);
    const std::string sql =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend\n%%sql\nSELECT test(col1,col1) FROM t1 limit 1;";
    Engine engine(catalog);
    RunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    for (auto _ : state) {
        std::vector<int8_t*> output(2);
        benchmark::DoNotOptimize(session.Run(output, 2));
        for (int8_t* row : output) {
            free(row);
        }
    }
}

static void BM_EngineWindowSumFeature1(benchmark::State& state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();

    // prepare data into table

    int64_t limit_cnt = state.range(0);
    int64_t size = state.range(1);
    auto catalog = Data_WindowCase1(size);

    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    Engine engine(catalog);
    RunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    for (auto _ : state) {
        std::vector<int8_t*> output(2);
        benchmark::DoNotOptimize(session.Run(output, limit_cnt));
        for (int8_t* row : output) {
            free(row);
        }
    }
}

static void BM_EngineRunBatchWindowSumFeature1(
    benchmark::State& state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();

    // prepare data into table

    int64_t limit_cnt = state.range(0);
    int64_t size = state.range(1);
    auto catalog = Data_WindowCase1(size);

    const std::string sql =
        "SELECT "
        "sum(col4) OVER w1 as w1_col4_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS BETWEEN "
        "30d "
        "PRECEDING AND CURRENT ROW) limit " +
        std::to_string(limit_cnt) + ";";
    Engine engine(catalog);
    RunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    for (auto _ : state) {
        std::vector<int8_t*> output(2);
        benchmark::DoNotOptimize(session.RunBatch(output, limit_cnt));
        for (int8_t* row : output) {
            free(row);
        }
    }
}
static void BM_EngineRunBatchWindowSumFeature5(
    benchmark::State& state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();

    int64_t size = state.range(1);
    auto catalog = Data_WindowCase1(size);
    int64_t limit_cnt = state.range(0);

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
    Engine engine(catalog);
    RunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    for (auto _ : state) {
        std::vector<int8_t*> output(2);
        benchmark::DoNotOptimize(session.RunBatch(output, limit_cnt));
        for (int8_t* row : output) {
            free(row);
        }
    }
}

static void BM_EngineWindowSumFeature5(benchmark::State& state) {  // NOLINT
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();

    // prepare data into table

    int64_t limit_cnt = state.range(0);
    int64_t size = state.range(1);
    auto catalog = Data_WindowCase1(size);
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
    Engine engine(catalog);
    RunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    for (auto _ : state) {
        std::vector<int8_t*> output(2);
        benchmark::DoNotOptimize(session.Run(output, limit_cnt));
        for (int8_t* row : output) {
            free(row);
        }
    }
}

BENCHMARK(BM_EngineSimpleSelectVarchar);
BENCHMARK(BM_EngineSimpleSelectDouble);
BENCHMARK(BM_EngineSimpleSelectInt32);
BENCHMARK(BM_EngineSimpleUDF);
BENCHMARK(BM_EngineWindowSumFeature1)
    ->Args({1, 2})
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
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});

BENCHMARK(BM_EngineRunBatchWindowSumFeature1)
    ->Args({1, 2})
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
    ->Args({1, 2})
    ->Args({1, 10})
    ->Args({1, 100})
    ->Args({1, 1000})
    ->Args({1, 10000})
    ->Args({100, 100})
    ->Args({1000, 1000})
    ->Args({10000, 10000});

}  // namespace vm
}  // namespace fesql

BENCHMARK_MAIN();
