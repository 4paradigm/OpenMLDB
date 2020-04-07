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

#include "vm/engine_bm_case.h"
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
#include "vm/test_base.h"

namespace fesql {
namespace vm {
using namespace ::llvm;                                      // NOLINT
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
    codec::RowBuilder builder(table.columns());
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

static std::shared_ptr<tablet::TabletCatalog> Data_WindowCase1(
    int32_t data_size) {
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

    // add request
    {
        fesql::type::TableDef request_def;
        BuildTableDef(request_def);
        request_def.set_name("t1");
        request_def.set_catalog("request");
        std::shared_ptr<::fesql::storage::Table> request(
            new ::fesql::storage::Table(1, 1, request_def));
        AddTable(catalog, request_def, request);
    }
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
        codec::RowBuilder builder(table_def.columns());
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
static int64_t RunTableRequest(RequestRunSession& session,  // NOLINT
                               std::shared_ptr<vm::TableHandler> table_handler,
                               int64_t limit_cnt) {
    auto iter = table_handler->GetIterator();
    int64_t cnt = 0;
    while (cnt < limit_cnt && iter->Valid()) {
        cnt++;
        Slice row;
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
    auto catalog = Data_WindowCase1(size);
    Engine engine(catalog);
    RequestRunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);

    auto table = catalog->GetTable("db", "t1");
    if (!table) {
        LOG(WARNING) << "table not exist";
        return;
    }

    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    std::unique_ptr<codec::RowView> row_view =
        std::move(std::unique_ptr<codec::RowView>(
            new codec::RowView(session.GetSchema())));

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
    auto catalog = Data_WindowCase1(size);
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
                benchmark::DoNotOptimize(session.Run());
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

void EngineSimpleSelectDouble(benchmark::State* state, MODE mode) {  // NOLINT
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
    BatchRunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
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
            auto res = session.Run();
            if (!res) {
                FAIL();
            }
            ASSERT_EQ(2u, res->GetCount());
            break;
        }
    }
}

void EngineSimpleSelectVarchar(benchmark::State* state, MODE mode) {  // NOLINT
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
    BatchRunSession session(true);
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
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
            auto res = session.Run();
            if (!res) {
                FAIL();
            }
            ASSERT_EQ(1u, res->GetCount());
            break;
        }
    }
}

void EngineSimpleSelectInt32(benchmark::State* state, MODE mode) {  // NOLINT
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
    BatchRunSession session(true);
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
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
            auto res = session.Run();
            if (!res) {
                FAIL();
            }
            ASSERT_EQ(1u, res->GetCount());
            break;
        }
    }
}

void EngineSimpleUDF(benchmark::State* state, MODE mode) {  // NOLINT
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
    BatchRunSession session(true);
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
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
            auto res = session.Run();
            if (!res) {
                FAIL();
            }
            ASSERT_EQ(1u, res->GetCount());
            break;
        }
    }
}

void EngineRequestSimpleSelectDouble(benchmark::State* state,
                                     MODE mode) {  // NOLINT
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
    // add request
    {
        fesql::type::TableDef request_def;
        BuildTableDef(request_def);
        request_def.set_name("t1");
        request_def.set_catalog("request");
        std::shared_ptr<::fesql::storage::Table> request(
            new ::fesql::storage::Table(1, 1, request_def));
        AddTable(catalog, request_def, request);
    }
    const std::string sql = "SELECT col4 FROM t1 limit 2;";
    Engine engine(catalog);
    RequestRunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    auto table_handler = catalog->GetTable("db", "t1");
    int32_t limit_cnt = 2;
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    RunTableRequest(session, table_handler, limit_cnt));
            }
            break;
        }
        case TEST: {
            ASSERT_EQ(limit_cnt,
                      RunTableRequest(session, table_handler, limit_cnt));
            break;
        }
    }
}

void EngineRequestSimpleSelectVarchar(benchmark::State* state,
                                      MODE mode) {  // NOLINT
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
    // add request
    {
        fesql::type::TableDef request_def;
        BuildTableDef(request_def);
        request_def.set_name("t1");
        request_def.set_catalog("request");
        std::shared_ptr<::fesql::storage::Table> request(
            new ::fesql::storage::Table(1, 1, request_def));
        AddTable(catalog, request_def, request);
    }
    const std::string sql = "SELECT col6 FROM t1 limit 1;";
    Engine engine(catalog);
    RequestRunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    auto table_handler = catalog->GetTable("db", "t1");
    int32_t limit_cnt = 1;
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    RunTableRequest(session, table_handler, limit_cnt));
            }
            break;
        }
        case TEST: {
            ASSERT_EQ(limit_cnt,
                      RunTableRequest(session, table_handler, limit_cnt));
            break;
        }
    }
}

void EngineRequestSimpleSelectInt32(benchmark::State* state,
                                    MODE mode) {  // NOLINT
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
    // add request
    {
        fesql::type::TableDef request_def;
        BuildTableDef(request_def);
        request_def.set_name("t1");
        request_def.set_catalog("request");
        std::shared_ptr<::fesql::storage::Table> request(
            new ::fesql::storage::Table(1, 1, request_def));
        AddTable(catalog, request_def, request);
    }

    const std::string sql = "SELECT col1 FROM t1 limit 1;";
    Engine engine(catalog);
    RequestRunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    auto table_handler = catalog->GetTable("db", "t1");
    int32_t limit_cnt = 1;
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    RunTableRequest(session, table_handler, limit_cnt));
            }
            break;
        }
        case TEST: {
            ASSERT_EQ(limit_cnt,
                      RunTableRequest(session, table_handler, limit_cnt));
            break;
        }
    }
}

void EngineRequestSimpleUDF(benchmark::State* state, MODE mode) {  // NOLINT
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
    // add request
    {
        fesql::type::TableDef request_def;
        BuildTableDef(request_def);
        request_def.set_name("t1");
        request_def.set_catalog("request");
        std::shared_ptr<::fesql::storage::Table> request(
            new ::fesql::storage::Table(1, 1, request_def));
        AddTable(catalog, request_def, request);
    }
    const std::string sql =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend\n%%sql\nSELECT test(col1,col1) FROM t1 limit 1;";
    Engine engine(catalog);
    RequestRunSession session;
    base::Status query_status;
    engine.Get(sql, "db", session, query_status);
    std::ostringstream runner_oss;
    session.GetRunner()->Print(runner_oss, "");
    LOG(INFO) << "runner plan:\n" << runner_oss.str() << std::endl;
    auto table_handler = catalog->GetTable("db", "t1");
    int32_t limit_cnt = 1;
    switch (mode) {
        case BENCHMARK: {
            for (auto _ : *state) {
                benchmark::DoNotOptimize(
                    RunTableRequest(session, table_handler, limit_cnt));
            }
            break;
        }
        case TEST: {
            ASSERT_EQ(limit_cnt,
                      RunTableRequest(session, table_handler, limit_cnt));
            break;
        }
    }
}

}  // namespace vm
}  // namespace fesql
