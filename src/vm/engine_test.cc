/*
 * engine_test.cc
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

#include "vm/engine.h"
#include <utility>
#include <vector>

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
#include "storage/codec.h"
#include "storage/window.h"
#include "vm/test_base.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace fesql {
namespace vm {

enum EngineRunMode { RUN, RUNBATCH, RUNONE };
class EngineTest : public ::testing::TestWithParam<EngineRunMode> {};

void BuildTableDef(::fesql::type::TableDef& table) {  // NOLINT
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
void BuildBuf(int8_t** buf, uint32_t* size) {
    ::fesql::type::TableDef table;
    BuildTableDef(table);
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
void BuildWindow(std::vector<fesql::storage::Row>& rows,  // NOLINT
                 int8_t** buf) {
    ::fesql::type::TableDef table;
    BuildTableDef(table);
    {
        storage::RowBuilder builder(table.columns());
        std::string str = "1";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));

        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendInt32(1);
        builder.AppendInt16(5);
        builder.AppendFloat(1.1f);
        builder.AppendDouble(11.1);
        builder.AppendInt64(1);
        builder.AppendString(str.c_str(), 1);
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str = "22";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendInt32(2);
        builder.AppendInt16(5);
        builder.AppendFloat(2.2f);
        builder.AppendDouble(22.2);
        builder.AppendInt64(2);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str = "333";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendInt32(3);
        builder.AppendInt16(55);
        builder.AppendFloat(3.3f);
        builder.AppendDouble(33.3);
        builder.AppendInt64(1);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str = "4444";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendInt32(4);
        builder.AppendInt16(55);
        builder.AppendFloat(4.4f);
        builder.AppendDouble(44.4);
        builder.AppendInt64(2);
        builder.AppendString("4444", str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "a";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("2", 1);
        builder.AppendInt32(5);
        builder.AppendInt16(55);
        builder.AppendFloat(5.5f);
        builder.AppendDouble(55.5);
        builder.AppendInt64(3);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }

    ::fesql::storage::WindowIteratorImpl* w =
        new ::fesql::storage::WindowIteratorImpl(rows);
    *buf = reinterpret_cast<int8_t*>(w);
}
void BuildWindowUnique(std::vector<fesql::storage::Row>& rows,  // NOLINT
                       int8_t** buf) {
    ::fesql::type::TableDef table;
    BuildTableDef(table);

    {
        storage::RowBuilder builder(table.columns());
        std::string str = "1";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));

        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendInt32(1);
        builder.AppendInt16(5);
        builder.AppendFloat(1.1f);
        builder.AppendDouble(11.1);
        builder.AppendInt64(1);
        builder.AppendString(str.c_str(), 1);
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str = "22";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("0", 1);
        builder.AppendInt32(2);
        builder.AppendInt16(5);
        builder.AppendFloat(2.2f);
        builder.AppendDouble(22.2);
        builder.AppendInt64(2);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str = "333";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendInt32(3);
        builder.AppendInt16(5);
        builder.AppendFloat(3.3f);
        builder.AppendDouble(33.3);
        builder.AppendInt64(3);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str = "4444";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("1", 1);
        builder.AppendInt32(4);
        builder.AppendInt16(5);
        builder.AppendFloat(4.4f);
        builder.AppendDouble(44.4);
        builder.AppendInt64(4);
        builder.AppendString("4444", str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }
    {
        storage::RowBuilder builder(table.columns());
        std::string str =
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
            "a";
        std::string str0 = "0";
        uint32_t total_size = builder.CalTotalLength(str.size() + str0.size());
        int8_t* ptr = static_cast<int8_t*>(malloc(total_size));
        builder.SetBuffer(ptr, total_size);
        builder.AppendString("2", 1);
        builder.AppendInt32(5);
        builder.AppendInt16(5);
        builder.AppendFloat(5.5f);
        builder.AppendDouble(55.5);
        builder.AppendInt64(5);
        builder.AppendString(str.c_str(), str.size());
        rows.push_back(fesql::storage::Row{.buf = ptr, .size = total_size});
    }

    ::fesql::storage::WindowIteratorImpl* w =
        new ::fesql::storage::WindowIteratorImpl(rows);
    *buf = reinterpret_cast<int8_t*>(w);
}
void StoreData(::fesql::storage::Table* table, int8_t* rows) {
    ::fesql::storage::WindowIteratorImpl* w =
        reinterpret_cast<::fesql::storage::WindowIteratorImpl*>(rows);
    ASSERT_TRUE(w->Valid());
    ::fesql::storage::Row row = w->Next();
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf), row.size));

    ASSERT_TRUE(w->Valid());
    row = w->Next();
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf), row.size));

    ASSERT_TRUE(w->Valid());
    row = w->Next();
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf), row.size));
    ASSERT_TRUE(w->Valid());
    row = w->Next();
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf), row.size));
    ASSERT_TRUE(w->Valid());
    row = w->Next();
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row.buf), row.size));
}

INSTANTIATE_TEST_CASE_P(EngineRUNAndBatchMode, EngineTest,
                        testing::Values(RUN, RUNBATCH));

TEST_P(EngineTest, test_normal) {
    ParamType mode = GetParam();
    int8_t* row1 = NULL;
    uint32_t size1 = 0;
    BuildBuf(&row1, &size1);
    type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col0");
    index->set_second_key("col5");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row1), size1));
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row1), size1));
    ASSERT_TRUE(table->Put(reinterpret_cast<char*>(row1), size1));
    auto catalog = BuildCommonCatalog(table_def, table);
    const std::string sql =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend\n%%sql\nSELECT col0, test(col1,col1), col2 , col6 FROM t1 "
        "limit 2;";
    Engine engine(catalog);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);

    DLOG(INFO) << "RUN IN MODE" << mode;
    std::vector<int8_t*> output;
    int32_t ret = -1;
    switch (mode) {
        case RUN:
            ret = session.Run(output, 10);
            break;
        case RUNBATCH:
            ret = session.RunBatch(output, 10);
            break;
        default: {
            ASSERT_TRUE(false);
        }
    }
    PrintSchema(session.GetSchema());
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, output.size());
    ::fesql::type::TableDef output_schema;
    for (auto column : session.GetSchema()) {
        ::fesql::type::ColumnDef* column_def = output_schema.add_columns();
        *column_def = column;
    }
    std::unique_ptr<storage::RowView> row_view =
        std::move(std::unique_ptr<storage::RowView>(
            new storage::RowView(output_schema.columns())));

    row_view->Reset(output[0]);
    {
        char* v;
        uint32_t size;
        row_view->GetString(0, &v, &size);
        ASSERT_EQ("0", std::string(v, size));
    }
    {
        int32_t v;
        row_view->GetInt32(1, &v);
        ASSERT_EQ(32 + 32 + 1, v);
    }
    {
        int16_t v;
        row_view->GetInt16(2, &v);
        ASSERT_EQ(16, v);
    }
    {
        char* v;
        uint32_t size;
        row_view->GetString(3, &v, &size);
        ASSERT_EQ("1", std::string(v, size));
    }

    row_view->Reset(output[1]);
    {
        char* v;
        uint32_t size;
        row_view->GetString(0, &v, &size);
        ASSERT_EQ("0", std::string(v, size));
    }
    {
        int32_t v;
        row_view->GetInt32(1, &v);
        ASSERT_EQ(32 + 32 + 1, v);
    }
    {
        int16_t v;
        row_view->GetInt16(2, &v);
        ASSERT_EQ(16, v);
    }
    {
        char* v;
        uint32_t size;
        row_view->GetString(3, &v, &size);
        ASSERT_EQ("1", std::string(v, size));
    }
    free(output[0]);
    free(output[1]);
}

TEST_F(EngineTest, test_window_agg) {
    type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    auto catalog = BuildCommonCatalog(table_def, table);
    int8_t* rows = NULL;
    std::vector<fesql::storage::Row> windows;
    BuildWindow(windows, &rows);
    StoreData(table.get(), rows);

    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;";
    Engine engine(catalog);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    PrintSchema(session.GetSchema());
    std::vector<int8_t*> output;
    int32_t ret = session.Run(output, 100);
    ASSERT_EQ(5, output.size());
    int8_t* output_1 = output[4];
    int8_t* output_22 = output[3];
    int8_t* output_333 = output[2];
    int8_t* output_4444 = output[1];
    int8_t* output_aaa = output[0];

    ASSERT_EQ(0, ret);
    ASSERT_EQ(5, output.size());

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_1 + 2)));
    ASSERT_EQ(1, *(reinterpret_cast<int32_t*>(output_1 + 7)));
    ASSERT_EQ(1.1f, *(reinterpret_cast<float*>(output_1 + 7 + 4)));
    ASSERT_EQ(11.1, *(reinterpret_cast<double*>(output_1 + 7 + 4 + 4)));
    ASSERT_EQ(5u, *(reinterpret_cast<int16_t*>(output_1 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L, *(reinterpret_cast<int64_t*>(output_1 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_22 + 2)));
    ASSERT_EQ(1 + 2, *(reinterpret_cast<int32_t*>(output_22 + 7)));
    ASSERT_EQ(1.1f + 2.2f, *(reinterpret_cast<float*>(output_22 + 7 + 4)));
    ASSERT_EQ(11.1 + 22.2, *(reinterpret_cast<double*>(output_22 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u,
              *(reinterpret_cast<int16_t*>(output_22 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_22 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_333 + 2)));
    ASSERT_EQ(3, *(reinterpret_cast<int32_t*>(output_333 + 7)));
    ASSERT_EQ(3.3f, *(reinterpret_cast<float*>(output_333 + 7 + 4)));
    ASSERT_EQ(33.3, *(reinterpret_cast<double*>(output_333 + 7 + 4 + 4)));
    ASSERT_EQ(55u, *(reinterpret_cast<int16_t*>(output_333 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L,
              *(reinterpret_cast<int64_t*>(output_333 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_4444 + 2)));
    ASSERT_EQ(3 + 4, *(reinterpret_cast<int32_t*>(output_4444 + 7)));
    ASSERT_EQ(3.3f + 4.4f, *(reinterpret_cast<float*>(output_4444 + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4,
              *(reinterpret_cast<double*>(output_4444 + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u,
              *(reinterpret_cast<int16_t*>(output_4444 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_4444 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_aaa + 2)));
    ASSERT_EQ(3 + 4 + 5, *(reinterpret_cast<int32_t*>(output_aaa + 7)));
    ASSERT_EQ(3.3f + 4.4f + 5.5f,
              *(reinterpret_cast<float*>(output_aaa + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4 + 55.5,
              *(reinterpret_cast<double*>(output_aaa + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u + 55u,
              *(reinterpret_cast<int16_t*>(output_aaa + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L + 3L,
              *(reinterpret_cast<int64_t*>(output_aaa + 7 + 4 + 4 + 8 + 2)));
    for (auto ptr : output) {
        free(ptr);
    }
}

TEST_F(EngineTest, test_window_agg_batch_run) {
    type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    auto catalog = BuildCommonCatalog(table_def, table);
    int8_t* rows = NULL;
    std::vector<fesql::storage::Row> windows;
    BuildWindow(windows, &rows);
    StoreData(table.get(), rows);
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;";
    Engine engine(catalog);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    std::vector<int8_t*> output;
    int32_t ret = session.RunBatch(output, 100);

    ASSERT_EQ(5, output.size());

    int8_t* output_333 = output[0];
    int8_t* output_4444 = output[1];
    int8_t* output_aaa = output[2];

    int8_t* output_1 = output[3];
    int8_t* output_22 = output[4];

    ASSERT_EQ(0, ret);
    ASSERT_EQ(5, output.size());

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_1 + 2)));
    ASSERT_EQ(1, *(reinterpret_cast<int32_t*>(output_1 + 7)));
    ASSERT_EQ(1.1f, *(reinterpret_cast<float*>(output_1 + 7 + 4)));
    ASSERT_EQ(11.1, *(reinterpret_cast<double*>(output_1 + 7 + 4 + 4)));
    ASSERT_EQ(5u, *(reinterpret_cast<int16_t*>(output_1 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L, *(reinterpret_cast<int64_t*>(output_1 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_22 + 2)));
    ASSERT_EQ(1 + 2, *(reinterpret_cast<int32_t*>(output_22 + 7)));
    ASSERT_EQ(1.1f + 2.2f, *(reinterpret_cast<float*>(output_22 + 7 + 4)));
    ASSERT_EQ(11.1 + 22.2, *(reinterpret_cast<double*>(output_22 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u,
              *(reinterpret_cast<int16_t*>(output_22 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_22 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_333 + 2)));
    ASSERT_EQ(3, *(reinterpret_cast<int32_t*>(output_333 + 7)));
    ASSERT_EQ(3.3f, *(reinterpret_cast<float*>(output_333 + 7 + 4)));
    ASSERT_EQ(33.3, *(reinterpret_cast<double*>(output_333 + 7 + 4 + 4)));
    ASSERT_EQ(55u, *(reinterpret_cast<int16_t*>(output_333 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L,
              *(reinterpret_cast<int64_t*>(output_333 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_4444 + 2)));
    ASSERT_EQ(3 + 4, *(reinterpret_cast<int32_t*>(output_4444 + 7)));
    ASSERT_EQ(3.3f + 4.4f, *(reinterpret_cast<float*>(output_4444 + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4,
              *(reinterpret_cast<double*>(output_4444 + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u,
              *(reinterpret_cast<int16_t*>(output_4444 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_4444 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_aaa + 2)));
    ASSERT_EQ(3 + 4 + 5, *(reinterpret_cast<int32_t*>(output_aaa + 7)));
    ASSERT_EQ(3.3f + 4.4f + 5.5f,
              *(reinterpret_cast<float*>(output_aaa + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4 + 55.5,
              *(reinterpret_cast<double*>(output_aaa + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u + 55u,
              *(reinterpret_cast<int16_t*>(output_aaa + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L + 3L,
              *(reinterpret_cast<int64_t*>(output_aaa + 7 + 4 + 4 + 8 + 2)));
    for (auto ptr : output) {
        free(ptr);
    }
}
TEST_F(EngineTest, test_window_agg_with_limit) {
    type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    auto catalog = BuildCommonCatalog(table_def, table);
    int8_t* rows = NULL;
    std::vector<fesql::storage::Row> windows;
    BuildWindow(windows, &rows);
    StoreData(table.get(), rows);
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 2;";
    Engine engine(catalog);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    std::vector<int8_t*> output;
    int32_t ret = session.Run(output, 100);

    ASSERT_EQ(2, output.size());

    int8_t* output_4444 = output[1];
    int8_t* output_aaa = output[0];

    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, output.size());

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_4444 + 2)));
    ASSERT_EQ(3 + 4, *(reinterpret_cast<int32_t*>(output_4444 + 7)));
    ASSERT_EQ(3.3f + 4.4f, *(reinterpret_cast<float*>(output_4444 + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4,
              *(reinterpret_cast<double*>(output_4444 + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u,
              *(reinterpret_cast<int16_t*>(output_4444 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_4444 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_aaa + 2)));
    ASSERT_EQ(3 + 4 + 5, *(reinterpret_cast<int32_t*>(output_aaa + 7)));
    ASSERT_EQ(3.3f + 4.4f + 5.5f,
              *(reinterpret_cast<float*>(output_aaa + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4 + 55.5,
              *(reinterpret_cast<double*>(output_aaa + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u + 55u,
              *(reinterpret_cast<int16_t*>(output_aaa + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L + 3L,
              *(reinterpret_cast<int64_t*>(output_aaa + 7 + 4 + 4 + 8 + 2)));
    for (auto ptr : output) {
        free(ptr);
    }
}

TEST_F(EngineTest, test_window_agg_with_limit_batch_run) {
    type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    auto catalog = BuildCommonCatalog(table_def, table);

    int8_t* rows = NULL;
    std::vector<fesql::storage::Row> windows;
    BuildWindow(windows, &rows);
    StoreData(table.get(), rows);
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 2;";
    Engine engine(catalog);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    std::vector<int8_t*> output;
    int32_t ret = session.RunBatch(output, 100);

    ASSERT_EQ(2, output.size());

    int8_t* output_333 = output[0];
    int8_t* output_4444 = output[1];

    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, output.size());

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_4444 + 2)));
    ASSERT_EQ(3 + 4, *(reinterpret_cast<int32_t*>(output_4444 + 7)));
    ASSERT_EQ(3.3f + 4.4f, *(reinterpret_cast<float*>(output_4444 + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4,
              *(reinterpret_cast<double*>(output_4444 + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u,
              *(reinterpret_cast<int16_t*>(output_4444 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_4444 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_333 + 2)));
    ASSERT_EQ(3, *(reinterpret_cast<int32_t*>(output_333 + 7)));
    ASSERT_EQ(3.3f, *(reinterpret_cast<float*>(output_333 + 7 + 4)));
    ASSERT_EQ(33.3, *(reinterpret_cast<double*>(output_333 + 7 + 4 + 4)));
    ASSERT_EQ(55u, *(reinterpret_cast<int16_t*>(output_333 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L,
              *(reinterpret_cast<int64_t*>(output_333 + 7 + 4 + 4 + 8 + 2)));
    for (auto ptr : output) {
        free(ptr);
    }
}
TEST_F(EngineTest, test_multi_windows_agg) {
    type::TableDef table_def;
    BuildTableDef(table_def);

    BuildTableDef(table_def);
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index1");
        index->add_first_keys("col2");
        index->set_second_key("col5");
    }
    {
        ::fesql::type::IndexDef* index = table_def.add_indexes();
        index->set_name("index2");
        index->add_first_keys("col1");
        index->set_second_key("col5");
    }

    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());

    int8_t* rows = NULL;
    std::vector<fesql::storage::Row> windows;
    BuildWindow(windows, &rows);
    StoreData(table.get(), rows);
    auto catalog = BuildCommonCatalog(table_def, table);
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;";
    Engine engine(catalog);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    std::vector<int8_t*> output;
    int32_t ret = session.Run(output, 10);
    int8_t* output_1 = output[4];
    int8_t* output_22 = output[3];
    int8_t* output_333 = output[2];
    int8_t* output_4444 = output[1];
    int8_t* output_aaa = output[0];

    ASSERT_EQ(0, ret);
    ASSERT_EQ(5, output.size());

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_1 + 2)));
    ASSERT_EQ(1, *(reinterpret_cast<int32_t*>(output_1 + 7)));
    ASSERT_EQ(1.1f, *(reinterpret_cast<float*>(output_1 + 7 + 4)));
    ASSERT_EQ(11.1, *(reinterpret_cast<double*>(output_1 + 7 + 4 + 4)));
    ASSERT_EQ(5u, *(reinterpret_cast<int16_t*>(output_1 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L, *(reinterpret_cast<int64_t*>(output_1 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_22 + 2)));
    ASSERT_EQ(1 + 2, *(reinterpret_cast<int32_t*>(output_22 + 7)));
    ASSERT_EQ(1.1f + 2.2f, *(reinterpret_cast<float*>(output_22 + 7 + 4)));
    ASSERT_EQ(11.1 + 22.2, *(reinterpret_cast<double*>(output_22 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u,
              *(reinterpret_cast<int16_t*>(output_22 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_22 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_333 + 2)));
    ASSERT_EQ(3, *(reinterpret_cast<int32_t*>(output_333 + 7)));
    ASSERT_EQ(3.3f, *(reinterpret_cast<float*>(output_333 + 7 + 4)));
    ASSERT_EQ(33.3, *(reinterpret_cast<double*>(output_333 + 7 + 4 + 4)));
    ASSERT_EQ(55u, *(reinterpret_cast<int16_t*>(output_333 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L,
              *(reinterpret_cast<int64_t*>(output_333 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_4444 + 2)));
    ASSERT_EQ(3 + 4, *(reinterpret_cast<int32_t*>(output_4444 + 7)));
    ASSERT_EQ(3.3f + 4.4f, *(reinterpret_cast<float*>(output_4444 + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4,
              *(reinterpret_cast<double*>(output_4444 + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u,
              *(reinterpret_cast<int16_t*>(output_4444 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_4444 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_aaa + 2)));
    ASSERT_EQ(3 + 4 + 5, *(reinterpret_cast<int32_t*>(output_aaa + 7)));
    ASSERT_EQ(3.3f + 4.4f + 5.5f,
              *(reinterpret_cast<float*>(output_aaa + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4 + 55.5,
              *(reinterpret_cast<double*>(output_aaa + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u + 55u,
              *(reinterpret_cast<int16_t*>(output_aaa + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L + 3L,
              *(reinterpret_cast<int64_t*>(output_aaa + 7 + 4 + 4 + 8 + 2)));
    for (auto ptr : output) {
        free(ptr);
    }
}

TEST_F(EngineTest, test_window_agg_unique_partition) {
    type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());

    int8_t* rows = NULL;
    std::vector<fesql::storage::Row> windows;
    BuildWindowUnique(windows, &rows);
    StoreData(table.get(), rows);
    auto catalog = BuildCommonCatalog(table_def, table);
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;";
    Engine engine(catalog);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    std::vector<int8_t*> output;
    int32_t ret = session.Run(output, 10);
    ASSERT_EQ(0, ret);
    int8_t* output_1 = output[4];
    int8_t* output_22 = output[3];
    int8_t* output_333 = output[2];
    int8_t* output_4444 = output[1];
    int8_t* output_aaa = output[0];

    ASSERT_EQ(0, ret);
    ASSERT_EQ(5, output.size());

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_1 + 2)));
    ASSERT_EQ(1, *(reinterpret_cast<int32_t*>(output_1 + 7)));
    ASSERT_FLOAT_EQ(1.1f, *(reinterpret_cast<float*>(output_1 + 7 + 4)));
    ASSERT_DOUBLE_EQ(11.1, *(reinterpret_cast<double*>(output_1 + 7 + 4 + 4)));
    ASSERT_EQ(5u, *(reinterpret_cast<int16_t*>(output_1 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L, *(reinterpret_cast<int64_t*>(output_1 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_22 + 2)));
    ASSERT_EQ(1 + 2, *(reinterpret_cast<int32_t*>(output_22 + 7)));
    ASSERT_FLOAT_EQ(1.1f + 2.2f,
                    *(reinterpret_cast<float*>(output_22 + 7 + 4)));
    ASSERT_DOUBLE_EQ(11.1 + 22.2,
                     *(reinterpret_cast<double*>(output_22 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u,
              *(reinterpret_cast<int16_t*>(output_22 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_22 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_333 + 2)));
    ASSERT_EQ(1 + 2 + 3, *(reinterpret_cast<int32_t*>(output_333 + 7)));
    ASSERT_FLOAT_EQ(1.1f + 2.2f + 3.3f,
                    *(reinterpret_cast<float*>(output_333 + 7 + 4)));
    ASSERT_DOUBLE_EQ(11.1 + 22.2 + 33.3,
                     *(reinterpret_cast<double*>(output_333 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u + 5u,
              *(reinterpret_cast<int16_t*>(output_333 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L + 3L,
              *(reinterpret_cast<int64_t*>(output_333 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_4444 + 2)));
    ASSERT_EQ(2 + 3 + 4, *(reinterpret_cast<int32_t*>(output_4444 + 7)));
    ASSERT_FLOAT_EQ(2.2f + 3.3f + 4.4f,
                    *(reinterpret_cast<float*>(output_4444 + 7 + 4)));
    ASSERT_DOUBLE_EQ(22.2 + 33.3 + 44.4,
                     *(reinterpret_cast<double*>(output_4444 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u + 5u,
              *(reinterpret_cast<int16_t*>(output_4444 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(2L + 3L + 4L,
              *(reinterpret_cast<int64_t*>(output_4444 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_aaa + 2)));
    ASSERT_EQ(3 + 4 + 5, *(reinterpret_cast<int32_t*>(output_aaa + 7)));
    ASSERT_FLOAT_EQ(3.3f + 4.4f + 5.5f,
                    *(reinterpret_cast<float*>(output_aaa + 7 + 4)));
    ASSERT_DOUBLE_EQ(33.3 + 44.4 + 55.5,
                     *(reinterpret_cast<double*>(output_aaa + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u + 5u,
              *(reinterpret_cast<int16_t*>(output_aaa + 7 + 4 + 4 + 8)));
    ASSERT_EQ(3L + 4L + 5L,
              *(reinterpret_cast<int64_t*>(output_aaa + 7 + 4 + 4 + 8 + 2)));

    for (auto ptr : output) {
        free(ptr);
    }
}

TEST_F(EngineTest, test_window_agg_unique_partition_batch_run) {
    type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col2");
    index->set_second_key("col5");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());

    int8_t* rows = NULL;
    std::vector<fesql::storage::Row> windows;
    BuildWindowUnique(windows, &rows);
    StoreData(table.get(), rows);
    auto catalog = BuildCommonCatalog(table_def, table);
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col2 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;";
    Engine engine(catalog);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    std::vector<int8_t*> output;
    int32_t ret = session.RunBatch(output, 10);
    ASSERT_EQ(0, ret);
    int8_t* output_1 = output[0];
    int8_t* output_22 = output[1];
    int8_t* output_333 = output[2];
    int8_t* output_4444 = output[3];
    int8_t* output_aaa = output[4];

    ASSERT_EQ(0, ret);
    ASSERT_EQ(5, output.size());

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_1 + 2)));
    ASSERT_EQ(1, *(reinterpret_cast<int32_t*>(output_1 + 7)));
    ASSERT_FLOAT_EQ(1.1f, *(reinterpret_cast<float*>(output_1 + 7 + 4)));
    ASSERT_DOUBLE_EQ(11.1, *(reinterpret_cast<double*>(output_1 + 7 + 4 + 4)));
    ASSERT_EQ(5u, *(reinterpret_cast<int16_t*>(output_1 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L, *(reinterpret_cast<int64_t*>(output_1 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_22 + 2)));
    ASSERT_EQ(1 + 2, *(reinterpret_cast<int32_t*>(output_22 + 7)));
    ASSERT_FLOAT_EQ(1.1f + 2.2f,
                    *(reinterpret_cast<float*>(output_22 + 7 + 4)));
    ASSERT_DOUBLE_EQ(11.1 + 22.2,
                     *(reinterpret_cast<double*>(output_22 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u,
              *(reinterpret_cast<int16_t*>(output_22 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_22 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_333 + 2)));
    ASSERT_EQ(1 + 2 + 3, *(reinterpret_cast<int32_t*>(output_333 + 7)));
    ASSERT_FLOAT_EQ(1.1f + 2.2f + 3.3f,
                    *(reinterpret_cast<float*>(output_333 + 7 + 4)));
    ASSERT_DOUBLE_EQ(11.1 + 22.2 + 33.3,
                     *(reinterpret_cast<double*>(output_333 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u + 5u,
              *(reinterpret_cast<int16_t*>(output_333 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L + 3L,
              *(reinterpret_cast<int64_t*>(output_333 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_4444 + 2)));
    ASSERT_EQ(2 + 3 + 4, *(reinterpret_cast<int32_t*>(output_4444 + 7)));
    ASSERT_FLOAT_EQ(2.2f + 3.3f + 4.4f,
                    *(reinterpret_cast<float*>(output_4444 + 7 + 4)));
    ASSERT_DOUBLE_EQ(22.2 + 33.3 + 44.4,
                     *(reinterpret_cast<double*>(output_4444 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u + 5u,
              *(reinterpret_cast<int16_t*>(output_4444 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(2L + 3L + 4L,
              *(reinterpret_cast<int64_t*>(output_4444 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_aaa + 2)));
    ASSERT_EQ(3 + 4 + 5, *(reinterpret_cast<int32_t*>(output_aaa + 7)));
    ASSERT_FLOAT_EQ(3.3f + 4.4f + 5.5f,
                    *(reinterpret_cast<float*>(output_aaa + 7 + 4)));
    ASSERT_DOUBLE_EQ(33.3 + 44.4 + 55.5,
                     *(reinterpret_cast<double*>(output_aaa + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u + 5u,
              *(reinterpret_cast<int16_t*>(output_aaa + 7 + 4 + 4 + 8)));
    ASSERT_EQ(3L + 4L + 5L,
              *(reinterpret_cast<int64_t*>(output_aaa + 7 + 4 + 4 + 8 + 2)));

    for (auto ptr : output) {
        free(ptr);
    }
}

TEST_F(EngineTest, test_window_agg_varchar_pk) {
    type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col0");
    index->set_second_key("col5");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());
    auto catalog = BuildCommonCatalog(table_def, table);
    int8_t* rows = NULL;
    std::vector<fesql::storage::Row> windows;
    BuildWindow(windows, &rows);
    StoreData(table.get(), rows);
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;";
    Engine engine(catalog);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    std::vector<int8_t*> output;
    int32_t ret = session.Run(output, 10);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(5, output.size());

    // pk:0 ts:2
    int8_t* output_22 = output[0];
    // pk:0 ts:1
    int8_t* output_1 = output[1];
    // pk:1 ts:2
    int8_t* output_4444 = output[2];
    // pk:1 ts:1
    int8_t* output_333 = output[3];
    // pk:2 ts:3
    int8_t* output_aaa = output[4];

    ASSERT_EQ(0, ret);
    ASSERT_EQ(5, output.size());

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_1 + 2)));
    ASSERT_EQ(1, *(reinterpret_cast<int32_t*>(output_1 + 7)));
    ASSERT_EQ(1.1f, *(reinterpret_cast<float*>(output_1 + 7 + 4)));
    ASSERT_EQ(11.1, *(reinterpret_cast<double*>(output_1 + 7 + 4 + 4)));
    ASSERT_EQ(5u, *(reinterpret_cast<int16_t*>(output_1 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L, *(reinterpret_cast<int64_t*>(output_1 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_22 + 2)));
    ASSERT_EQ(1 + 2, *(reinterpret_cast<int32_t*>(output_22 + 7)));
    ASSERT_EQ(1.1f + 2.2f, *(reinterpret_cast<float*>(output_22 + 7 + 4)));
    ASSERT_EQ(11.1 + 22.2, *(reinterpret_cast<double*>(output_22 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u,
              *(reinterpret_cast<int16_t*>(output_22 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_22 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_333 + 2)));
    ASSERT_EQ(3, *(reinterpret_cast<int32_t*>(output_333 + 7)));
    ASSERT_EQ(3.3f, *(reinterpret_cast<float*>(output_333 + 7 + 4)));
    ASSERT_EQ(33.3, *(reinterpret_cast<double*>(output_333 + 7 + 4 + 4)));
    ASSERT_EQ(55u, *(reinterpret_cast<int16_t*>(output_333 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L,
              *(reinterpret_cast<int64_t*>(output_333 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_4444 + 2)));
    ASSERT_EQ(3 + 4, *(reinterpret_cast<int32_t*>(output_4444 + 7)));
    ASSERT_EQ(3.3f + 4.4f, *(reinterpret_cast<float*>(output_4444 + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4,
              *(reinterpret_cast<double*>(output_4444 + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u,
              *(reinterpret_cast<int16_t*>(output_4444 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_4444 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_aaa + 2)));
    ASSERT_EQ(5, *(reinterpret_cast<int32_t*>(output_aaa + 7)));
    ASSERT_EQ(5.5f, *(reinterpret_cast<float*>(output_aaa + 7 + 4)));
    ASSERT_EQ(55.5, *(reinterpret_cast<double*>(output_aaa + 7 + 4 + 4)));
    ASSERT_EQ(55u, *(reinterpret_cast<int16_t*>(output_aaa + 7 + 4 + 4 + 8)));
    ASSERT_EQ(3L,
              *(reinterpret_cast<int64_t*>(output_aaa + 7 + 4 + 4 + 8 + 2)));
    for (auto ptr : output) {
        free(ptr);
    }
}

TEST_F(EngineTest, test_window_agg_varchar_pk_batch_run) {
    type::TableDef table_def;
    BuildTableDef(table_def);
    ::fesql::type::IndexDef* index = table_def.add_indexes();
    index->set_name("index1");
    index->add_first_keys("col0");
    index->set_second_key("col5");
    std::shared_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table(1, 1, table_def));
    ASSERT_TRUE(table->Init());

    int8_t* rows = NULL;
    std::vector<fesql::storage::Row> windows;
    BuildWindow(windows, &rows);
    StoreData(table.get(), rows);
    auto catalog = BuildCommonCatalog(table_def, table);
    const std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum, "
        "sum(col2) OVER w1 as w1_col2_sum, "
        "sum(col5) OVER w1 as w1_col5_sum "
        "FROM t1 WINDOW w1 AS (PARTITION BY col0 ORDER BY col5 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;";
    Engine engine(catalog);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    std::vector<int8_t*> output;
    int32_t ret = session.RunBatch(output, 10);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(5, output.size());

    // pk:0 ts:1
    int8_t* output_1 = output[0];
    // pk:0 ts:2
    int8_t* output_22 = output[1];

    // pk:1 ts:1
    int8_t* output_333 = output[2];
    // pk:1 ts:2
    int8_t* output_4444 = output[3];

    // pk:2 ts:3
    int8_t* output_aaa = output[4];

    ASSERT_EQ(0, ret);
    ASSERT_EQ(5, output.size());

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_1 + 2)));
    ASSERT_EQ(1, *(reinterpret_cast<int32_t*>(output_1 + 7)));
    ASSERT_EQ(1.1f, *(reinterpret_cast<float*>(output_1 + 7 + 4)));
    ASSERT_EQ(11.1, *(reinterpret_cast<double*>(output_1 + 7 + 4 + 4)));
    ASSERT_EQ(5u, *(reinterpret_cast<int16_t*>(output_1 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L, *(reinterpret_cast<int64_t*>(output_1 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_22 + 2)));
    ASSERT_EQ(1 + 2, *(reinterpret_cast<int32_t*>(output_22 + 7)));
    ASSERT_EQ(1.1f + 2.2f, *(reinterpret_cast<float*>(output_22 + 7 + 4)));
    ASSERT_EQ(11.1 + 22.2, *(reinterpret_cast<double*>(output_22 + 7 + 4 + 4)));
    ASSERT_EQ(5u + 5u,
              *(reinterpret_cast<int16_t*>(output_22 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_22 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_333 + 2)));
    ASSERT_EQ(3, *(reinterpret_cast<int32_t*>(output_333 + 7)));
    ASSERT_EQ(3.3f, *(reinterpret_cast<float*>(output_333 + 7 + 4)));
    ASSERT_EQ(33.3, *(reinterpret_cast<double*>(output_333 + 7 + 4 + 4)));
    ASSERT_EQ(55u, *(reinterpret_cast<int16_t*>(output_333 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L,
              *(reinterpret_cast<int64_t*>(output_333 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_4444 + 2)));
    ASSERT_EQ(3 + 4, *(reinterpret_cast<int32_t*>(output_4444 + 7)));
    ASSERT_EQ(3.3f + 4.4f, *(reinterpret_cast<float*>(output_4444 + 7 + 4)));
    ASSERT_EQ(33.3 + 44.4,
              *(reinterpret_cast<double*>(output_4444 + 7 + 4 + 4)));
    ASSERT_EQ(55u + 55u,
              *(reinterpret_cast<int16_t*>(output_4444 + 7 + 4 + 4 + 8)));
    ASSERT_EQ(1L + 2L,
              *(reinterpret_cast<int64_t*>(output_4444 + 7 + 4 + 4 + 8 + 2)));

    ASSERT_EQ(7 + 4 + 4 + 8 + 2 + 8,
              *(reinterpret_cast<int32_t*>(output_aaa + 2)));
    ASSERT_EQ(5, *(reinterpret_cast<int32_t*>(output_aaa + 7)));
    ASSERT_EQ(5.5f, *(reinterpret_cast<float*>(output_aaa + 7 + 4)));
    ASSERT_EQ(55.5, *(reinterpret_cast<double*>(output_aaa + 7 + 4 + 4)));
    ASSERT_EQ(55u, *(reinterpret_cast<int16_t*>(output_aaa + 7 + 4 + 4 + 8)));
    ASSERT_EQ(3L,
              *(reinterpret_cast<int64_t*>(output_aaa + 7 + 4 + 4 + 8 + 2)));
    for (auto ptr : output) {
        free(ptr);
    }
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
