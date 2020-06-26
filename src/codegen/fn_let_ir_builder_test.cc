/*
 * fn_let_ir_builder_test.cc
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
#include "codegen/fn_let_ir_builder_test.h"
#include <memory>
#include <string>
#include <vector>
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "codegen/codegen_base_test.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/fn_let_ir_builder.h"
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
#include "udf/udf.h"
#include "vm/jit.h"
#include "vm/sql_compiler.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

namespace fesql {
namespace codegen {
using fesql::codec::ArrayListV;
using fesql::codec::Row;
static node::NodeManager manager;

class FnLetIRBuilderTest : public ::testing::Test {
 public:
    FnLetIRBuilderTest() {}
    ~FnLetIRBuilderTest() {}
};

TEST_F(FnLetIRBuilderTest, test_primary) {
    // Create an LLJIT instance.
    std::string sql = "SELECT col1, col6, 1.0, \"hello\"  FROM t1 limit 10;";

    int8_t* buf = NULL;
    uint32_t size = 0;
    fesql::type::TableDef table1;
    BuildT1Buf(table1, &buf, &size);
    int8_t* output = NULL;
    int8_t* row_ptrs[1] = {buf};
    int8_t* window_ptr = nullptr;
    int32_t row_sizes[1] = {static_cast<int32_t>(size)};
    vm::Schema schema;
    vm::ColumnSourceList column_sources;
    node::NodeManager manager;
    CheckFnLetBuilder(&manager, table1, "", sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &column_sources, &output);
    uint32_t out_size = *reinterpret_cast<uint32_t*>(output + 2);
    ASSERT_EQ(4, schema.size());
    ASSERT_EQ(4, column_sources.size());

    ASSERT_EQ(vm::kSourceColumn, column_sources[0].type());
    ASSERT_EQ(0, column_sources[0].column_idx());
    ASSERT_EQ(0, column_sources[0].schema_idx());

    ASSERT_EQ(vm::kSourceColumn, column_sources[1].type());
    ASSERT_EQ(5, column_sources[1].column_idx());
    ASSERT_EQ(0, column_sources[1].schema_idx());

    ASSERT_EQ(vm::kSourceConst, column_sources[2].type());
    ASSERT_EQ(1.0, column_sources[2].const_value()->GetDouble());

    ASSERT_EQ(vm::kSourceConst, column_sources[3].type());
    ASSERT_EQ("hello", column_sources[3].const_value()->GetExprString());

    ASSERT_EQ(out_size, 27u);
    ASSERT_EQ(32u, *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(1.0, *reinterpret_cast<double*>(output + 11));
    ASSERT_EQ(21u, *reinterpret_cast<uint8_t*>(output + 19));
    ASSERT_EQ(22u, *reinterpret_cast<uint8_t*>(output + 20));
    std::string str(reinterpret_cast<char*>(output + 21), 1);
    ASSERT_EQ("1", str);
    std::string str2(reinterpret_cast<char*>(output + 22), 5);
    ASSERT_EQ("hello", str2);
    free(buf);
}
TEST_F(FnLetIRBuilderTest, test_multi_row_simple_query) {
    // Create an LLJIT instance.
    std::string sql =
        "SELECT t1.col1 as t1_col1, col6 as col6, t2.col5 as "
        "t2_col5, "
        "t1.col4+t2.col4 as add_col4 FROM t1,t2 limit 10;";

    // Create the add1 function entry and insert this entry into module M. The
    // function will have a return type of "int" and take an argument of "int".
    fesql::type::TableDef table1;
    fesql::type::TableDef table2;

    int8_t* buf = NULL;
    int8_t* buf2 = NULL;
    uint32_t size = 0;
    uint32_t size2 = 0;
    BuildT1Buf(table1, &buf, &size);
    BuildT2Buf(table2, &buf2, &size2);
    int8_t* output = NULL;

    int8_t* row_ptrs[2] = {buf, buf2};
    int8_t* window_ptr = nullptr;
    int32_t row_sizes[2] = {static_cast<int32_t>(size),
                            static_cast<int32_t>(size2)};
    vm::Schema schema;
    vm::ColumnSourceList column_sources;
    node::NodeManager manager;

    vm::SchemaSourceList name_schema_list;
    name_schema_list.AddSchemaSource(table1.name(), &table1.columns());
    name_schema_list.AddSchemaSource(table2.name(), &table2.columns());
    CheckFnLetBuilder(&manager, name_schema_list, "", sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &column_sources, &output);

    ASSERT_EQ(4, schema.size());
    uint32_t out_size = *reinterpret_cast<uint32_t*>(output + 2);
    ASSERT_EQ(out_size, 29u);
    vm::RowView row_view(schema);
    row_view.Reset(output);
    {
        int32_t value;
        ASSERT_EQ(0, row_view.GetInt32(0, &value));
        ASSERT_EQ(32, value);
    }
    {
        char* buf;
        uint32_t size;
        ASSERT_EQ(0, row_view.GetString(1, &buf, &size));
        ASSERT_EQ("1", std::string(buf, size));
    }
    {
        int64_t value;
        ASSERT_EQ(0, row_view.GetInt64(2, &value));
        ASSERT_EQ(640, 640);
    }
    {
        double value;
        ASSERT_EQ(0, row_view.GetDouble(3, &value));
        ASSERT_EQ(3.1 + 33.1, value);
    }
    free(buf);
    free(buf2);
}

TEST_F(FnLetIRBuilderTest, test_udf) {
    // Create an LLJIT instance.
    const std::string udf_sql =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend";
    const std::string sql = "SELECT test(col1,col1), col6 FROM t1 limit 10;";
    int8_t* buf = NULL;
    uint32_t size = 0;
    type::TableDef table1;
    BuildT1Buf(table1, &buf, &size);
    int8_t* output = NULL;
    int8_t* row_ptrs[1] = {buf};
    int8_t* window_ptr = nullptr;
    int32_t row_sizes[1] = {static_cast<int32_t>(size)};
    vm::Schema schema;
    node::NodeManager manager;
    CheckFnLetBuilder(&manager, table1, udf_sql, sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &output);
    ASSERT_EQ(2, schema.size());
    uint32_t out_size = *reinterpret_cast<uint32_t*>(output + 2);
    ASSERT_EQ(out_size, 13u);
    ASSERT_EQ(65u, *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(12u, *reinterpret_cast<uint8_t*>(output + 11));
    std::string str(reinterpret_cast<char*>(output + 12), 1);
    ASSERT_EQ("1", str);
    free(buf);
}

TEST_F(FnLetIRBuilderTest, test_simple_project) {
    std::string sql = "SELECT col1 FROM t1 limit 10;";
    int8_t* ptr = NULL;
    uint32_t size = 0;
    type::TableDef table1;
    BuildT1Buf(table1, &ptr, &size);
    int8_t* output = NULL;
    int8_t* row_ptrs[1] = {ptr};
    int8_t* window_ptr = nullptr;
    int32_t row_sizes[1] = {static_cast<int32_t>(size)};
    vm::Schema schema;
    node::NodeManager manager;
    CheckFnLetBuilder(&manager, table1, "", sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &output);
    ASSERT_EQ(1, schema.size());
    ASSERT_EQ(11u, *reinterpret_cast<uint32_t*>(output + 2));
    ASSERT_EQ(32u, *reinterpret_cast<uint32_t*>(output + 7));
    free(ptr);
}

TEST_F(FnLetIRBuilderTest, test_extern_udf_project) {
    std::string sql = "SELECT inc(col1) FROM t1 limit 10;";
    int8_t* ptr = NULL;
    uint32_t size = 0;
    type::TableDef table1;
    BuildT1Buf(table1, &ptr, &size);
    int8_t* output = NULL;
    int8_t* row_ptrs[1] = {ptr};
    int8_t* window_ptr = nullptr;
    int32_t row_sizes[1] = {static_cast<int32_t>(size)};
    vm::Schema schema;
    node::NodeManager manager;
    CheckFnLetBuilder(&manager, table1, "", sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &output);
    ASSERT_EQ(1, schema.size());
    ASSERT_EQ(11u, *reinterpret_cast<uint32_t*>(output + 2));
    ASSERT_EQ(33u, *reinterpret_cast<uint32_t*>(output + 7));
    free(ptr);
}


TEST_F(FnLetIRBuilderTest, test_extern_agg_sum_project) {
    std::string sql =
        "SELECT "
        "sum(col1) OVER w1 as w1_col1_sum , "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "sum(col4) OVER w1 as w1_col4_sum,  "
        "sum(col2) OVER w1 as w1_col2_sum,  "
        "sum(col5) OVER w1 as w1_col5_sum  "
        "FROM t1 WINDOW "
        "w1 AS (PARTITION BY COL2 ORDER BY `TS` ROWS_RANGE BETWEEN 3 PRECEDING "
        "AND "
        "3 "
        "FOLLOWING) limit 10;";

    int8_t* ptr = NULL;
    std::vector<Row> window;
    type::TableDef table1;
    BuildWindow(table1, window, &ptr);
    int8_t* output = NULL;
    int8_t* row_ptrs[1] = {window.back().buf()};
    int8_t* window_ptr = ptr;
    int32_t row_sizes[1] = {static_cast<int32_t>(window.back().size())};
    vm::Schema schema;
    node::NodeManager manager;
    CheckFnLetBuilder(&manager, table1, "", sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &output);
    ASSERT_EQ(1u + 11u + 111u + 1111u + 11111u,
              *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(3.1f + 33.1f + 333.1f + 3333.1f + 33333.1f,
              *reinterpret_cast<float*>(output + 7 + 4));
    ASSERT_EQ(4.1 + 44.1 + 444.1 + 4444.1 + 44444.1,
              *reinterpret_cast<double*>(output + 7 + 4 + 4));
    ASSERT_EQ(2 + 22 + 222 + 2222 + 22222,
              *reinterpret_cast<int16_t*>(output + 7 + 4 + 4 + 8));
    ASSERT_EQ(5L + 55L + 555L + 5555L + 55555L,
              *reinterpret_cast<int64_t*>(output + 7 + 4 + 4 + 8 + 2));
    free(ptr);
}

TEST_F(FnLetIRBuilderTest, test_simple_window_project_mix) {
    std::string sql =
        "SELECT "
        "col1, "
        "sum(col1) OVER w1 as w1_col1_sum , "
        "col3, "
        "sum(col3) OVER w1 as w1_col3_sum, "
        "col4, "
        "sum(col4) OVER w1 as w1_col4_sum,  "
        "sum(col2) OVER w1 as w1_col2_sum,  "
        "sum(col5) OVER w1 as w1_col5_sum  "
        "FROM t1 WINDOW w1 AS (PARTITION BY COL2 ORDER BY `TS` RANGE BETWEEN 3 "
        "PRECEDING AND 3 FOLLOWING) limit 10;";

    int8_t* ptr = NULL;
    std::vector<Row> window;
    type::TableDef table1;
    BuildWindow(table1, window, &ptr);
    int8_t* output = NULL;
    int8_t* row_ptrs[1] = {window.back().buf()};
    int8_t* window_ptr = ptr;
    int32_t row_sizes[1] = {static_cast<int32_t>(window.back().size())};
    vm::Schema schema;
    node::NodeManager manager;
    CheckFnLetBuilder(&manager, table1, "", sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &output);
    ASSERT_EQ(11111u, *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(1u + 11u + 111u + 1111u + 11111u,
              *reinterpret_cast<uint32_t*>(output + 7 + 4));
    ASSERT_EQ(33333.1f, *reinterpret_cast<float*>(output + 7 + 4 + 4));
    ASSERT_EQ(3.1f + 33.1f + 333.1f + 3333.1f + 33333.1f,
              *reinterpret_cast<float*>(output + 7 + 4 + 4 + 4));
    ASSERT_EQ(44444.1, *reinterpret_cast<double*>(output + 7 + 4 + 4 + 4 + 4));
    ASSERT_EQ(4.1 + 44.1 + 444.1 + 4444.1 + 44444.1,
              *reinterpret_cast<double*>(output + 7 + 4 + 4 + 4 + 4 + 8));
    ASSERT_EQ(2 + 22 + 222 + 2222 + 22222,
              *reinterpret_cast<int16_t*>(output + 7 + 4 + 4 + 4 + 4 + 8 + 8));
    ASSERT_EQ(
        5L + 55L + 555L + 5555L + 55555L,
        *reinterpret_cast<int64_t*>(output + 7 + 4 + 4 + 4 + 4 + 8 + 8 + 2));
    free(ptr);
}

TEST_F(FnLetIRBuilderTest, test_join_window_project_mix) {
    std::string sql =
        "SELECT "
        "t1.col1, "
        "sum(t1.col1) OVER w1 as w1_col1_sum , "
        "t1.col3, "
        "sum(t1.col3) OVER w1 as w1_col3_sum, "
        "t2.col4, "
        "sum(t2.col4) OVER w1 as w1_col4_sum,  "
        "sum(t2.col2) OVER w1 as w1_col2_sum,  "
        "sum(t1.col5) OVER w1 as w1_col5_sum  "
        "FROM t1 last join t2 order by t2.col5 on t1.col1=t2.col1 "
        "WINDOW w1 AS (PARTITION BY COL2 ORDER BY `TS` RANGE BETWEEN 3 "
        "PRECEDING AND 3 FOLLOWING) limit 10;";

    int8_t* ptr = NULL;
    std::vector<Row> window;
    type::TableDef table1;
    BuildWindow(table1, window, &ptr);

    int8_t* ptr2 = NULL;
    std::vector<Row> window2;
    type::TableDef table2;
    BuildWindow2(table2, window2, &ptr2);

    ASSERT_EQ(window.size(), window2.size());
    std::vector<Row> window_t12;

    for (size_t i = 0; i < window.size(); i++) {
        window_t12.push_back(Row(1, window[i], 1, window2[i]));
    }

    ArrayListV<Row>* w = new ArrayListV<Row>(&window_t12);
    int8_t* output = NULL;
    int8_t** row_ptrs = window_t12.back().GetRowPtrs();
    int8_t* window_ptr = reinterpret_cast<int8_t*>(w);
    int32_t* row_sizes = window_t12.back().GetRowSizes();
    vm::Schema schema;

    vm::SchemaSourceList name_schema_list;
    name_schema_list.AddSchemaSource(table1.name(), &table1.columns());
    name_schema_list.AddSchemaSource(table2.name(), &table2.columns());

    vm::ColumnSourceList column_sources;
    node::NodeManager manager;
    CheckFnLetBuilder(&manager, name_schema_list, "", sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &column_sources, &output);

    ASSERT_EQ(8u, column_sources.size());
    {
        // t1.col1
        ASSERT_EQ(vm::kSourceColumn, column_sources[0].type());
        ASSERT_EQ(0, column_sources[0].column_idx());
        ASSERT_EQ(0, column_sources[0].schema_idx());
    }

    // sum(t1.col1)
    { ASSERT_EQ(vm::kSourceNone, column_sources[1].type()); }

    // t1.col3
    {
        ASSERT_EQ(vm::kSourceColumn, column_sources[2].type());
        ASSERT_EQ(2, column_sources[2].column_idx());
        ASSERT_EQ(0, column_sources[2].schema_idx());
    }
    // sum(t1.col3)
    { ASSERT_EQ(vm::kSourceNone, column_sources[3].type()); }

    // t2.col4
    {
        ASSERT_EQ(vm::kSourceColumn, column_sources[4].type());
        ASSERT_EQ(3, column_sources[4].column_idx());
        ASSERT_EQ(1, column_sources[4].schema_idx());
    }
    // sum(t2.col4)
    { ASSERT_EQ(vm::kSourceNone, column_sources[5].type()); }
    // sum(t2.col2)
    { ASSERT_EQ(vm::kSourceNone, column_sources[6].type()); }
    // sum(t1.col5)
    { ASSERT_EQ(vm::kSourceNone, column_sources[7].type()); }

    // t1.col1
    ASSERT_EQ(11111u, *reinterpret_cast<uint32_t*>(output + 7));
    // sum(t1.col1)
    ASSERT_EQ(1u + 11u + 111u + 1111u + 11111u,
              *reinterpret_cast<uint32_t*>(output + 7 + 4));
    // t1.col3
    ASSERT_EQ(33333.1f, *reinterpret_cast<float*>(output + 7 + 4 + 4));
    // sum(t1.col3)
    ASSERT_EQ(3.1f + 33.1f + 333.1f + 3333.1f + 33333.1f,
              *reinterpret_cast<float*>(output + 7 + 4 + 4 + 4));

    ASSERT_EQ(55.5, *reinterpret_cast<double*>(output + 7 + 4 + 4 + 4 + 4));
    ASSERT_EQ(11.1 + 22.2 + 33.3 + 44.4 + 55.5,
              *reinterpret_cast<double*>(output + 7 + 4 + 4 + 4 + 4 + 8));
    ASSERT_EQ(55 + 55 + 55 + 5 + 5,
              *reinterpret_cast<int16_t*>(output + 7 + 4 + 4 + 4 + 4 + 8 + 8));
    ASSERT_EQ(
        5L + 55L + 555L + 5555L + 55555L,
        *reinterpret_cast<int64_t*>(output + 7 + 4 + 4 + 4 + 4 + 8 + 8 + 2));
    free(ptr);
}

TEST_F(FnLetIRBuilderTest, test_extern_agg_min_project) {
    std::string sql =
        "SELECT "
        "min(col1) OVER w1 as w1_col1_min , "
        "min(col3) OVER w1 as w1_col3_min, "
        "min(col4) OVER w1 as w1_col4_min,  "
        "min(col2) OVER w1 as w1_col2_min,  "
        "min(col5) OVER w1 as w1_col5_min  "
        "FROM t1 WINDOW w1 AS (PARTITION BY COL2 ORDER BY `TS` RANGE BETWEEN 3 "
        "PRECEDING AND 3 FOLLOWING) limit 10;";
    int8_t* ptr = NULL;
    std::vector<Row> window;
    type::TableDef table1;
    BuildWindow(table1, window, &ptr);
    int8_t* output = NULL;
    int8_t* row_ptrs[1] = {window.back().buf()};
    int8_t* window_ptr = ptr;
    int32_t row_sizes[1] = {static_cast<int32_t>(window.back().size())};
    vm::Schema schema;
    node::NodeManager manager;
    CheckFnLetBuilder(&manager, table1, "", sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &output);
    ASSERT_EQ(7u + 4u + 4u + 8u + 2u + 8u,
              *reinterpret_cast<uint32_t*>(output + 2));
    ASSERT_EQ(1u, *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(3.1f, *reinterpret_cast<float*>(output + 7 + 4));
    ASSERT_EQ(4.1, *reinterpret_cast<double*>(output + 7 + 4 + 4));
    ASSERT_EQ(2, *reinterpret_cast<int16_t*>(output + 7 + 4 + 4 + 8));
    ASSERT_EQ(5L, *reinterpret_cast<int64_t*>(output + 7 + 4 + 4 + 8 + 2));
    free(ptr);
}
TEST_F(FnLetIRBuilderTest, test_extern_agg_max_project) {
    std::string sql =
        "SELECT "
        "max(col1) OVER w1 as w1_col1_max , "
        "max(col3) OVER w1 as w1_col3_max, "
        "max(col4) OVER w1 as w1_col4_max,  "
        "max(col2) OVER w1 as w1_col2_max,  "
        "max(col5) OVER w1 as w1_col5_max  "
        "FROM t1 WINDOW "
        "w1 AS (PARTITION BY COL2 ORDER BY `TS` RANGE BETWEEN 3 PRECEDING AND "
        "3 "
        "FOLLOWING) limit 10;";

    int8_t* ptr = NULL;
    std::vector<Row> window;
    type::TableDef table1;
    BuildWindow(table1, window, &ptr);
    int8_t* output = NULL;
    int8_t* row_ptrs[1] = {window.back().buf()};
    int8_t* window_ptr = ptr;
    int32_t row_sizes[1] = {static_cast<int32_t>(window.back().size())};
    vm::Schema schema;
    node::NodeManager manager;
    CheckFnLetBuilder(&manager, table1, "", sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &output);
    ASSERT_EQ(7u + 4u + 4u + 8u + 2u + 8u,
              *reinterpret_cast<uint32_t*>(output + 2));
    ASSERT_EQ(11111u, *reinterpret_cast<uint32_t*>(output + 7));
    ASSERT_EQ(33333.1f, *reinterpret_cast<float*>(output + 7 + 4));
    ASSERT_EQ(44444.1, *reinterpret_cast<double*>(output + 7 + 4 + 4));
    ASSERT_EQ(22222, *reinterpret_cast<int16_t*>(output + 7 + 4 + 4 + 8));
    ASSERT_EQ(55555L, *reinterpret_cast<int64_t*>(output + 7 + 4 + 4 + 8 + 2));
    free(ptr);
}

TEST_F(FnLetIRBuilderTest, test_col_at_udf) {
    std::string udf_str =
        "%%fun\n"
        "def test_at(col:list<float>, pos:i32):float\n"
        "\treturn col[pos]\n"
        "end\n"
        "def test_at(col:list<int>, pos:i32):i32\n"
        "\treturn col[pos]\n"
        "end\n"
        "def test_add(x:i32, y:i32):i32\n"
        "\treturn x+y\n"
        "end\n"
        "def count_list(col:list<float>, pos:i32):i32\n"
        "\tquery_value=test_at(col,pos)\n"
        "\tcnt = 0\n"
        "\tfor x in col\n"
        "\t\tif query_value >= x\n"
        "\t\t\tcnt += 1\n"
        "\treturn cnt\n"
        "end\n";
    std::string sql =
        "SELECT "
        "test_at(col3,0) OVER w1 as col3_at_0, "
        "test_at(col1,1) OVER w1 as col1_at_1, "
        "count_list(col3,2) OVER w1 as col3_at_1 "
        "FROM t1 WINDOW "
        "w1 AS (PARTITION BY COL2 ORDER BY `TS` RANGE BETWEEN 3 PRECEDING AND "
        "3 "
        "FOLLOWING) limit 10;";

    int8_t* ptr = NULL;
    std::vector<Row> window;
    type::TableDef table1;
    BuildWindow(table1, window, &ptr);
    int8_t* output = NULL;
    int8_t* row_ptrs[1] = {window.back().buf()};
    int8_t* window_ptr = ptr;
    int32_t row_sizes[1] = {static_cast<int32_t>(window.back().size())};
    vm::Schema schema;
    node::NodeManager manager;
    CheckFnLetBuilder(&manager, table1, udf_str, sql, row_ptrs, window_ptr,
                      row_sizes, &schema, &output);
    ASSERT_EQ(3, schema.size());
    ASSERT_EQ(7u + 4u + 4u + 4u, *reinterpret_cast<uint32_t*>(output + 2));
    ASSERT_EQ(3.1f, *reinterpret_cast<float*>(output + 7));
    ASSERT_EQ(11, *reinterpret_cast<int32_t*>(output + 7 + 4));
    ASSERT_EQ(3, *reinterpret_cast<int32_t*>(output + 7 + 4 + 4));
    free(ptr);
}
}  // namespace codegen
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
