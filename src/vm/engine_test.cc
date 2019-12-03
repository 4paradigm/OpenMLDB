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
#include "vm/table_mgr.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace fesql {
namespace vm {

class TableMgrImpl : public TableMgr {
 public:
    explicit TableMgrImpl(std::shared_ptr<TableStatus> status)
        : status_(status) {}
    ~TableMgrImpl() {}
    std::shared_ptr<TableStatus> GetTableDef(const std::string&,
                                             const std::string&) {
        return status_;
    }
    std::shared_ptr<TableStatus> GetTableDef(const std::string&,
                                             const uint32_t) {
        return status_;
    }

 private:
    std::shared_ptr<TableStatus> status_;
};

class EngineTest : public ::testing::Test {};

TEST_F(EngineTest, test_normal) {
    std::unique_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table("t1", 1, 1, 1));
    ASSERT_TRUE(table->Init());
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
    *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
    *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;

    ASSERT_TRUE(table->Put("k1", 1, reinterpret_cast<char*>(ptr), 28));
    ASSERT_TRUE(table->Put("k1", 2, reinterpret_cast<char*>(ptr), 28));
    std::shared_ptr<TableStatus> status(new TableStatus());
    status->table = std::move(table);
    status->table_def.set_name("t1");
    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }

    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col15");
    }

    TableMgrImpl table_mgr(status);
    const std::string sql =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend\n%%sql\nSELECT test(col1,col1), col2 FROM t1 limit 10;";
    Engine engine(&table_mgr);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    const uint32_t length = session.GetRowSize();
    std::vector<int8_t*> output;
    int32_t ret = session.Run(output, 2);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(2, output.size());
    ASSERT_EQ(length, 8);
    int8_t* output1 = output[0];
    int8_t* output2 = output[1];
    ASSERT_EQ(3, *(reinterpret_cast<int32_t*>(output1 + 2)));
    ASSERT_EQ(2, *(reinterpret_cast<int16_t*>(output1 + 6)));
    ASSERT_EQ(3, *(reinterpret_cast<int32_t*>(output2 + 2)));
    free(output1);
    free(output2);
}

TEST_F(EngineTest, test_window_agg) {
    std::unique_ptr<::fesql::storage::Table> table(
        new ::fesql::storage::Table("t1", 1, 1, 1));
    ASSERT_TRUE(table->Init());
    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 5;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 1.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 11.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 1;
        ASSERT_TRUE(table->Put("5", 1, reinterpret_cast<char*>(ptr), 28));
    }
    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 2;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 5;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 2.2f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 22.2;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 2;
        ASSERT_TRUE(table->Put("5", 2, reinterpret_cast<char*>(ptr), 28));
    }

    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 3;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 55;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.3f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 33.3;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 1;
        ASSERT_TRUE(table->Put("55", 1, reinterpret_cast<char*>(ptr), 28));
    }
    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 4;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 55;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 4.4f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.4;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 2;
        ASSERT_TRUE(table->Put("55", 2, reinterpret_cast<char*>(ptr), 28));
    }
    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 5;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 55;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 5.5f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 55.5;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 3;
        ASSERT_TRUE(table->Put("55", 3, reinterpret_cast<char*>(ptr), 28));
    }

    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 6;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 55;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 6.6f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 66.6;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 4;
        ASSERT_TRUE(table->Put("55", 4, reinterpret_cast<char*>(ptr), 28));
    }
    std::shared_ptr<TableStatus> status(new TableStatus());
    status->table = std::move(table);
    status->table_def.set_name("t1");
    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }

    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = status->table_def.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col15");
    }

    TableMgrImpl table_mgr(status);
    const std::string sql =
        "SELECT sum(col1) OVER w1 as w1_col1_sum, sum(col1) OVER w1 as "
        "w1_col1_sum2 FROM t1 "
        "WINDOW w1 AS (PARTITION BY col2 ORDER BY col15 ROWS BETWEEN 3 "
        "PRECEDING AND CURRENT ROW) limit 10;";
    Engine engine(&table_mgr);
    RunSession session;
    base::Status get_status;
    bool ok = engine.Get(sql, "db", session, get_status);
    ASSERT_TRUE(ok);
    const uint32_t length = session.GetRowSize();
    std::vector<int8_t*> output;
    int32_t ret = session.Run(output, 10);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(6, output.size());
    ASSERT_EQ(length, 10);
    //    ASSERT_EQ(1, *((int32_t*)(output[0] + 2)));
    ASSERT_EQ(1 + 2, *(reinterpret_cast<int32_t*>(output[1] + 2)));
    ASSERT_EQ(3 + 4 + 5 + 6, *(reinterpret_cast<int32_t*>(output[2] + 2)));
    //    ASSERT_EQ(3+4, *((int32_t*)(output[3] + 2)));
    //    ASSERT_EQ(3+4+5, *((int32_t*)(output[4] + 2)));
    //    ASSERT_EQ(4+5+6, *((int32_t*)(output[5] + 2)));
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
