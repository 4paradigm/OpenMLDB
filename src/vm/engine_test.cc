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

    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    *reinterpret_cast<int32_t*>(ptr + 2) = 1;
    *reinterpret_cast<int16_t*>(ptr + 2 + 4) = 2;
    *reinterpret_cast<float*>(ptr + 2 + 4 + 2) = 3.1f;
    *reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4) = 4.1;
    *reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8) = 5;

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
    ASSERT_EQ(3, *reinterpret_cast<int32_t*>(output1 + 2));
    ASSERT_EQ(2, *reinterpret_cast<int16_t*>(output1 + 6));
    ASSERT_EQ(3, *reinterpret_cast<int32_t*>(output2 + 2));
    free(output1);
    free(output2);
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
