/*
 * sql_compiler_test.cc
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

#include "vm/sql_compiler.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "gtest/gtest.h"
#include "vm/table_mgr.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"

using namespace llvm;
using namespace llvm::orc;

ExitOnError ExitOnErr;

namespace fesql {
namespace vm {

class TableMgrImpl : public TableMgr {

 public:
    TableMgrImpl(TableStatus* status):status_(status) {}
    ~TableMgrImpl() {}
    bool GetTableDef(const std::string&,
            const std::string&,
            TableStatus** status) {
        *status = status_;
        return true;
    }
    bool GetTableDef(const uint64_t, const uint64_t,
            TableStatus** table) {
        *table = status_;
        return true;
    }
 private:
    TableStatus* status_;
};

class SQLCompilerTest : public ::testing::Test {};

TEST_F(SQLCompilerTest, test_normal) {
    TableStatus status;
    status.table_def.set_name("t1");
    {
        ::fesql::type::ColumnDef* column = status.table_def.add_columns();
        column->set_type(::fesql::type::kInt32);
        column->set_name("col1");
    }
    {
        ::fesql::type::ColumnDef* column = status.table_def.add_columns();
        column->set_type(::fesql::type::kInt16);
        column->set_name("col2");
    }
    {
        ::fesql::type::ColumnDef* column = status.table_def.add_columns();
        column->set_type(::fesql::type::kFloat);
        column->set_name("col3");
    }

    {
        ::fesql::type::ColumnDef* column = status.table_def.add_columns();
        column->set_type(::fesql::type::kDouble);
        column->set_name("col4");
    }

    {
        ::fesql::type::ColumnDef* column = status.table_def.add_columns();
        column->set_type(::fesql::type::kInt64);
        column->set_name("col15");
    }
    const std::string sql = "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return d\nend\n%%sql\nSELECT test(col1,col1) FROM t1 limit 10;";
    TableMgrImpl table_mgr(&status);
    SQLCompiler sql_compiler(&table_mgr);
    SQLContext sql_context;
    sql_context.sql = sql;
    bool ok = sql_compiler.Compile(sql_context);
    ASSERT_TRUE(ok);
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
