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

#include "codegen/fn_let_ir_builder.h"
#include <memory>
#include <string>
#include <vector>
#include "codegen/fn_ir_builder.h"
#include "gtest/gtest.h"

#include "parser/parser.h"
#include "plan/planner.h"

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

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace codegen {

static node::NodeManager manager;
class FnLetIRBuilderTest : public ::testing::Test {
 public:
    FnLetIRBuilderTest() {}
    ~FnLetIRBuilderTest() {}
};

void AddFunc(const std::string& fn, ::llvm::Module* m) {
    ::fesql::node::NodePointVector trees;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::base::Status status;
    int ret = parser.parse(fn, trees, &manager, status);
    ASSERT_EQ(0, ret);
    FnIRBuilder fn_ir_builder(m);
    bool ok = fn_ir_builder.Build((node::FnNodeList*)trees[0]);
    ASSERT_TRUE(ok);
}

TEST_F(FnLetIRBuilderTest, test_udf) {
    ::fesql::type::TableDef table;
    table.set_name("t1");
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
        column->set_name("col15");
    }
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    const std::string test =
        "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return "
        "d\nend";
    AddFunc(test, m.get());
    int ret = parser.parse("SELECT test(col1,col1) FROM t1 limit 10;", list,
                           &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);
    ::fesql::node::PlanNodeList trees;
    ret = planner.CreatePlanTree(list, trees, status);
    ASSERT_EQ(0, ret);
    ::fesql::node::ProjectListPlanNode* pp_node_ptr =
        (::fesql::node::ProjectListPlanNode*)(trees[0]
                                                  ->GetChildren()[0]
                                                  ->GetChildren()[0]);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    RowFnLetIRBuilder ir_builder(&table, m.get());
    std::vector<::fesql::type::ColumnDef> schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1u, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));
    int32_t (*decode)(int8_t*, int32_t, int8_t*) =
        (int32_t(*)(int8_t*, int32_t, int8_t*))load_fn_jit.getAddress();
    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    int32_t i = 0;
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
    *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
    *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
    int32_t ret2 = decode(ptr, 28, reinterpret_cast<int8_t*>(&i));
    ASSERT_EQ(ret2, 0u);
    ASSERT_EQ(i, 3u);
    free(ptr);
}

TEST_F(FnLetIRBuilderTest, test_project) {
    ::fesql::type::TableDef table;
    table.set_name("t1");
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
        column->set_name("col15");
    }
    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret =
        parser.parse("SELECT col1 FROM t1 limit 10;", list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);

    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    ::fesql::node::ProjectListPlanNode* pp_node_ptr =
        (::fesql::node::ProjectListPlanNode*)(plan[0]
                                                  ->GetChildren()[0]
                                                  ->GetChildren()[0]);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    RowFnLetIRBuilder ir_builder(&table, m.get());
    std::vector<::fesql::type::ColumnDef> schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1u, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));

    int32_t (*decode)(int8_t*, int32_t, int8_t*) =
        (int32_t(*)(int8_t*, int32_t, int8_t*))load_fn_jit.getAddress();

    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    int32_t i = 0;
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
    *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
    *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
    int32_t ret2 = decode(ptr, 28, reinterpret_cast<int8_t*>(&i));
    ASSERT_EQ(ret2, 0u);
    ASSERT_EQ(i, 1u);
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
