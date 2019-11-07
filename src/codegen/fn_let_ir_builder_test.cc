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
#include "gtest/gtest.h"

#include "parser/parser.h"
#include "plan/planner.h"

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
namespace codegen {

class FnLetIRBuilderTest : public ::testing::Test {

public:
     FnLetIRBuilderTest() {}
    ~FnLetIRBuilderTest() {}
};

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
    int ret = parser.parse("SELECT col1 FROM t1 limit 10;", list, &manager);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);
    ::fesql::node::PlanNode *plan_ptr = planner.CreatePlan(list[0]);
    ::fesql::node::ProjectListPlanNode* pp_node_ptr 
        = (::fesql::node::ProjectListPlanNode*)(plan_ptr->GetChildren()[0]->GetChildren()[0]);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    RowFnLetIRBuilder ir_builder(&table, m.get());
    std::vector<::fesql::type::ColumnDef> schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));

    int32_t (*decode)(int8_t*, int8_t*) = (int32_t (*)(int8_t*, int8_t*))load_fn_jit.getAddress();
    std::cout << decode << std::endl;

    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    int8_t* out_ptr = static_cast<int8_t*>(malloc(4));
    *((int32_t*)ptr + 2) = 1;
    *((int16_t*)(ptr +2+4)) = 2;
    *((float*)(ptr +2+ 4 + 2)) = 3.1f;
    *((double*)(ptr +2+ 4 + 2 + 4)) = 4.1;
    *((int64_t*)(ptr +2+ 4 + 2 + 4 + 8)) = 5;
    int32_t ret2 = decode(ptr, out_ptr);
    ASSERT_EQ(ret2, 0);
}

} // namespace of codegen
} // namespace of fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}



