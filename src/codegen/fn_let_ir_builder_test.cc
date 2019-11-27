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
#include "base/window.cc"
#include "udf/udf.h"
#include <vm/jit.h>
#include "codegen/fn_ir_builder.h"
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

static node::NodeManager manager;

/// Check E. If it's in a success state then return the contained value. If
/// it's in a failure state log the error(s) and exit.
template <typename T> T FeCheck(::llvm::Expected<T> &&E) {
    if (E.takeError()) {}
    return std::move(*E);
}
void GetSchema(::fesql::type::TableDef& table) {
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
        column->set_name("col5");
    }
}

class FnLetIRBuilderTest : public ::testing::Test {

public:
     FnLetIRBuilderTest() {
         GetSchema(table);
     }
    ~FnLetIRBuilderTest() {}

 protected:
    ::fesql::type::TableDef table;
};

void AddFunc(const std::string& fn,
        ::llvm::Module* m) {
    ::fesql::node::NodePointVector trees;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::base::Status status;
    int ret = parser.parse(fn, trees, &manager, status);
    ASSERT_EQ(0, ret);
    FnIRBuilder fn_ir_builder(m);
    bool ok = fn_ir_builder.Build((node::FnNodeList *) trees[0]);
    ASSERT_TRUE(ok);
}

TEST_F(FnLetIRBuilderTest, test_udf) {

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);
    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    const std::string test = "%%fun\ndef test(a:i32,b:i32):i32\n    c=a+b\n    d=c+1\n    return d\nend";
    AddFunc(test, m.get());
    int ret = parser.parse("SELECT test(col1,col1) FROM t1 limit 10;", list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);
    ::fesql::node::PlanNodeList trees;
    ret = planner.CreatePlanTree(list, trees, status);
    ASSERT_EQ(0, ret);
    ::fesql::node::ProjectListPlanNode* pp_node_ptr 
        = (::fesql::node::ProjectListPlanNode*)(trees[0]->GetChildren()[0]->GetChildren()[0]);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    RowFnLetIRBuilder ir_builder(&table, m.get());
    std::vector<::fesql::type::ColumnDef> schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1u, schema.size());
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));
    int32_t (*decode)(int8_t*, int8_t*) = (int32_t (*)(int8_t*, int8_t*))load_fn_jit.getAddress();
    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    int32_t i = 0;
    *((int32_t*)(ptr + 2)) = 1;
    *((int16_t*)(ptr +2+4)) = 2;
    *((float*)(ptr +2+ 4 + 2)) = 3.1f;
    *((double*)(ptr +2+ 4 + 2 + 4)) = 4.1;
    *((int64_t*)(ptr +2+ 4 + 2 + 4 + 8)) = 5;
    int32_t ret2 = decode(ptr, (int8_t*)&i);
    ASSERT_EQ(ret2, 0u);
    ASSERT_EQ(i, 3u);
}

void RegisterUDFToModule(::llvm::Module* m) {
    ::llvm::Type* i32_ty = ::llvm::Type::getInt32Ty(m->getContext());
    ::llvm::Type* i8_ptr_ty = ::llvm::Type::getInt8PtrTy(m->getContext());
    m->getOrInsertFunction("inc_int32", i32_ty, i32_ty);
    m->getOrInsertFunction("sum_int32", i32_ty, i8_ptr_ty);
    m->getOrInsertFunction("col", i8_ptr_ty, i8_ptr_ty, i32_ty, i32_ty, i32_ty);
}

TEST_F(FnLetIRBuilderTest, test_simple_project) {

    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret = parser.parse("SELECT col1 FROM t1 limit 10;", list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);

    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    ::fesql::node::ProjectListPlanNode* pp_node_ptr 
        = (::fesql::node::ProjectListPlanNode*)(plan[0]->GetChildren()[0]->GetChildren()[0]);

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
    ExitOnErr(J->addIRModule(std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("test_project_fn"));

    int32_t (*decode)(int8_t*, int8_t*) = (int32_t (*)(int8_t*, int8_t*))load_fn_jit.getAddress();
    std::cout << decode << std::endl;

    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    int32_t i = 0;
    *((int32_t*)(ptr + 2)) = 1;
    *((int16_t*)(ptr +2+4)) = 2;
    *((float*)(ptr +2+ 4 + 2)) = 3.1f;
    *((double*)(ptr +2+ 4 + 2 + 4)) = 4.1;
    *((int64_t*)(ptr +2+ 4 + 2 + 4 + 8)) = 5;
    int32_t ret2 = decode(ptr, (int8_t*)&i);
    ASSERT_EQ(ret2, 0u);
    ASSERT_EQ(i, 1u);
}

TEST_F(FnLetIRBuilderTest, test_extern_udf_project) {

    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret = parser.parse("SELECT inc_int32(col1) FROM t1 limit 10;", list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);

    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    ::fesql::node::ProjectListPlanNode* pp_node_ptr
        = (::fesql::node::ProjectListPlanNode*)(plan[0]->GetChildren()[0]->GetChildren()[0]);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);

    RegisterUDFToModule(m.get());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    RowFnLetIRBuilder ir_builder(&table, m.get());
    std::vector<::fesql::type::ColumnDef> schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1u, schema.size());
    m->print(::llvm::errs(), NULL);

    auto J = FeCheck((::fesql::vm::FeSQLJITBuilder().create()));
    ::llvm::orc::JITDylib& jd_lib = J->createJITDylib("test");
    ::llvm::orc::VModuleKey module_key = J->CreateVModule();
    ExitOnErr(J->AddIRModule(jd_lib,
                                       std::move(::llvm::orc::ThreadSafeModule(std::move(m), std::move(ctx))),
                             module_key));
    J->AddSymbol(jd_lib, "inc_int32", reinterpret_cast<void*>(&fesql::udf::inc_int32));
    auto load_fn_jit = ExitOnErr(J->lookup(jd_lib, "test_project_fn"));
    int32_t (*decode)(int8_t*, int8_t*) = (int32_t (*)(int8_t*, int8_t*))load_fn_jit.getAddress();
    std::cout << decode << std::endl;

    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    int32_t i = 0;
    *((int32_t*)(ptr + 2)) = 1;
    *((int16_t*)(ptr +2+4)) = 2;
    *((float*)(ptr +2+ 4 + 2)) = 3.1f;
    *((double*)(ptr +2+ 4 + 2 + 4)) = 4.1;
    *((int64_t*)(ptr +2+ 4 + 2 + 4 + 8)) = 5;
    int32_t ret2 = decode(ptr, (int8_t*)&i);
    ASSERT_EQ(ret2, 0u);
    ASSERT_EQ(i, 2u);
}

TEST_F(FnLetIRBuilderTest, test_extern_agg_udf_project) {
    // prepare row buf
    std::vector<base::Row> rows;
    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *((int32_t*)(ptr + 2)) = 1;
        *((int16_t*)(ptr + 2 + 4)) = 2;
        *((float*)(ptr + 2 + 4 + 2)) = 3.1f;
        *((double*)(ptr + 2 + 4 + 2 + 4)) = 4.1;
        *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
        rows.push_back(base::Row{.buf = ptr});
    }

    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *((int32_t*)(ptr + 2)) = 11;
        *((int16_t*)(ptr + 2 + 4)) = 22;
        *((float*)(ptr + 2 + 4 + 2)) = 33.1f;
        *((double*)(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        rows.push_back(base::Row{.buf = ptr});
    }

    {
        int8_t* ptr = static_cast<int8_t*>(malloc(28));
        *((int32_t*)(ptr + 2)) = 111;
        *((int16_t*)(ptr + 2 + 4)) = 222;
        *((float*)(ptr + 2 + 4 + 2)) = 333.1f;
        *((double*)(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        rows.push_back(base::Row{.buf = ptr});
    }

    ::fesql::base::WindowIteratorImpl w(rows);

    ::fesql::node::NodePointVector list;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::node::NodeManager manager;
    ::fesql::base::Status status;
    int ret = parser.parse("SELECT sum(col1) OVER w1 as w1_col1_sum FROM t1 "
                           "WINDOW w1 AS (PARTITION BY COL2\n"
                           "              ORDER BY `TS` ROWS BETWEEN 3 PRECEDING AND 3 "
                           "FOLLOWING) limit 10;", list, &manager, status);
    ASSERT_EQ(0, ret);
    ASSERT_EQ(1u, list.size());
    ::fesql::plan::SimplePlanner planner(&manager);

    ::fesql::node::PlanNodeList plan;
    ret = planner.CreatePlanTree(list, plan, status);
    ASSERT_EQ(0, ret);
    ::fesql::node::ProjectListPlanNode* pp_node_ptr
        = (::fesql::node::ProjectListPlanNode*)(plan[0]->GetChildren()[0]->GetChildren()[0]);

    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_project", *ctx);

    RegisterUDFToModule(m.get());
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    RowFnLetIRBuilder ir_builder(&table, m.get());
    std::vector<::fesql::type::ColumnDef> schema;
    bool ok = ir_builder.Build("test_project_fn", pp_node_ptr, schema);
    ASSERT_TRUE(ok);
    ASSERT_EQ(1u, schema.size());
    m->print(::llvm::errs(), NULL);

    auto J = FeCheck((::fesql::vm::FeSQLJITBuilder().create()));
    ::llvm::orc::JITDylib& jd_lib = J->createJITDylib("test");
    ::llvm::orc::VModuleKey module_key = J->CreateVModule();
    ExitOnErr(J->AddIRModule(jd_lib,
                             std::move(::llvm::orc::ThreadSafeModule(std::move(m), std::move(ctx))),
                             module_key));
    J->AddSymbol(jd_lib, "sum_int32", reinterpret_cast<void*>(&fesql::udf::sum_int32));
    J->AddSymbol(jd_lib, "col", reinterpret_cast<void*>(&fesql::udf::col));
    auto load_fn_jit = ExitOnErr(J->lookup(jd_lib, "test_project_fn"));
    int32_t (*decode)(int8_t*, int8_t*) = (int32_t (*)(int8_t*, int8_t*))load_fn_jit.getAddress();
    std::cout << decode << std::endl;

    int32_t i;
    int32_t ret2 = decode((int8_t*)(&w), (int8_t*)&i);
    ASSERT_EQ(ret2, 0u);
    ASSERT_EQ(i, 1+11+111);
}

} // namespace of codegen
} // namespace of fesqjit_test.ccl

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}



