/*
 * ir_base_builder_test.cc
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

#include <memory>
#include "codegen/buf_ir_builder.h"
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

using namespace llvm;  // NOLINT
using namespace llvm::orc; // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace codegen {

class BufIRBuilderTest : public ::testing::Test {
 public:
    BufIRBuilderTest() {}
    ~BufIRBuilderTest() {}
};

TEST_F(BufIRBuilderTest, test_load_int32) {
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
    auto m = make_unique<Module>("test_load_float", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function* load_fn =
        Function::Create(FunctionType::get(Type::getInt32Ty(*ctx),
                                           {Type::getInt8PtrTy(*ctx)}, false),
                         Function::ExternalLinkage, "load_fn", m.get());

    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    ScopeVar sv;
    BufIRBuilder buf_builder(&table, entry_block, &sv);
    IRBuilder<> builder(entry_block);
    Argument* arg0 = &*load_fn->arg_begin();
    ::llvm::Value* val = NULL;
    bool ok = buf_builder.BuildGetField("col1", arg0, &val);
    ASSERT_TRUE(ok);
    builder.CreateRet(val);
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    int32_t (*decode)(int8_t*) = (int32_t(*)(int8_t*))load_fn_jit.getAddress();
    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    *((int16_t*)ptr) = 1;
    *((int32_t*)(ptr + 2)) = 1;
    *((int16_t*)(ptr + 2 + 4)) = 2;
    *((float*)(ptr + 2 + 4 + 2)) = 3.1f;
    *((double*)(ptr + 2 + 4 + 2 + 4)) = 4.1;
    *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
    std::cout << *(int16_t*)(ptr) << std::endl;
    std::cout << *(int32_t*)(ptr + 2) << std::endl;
    std::cout << *(int16_t*)(ptr + 2 + 4) << std::endl;
    std::cout << *(float*)(ptr + 2 + 4 + 2) << std::endl;
    std::cout << *(double*)(ptr + 2 + 4 + 2 + 4) << std::endl;
    std::cout << *(int64_t*)(ptr + 2 + 4 + 2 + 4 + 8) << std::endl;
    int32_t ret = decode(ptr);
    ASSERT_EQ(ret, 1);
}

TEST_F(BufIRBuilderTest, test_load_float) {
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
    auto m = make_unique<Module>("test_load_float", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function* load_fn =
        Function::Create(FunctionType::get(Type::getInt16Ty(*ctx),
                                           {Type::getInt8PtrTy(*ctx)}, false),
                         Function::ExternalLinkage, "load_fn", m.get());

    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    ScopeVar sv;
    BufIRBuilder buf_builder(&table, entry_block, &sv);
    IRBuilder<> builder(entry_block);
    Argument* arg0 = &*load_fn->arg_begin();
    ::llvm::Value* val = NULL;
    bool ok = buf_builder.BuildGetField("col2", arg0, &val);
    ASSERT_TRUE(ok);
    builder.CreateRet(val);
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    int16_t (*decode)(int8_t*) = (int16_t(*)(int8_t*))load_fn_jit.getAddress();
    int8_t* ptr = static_cast<int8_t*>(malloc(28));

    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
    *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
    *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;

    int16_t ret = decode(ptr);
    ASSERT_EQ(ret, 2);
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
