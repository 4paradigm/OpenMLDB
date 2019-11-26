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
using namespace llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace codegen {

class BufIRBuilderTest : public ::testing::Test {
 public:
    BufIRBuilderTest() {}
    ~BufIRBuilderTest() {}
};

template <class T>
void RunCase(T expected, const ::fesql::type::Type& type,
             const std::string& col, int8_t* row, int32_t row_size) {
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
        column->set_name("col5");
    }

    {
        ::fesql::type::ColumnDef* column = table.add_columns();
        column->set_type(::fesql::type::kVarchar);
        column->set_name("col6");
    }

    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_float", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    ::llvm::Type* retTy = NULL;
    switch (type) {
        case ::fesql::type::kInt16:
            retTy = Type::getInt16Ty(*ctx);
            break;
        case ::fesql::type::kInt32:
            retTy = Type::getInt32Ty(*ctx);
            break;
        case ::fesql::type::kInt64:
            retTy = Type::getInt64Ty(*ctx);
            break;
        case ::fesql::type::kDouble:
            retTy = Type::getDoubleTy(*ctx);
            break;
        case ::fesql::type::kFloat:
            retTy = Type::getFloatTy(*ctx);
            break;
        default:
            ASSERT_TRUE(false);
    }
    Function* fn = Function::Create(
        FunctionType::get(
            retTy, {Type::getInt8PtrTy(*ctx), Type::getInt32Ty(*ctx)}, false),
        Function::ExternalLinkage, "fn", m.get());
    BasicBlock* entry_block = BasicBlock::Create(*ctx, "EntryBlock", fn);
    ScopeVar sv;
    BufIRBuilder buf_builder(&table, entry_block, &sv);
    IRBuilder<> builder(entry_block);
    Function::arg_iterator it = fn->arg_begin();
    Argument* arg0 = &*it;
    ++it;
    Argument* arg1 = &*it;
    ::llvm::Value* val = NULL;
    bool ok = buf_builder.BuildGetField(col, arg0, arg1, &val);
    ASSERT_TRUE(ok);
    builder.CreateRet(val);
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(::llvm::orc::LLJITBuilder().create());
    ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("fn"));
    T(*decode)
    (int8_t*, int32_t) =
        reinterpret_cast<T (*)(int8_t*, int32_t)>(load_fn_jit.getAddress());
    ASSERT_EQ(expected, decode(row, row_size));
}

TEST_F(BufIRBuilderTest, test_load_int16) {
    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    *reinterpret_cast<int32_t*>(ptr + 2) = 1;
    *reinterpret_cast<int16_t*>(ptr + 2 + 4) = 2;
    *reinterpret_cast<float*>(ptr + 2 + 4 + 2) = 3.1f;
    *reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4) = 4.1;
    *reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8) = 5;
    RunCase<int16_t>(2, ::fesql::type::kInt16, "col2", ptr, 28);
    free(ptr);
}

TEST_F(BufIRBuilderTest, test_load_float) {
    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    *reinterpret_cast<int32_t*>(ptr + 2) = 1;
    *reinterpret_cast<int16_t*>(ptr + 2 + 4) = 2;
    *reinterpret_cast<float*>(ptr + 2 + 4 + 2) = 3.1f;
    *reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4) = 4.1;
    *reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8) = 5;
    RunCase<float>(3.1f, ::fesql::type::kFloat, "col3", ptr, 28);
    free(ptr);
}

TEST_F(BufIRBuilderTest, test_load_int32) {
    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    *reinterpret_cast<int32_t*>(ptr + 2) = 1;
    *reinterpret_cast<int16_t*>(ptr + 2 + 4) = 2;
    *reinterpret_cast<float*>(ptr + 2 + 4 + 2) = 3.1f;
    *reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4) = 4.1;
    *reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8) = 5;
    RunCase<int32_t>(1, ::fesql::type::kInt32, "col1", ptr, 28);
    free(ptr);
}

TEST_F(BufIRBuilderTest, test_load_double) {
    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    *reinterpret_cast<int32_t*>(ptr + 2) = 1;
    *reinterpret_cast<int16_t*>(ptr + 2 + 4) = 2;
    *reinterpret_cast<float*>(ptr + 2 + 4 + 2) = 3.1f;
    *reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4) = 4.1;
    *reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8) = 5;
    RunCase<double>(4.1, ::fesql::type::kDouble, "col4", ptr, 28);
    free(ptr);
}

TEST_F(BufIRBuilderTest, test_load_int64) {
    int8_t* ptr = static_cast<int8_t*>(malloc(28));
    *reinterpret_cast<int32_t*>(ptr + 2) = 1;
    *reinterpret_cast<int16_t*>(ptr + 2 + 4) = 2;
    *reinterpret_cast<float*>(ptr + 2 + 4 + 2) = 3.1f;
    *reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4) = 4.1;
    *reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8) = 5;
    RunCase<int64_t>(5, ::fesql::type::kDouble, "col5", ptr, 28);
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
