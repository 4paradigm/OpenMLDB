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

#include "codegen/ir_base_builder.h"
#include "gtest/gtest.h"

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

class IRBaseBuilderTest : public ::testing::Test {

public:
    IRBaseBuilderTest() {}
    ~IRBaseBuilderTest() {}
};
TEST_F(IRBaseBuilderTest, test_load_float) {
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_float", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function *load_fn =
    Function::Create(FunctionType::get(Type::getFloatTy(*ctx),
                                         {Type::getInt8PtrTy(*ctx)}, 
                                         false),
                       Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*load_fn->arg_begin(); 
    Value *offset = builder.getInt32(4);
    Type* float_type = Type::getFloatTy(*ctx);
    Value *output = NULL;
    bool ok = BuildLoadRelative(builder, *ctx, arg0, offset, float_type, &output);
    ASSERT_TRUE(ok);
    builder.CreateRet(output);
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    float (*decode)(int8_t*) = (float (*)(int8_t*))load_fn_jit.getAddress();
    float* test = new float[2];
    test[0] = 1.1f;
    test[1] = 2.1f;
    int8_t* input_ptr = (int8_t*) test;
    float ret = decode(input_ptr);
    ASSERT_EQ(ret, 2.1f);
}

TEST_F(IRBaseBuilderTest, test_load_int64) {
    // Create an LLJIT instance.
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test_load_int64", *ctx);
    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function *load_fn =
    Function::Create(FunctionType::get(Type::getInt64Ty(*ctx),
                                         {Type::getInt8PtrTy(*ctx)}, 
                                         false),
                       Function::ExternalLinkage, "load_fn", m.get());
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    Argument *arg0 = &*load_fn->arg_begin(); 
    Value *offset = builder.getInt32(8);
    IntegerType* int64_type = Type::getInt64Ty(*ctx);
    Value *output = NULL;
    bool ok = BuildLoadRelative(builder, *ctx, arg0, offset, int64_type, &output);
    ASSERT_TRUE(ok);
    builder.CreateRet(output);
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
    auto load_fn_jit = ExitOnErr(J->lookup("load_fn"));
    int64_t (*decode)(int8_t*) = (int64_t (*)(int8_t*))load_fn_jit.getAddress();
    int64_t* test = new int64_t[2];
    test[0] = 1;
    test[1] = 2;
    int8_t* input_ptr = (int8_t*) test;
    int64_t ret = decode(input_ptr);
    ASSERT_EQ(ret, 2);
}


} // namespace of codegen
} // namespace of fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}



