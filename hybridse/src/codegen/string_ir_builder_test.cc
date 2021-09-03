/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "codegen/string_ir_builder.h"
#include <memory>
#include <string>
#include <utility>
#include "codegen/ir_base_builder.h"
#include "gtest/gtest.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "node/node_manager.h"

using namespace llvm;       // NOLINT
using namespace llvm::orc;  // NOLINT
ExitOnError ExitOnErr;
namespace hybridse {
namespace codegen {
class StringIRBuilderTest : public ::testing::Test {
 public:
    StringIRBuilderTest() {}
    ~StringIRBuilderTest() {}
};

TEST_F(StringIRBuilderTest, NewStringTest) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("string_ir_test", *ctx);
    StringIRBuilder string_ir_builder(m.get());
    Int64IRBuilder int64_builder;
    Function *load_fn = Function::Create(
        FunctionType::get(::llvm::Type::getVoidTy(m->getContext()),
                          {string_ir_builder.GetType()->getPointerTo()}, false),
        Function::ExternalLinkage, "new_empty_string", m.get());

    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    ::llvm::Value *string;
    ASSERT_TRUE(string_ir_builder.NewString(entry_block, &string));
    ::llvm::Value *data;
    ::llvm::Value *size;
    ASSERT_TRUE(string_ir_builder.GetSize(entry_block, string, &size));
    ASSERT_TRUE(string_ir_builder.GetData(entry_block, string, &data));
    ASSERT_TRUE(string_ir_builder.SetData(entry_block, arg0, data));
    ASSERT_TRUE(string_ir_builder.SetSize(entry_block, arg0, size));

    builder.CreateRetVoid();
    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("new_empty_string"));
    void (*decode)(codec::StringRef *) =
        (void (*)(codec::StringRef *))load_fn_jit.getAddress();
    codec::StringRef dist;
    decode(&dist);
    ASSERT_EQ("", dist.ToString());
}
TEST_F(StringIRBuilderTest, StringGetAndSet) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("string_ir_test", *ctx);
    StringIRBuilder string_ir_builder(m.get());
    Int64IRBuilder int64_builder;
    Function *load_fn = Function::Create(
        FunctionType::get(::llvm::Type::getVoidTy(m->getContext()),
                          {string_ir_builder.GetType()->getPointerTo(),
                           string_ir_builder.GetType()->getPointerTo()},
                          false),
        Function::ExternalLinkage, "new_string", m.get());

    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    Argument *arg1 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ::llvm::Value *data;
    ::llvm::Value *size;
    ASSERT_TRUE(string_ir_builder.GetSize(entry_block, arg0, &size));
    ASSERT_TRUE(string_ir_builder.GetData(entry_block, arg0, &data));
    ASSERT_TRUE(string_ir_builder.SetData(entry_block, arg1, data));
    ASSERT_TRUE(string_ir_builder.SetSize(entry_block, arg1, size));
    builder.CreateRetVoid();

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("new_string"));
    char *hello_data = strdup("hello");
    size_t hello_size = strlen("hello");

    codec::StringRef src(hello_size, hello_data);
    codec::StringRef dist;
    void (*decode)(codec::StringRef *, codec::StringRef *) = (void (*)(
        codec::StringRef *, codec::StringRef *))load_fn_jit.getAddress();
    decode(&src, &dist);
    ASSERT_EQ("hello", dist.ToString());
    free(hello_data);
}

TEST_F(StringIRBuilderTest, StringCopyFromTest) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("string_ir_test", *ctx);
    StringIRBuilder string_ir_builder(m.get());
    Int64IRBuilder int64_builder;
    Function *load_fn = Function::Create(
        FunctionType::get(::llvm::Type::getVoidTy(m->getContext()),
                          {string_ir_builder.GetType()->getPointerTo(),
                           string_ir_builder.GetType()->getPointerTo()},
                          false),
        Function::ExternalLinkage, "copy_from_string", m.get());

    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    Argument *arg1 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ASSERT_TRUE(string_ir_builder.CopyFrom(entry_block, arg0, arg1));
    builder.CreateRetVoid();

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("copy_from_string"));
    codec::StringRef src(strlen("hello"), strdup("hello"));
    codec::StringRef dist;
    void (*decode)(codec::StringRef *, codec::StringRef *) = (void (*)(
        codec::StringRef *, codec::StringRef *))load_fn_jit.getAddress();
    decode(&src, &dist);
    ASSERT_EQ("hello", dist.ToString());
    free(const_cast<char *>(src.data_));
}

TEST_F(StringIRBuilderTest, StringRefOp) {
    codec::StringRef s1(strlen("string1"), strdup("string1"));
    codec::StringRef s0(strlen("string0"), strdup("string0"));
    codec::StringRef s2(strlen("string2"), strdup("string2"));
    codec::StringRef s1_2(strlen("string1"), strdup("string1"));
    ASSERT_EQ(7, s1.size_);
    ASSERT_TRUE(s1 == s1_2);
    ASSERT_TRUE(s1 != s2);
    ASSERT_TRUE(s1 >= s0);
    ASSERT_TRUE(s1 > s0);
    ASSERT_TRUE(s1 < s2);
    ASSERT_TRUE(s1 <= s2);

    free(const_cast<char *>(s0.data_));
    free(const_cast<char *>(s1.data_));
    free(const_cast<char *>(s2.data_));
    free(const_cast<char *>(s1_2.data_));
}

}  // namespace codegen
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
