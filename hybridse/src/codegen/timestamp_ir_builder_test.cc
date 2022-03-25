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

#include "codegen/timestamp_ir_builder.h"
#include <memory>
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
using openmldb::base::Timestamp;
class TimestampIRBuilderTest : public ::testing::Test {
 public:
    TimestampIRBuilderTest() {}
    ~TimestampIRBuilderTest() {}
};

TEST_F(TimestampIRBuilderTest, BuildTimestampWithTs_TEST) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("timestamp_test", *ctx);
    TimestampIRBuilder timestamp_builder(m.get());
    Int64IRBuilder int64_builder;
    ::llvm::Type *timestamp_type = timestamp_builder.GetType();
    Function *load_fn = Function::Create(
        FunctionType::get(timestamp_type->getPointerTo(),
                          {int64_builder.GetType(m.get())}, false),
        Function::ExternalLinkage, "build_timestamp", m.get());

    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ::llvm::Value *ts = arg0;
    ::llvm::Value *output;
    ASSERT_TRUE(timestamp_builder.NewTimestamp(entry_block, ts, &output));
    builder.CreateRet(output);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("build_timestamp"));
    Timestamp *(*decode)(int64_t) =
        (Timestamp * (*)(int64_t)) load_fn_jit.getAddress();

    ASSERT_EQ(static_cast<int64_t>(decode(1590115420000L)->ts_),
              1590115420000L);
    ASSERT_EQ(static_cast<int64_t>(decode(1L)->ts_), 1L);
}

TEST_F(TimestampIRBuilderTest, GetTsTest) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("timestamp_test", *ctx);
    TimestampIRBuilder timestamp_builder(m.get());
    Int64IRBuilder int64_builder;
    Function *load_fn = Function::Create(
        FunctionType::get(int64_builder.GetType(m.get()),
                          {int64_builder.GetType(m.get())}, false),
        Function::ExternalLinkage, "build_timestamp", m.get());
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ::llvm::Value *ts = arg0;
    ::llvm::Value *timestamp;
    ASSERT_TRUE(timestamp_builder.NewTimestamp(entry_block, ts, &timestamp));
    ::llvm::Value *output;
    timestamp_builder.GetTs(entry_block, timestamp, &output);
    builder.CreateRet(output);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("build_timestamp"));
    int64_t (*decode)(int64_t) = (int64_t(*)(int64_t))load_fn_jit.getAddress();

    ASSERT_EQ(decode(1590115420000L), 1590115420000L);
    ASSERT_EQ(decode(1L), 1L);
}
TEST_F(TimestampIRBuilderTest, MinuteTest) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("timestamp_test", *ctx);
    TimestampIRBuilder timestamp_builder(m.get());
    Int64IRBuilder int64_builder;
    Function *load_fn = Function::Create(
        FunctionType::get(Int32IRBuilder::GetType(m.get()),
                          {timestamp_builder.GetType()->getPointerTo()}, false),
        Function::ExternalLinkage, "minute", m.get());
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ::llvm::Value *timestamp = arg0;
    ::llvm::Value *ret;
    base::Status status;
    ASSERT_TRUE(timestamp_builder.Minute(entry_block, timestamp, &ret, status));
    builder.CreateRet(ret);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("minute"));
    int32_t (*decode)(Timestamp *) =
        (int32_t(*)(Timestamp *))load_fn_jit.getAddress();

    Timestamp time(1590115420000L);
    ASSERT_EQ(43, decode(&time));
}
TEST_F(TimestampIRBuilderTest, SecondTest) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("timestamp_test", *ctx);
    TimestampIRBuilder timestamp_builder(m.get());
    Int64IRBuilder int64_builder;
    Function *load_fn = Function::Create(
        FunctionType::get(Int32IRBuilder::GetType(m.get()),
                          {timestamp_builder.GetType()->getPointerTo()}, false),
        Function::ExternalLinkage, "second", m.get());
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ::llvm::Value *timestamp = arg0;
    ::llvm::Value *ret;
    base::Status status;
    ASSERT_TRUE(timestamp_builder.Second(entry_block, timestamp, &ret, status));
    builder.CreateRet(ret);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("second"));
    int32_t (*decode)(Timestamp *) =
        (int32_t(*)(Timestamp *))load_fn_jit.getAddress();

    Timestamp time(1590115420000L);
    ASSERT_EQ(40, decode(&time));
}
TEST_F(TimestampIRBuilderTest, HourTest) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("timestamp_test", *ctx);
    TimestampIRBuilder timestamp_builder(m.get());
    Int64IRBuilder int64_builder;
    Function *load_fn = Function::Create(
        FunctionType::get(Int32IRBuilder::GetType(m.get()),
                          {timestamp_builder.GetType()->getPointerTo()}, false),
        Function::ExternalLinkage, "hour", m.get());

    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ::llvm::Value *timestamp = arg0;
    ::llvm::Value *ret;
    base::Status status;
    ASSERT_TRUE(timestamp_builder.Hour(entry_block, timestamp, &ret, status));
    builder.CreateRet(ret);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("hour"));
    int32_t (*decode)(Timestamp *) =
        (int32_t(*)(Timestamp *))load_fn_jit.getAddress();

    Timestamp time(1590115420000L);
    ASSERT_EQ(10, decode(&time));
}
TEST_F(TimestampIRBuilderTest, SetTsTest) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("timestamp_test", *ctx);
    TimestampIRBuilder timestamp_builder(m.get());
    Int64IRBuilder int64_builder;
    Function *load_fn = Function::Create(
        FunctionType::get(::llvm::Type::getVoidTy(m->getContext()),
                          {timestamp_builder.GetType()->getPointerTo(),
                           int64_builder.GetType(m.get())},
                          false),
        Function::ExternalLinkage, "build_timestamp", m.get());
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    Argument *arg1 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ::llvm::Value *timestamp = arg0;
    ::llvm::Value *ts = arg1;
    timestamp_builder.SetTs(entry_block, timestamp, ts);
    builder.CreateRetVoid();

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("build_timestamp"));
    void (*set_ts)(Timestamp *, int64_t) =
        (void (*)(Timestamp *, int64_t))load_fn_jit.getAddress();

    Timestamp data;
    set_ts(&data, 1590115420000L);
    ASSERT_EQ(data.ts_, 1590115420000L);
    set_ts(&data, 1L);
    ASSERT_EQ(data.ts_, 1L);
}

TEST_F(TimestampIRBuilderTest, TimestampOp) {
    Timestamp t1(1);
    Timestamp t2(10);

    ASSERT_EQ(11L, (t1 + t2).ts_);
    ASSERT_EQ(9L, (t2 - t1).ts_);
    ASSERT_EQ(3, (t2 / 3L).ts_);

    ASSERT_TRUE(t2 >= Timestamp(10));
    ASSERT_TRUE(t2 <= Timestamp(10));

    ASSERT_TRUE(t2 > Timestamp(9));
    ASSERT_TRUE(t2 < Timestamp(11));

    ASSERT_TRUE(t2 == Timestamp(10));
    ASSERT_TRUE(t2 != Timestamp(9));
    if (t2.ts_ > INT32_MAX) {
        Timestamp t3(10);
        t3 += t1;
        ASSERT_EQ(11, t3.ts_);
    }
    {
        Timestamp t3(10);
        t3 -= t1;
        ASSERT_EQ(9, t3.ts_);
    }
}

}  // namespace codegen
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
