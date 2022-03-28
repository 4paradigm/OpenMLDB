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

#include "codegen/date_ir_builder.h"
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
using openmldb::base::Date;
class DateIRBuilderTest : public ::testing::Test {
 public:
    DateIRBuilderTest() {}
    ~DateIRBuilderTest() {}
};

TEST_F(DateIRBuilderTest, DayTest) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("date_test", *ctx);
    DateIRBuilder date_builder(m.get());
    Function *load_fn = Function::Create(
        FunctionType::get(Int32IRBuilder::GetType(m.get()),
                          {Int32IRBuilder::GetType(m.get())}, false),
        Function::ExternalLinkage, "date_day", m.get());
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ::llvm::Value *code = arg0;
    ::llvm::Value *date;
    ASSERT_TRUE(date_builder.NewDate(entry_block, code, &date));
    ::llvm::Value *output;
    base::Status status;
    ASSERT_TRUE(date_builder.Day(entry_block, date, &output, status));
    builder.CreateRet(output);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("date_day"));
    int64_t (*decode)(int64_t) = (int64_t(*)(int64_t))load_fn_jit.getAddress();

    ASSERT_EQ(decode(Date(2020, 05, 27).date_), 27);
    ASSERT_EQ(decode(Date(1900, 05, 28).date_), 28);
    // 溢出处理
    ASSERT_EQ(decode(Date(1800, 05, 31).date_), 0);
    ASSERT_EQ(decode(Date(1900, 05, 32).date_), 0);
}

TEST_F(DateIRBuilderTest, YearTest) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("date_test", *ctx);
    DateIRBuilder date_builder(m.get());
    Function *load_fn = Function::Create(
        FunctionType::get(Int32IRBuilder::GetType(m.get()),
                          {Int32IRBuilder::GetType(m.get())}, false),
        Function::ExternalLinkage, "date_year", m.get());
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ::llvm::Value *code = arg0;
    ::llvm::Value *date;
    ASSERT_TRUE(date_builder.NewDate(entry_block, code, &date));
    ::llvm::Value *output;
    base::Status status;
    ASSERT_TRUE(date_builder.Year(entry_block, date, &output, status));
    builder.CreateRet(output);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("date_year"));
    int64_t (*decode)(int64_t) = (int64_t(*)(int64_t))load_fn_jit.getAddress();

    ASSERT_EQ(decode(Date(2020, 05, 27).date_), 2020);
    ASSERT_EQ(decode(Date(1900, 05, 28).date_), 1900);

    // Date溢出
    ASSERT_EQ(decode(Date(1800, 05, 31).date_), 1900);
}

TEST_F(DateIRBuilderTest, MonthTest) {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("date_test", *ctx);
    DateIRBuilder date_builder(m.get());
    Function *load_fn = Function::Create(
        FunctionType::get(Int32IRBuilder::GetType(m.get()),
                          {Int32IRBuilder::GetType(m.get())}, false),
        Function::ExternalLinkage, "date_month", m.get());
    auto iter = load_fn->arg_begin();
    Argument *arg0 = &(*iter);
    iter++;
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", load_fn);
    IRBuilder<> builder(entry_block);
    ::llvm::Value *code = arg0;
    ::llvm::Value *date;
    ASSERT_TRUE(date_builder.NewDate(entry_block, code, &date));

    base::Status status;
    ::llvm::Value *output;
    ASSERT_TRUE(date_builder.Month(entry_block, date, &output, status));
    builder.CreateRet(output);

    m->print(::llvm::errs(), NULL);
    auto J = ExitOnErr(LLJITBuilder().create());
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto load_fn_jit = ExitOnErr(J->lookup("date_month"));
    int64_t (*decode)(int64_t) = (int64_t(*)(int64_t))load_fn_jit.getAddress();

    ASSERT_EQ(decode(Date(2020, 05, 27).date_), 05);
    ASSERT_EQ(decode(Date(1900, 05, 28).date_), 05);
    ASSERT_EQ(decode(Date(1800, 05, 31).date_), 1);
}

TEST_F(DateIRBuilderTest, DateOp) {
    Date t1(2020, 05, 27);
    Date t2(2020, 05, 27);
    Date t3(2020, 05, 28);
    Date t4(2020, 05, 26);

    ASSERT_EQ(t1, t2);
    ASSERT_TRUE(t1 >= t2);
    ASSERT_TRUE(t1 < t3);
    ASSERT_TRUE(t1 <= t3);
    ASSERT_TRUE(t1 >= t4);
    ASSERT_TRUE(t1 > t4);
}

}  // namespace codegen
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
