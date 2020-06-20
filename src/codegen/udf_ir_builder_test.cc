/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_ir_builder_test.cc
 *
 * Author: chenjing
 * Date: 2020/6/17
 *--------------------------------------------------------------------------
 **/
#include "codegen/udf_ir_builder.h"
#include <memory>
#include <string>
#include <utility>
#include "codec/list_iterator_codec.h"
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
#include "llvm/Transforms/Utils.h"
#include "node/node_manager.h"
#include "parser/parser.h"
#include "udf/udf.h"
#include "vm/sql_compiler.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

ExitOnError ExitOnErr;

namespace fesql {
namespace codegen {

class UDFIRBuilderTest : public ::testing::Test {
 public:
    UDFIRBuilderTest() {}
    ~UDFIRBuilderTest() {}
};
template <class T, class... Args>
void CheckNativeUDF(const std::string udf_name, T exp, Args... args) {
    base::Status status;
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("udf_test", *ctx);
    fesql::udf::RegisterUDFToModule(m.get());
    m->print(::llvm::errs(), NULL, true, true);

    auto J = ExitOnErr(LLJITBuilder().create());
    auto &jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto fn = ExitOnErr(J->lookup(udf_name));
    T (*udf)(Args...) = (T(*)(Args...))fn.getAddress();
    ASSERT_EQ(exp, udf(args...));
}
TEST_F(UDFIRBuilderTest, day_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("day.date", 22, &date);
}
TEST_F(UDFIRBuilderTest, month_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("month.date", 5, &date);
}
TEST_F(UDFIRBuilderTest, year_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("year.date", 2020, &date);
}

TEST_F(UDFIRBuilderTest, day_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckNativeUDF<int32_t, codec::Timestamp *>("day.timestamp", 22, &time);
}

TEST_F(UDFIRBuilderTest, month_timestamp_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("month.date", 5, &date);
}
TEST_F(UDFIRBuilderTest, year_timestamp_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("year.date", 2020, &date);
}
TEST_F(UDFIRBuilderTest, day_int64_udf_test) {
    CheckNativeUDF<int32_t, int64_t>("day.int64", 22, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, month_int64_udf_test) {
    CheckNativeUDF<int32_t, int64_t>("month.int64", 5, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, year_int64_udf_test) {
    CheckNativeUDF<int32_t, int64_t>("year.int64", 2020, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, inc_int32_udf_test) {
    CheckNativeUDF<int32_t, int32_t>("inc.int32", 2021, 2020);
}
}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
