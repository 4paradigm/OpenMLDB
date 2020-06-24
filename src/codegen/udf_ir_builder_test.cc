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
#include <vector>
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
    CheckNativeUDF<int32_t, codec::Date *>("dayofmonth.date", 22, &date);
}
TEST_F(UDFIRBuilderTest, month_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("month.date", 5, &date);
}
TEST_F(UDFIRBuilderTest, year_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("year.date", 2020, &date);
}
TEST_F(UDFIRBuilderTest, weekday_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("dayofweek.date", 5, &date);
}
TEST_F(UDFIRBuilderTest, week_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("week.date", 20, &date);
    {
        codec::Date date(2020, 05, 23);
        CheckNativeUDF<int32_t, codec::Date *>("week.date", 20, &date);
    }
    {
        codec::Date date(2020, 05, 24);
        CheckNativeUDF<int32_t, codec::Date *>("week.date", 21, &date);
    }
}

TEST_F(UDFIRBuilderTest, minute_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckNativeUDF<int32_t, codec::Timestamp *>("minute.timestamp", 43, &time);
}
TEST_F(UDFIRBuilderTest, second_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckNativeUDF<int32_t, codec::Timestamp *>("second.timestamp", 40, &time);
}
TEST_F(UDFIRBuilderTest, hour_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckNativeUDF<int32_t, codec::Timestamp *>("hour.timestamp", 10, &time);
}
TEST_F(UDFIRBuilderTest, day_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckNativeUDF<int32_t, codec::Timestamp *>("dayofmonth.timestamp", 22, &time);
}

TEST_F(UDFIRBuilderTest, weekday_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckNativeUDF<int32_t, codec::Timestamp *>("dayofweek.timestamp", 5, &time);
}
TEST_F(UDFIRBuilderTest, week_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckNativeUDF<int32_t, codec::Timestamp *>("week.timestamp", 20, &time);
}

TEST_F(UDFIRBuilderTest, month_timestamp_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("month.date", 5, &date);
}
TEST_F(UDFIRBuilderTest, year_timestamp_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckNativeUDF<int32_t, codec::Date *>("year.date", 2020, &date);
}

TEST_F(UDFIRBuilderTest, minute_int64_udf_test) {
    CheckNativeUDF<int32_t, int64_t>("minute.int64", 43, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, second_int64_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckNativeUDF<int32_t, int64_t>("second.int64", 40, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, hour_int64_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckNativeUDF<int32_t, int64_t>("hour.int64", 10, 1590115420000L);
}

TEST_F(UDFIRBuilderTest, day_int64_udf_test) {
    CheckNativeUDF<int32_t, int64_t>("dayofmonth.int64", 22, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, month_int64_udf_test) {
    CheckNativeUDF<int32_t, int64_t>("month.int64", 5, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, year_int64_udf_test) {
    CheckNativeUDF<int32_t, int64_t>("year.int64", 2020, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, dayofweek_int64_udf_test) {
    CheckNativeUDF<int32_t, int64_t>("dayofweek.int64", 5, 1590115420000L);
    CheckNativeUDF<int32_t, int64_t>("dayofweek.int64", 6,
                                     1590115420000L + 86400000L);

    // Sunday
    CheckNativeUDF<int32_t, int64_t>("dayofweek.int64", 0,
                                     1590115420000L + 2 * 86400000L);
    CheckNativeUDF<int32_t, int64_t>("dayofweek.int64", 1,
                                     1590115420000L + 3 * 86400000L);
}
TEST_F(UDFIRBuilderTest, week_int64_udf_test) {
    CheckNativeUDF<int32_t, int64_t>("week.int64", 20, 1590115420000L);
    CheckNativeUDF<int32_t, int64_t>("week.int64", 20,
                                     1590115420000L + 86400000L);

//     Sunday
    CheckNativeUDF<int32_t, int64_t>("dayofmonth.int64", 24,
                                     1590115420000L + 2 * 86400000L);
    CheckNativeUDF<int32_t, int64_t>("week.int64", 21,
                                     1590115420000L + 2 * 86400000L);
    //     Monday
    CheckNativeUDF<int32_t, int64_t>("week.int64", 21,
                                     1590115420000L + 3 * 86400000L);
    CheckNativeUDF<int32_t, int64_t>("week.int64", 21,
                                     1590115420000L + 4 * 86400000L);
    CheckNativeUDF<int32_t, int64_t>("week.int64", 21,
                                     1590115420000L + 5 * 86400000L);
    CheckNativeUDF<int32_t, int64_t>("week.int64", 21,
                                     1590115420000L + 6 * 86400000L);
    CheckNativeUDF<int32_t, int64_t>("week.int64", 21,
                                     1590115420000L + 7 * 86400000L);
    CheckNativeUDF<int32_t, int64_t>("week.int64", 21,
                                     1590115420000L + 8 * 86400000L);
    CheckNativeUDF<int32_t, int64_t>("week.int64", 22,
                                     1590115420000L + 9 * 86400000L);
}
TEST_F(UDFIRBuilderTest, inc_int32_udf_test) {
    CheckNativeUDF<int32_t, int32_t>("inc.int32", 2021, 2020);
}
TEST_F(UDFIRBuilderTest, distinct_count_udf_test) {
    std::vector<int32_t> vec = {1, 1, 3, 3, 5, 5, 7, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    CheckNativeUDF<int32_t, codec::ListRef *>("count.list_int32", 9, &list_ref);
    CheckNativeUDF<int32_t, codec::ListRef *>("distinct_count.list_int32", 5,
                                              &list_ref);
}
}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
