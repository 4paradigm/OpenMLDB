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
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "udf/udf_test.h"
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
void CheckExternalUDF(const std::string udf_name, T exp, Args... args) {
    base::Status status;
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("udf_test", *ctx);
    udf::DefaultUDFLibrary lib;
    ASSERT_TRUE(fesql::udf::RegisterUDFToModule(m.get()));
    m->print(::llvm::errs(), NULL, true, true);

    auto J = ExitOnErr(LLJITBuilder().create());
    auto &jd = J->getMainJITDylib();
    ::llvm::orc::MangleAndInterner mi(J->getExecutionSession(),
                                      J->getDataLayout());
    lib.InitJITSymbols(J.get());
    ::fesql::vm::InitCodecSymbol(jd, mi);
    ::fesql::udf::InitUDFSymbol(jd, mi);
    ExitOnErr(J->addIRModule(ThreadSafeModule(std::move(m), std::move(ctx))));
    auto fn = ExitOnErr(J->lookup(udf_name));
    T (*udf)(Args...) = (T(*)(Args...))fn.getAddress();
    ASSERT_EQ(exp, udf(args...));
}

template <class T, class... Args>
void CheckUDF(const std::string &name, T expect, Args... args) {
    auto function = udf::UDFFunctionBuilder(name)
                        .args<Args...>()
                        .template returns<T>()
                        .build();
    ASSERT_TRUE(function.valid());
    auto result = function(args...);
    ASSERT_EQ(expect, result);
}

template <class T, class... Args>
void CheckFloatUDF(const std::string &name, T expect, Args... args) {
    auto function = udf::UDFFunctionBuilder(name)
                        .args<Args...>()
                        .template returns<T>()
                        .build();
    ASSERT_TRUE(function.valid());
    auto result = function(args...);
    LOG(INFO) << expect << "  " << result;
    ASSERT_FLOAT_EQ(expect, result);
}

TEST_F(UDFIRBuilderTest, dayofmonth_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckUDF<int32_t, codec::Date>("dayofmonth", 22, date);
}
TEST_F(UDFIRBuilderTest, month_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckUDF<int32_t, codec::Date>("month", 5, date);
}
TEST_F(UDFIRBuilderTest, year_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckUDF<int32_t, codec::Date>("year", 2020, date);
}
TEST_F(UDFIRBuilderTest, dayofweek_date_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckUDF<int32_t, codec::Date>("dayofweek", 6, date);
}
TEST_F(UDFIRBuilderTest, weekofyear_date_udf_test) {
    {
        codec::Date date(2020, 01, 01);
        CheckUDF<int32_t, codec::Date>("weekofyear", 1, date);
    }
    {
        codec::Date date(2020, 01, 02);
        CheckUDF<int32_t, codec::Date>("weekofyear", 1, date);
    }
    {
        codec::Date date(2020, 01, 03);
        CheckUDF<int32_t, codec::Date>("weekofyear", 1, date);
    }
    {
        codec::Date date(2020, 01, 04);
        CheckUDF<int32_t, codec::Date>("weekofyear", 1, date);
    }
    {
        codec::Date date(2020, 01, 05);
        CheckUDF<int32_t, codec::Date>("weekofyear", 1, date);
    }
    {
        codec::Date date(2020, 01, 06);
        CheckUDF<int32_t, codec::Date>("weekofyear", 2, date);
    }
    {
        codec::Date date(2020, 05, 22);
        CheckUDF<int32_t, codec::Date>("weekofyear", 21, date);
    }
    {
        codec::Date date(2020, 05, 23);
        CheckUDF<int32_t, codec::Date>("weekofyear", 21, date);
    }
    {
        codec::Date date(2020, 05, 24);
        CheckUDF<int32_t, codec::Date>("weekofyear", 21, date);
    }
    {
        codec::Date date(2020, 05, 25);
        CheckUDF<int32_t, codec::Date>("weekofyear", 22, date);
    }
}

TEST_F(UDFIRBuilderTest, minute_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckUDF<int32_t, codec::Timestamp>("minute", 43, time);
}
TEST_F(UDFIRBuilderTest, second_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckUDF<int32_t, codec::Timestamp>("second", 40, time);
}
TEST_F(UDFIRBuilderTest, hour_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckUDF<int32_t, codec::Timestamp>("hour", 10, time);
}
TEST_F(UDFIRBuilderTest, dayofmonth_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckUDF<int32_t, codec::Timestamp>("dayofmonth", 22, time);
}

TEST_F(UDFIRBuilderTest, dayofweek_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckUDF<int32_t, codec::Timestamp>("dayofweek", 6, time);
}
TEST_F(UDFIRBuilderTest, weekofyear_timestamp_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckUDF<int32_t, codec::Timestamp>("weekofyear", 21, time);
}

TEST_F(UDFIRBuilderTest, month_timestamp_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckUDF<int32_t, codec::Date>("month", 5, date);
}
TEST_F(UDFIRBuilderTest, year_timestamp_udf_test) {
    codec::Date date(2020, 05, 22);
    CheckUDF<int32_t, codec::Date>("year", 2020, date);
}

TEST_F(UDFIRBuilderTest, minute_int64_udf_test) {
    CheckUDF<int32_t, int64_t>("minute", 43, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, second_int64_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckUDF<int32_t, int64_t>("second", 40, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, hour_int64_udf_test) {
    codec::Timestamp time(1590115420000L);
    CheckUDF<int32_t, int64_t>("hour", 10, 1590115420000L);
}

TEST_F(UDFIRBuilderTest, dayofmonth_int64_udf_test) {
    CheckUDF<int32_t, int64_t>("dayofmonth", 22, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, month_int64_udf_test) {
    CheckUDF<int32_t, int64_t>("month", 5, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, year_int64_udf_test) {
    CheckUDF<int32_t, int64_t>("year", 2020, 1590115420000L);
}
TEST_F(UDFIRBuilderTest, dayofweek_int64_udf_test) {
    CheckUDF<int32_t, int64_t>("dayofweek", 6, 1590115420000L);
    CheckUDF<int32_t, int64_t>("dayofweek", 7, 1590115420000L + 86400000L);

    // Sunday
    CheckUDF<int32_t, int64_t>("dayofweek", 1, 1590115420000L + 2 * 86400000L);
    CheckUDF<int32_t, int64_t>("dayofweek", 2, 1590115420000L + 3 * 86400000L);
}
TEST_F(UDFIRBuilderTest, weekofyear_int64_udf_test) {
    CheckUDF<int32_t, int64_t>("weekofyear", 21, 1590115420000L);
    CheckUDF<int32_t, int64_t>("weekofyear", 21, 1590115420000L + 86400000L);

    //     Sunday
    CheckUDF<int32_t, int64_t>("dayofmonth", 24,
                               1590115420000L + 2 * 86400000L);
    CheckUDF<int32_t, int64_t>("weekofyear", 21,
                               1590115420000L + 2 * 86400000L);
    //     Monday
    CheckUDF<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 3 * 86400000L);
    CheckUDF<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 4 * 86400000L);
    CheckUDF<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 5 * 86400000L);
    CheckUDF<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 6 * 86400000L);
    CheckUDF<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 7 * 86400000L);
    CheckUDF<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 8 * 86400000L);
    CheckUDF<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 9 * 86400000L);

    // Monday
    CheckUDF<int32_t, int64_t>("weekofyear", 23,
                               1590115420000L + 10 * 86400000L);
}
TEST_F(UDFIRBuilderTest, inc_int32_udf_test) {
    CheckUDF<int32_t, int32_t>("inc", 2021, 2020);
}
TEST_F(UDFIRBuilderTest, distinct_count_udf_test) {
    std::vector<int32_t> vec = {1, 1, 3, 3, 5, 5, 7, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    CheckUDF<int64_t, codec::ListRef<int32_t>>("count", 9, list_ref);
    CheckUDF<int64_t, codec::ListRef<int32_t>>("distinct_count", 5, list_ref);
}
TEST_F(UDFIRBuilderTest, sum_udf_test) {
    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckUDF<int32_t, codec::ListRef<int32_t>>("sum", 1 + 3 + 5 + 7 + 9,
                                               list_ref);
}
TEST_F(UDFIRBuilderTest, min_udf_test) {
    std::vector<int32_t> vec = {10, 8, 6, 4, 2, 1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckUDF<int32_t, codec::ListRef<int32_t>>("min", 1, list_ref);
}
TEST_F(UDFIRBuilderTest, max_udf_test) {
    std::vector<int32_t> vec = {10, 8, 6, 4, 2, 1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckUDF<int32_t, codec::ListRef<int32_t>>("max", 10, list_ref);
}

TEST_F(UDFIRBuilderTest, max_timestamp_udf_test) {
    std::vector<codec::Timestamp> vec = {
        codec::Timestamp(1590115390000L), codec::Timestamp(1590115410000L),
        codec::Timestamp(1590115420000L), codec::Timestamp(1590115430000L),
        codec::Timestamp(1590115400000L)};
    codec::ArrayListV<codec::Timestamp> list(&vec);
    codec::ListRef<codec::Timestamp> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    codec::Timestamp max_time;
    CheckUDF<codec::Timestamp, codec::ListRef<codec::Timestamp>>(
        "max", codec::Timestamp(1590115430000L), list_ref);
}
TEST_F(UDFIRBuilderTest, min_timestamp_udf_test) {
    std::vector<codec::Timestamp> vec = {
        codec::Timestamp(1590115390000L), codec::Timestamp(1590115410000L),
        codec::Timestamp(1590115420000L), codec::Timestamp(1590115430000L),
        codec::Timestamp(1590115400000L)};
    codec::ArrayListV<codec::Timestamp> list(&vec);
    codec::ListRef<codec::Timestamp> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    codec::Timestamp max_time;
    CheckUDF<codec::Timestamp, codec::ListRef<codec::Timestamp>>(
        "min", codec::Timestamp(1590115390000L), list_ref);
}

TEST_F(UDFIRBuilderTest, log_udf_test) {
    CheckUDF<float, float>("log", log(2.0f), 2.0f);
    CheckUDF<double, double>("log", log(2.0), 2.0);
    CheckUDF<double, double, int16_t>("log", 2, 10, 100);
    CheckUDF<double, double, int32_t>("log", 16, 2, 65536);
    CheckUDF<double, int32_t>("log2", log2(65536), 65536);
    CheckUDF<double, double>("log2", log2(2.0), 2.0);
    CheckUDF<double, int32_t>("log10", log10(65536), 65536);
    CheckUDF<double, double>("log10", log10(2.0), 2.0);
}

TEST_F(UDFIRBuilderTest, abs_udf_test) {
    CheckUDF<int16_t, int16_t>("abs", 32767, 32767);
    CheckUDF<int16_t, int16_t>("abs", 32767, -32767);
    CheckUDF<int32_t, int32_t>("abs", 32768, 32768);
    CheckUDF<int32_t, int32_t>("abs", 32769, -32769);
    CheckUDF<int64_t, int64_t>("abs", 2147483648, 2147483648);
    CheckUDF<int64_t, int64_t>("abs", 2147483649, -2147483649);
    CheckUDF<float, float>("abs", 2.1f, 2.1f);
    CheckUDF<float, float>("abs", 2.1f, -2.1f);
    CheckUDF<double, double>("abs", 2.1, 2.1);
    CheckUDF<double, double>("abs", 2.1, -2.1);
}

TEST_F(UDFIRBuilderTest, acos_udf_test) {
    CheckUDF<double, int16_t>("acos", 0, 1);
    CheckUDF<double, int16_t>("acos", 1.5707963267948966, 0);
    CheckUDF<double, int32_t>("acos", 0, 1);
    CheckUDF<double, int32_t>("acos", 1.5707963267948966, 0);
    CheckUDF<double, int64_t>("acos", 0, 1);
    CheckUDF<double, int64_t>("acos", 1.5707963267948966, 0);
    CheckFloatUDF<float, float>("acos", 1.0471976, 0.5f);
    CheckUDF<double, double>("acos", 1.0471975511965979, 0.5);
    // CheckUDF<double, double>("acos", nan, -2.1);
}

TEST_F(UDFIRBuilderTest, asin_udf_test) {
    CheckUDF<double, int16_t>("asin", 0, 0);
    CheckUDF<double, int16_t>("asin", 1.5707963267948966, 1);
    CheckUDF<double, int32_t>("asin", 0, 0);
    CheckUDF<double, int32_t>("asin", 1.5707963267948966, 1);
    CheckUDF<double, int64_t>("asin", 0, 0);
    CheckUDF<double, int64_t>("asin", 1.5707963267948966, 1);
    CheckFloatUDF<float, float>("asin", 0.2013579, 0.2f);
    CheckUDF<double, double>("asin", 0.2013579207903308, 0.2);
    // CheckUDF<double, double>("asin", nan, -2.1);
}

TEST_F(UDFIRBuilderTest, atan_udf_test) {
    CheckUDF<double, int16_t>("atan", 0, 0);
    CheckUDF<double, int16_t>("atan", 1.1071487177940904, 2);
    CheckUDF<double, int32_t>("atan", -1.1071487177940904, -2);
    CheckUDF<double, int32_t>("atan", 1.1071487177940904, 2);
    CheckUDF<double, int64_t>("atan", 0, 0);
    CheckUDF<double, int64_t>("atan", -1.1071487177940904, -2);
    CheckFloatUDF<float, float>("atan", -1.5485827, -45.01f);
    CheckUDF<double, double>("atan", 0.1462226769376524, 0.1472738);
    CheckUDF<double, int16_t, int32_t>("atan", 2.3561944901923448, 2, -2);
    CheckUDF<double, int64_t, int32_t>("atan", 2.3561944901923448, 2, -2);
    CheckUDF<double, int64_t, float>("atan", 2.3561944901923448, 2, -2);
    CheckUDF<double, double, int32_t>("atan", 2.3561944901923448, 2, -2);
}

TEST_F(UDFIRBuilderTest, atan2_udf_test) {
    CheckUDF<double, int16_t, int32_t>("atan2", 2.3561944901923448, 2, -2);
    CheckUDF<double, int64_t, int32_t>("atan2", 2.3561944901923448, 2, -2);
    CheckUDF<double, int64_t, float>("atan2", 2.3561944901923448, 2, -2);
    CheckUDF<double, double, int32_t>("atan2", 2.3561944901923448, 2, -2);
}

TEST_F(UDFIRBuilderTest, ceil_udf_test) {
    CheckUDF<double, int16_t>("ceil", 5, 5);
    CheckUDF<double, int32_t>("ceil", 32769, 32769);
    CheckUDF<double, int64_t>("ceil", 2147483649, 2147483649);
    CheckUDF<float, float>("ceil", 0, -0.1);
    CheckUDF<float, float>("ceil", 2, 1.23);
    CheckUDF<double, double>("ceil", -1, -1.23);
    CheckUDF<double, double>("ceil", 0, 0);
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
