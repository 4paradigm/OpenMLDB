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

template <class Ret, class... Args>
void CheckUDF(const std::string &name, Ret expect, Args... args) {
    auto function = udf::UDFFunctionBuilder(name)
                        .args<Args...>()
                        .template returns<Ret>()
                        .library(udf::DefaultUDFLibrary::get())
                        .build();
    ASSERT_TRUE(function.valid());
    auto result = function(args...);
    udf::EqualValChecker<Ret>::check(expect, result);
}

template <typename T>
codec::ListRef<T> MakeList(const std::initializer_list<T> &vec) {
    codec::ArrayListV<T> *list =
        new codec::ArrayListV<T>(new std::vector<T>(vec));
    codec::ListRef<T> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(list);
    return list_ref;
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
    CheckUDF<double, int32_t>("log2", log2(65536), 65536);
    CheckUDF<double, double>("log2", log2(2.0), 2.0);
    CheckUDF<double, int32_t>("log10", log10(65536), 65536);
    CheckUDF<double, double>("log10", log10(2.0), 2.0);
}

TEST_F(UDFIRBuilderTest, count_where_test) {
    CheckUDF<int64_t, codec::ListRef<int32_t>, codec::ListRef<bool>>(
        "count_where", 2, MakeList<int32_t>({4, 5, 6}),
        MakeList<bool>({true, false, true}));
}

TEST_F(UDFIRBuilderTest, avg_test) {
    CheckUDF<double, codec::ListRef<int16_t>>("avg", 2.5,
                                              MakeList<int16_t>({1, 2, 3, 4}));
    CheckUDF<double, codec::ListRef<int32_t>>("avg", 2.5,
                                              MakeList<int32_t>({1, 2, 3, 4}));
    CheckUDF<double, codec::ListRef<int64_t>>("avg", 2.5,
                                              MakeList<int64_t>({1, 2, 3, 4}));
    CheckUDF<double, codec::ListRef<float>>("avg", 2.5,
                                            MakeList<float>({1, 2, 3, 4}));
    CheckUDF<double, codec::ListRef<double>>("avg", 2.5,
                                             MakeList<double>({1, 2, 3, 4}));

    // empty list
    CheckUDF<double, codec::ListRef<double>>("avg", 0.0 / 0,
                                             MakeList<double>({}));
}

}  // namespace codegen
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
