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

codec::ListRef<bool> MakeBoolList(const std::initializer_list<int> &vec) {
    codec::BoolArrayListV *list =
        new codec::BoolArrayListV(new std::vector<int>(vec));
    codec::ListRef<bool> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(list);
    return list_ref;
}

template <class T, class... Args>
void CheckUDFFail(const std::string &name, T expect, Args... args) {
    auto function = udf::UDFFunctionBuilder(name)
                        .args<Args...>()
                        .template returns<T>()
                        .build();
    ASSERT_FALSE(function.valid());
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
    // CheckUDF<double, int16_t>("acos", 0, 1);
    // CheckUDF<double, int16_t>("acos", 1.5707963267948966, 0);
    CheckUDF<double, int32_t>("acos", 0, 1);
    CheckUDF<double, int32_t>("acos", 1.5707963267948966, 0);
    CheckUDF<double, int64_t>("acos", 0, 1);
    CheckUDF<double, int64_t>("acos", 1.5707963267948966, 0);
    CheckUDF<float, float>("acos", acosf(0.5f), 0.5f);
    CheckUDF<double, double>("acos", 1.0471975511965979, 0.5);
    // CheckUDF<double, double>("acos", nan, -2.1);
}

TEST_F(UDFIRBuilderTest, asin_udf_test) {
    // CheckUDF<double, int16_t>("asin", 0, 0);
    // CheckUDF<double, int16_t>("asin", 1.5707963267948966, 1);
    CheckUDF<double, int32_t>("asin", 0, 0);
    CheckUDF<double, int32_t>("asin", 1.5707963267948966, 1);
    CheckUDF<double, int64_t>("asin", 0, 0);
    CheckUDF<double, int64_t>("asin", 1.5707963267948966, 1);
    CheckUDF<float, float>("asin", asinf(0.2f), 0.2f);
    CheckUDF<double, double>("asin", 0.2013579207903308, 0.2);
    // CheckUDF<double, double>("asin", nan, -2.1);
}

TEST_F(UDFIRBuilderTest, atan_udf_test) {
    // CheckUDF<double, int16_t>("atan", 0, 0);
    // CheckUDF<double, int16_t>("atan", 1.1071487177940904, 2);
    CheckUDF<double, int32_t>("atan", -1.1071487177940904, -2);
    CheckUDF<double, int32_t>("atan", 1.1071487177940904, 2);
    CheckUDF<double, int64_t>("atan", 0, 0);
    CheckUDF<double, int64_t>("atan", -1.1071487177940904, -2);
    CheckUDF<float, float>("atan", atan(-45.01f), -45.01f);
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
    CheckUDF<int16_t, int16_t>("ceil", 5, 5);
    CheckUDF<int32_t, int32_t>("ceil", 32769, 32769);
    CheckUDF<int64_t, int64_t>("ceil", 2147483649, 2147483649);
    CheckUDF<int, float>("ceil", 0, -0.1);
    CheckUDF<int, float>("ceil", 2, 1.23);
    CheckUDF<int, double>("ceil", -1, -1.23);
    CheckUDF<int, double>("ceil", 0, 0);
}

TEST_F(UDFIRBuilderTest, substring_pos_len_udf_test) {
    CheckUDF<codec::StringRef, codec::StringRef, int32_t, int32_t>(
        "substring", codec::StringRef("12345"), codec::StringRef("1234567890"),
        1, 5);

    CheckUDF<codec::StringRef, codec::StringRef, int32_t, int32_t>(
        "substring", codec::StringRef("23456"), codec::StringRef("1234567890"),
        2, 5);

    CheckUDF<codec::StringRef, codec::StringRef, int32_t, int32_t>(
        "substring", codec::StringRef("23456"), codec::StringRef("1234567890"),
        -9, 5);

    CheckUDF<codec::StringRef, codec::StringRef, int32_t, int32_t>(
        "substring", codec::StringRef("90"), codec::StringRef("1234567890"), -2,
        5);

    CheckUDF<codec::StringRef, codec::StringRef, int32_t, int32_t>(
        "substring", codec::StringRef(""), codec::StringRef("1234567890"), 2,
        0);

    CheckUDF<codec::StringRef, codec::StringRef, int32_t, int32_t>(
        "substring", codec::StringRef(""), codec::StringRef("1234567890"), 2,
        -1);
}

TEST_F(UDFIRBuilderTest, substring_pos_udf_test) {
    CheckUDF<codec::StringRef, codec::StringRef, int32_t>(
        "substring", codec::StringRef("1234567890"),
        codec::StringRef("1234567890"), 1);

    CheckUDF<codec::StringRef, codec::StringRef, int32_t>(
        "substring", codec::StringRef("234567890"),
        codec::StringRef("1234567890"), 2);

    CheckUDF<codec::StringRef, codec::StringRef, int32_t>(
        "substring", codec::StringRef("234567890"),
        codec::StringRef("1234567890"), -9);

    CheckUDF<codec::StringRef, codec::StringRef, int32_t>(
        "substring", codec::StringRef("90"), codec::StringRef("1234567890"),
        -2);

    CheckUDF<codec::StringRef, codec::StringRef, int32_t>(
        "substring", codec::StringRef(""), codec::StringRef("1234567890"), 12);
    CheckUDF<codec::StringRef, codec::StringRef, int32_t>(
        "substring", codec::StringRef(""), codec::StringRef("1234567890"), -12);
}

TEST_F(UDFIRBuilderTest, concat_str_udf_test) {
    //    concat("12345") == "12345"
    CheckUDF<codec::StringRef, codec::StringRef>(
        "concat", codec::StringRef("12345"),
        codec::StringRef(std::string("12345")));

    // concat("12345", "67890") == "1234567890"
    CheckUDF<codec::StringRef, codec::StringRef, codec::StringRef>(
        "concat", codec::StringRef("1234567890"),
        codec::StringRef(std::string("12345")), codec::StringRef("67890"));

    // concat("123", "4567890", "abcde") == "1234567890abcde"
    CheckUDF<codec::StringRef, codec::StringRef, codec::StringRef,
             codec::StringRef>("concat", codec::StringRef("1234567890abcde"),
                               codec::StringRef(std::string("123")),
                               codec::StringRef("4567890"),
                               codec::StringRef("abcde"));

    // concat("1", "23", "456", "7890", "abc", "de") == "1234567890abcde"
    CheckUDF<codec::StringRef, codec::StringRef, codec::StringRef,
             codec::StringRef>("concat", codec::StringRef("1234567890abcde"),
                               codec::StringRef("1"), codec::StringRef("23"),
                               codec::StringRef("456"),
                               codec::StringRef("7890"),
                               codec::StringRef("abc"), codec::StringRef("de"));

    //    concat() == ""
    CheckUDFFail<codec::StringRef>("concat", codec::StringRef("no result"));
}
TEST_F(UDFIRBuilderTest, concat_anytype_udf_test) {
    CheckUDF<codec::StringRef, codec::StringRef, int32_t>(
        "concat", codec::StringRef("1234567890"), codec::StringRef("12345"),
        67890);

    CheckUDF<codec::StringRef, float, int32_t>(
        "concat", codec::StringRef("1234.567890"), 1234.5f, 67890);

    CheckUDF<codec::StringRef, codec::StringRef, int16_t, int32_t, int64_t,
             float, double, codec::Timestamp, codec::Date>(
        "concat", codec::StringRef("12345.67.82020-05-22 10:43:402020-06-23"),
        codec::StringRef("1"), static_cast<int16_t>(2), 3, 4L, 5.6f, 7.8,
        codec::Timestamp(1590115420000L), codec::Date(2020, 06, 23));
}

TEST_F(UDFIRBuilderTest, concat_ws_anytype_udf_test) {
    // concat on string "--"
    CheckUDF<codec::StringRef, codec::StringRef, codec::StringRef, int32_t>(
        "concat_ws", codec::StringRef("12345--67890"), codec::StringRef("--"),
        codec::StringRef("12345"), 67890);

    // concat on int32
    CheckUDF<codec::StringRef, int32_t, float, int32_t>(
        "concat_ws", codec::StringRef("1234.5067890"), 0, 1234.5f, 67890);

    // concat on string "#"
    CheckUDF<codec::StringRef, codec::StringRef, codec::StringRef, int16_t,
             int32_t, int64_t, float, double, codec::Timestamp, codec::Date>(
        "concat_ws",

        codec::StringRef("1#2#3#4#5.6#7.8#2020-05-22 10:43:40#2020-06-23"),
        codec::StringRef("#"), codec::StringRef("1"), static_cast<int16_t>(2),
        3, 4L, 5.6f, 7.8, codec::Timestamp(1590115420000L),
        codec::Date(2020, 06, 23));
}

TEST_F(UDFIRBuilderTest, to_string_test) {
    CheckUDF<codec::StringRef, int32_t>("string", codec::StringRef("67890"),
                                        67890);
    CheckUDF<codec::StringRef, int16_t>("string", codec::StringRef("128"),
                                        static_cast<int16_t>(128));
    CheckUDF<codec::StringRef, float>("string", codec::StringRef("1.234"),
                                      1.234f);
    CheckUDF<codec::StringRef, double>("string", codec::StringRef("1.234"),
                                       1.234);

    CheckUDF<codec::StringRef, int64_t>(
        "string", codec::StringRef("1234567890"), 1234567890L);

    CheckUDF<codec::StringRef, int64_t>(
        "string", codec::StringRef("1234567890"), 1234567890L);
    CheckUDF<codec::StringRef, codec::Timestamp>(
        "string", codec::StringRef("2020-05-22 10:43:40"),
        codec::Timestamp(1590115420000L));

    CheckUDF<codec::StringRef, codec::Date>(
        "string", codec::StringRef("2020-05-22"), codec::Date(2020, 5, 22));
}

TEST_F(UDFIRBuilderTest, timestamp_format_test) {
    CheckUDF<codec::StringRef, codec::Timestamp, codec::StringRef>(
        "date_format", codec::StringRef("2020-05-22 10:43:40"),
        codec::Timestamp(1590115420000L),
        codec::StringRef("%Y-%m-%d %H:%M:%S"));

    CheckUDF<codec::StringRef, codec::Timestamp, codec::StringRef>(
        "date_format", codec::StringRef("2020-05-22"),
        codec::Timestamp(1590115420000L), codec::StringRef("%Y-%m-%d"));

    CheckUDF<codec::StringRef, codec::Timestamp, codec::StringRef>(
        "date_format", codec::StringRef("10:43:40"),
        codec::Timestamp(1590115420000L), codec::StringRef("%H:%M:%S"));
}

TEST_F(UDFIRBuilderTest, date_format_test) {
    CheckUDF<codec::StringRef, codec::Date, codec::StringRef>(
        "date_format", codec::StringRef("2020-05-22 00:00:00"),
        codec::Date(2020, 05, 22), codec::StringRef("%Y-%m-%d %H:%M:%S"));

    CheckUDF<codec::StringRef, codec::Date, codec::StringRef>(
        "date_format", codec::StringRef("2020-05-22"),
        codec::Date(2020, 05, 22), codec::StringRef("%Y-%m-%d"));

    CheckUDF<codec::StringRef, codec::Date, codec::StringRef>(
        "date_format", codec::StringRef("00:00:00"), codec::Date(2020, 05, 22),
        codec::StringRef("%H:%M:%S"));
}

TEST_F(UDFIRBuilderTest, count_where_test) {
    CheckUDF<int64_t, codec::ListRef<int32_t>, codec::ListRef<bool>>(
        "count_where", 2, MakeList<int32_t>({4, 5, 6}),
        MakeBoolList({true, false, true}));

    CheckUDF<int64_t, codec::ListRef<codec::StringRef>, codec::ListRef<bool>>(
        "count_where", 2,
        MakeList({codec::StringRef("1"), codec::StringRef("2"),
                  codec::StringRef("3")}),
        MakeBoolList({true, false, true}));
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
