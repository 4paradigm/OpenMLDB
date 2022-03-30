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
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "udf/udf_test.h"
#include "vm/sql_compiler.h"

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

ExitOnError ExitOnErr;

namespace hybridse {
namespace codegen {

using openmldb::base::Date;
using openmldb::base::StringRef;
using openmldb::base::Timestamp;
using udf::Nullable;

class UdfIRBuilderTest : public ::testing::Test {
 public:
    UdfIRBuilderTest() {}

    ~UdfIRBuilderTest() {}
};

template <class Ret, class... Args>
void CheckUdf(const std::string &name, Ret expect, Args... args) {
    auto function = udf::UdfFunctionBuilder(name)
                        .args<Args...>()
                        .template returns<Ret>()
                        .library(udf::DefaultUdfLibrary::get())
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
void CheckUdfFail(const std::string &name, T expect, Args... args) {
    auto function = udf::UdfFunctionBuilder(name)
                        .args<Args...>()
                        .template returns<T>()
                        .build();
    ASSERT_FALSE(function.valid());
}

TEST_F(UdfIRBuilderTest, dayofmonth_date_udf_test) {
    CheckUdf<int32_t, Date>("dayofmonth", 22, Date(2020, 05, 22));
    CheckUdf<Nullable<int32_t>, Nullable<Date>>("dayofmonth", nullptr, nullptr);
}
TEST_F(UdfIRBuilderTest, month_date_udf_test) {
    CheckUdf<int32_t, Date>("month", 5, Date(2020, 05, 22));
    CheckUdf<Nullable<int32_t>, Nullable<Date>>("month", nullptr, nullptr);
}
TEST_F(UdfIRBuilderTest, year_date_udf_test) {
    CheckUdf<int32_t, Date>("year", 2020, Date(2020, 05, 22));
    CheckUdf<Nullable<int32_t>, Nullable<Date>>("year", nullptr, nullptr);
}
TEST_F(UdfIRBuilderTest, dayofweek_date_udf_test) {
    Date date(2020, 05, 22);
    CheckUdf<int32_t, Date>("dayofweek", 6, date);
}
TEST_F(UdfIRBuilderTest, dayofyear_date_udf_test) {
    {
        Date date(2020, 05, 22);
        CheckUdf<int32_t, Date>("dayofyear", 143, date);
    }
    {
        Date date(2021, 01, 01);
        CheckUdf<int32_t, Date>("dayofyear", 1, date);
    }
    {
        Date date(2020, 12, 31);
        CheckUdf<int32_t, Date>("dayofyear", 366, date);
    }
    {
        Date date(2021, 12, 31);
        CheckUdf<int32_t, Date>("dayofyear", 365, date);
    }
    {
        Date date(2021, 13, 31);
        CheckUdf<int32_t, Date>("dayofyear", 0, date);
    }
    {
        Date date(2021, 0, 31);
        CheckUdf<int32_t, Date>("dayofyear", 0, date);
    }
    {
        Date date(2021, -1, 31);
        CheckUdf<int32_t, Date>("dayofyear", 0, date);
    }
    {
        Date date(2021, 12, 32);
        CheckUdf<int32_t, Date>("dayofyear", 0, date);
    }
    {
        Date date(2021, 12, 0);
        CheckUdf<int32_t, Date>("dayofyear", 0, date);
    }
    {
        Date date(2021, 12, -10);
        CheckUdf<int32_t, Date>("dayofyear", 0, date);
    }
    {
        Date date(2021, 2, 29);
        CheckUdf<int32_t, Date>("dayofyear", 0, date);
    }
}
TEST_F(UdfIRBuilderTest, weekofyear_date_udf_test) {
    {
        Date date(2020, 01, 01);
        CheckUdf<int32_t, Date>("weekofyear", 1, date);
    }
    {
        Date date(2020, 01, 02);
        CheckUdf<int32_t, Date>("weekofyear", 1, date);
    }
    {
        Date date(2020, 01, 03);
        CheckUdf<int32_t, Date>("weekofyear", 1, date);
    }
    {
        Date date(2020, 01, 04);
        CheckUdf<int32_t, Date>("weekofyear", 1, date);
    }
    {
        Date date(2020, 01, 05);
        CheckUdf<int32_t, Date>("weekofyear", 1, date);
    }
    {
        Date date(2020, 01, 06);
        CheckUdf<int32_t, Date>("weekofyear", 2, date);
    }
    {
        Date date(2020, 05, 22);
        CheckUdf<int32_t, Date>("weekofyear", 21, date);
    }
    {
        Date date(2020, 05, 23);
        CheckUdf<int32_t, Date>("weekofyear", 21, date);
    }
    {
        Date date(2020, 05, 24);
        CheckUdf<int32_t, Date>("weekofyear", 21, date);
    }
    {
        Date date(2020, 05, 25);
        CheckUdf<int32_t, Date>("weekofyear", 22, date);
    }
}

TEST_F(UdfIRBuilderTest, minute_timestamp_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("minute", 43, time);
}
TEST_F(UdfIRBuilderTest, second_timestamp_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("second", 40, time);
}
TEST_F(UdfIRBuilderTest, hour_timestamp_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("hour", 10, time);
}
TEST_F(UdfIRBuilderTest, dayofmonth_timestamp_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("dayofmonth", 22, time);
}

TEST_F(UdfIRBuilderTest, dayofweek_timestamp_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("dayofweek", 6, time);
}
TEST_F(UdfIRBuilderTest, dayofyear_timestamp_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("dayofyear", 143, time);
}
TEST_F(UdfIRBuilderTest, weekofyear_timestamp_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("weekofyear", 21, time);
}

TEST_F(UdfIRBuilderTest, month_timestamp_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("month", 5, time);
}
TEST_F(UdfIRBuilderTest, year_timestamp_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("year", 2020, time);
}

TEST_F(UdfIRBuilderTest, minute_int64_udf_test) {
    CheckUdf<int32_t, int64_t>("minute", 43, 1590115420000L);
}
TEST_F(UdfIRBuilderTest, second_int64_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, int64_t>("second", 40, 1590115420000L);
}
TEST_F(UdfIRBuilderTest, hour_int64_udf_test) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, int64_t>("hour", 10, 1590115420000L);
}

TEST_F(UdfIRBuilderTest, dayofmonth_int64_udf_test) {
    CheckUdf<int32_t, int64_t>("dayofmonth", 22, 1590115420000L);
}
TEST_F(UdfIRBuilderTest, month_int64_udf_test) {
    CheckUdf<int32_t, int64_t>("month", 5, 1590115420000L);
}
TEST_F(UdfIRBuilderTest, year_int64_udf_test) {
    CheckUdf<int32_t, int64_t>("year", 2020, 1590115420000L);
}
TEST_F(UdfIRBuilderTest, dayofweek_int64_udf_test) {
    CheckUdf<int32_t, int64_t>("dayofweek", 6, 1590115420000L);
    CheckUdf<int32_t, int64_t>("dayofweek", 7, 1590115420000L + 86400000L);

    // Sunday
    CheckUdf<int32_t, int64_t>("dayofweek", 1, 1590115420000L + 2 * 86400000L);
    CheckUdf<int32_t, int64_t>("dayofweek", 2, 1590115420000L + 3 * 86400000L);
}
TEST_F(UdfIRBuilderTest, dayofyear_int64_udf_test) {
    CheckUdf<int32_t, int64_t>("dayofyear", 143, 1590115420000L);
    CheckUdf<int32_t, int64_t>("dayofyear", 144, 1590115420000L + 86400000L);
    CheckUdf<int32_t, int64_t>("dayofyear", 145, 1590115420000L + 2 * 86400000L);
    CheckUdf<int32_t, int64_t>("dayofyear", 146, 1590115420000L + 3 * 86400000L);
}
TEST_F(UdfIRBuilderTest, weekofyear_int64_udf_test) {
    CheckUdf<int32_t, int64_t>("weekofyear", 21, 1590115420000L);
    CheckUdf<int32_t, int64_t>("weekofyear", 21, 1590115420000L + 86400000L);

    //     Sunday
    CheckUdf<int32_t, int64_t>("dayofmonth", 24,
                               1590115420000L + 2 * 86400000L);
    CheckUdf<int32_t, int64_t>("weekofyear", 21,
                               1590115420000L + 2 * 86400000L);
    //     Monday
    CheckUdf<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 3 * 86400000L);
    CheckUdf<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 4 * 86400000L);
    CheckUdf<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 5 * 86400000L);
    CheckUdf<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 6 * 86400000L);
    CheckUdf<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 7 * 86400000L);
    CheckUdf<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 8 * 86400000L);
    CheckUdf<int32_t, int64_t>("weekofyear", 22,
                               1590115420000L + 9 * 86400000L);

    // Monday
    CheckUdf<int32_t, int64_t>("weekofyear", 23,
                               1590115420000L + 10 * 86400000L);
}
TEST_F(UdfIRBuilderTest, inc_int32_udf_test) {
    CheckUdf<int32_t, int32_t>("inc", 2021, 2020);
}
TEST_F(UdfIRBuilderTest, distinct_count_udf_test) {
    std::vector<int32_t> vec = {1, 1, 3, 3, 5, 5, 7, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    CheckUdf<int64_t, codec::ListRef<int32_t>>("count", 9, list_ref);
    CheckUdf<int64_t, codec::ListRef<int32_t>>("distinct_count", 5, list_ref);
}
TEST_F(UdfIRBuilderTest, sum_udf_test) {
    std::vector<int32_t> vec = {1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckUdf<int32_t, codec::ListRef<int32_t>>("sum", 1 + 3 + 5 + 7 + 9,
                                               list_ref);
}
TEST_F(UdfIRBuilderTest, min_udf_test) {
    std::vector<int32_t> vec = {10, 8, 6, 4, 2, 1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckUdf<int32_t, codec::ListRef<int32_t>>("min", 1, list_ref);
}
TEST_F(UdfIRBuilderTest, max_udf_test) {
    std::vector<int32_t> vec = {10, 8, 6, 4, 2, 1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckUdf<int32_t, codec::ListRef<int32_t>>("max", 10, list_ref);
}

TEST_F(UdfIRBuilderTest, max_timestamp_udf_test) {
    std::vector<Timestamp> vec = {
        Timestamp(1590115390000L), Timestamp(1590115410000L),
        Timestamp(1590115420000L), Timestamp(1590115430000L),
        Timestamp(1590115400000L)};
    codec::ArrayListV<Timestamp> list(&vec);
    codec::ListRef<Timestamp> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    Timestamp max_time;
    CheckUdf<Timestamp, codec::ListRef<Timestamp>>(
        "max", Timestamp(1590115430000L), list_ref);
}
TEST_F(UdfIRBuilderTest, min_timestamp_udf_test) {
    std::vector<Timestamp> vec = {
        Timestamp(1590115390000L), Timestamp(1590115410000L),
        Timestamp(1590115420000L), Timestamp(1590115430000L),
        Timestamp(1590115400000L)};
    codec::ArrayListV<Timestamp> list(&vec);
    codec::ListRef<Timestamp> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    Timestamp max_time;
    CheckUdf<Timestamp, codec::ListRef<Timestamp>>(
        "min", Timestamp(1590115390000L), list_ref);
}

TEST_F(UdfIRBuilderTest, log_udf_test) {
    CheckUdf<float, float>("log", log(2.0f), 2.0f);
    CheckUdf<double, double>("log", log(2.0), 2.0);
    CheckUdf<float, float>("ln", log(2.0f), 2.0f);
    CheckUdf<double, double>("ln", log(2.0), 2.0);
    CheckUdf<double, int32_t>("log2", log2(65536), 65536);
    CheckUdf<double, double>("log2", log2(2.0), 2.0);
    CheckUdf<double, int32_t>("log10", log10(65536), 65536);
    CheckUdf<double, double>("log10", log10(2.0), 2.0);
}

TEST_F(UdfIRBuilderTest, abs_udf_test) {
    CheckUdf<int32_t, int16_t>("abs", 32767, 32767);
    CheckUdf<int32_t, int16_t>("abs", 1, -1);
    CheckUdf<int32_t, int32_t>("abs", 32768, 32768);
    CheckUdf<int32_t, int32_t>("abs", 32769, -32769);
    CheckUdf<int64_t, int64_t>("abs", 2147483648, 2147483648);
    CheckUdf<int64_t, int64_t>("abs", 2147483649, -2147483649);
    CheckUdf<double, float>("abs", 2.1f, 2.1f);
    CheckUdf<double, float>("abs", 2.1f, -2.1f);
    CheckUdf<double, double>("abs", 2.1, 2.1);
    CheckUdf<double, double>("abs", 2.1, -2.1);
}

TEST_F(UdfIRBuilderTest, acos_udf_test) {
    CheckUdf<double, int16_t>("acos", 0, 1);
    CheckUdf<double, int16_t>("acos", 1.5707963267948966, 0);
    CheckUdf<double, int32_t>("acos", 0, 1);
    CheckUdf<double, int32_t>("acos", 1.5707963267948966, 0);
    CheckUdf<double, int64_t>("acos", 0, 1);
    CheckUdf<double, int64_t>("acos", 1.5707963267948966, 0);
    CheckUdf<float, float>("acos", acosf(0.5f), 0.5f);
    CheckUdf<double, double>("acos", 1.0471975511965979, 0.5);
    // CheckUdf<double, double>("acos", nan, -2.1);
}

TEST_F(UdfIRBuilderTest, asin_udf_test) {
    CheckUdf<double, int16_t>("asin", 0, 0);
    CheckUdf<double, int16_t>("asin", 1.5707963267948966, 1);
    CheckUdf<double, int32_t>("asin", 0, 0);
    CheckUdf<double, int32_t>("asin", 1.5707963267948966, 1);
    CheckUdf<double, int64_t>("asin", 0, 0);
    CheckUdf<double, int64_t>("asin", 1.5707963267948966, 1);
    CheckUdf<float, float>("asin", asinf(0.2f), 0.2f);
    CheckUdf<double, double>("asin", 0.2013579207903308, 0.2);
    // CheckUdf<double, double>("asin", nan, -2.1);
}

TEST_F(UdfIRBuilderTest, atan_udf_test_0) {
    CheckUdf<double, int16_t>("atan", 0, 0);
}
TEST_F(UdfIRBuilderTest, atan_udf_test_1) {
    CheckUdf<double, int16_t>("atan", 1.1071487177940904, 2);
}
TEST_F(UdfIRBuilderTest, atan_udf_test_2) {
    CheckUdf<double, int32_t>("atan", -1.1071487177940904, -2);
}
TEST_F(UdfIRBuilderTest, atan_udf_test_3) {
    CheckUdf<double, int32_t>("atan", 1.1071487177940904, 2);
}
TEST_F(UdfIRBuilderTest, atan_udf_test_4) {
    CheckUdf<double, int64_t>("atan", 0, 0);
}
TEST_F(UdfIRBuilderTest, atan_udf_test_5) {
    CheckUdf<double, int64_t>("atan", -1.1071487177940904, -2);
}
TEST_F(UdfIRBuilderTest, atan_udf_test_6) {
    CheckUdf<float, float>("atan", atan(-45.01f), -45.01f);
}
TEST_F(UdfIRBuilderTest, atan_udf_test_7) {
    CheckUdf<double, double>("atan", 0.1462226769376524, 0.1472738);
}
TEST_F(UdfIRBuilderTest, atan_udf_test_8) {
    CheckUdf<double, int16_t, int32_t>("atan", 2.3561944901923448, 2, -2);
}
TEST_F(UdfIRBuilderTest, atan_udf_test_9) {
    CheckUdf<double, int64_t, int32_t>("atan", 2.3561944901923448, 2, -2);
}
TEST_F(UdfIRBuilderTest, atan_udf_test_10) {
    CheckUdf<double, int64_t, float>("atan", 2.3561944901923448, 2, -2);
}

TEST_F(UdfIRBuilderTest, atan2_udf_test_15) {
    CheckUdf<double, double, int32_t>("atan2", 2.3561944901923448, 2, -2);
}

TEST_F(UdfIRBuilderTest, ceil_udf_test) {
    CheckUdf<int64_t, int16_t>("ceil", 5, 5);
    CheckUdf<int64_t, int32_t>("ceil", 32769, 32769);
    CheckUdf<int64_t, int64_t>("ceil", 2147483649, 2147483649);
    CheckUdf<double, float>("ceil", 0, -0.1f);
    CheckUdf<double, float>("ceil", 2, 1.23f);
    CheckUdf<double, double>("ceil", -1, -1.23);
    CheckUdf<double, double>("ceil", 0, 0);
}

TEST_F(UdfIRBuilderTest, ceiling_udf_test) {
    CheckUdf<int64_t, int16_t>("ceiling", 5, 5);
    CheckUdf<int64_t, int32_t>("ceiling", 32769, 32769);
    CheckUdf<int64_t, int64_t>("ceiling", 2147483649, 2147483649);
    CheckUdf<double, float>("ceiling", 0, -0.1f);
    CheckUdf<double, float>("ceiling", 2, 1.23f);
    CheckUdf<double, double>("ceiling", -1, -1.23);
    CheckUdf<double, double>("ceiling", 0, 0);
}

TEST_F(UdfIRBuilderTest, cos_udf_test) {
    CheckUdf<double, int16_t>("cos", cos(5), 5);
    CheckUdf<double, int32_t>("cos", cos(65536), 65536);
    CheckUdf<double, int64_t>("cos", cos(2147483648), 2147483648);
    CheckUdf<float, float>("cos", cosf(0.5f), 0.5f);
    CheckUdf<double, double>("cos", cos(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, cot_udf_test) {
    CheckUdf<double, int16_t>("cot", cos(5) / sin(5), 5);
    CheckUdf<double, int32_t>("cot", cos(65536) / sin(65536), 65536);
    CheckUdf<double, int64_t>("cot", cos(2147483648) / sin(2147483648),
                              2147483648);
    CheckUdf<float, float>("cot", cosf(0.5f) / sin(0.5f), 0.5f);
    CheckUdf<double, double>("cot", cos(0.5) / sin(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, exp_udf_test) {
    CheckUdf<double, int16_t>("exp", exp(5), 5);
    CheckUdf<double, int32_t>("exp", exp(65536), 65536);
    CheckUdf<double, int64_t>("exp", exp(2147483648), 2147483648);
    CheckUdf<float, float>("exp", expf(0.5f), 0.5f);
    CheckUdf<double, double>("exp", exp(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, floor_udf_test) {
    CheckUdf<int64_t, int16_t>("floor", 5, 5);
    CheckUdf<int64_t, int32_t>("floor", 32769, 32769);
    CheckUdf<int64_t, int64_t>("floor", 2147483649, 2147483649);
    CheckUdf<double, float>("floor", -1, -0.1f);
    CheckUdf<double, float>("floor", 1, 1.23f);
    CheckUdf<double, double>("floor", -2, -1.23);
    CheckUdf<double, double>("floor", 0, 0);
}

TEST_F(UdfIRBuilderTest, pow_udf_test) {
    CheckUdf<double, int16_t, int32_t>("pow", pow(2, 65536), 2, 65536);
    CheckUdf<double, int64_t, int32_t>("pow", pow(2147483648, 65536),
                                       2147483648, 65536);
    CheckUdf<double, int64_t, float>("pow", pow(2147483648, 2.1f), 2147483648,
                                     2.1f);
    CheckUdf<float, float, float>("pow", powf(2147483648, 2.1f), 2147483648,
                                  2.1f);
    CheckUdf<double, double, int32_t>("pow", pow(2147483648, 65536), 2147483648,
                                      65536);
}

TEST_F(UdfIRBuilderTest, power_udf_test) {
    CheckUdf<double, int16_t, int32_t>("power", pow(2, 65536), 2, 65536);
    CheckUdf<double, int64_t, int32_t>("power", pow(2147483648, 65536),
                                       2147483648, 65536);
    CheckUdf<double, int64_t, float>("power", pow(2147483648, 2.1f), 2147483648,
                                     2.1f);
    CheckUdf<float, float, float>("power", powf(2147483648, 2.1f), 2147483648,
                                  2.1f);
    CheckUdf<double, double, int32_t>("power", pow(2147483648, 65536),
                                      2147483648, 65536);
}

TEST_F(UdfIRBuilderTest, round_udf_test) {
    CheckUdf<int32_t, int16_t>("round", round(5), 5);
    CheckUdf<int32_t, int32_t>("round", round(65536), 65536);
    CheckUdf<int64_t, int64_t>("round", round(2147483648), 2147483648);
    CheckUdf<double, float>("round", roundf(0.5f), 0.5f);
    CheckUdf<double, double>("round", round(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, sin_udf_test) {
    CheckUdf<double, int16_t>("sin", sin(5), 5);
    CheckUdf<double, int32_t>("sin", sin(65536), 65536);
    CheckUdf<double, int64_t>("sin", sin(2147483648), 2147483648);
    CheckUdf<float, float>("sin", sinf(0.5f), 0.5f);
    CheckUdf<double, double>("sin", sin(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, sqrt_udf_test) {
    CheckUdf<double, int16_t>("sqrt", sqrt(5), 5);
    CheckUdf<double, int32_t>("sqrt", sqrt(65536), 65536);
    CheckUdf<double, int64_t>("sqrt", sqrt(2147483648), 2147483648);
    CheckUdf<float, float>("sqrt", sqrtf(0.5f), 0.5f);
    CheckUdf<double, double>("sqrt", sqrt(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, tan_udf_test) {
    CheckUdf<double, int16_t>("tan", tan(5), 5);
    CheckUdf<double, int32_t>("tan", tan(65536), 65536);
    CheckUdf<double, int64_t>("tan", tan(2147483648), 2147483648);
    CheckUdf<float, float>("tan", tanf(0.5f), 0.5f);
    CheckUdf<double, double>("tan", tan(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, trunc_udf_test) {
    CheckUdf<int32_t, int16_t>("truncate", trunc(5), 5);
    CheckUdf<int32_t, int32_t>("truncate", trunc(65536), 65536);
    CheckUdf<int64_t, int64_t>("truncate", trunc(2147483648), 2147483648);
    CheckUdf<double, float>("truncate", truncf(0.5f), 0.5f);
    CheckUdf<double, double>("truncate", trunc(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, substring_pos_len_udf_test) {
    CheckUdf<StringRef, StringRef, int32_t, int32_t>(
        "substring", StringRef("12345"), StringRef("1234567890"), 1, 5);

    CheckUdf<StringRef, StringRef, int32_t, int32_t>(
        "substring", StringRef("23456"), StringRef("1234567890"), 2, 5);

    CheckUdf<StringRef, StringRef, int32_t, int32_t>(
        "substring", StringRef("23456"), StringRef("1234567890"), -9, 5);

    CheckUdf<StringRef, StringRef, int32_t, int32_t>(
        "substring", StringRef("90"), StringRef("1234567890"), -2, 5);

    CheckUdf<StringRef, StringRef, int32_t, int32_t>(
        "substring", StringRef(""), StringRef("1234567890"), 2, 0);

    CheckUdf<StringRef, StringRef, int32_t, int32_t>(
        "substring", StringRef(""), StringRef("1234567890"), 2, -1);
}

TEST_F(UdfIRBuilderTest, substring_pos_udf_test) {
    CheckUdf<StringRef, StringRef, int32_t>(
        "substring", StringRef("1234567890"), StringRef("1234567890"), 1);

    CheckUdf<StringRef, StringRef, int32_t>("substring", StringRef("234567890"),
                                            StringRef("1234567890"), 2);

    CheckUdf<StringRef, StringRef, int32_t>("substring", StringRef("234567890"),
                                            StringRef("1234567890"), -9);

    CheckUdf<StringRef, StringRef, int32_t>("substring", StringRef("90"),
                                            StringRef("1234567890"), -2);

    CheckUdf<StringRef, StringRef, int32_t>("substring", StringRef(""),
                                            StringRef("1234567890"), 12);
    CheckUdf<StringRef, StringRef, int32_t>("substring", StringRef(""),
                                            StringRef("1234567890"), -12);
}

TEST_F(UdfIRBuilderTest, upper_ucase) {
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("upper", StringRef("SQL"), StringRef("Sql"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("ucase", StringRef("SQL"), StringRef("Sql"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("ucase", StringRef("!ABC?"), StringRef("!Abc?"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("ucase", StringRef(""), StringRef(""));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("upper", StringRef(""), StringRef(""));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("ucase", nullptr, nullptr);
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("upper", nullptr, nullptr);
}

TEST_F(UdfIRBuilderTest, lower_lcase) {
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("lower", StringRef("sql"), StringRef("SQl"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("lcase", StringRef("sql"), StringRef("SQl"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("lcase", StringRef("!abc?"), StringRef("!Abc?"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("lcase", StringRef(""), StringRef(""));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("lower", StringRef(""), StringRef(""));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("lcase", nullptr, nullptr);
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("lower", nullptr, nullptr);
    char* buf1 = reinterpret_cast<char*>(malloc(2 * 1024 * 1024 + 1));
    char* buf2 = reinterpret_cast<char*>(malloc(2 * 1024 * 1024 - 1));
    char* buf3 = reinterpret_cast<char*>(malloc(2 * 1024 * 1024 - 1));
    memset(buf1, 'A', 2 * 1024 * 1024 + 1);
    memset(buf2, 'A', 2 * 1024 * 1024 - 1);
    memset(buf3, 'a', 2 * 1024 * 1024 - 1);
    StringRef large_str = StringRef(2 * 1024 * 1024 - 1, buf2);
    StringRef large_str1 = StringRef(2 * 1024 * 1024 - 1, buf3);
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("lower", nullptr, StringRef(2 * 1024 * 1024 + 1, buf1));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("lower", large_str1, large_str);
    delete buf1;
    delete buf2;
    delete buf3;
}

TEST_F(UdfIRBuilderTest, concat_str_udf_test) {
    //    concat("12345") == "12345"
    CheckUdf<StringRef, StringRef>("concat", StringRef("12345"),
                                   StringRef("12345"));

    // concat("12345", "67890") == "1234567890"
    CheckUdf<StringRef, StringRef, StringRef>("concat", StringRef("1234567890"),
                                              StringRef("12345"),
                                              StringRef("67890"));

    // concat("123", "4567890", "abcde") == "1234567890abcde"
    CheckUdf<StringRef, StringRef, StringRef, StringRef>(
        "concat", StringRef("1234567890abcde"), StringRef("123"),
        StringRef("4567890"), StringRef("abcde"));

    // concat("1", "23", "456", "7890", "abc", "de") == "1234567890abcde"
    CheckUdf<StringRef, StringRef, StringRef, StringRef>(
        "concat", StringRef("1234567890abcde"), StringRef("1"), StringRef("23"),
        StringRef("456"), StringRef("7890"), StringRef("abc"), StringRef("de"));

    //    concat() == ""
    CheckUdfFail<StringRef>("concat", StringRef("no result"));
}
TEST_F(UdfIRBuilderTest, concat_anytype_udf_test) {
    CheckUdf<StringRef, StringRef, int32_t>("concat", StringRef("1234567890"),
                                            StringRef("12345"), 67890);

    CheckUdf<StringRef, float, int32_t>("concat", StringRef("1234.567890"),
                                        1234.5f, 67890);

    CheckUdf<StringRef, StringRef, int16_t, int32_t, int64_t, float, double,
             Timestamp, Date>(
        "concat", StringRef("12345.67.82020-05-22 10:43:402020-06-23"),
        StringRef("1"), static_cast<int16_t>(2), 3, 4L, 5.6f, 7.8,
        Timestamp(1590115420000L), Date(2020, 06, 23));
}

TEST_F(UdfIRBuilderTest, concat_ws_anytype_udf_test) {
    // concat on string "--"
    CheckUdf<StringRef, StringRef, StringRef, int32_t>(
        "concat_ws", StringRef("12345--67890"), StringRef("--"),
        StringRef("12345"), 67890);

    // concat on int32
    CheckUdf<StringRef, int32_t, float, int32_t>(
        "concat_ws", StringRef("1234.5067890"), 0, 1234.5f, 67890);

    // concat on string "#"
    CheckUdf<StringRef, StringRef, StringRef, int16_t, int32_t, int64_t, float,
             double, Timestamp, Date>(
        "concat_ws",

        StringRef("1#2#3#4#5.6#7.8#2020-05-22 10:43:40#2020-06-23"),
        StringRef("#"), StringRef("1"), static_cast<int16_t>(2), 3, 4L, 5.6f,
        7.8, Timestamp(1590115420000L), Date(2020, 06, 23));
}

TEST_F(UdfIRBuilderTest, to_string_test) {
    CheckUdf<StringRef, bool>("string", StringRef("true"), true);
    CheckUdf<StringRef, bool>("string", StringRef("false"), false);
    CheckUdf<StringRef, int32_t>("string", StringRef("67890"), 67890);
    CheckUdf<StringRef, int16_t>("string", StringRef("128"),
                                 static_cast<int16_t>(128));
    CheckUdf<StringRef, float>("string", StringRef("1.234"), 1.234f);
    CheckUdf<StringRef, double>("string", StringRef("1.234"), 1.234);

    CheckUdf<StringRef, int64_t>("string", StringRef("1234567890"),
                                 1234567890L);

    CheckUdf<StringRef, int64_t>("string", StringRef("1234567890"),
                                 1234567890L);
    CheckUdf<StringRef, Timestamp>("string", StringRef("2020-05-22 10:43:40"),
                                   Timestamp(1590115420000L));

    CheckUdf<StringRef, Date>("string", StringRef("2020-05-22"),
                              Date(2020, 5, 22));
}

TEST_F(UdfIRBuilderTest, timestamp_format_test) {
    CheckUdf<StringRef, Timestamp, StringRef>(
        "date_format", StringRef("2020-05-22 10:43:40"),
        Timestamp(1590115420000L), StringRef("%Y-%m-%d %H:%M:%S"));

    CheckUdf<StringRef, Timestamp, StringRef>(
        "date_format", StringRef("2020-05-22"), Timestamp(1590115420000L),
        StringRef("%Y-%m-%d"));

    CheckUdf<StringRef, Timestamp, StringRef>(
        "date_format", StringRef("10:43:40"), Timestamp(1590115420000L),
        StringRef("%H:%M:%S"));
}

TEST_F(UdfIRBuilderTest, date_format_test) {
    CheckUdf<StringRef, Date, StringRef>(
        "date_format", StringRef("2020-05-22 00:00:00"), Date(2020, 05, 22),
        StringRef("%Y-%m-%d %H:%M:%S"));

    CheckUdf<StringRef, Date, StringRef>("date_format", StringRef("2020-05-22"),
                                         Date(2020, 05, 22),
                                         StringRef("%Y-%m-%d"));

    CheckUdf<StringRef, Date, StringRef>("date_format", StringRef("00:00:00"),
                                         Date(2020, 05, 22),
                                         StringRef("%H:%M:%S"));
}

TEST_F(UdfIRBuilderTest, strcmp_udf_test) {
    CheckUdf<int32_t, StringRef, StringRef>("strcmp", 0, StringRef("12345"),
                                            StringRef("12345"));
    CheckUdf<int32_t, StringRef, StringRef>("strcmp", 0, StringRef(""),
                                            StringRef(""));
    CheckUdf<int32_t, StringRef, StringRef>("strcmp", -1, StringRef("12345"),
                                            StringRef("123456"));
    CheckUdf<int32_t, StringRef, StringRef>("strcmp", -1, StringRef(""),
                                            StringRef("123456"));

    CheckUdf<int32_t, StringRef, StringRef>("strcmp", 1, StringRef("12345"),
                                            StringRef("1234"));
    CheckUdf<int32_t, StringRef, StringRef>("strcmp", 1, StringRef("12345"),
                                            StringRef(""));

    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(
        "strcmp", nullptr, nullptr, nullptr);
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(
        "strcmp", nullptr, StringRef("12345"), nullptr);
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(
        "strcmp", nullptr, nullptr, StringRef("12345"));
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(
        "strcmp", nullptr, nullptr, StringRef(""));
}

TEST_F(UdfIRBuilderTest, null_process_test) {
    CheckUdf<bool, Nullable<double>>("is_null", true, nullptr);
    CheckUdf<bool, Nullable<double>>("is_null", false, 1.0);

    CheckUdf<double, Nullable<double>, Nullable<double>>("if_null", 1.0, 1.0,
                                                         nullptr);
    CheckUdf<double, Nullable<double>, Nullable<double>>("if_null", 1.0, 1.0,
                                                         2.0);
    CheckUdf<Nullable<double>, Nullable<double>, Nullable<double>>(
        "if_null", nullptr, nullptr, nullptr);
    CheckUdf<double, Nullable<double>, Nullable<double>>("if_null", 2.0,
                                                         nullptr, 2.0);
    // nvl is synonym to is_null
    CheckUdf<double, Nullable<double>, Nullable<double>>("nvl", 2.0, nullptr, 2.0);
    CheckUdf<double, Nullable<double>, Nullable<double>>("nvl", 1.0, 1.0, 2.0);

    // nvl2
    CheckUdf<double, Nullable<double>, double, double>("nvl2", 2.0, nullptr, 1.0, 2.0);
    CheckUdf<double, Nullable<double>, double, double>("nvl2", 1.0, 12.0, 1.0, 2.0);
    CheckUdf<StringRef, Nullable<int>, StringRef, StringRef>("nvl2", StringRef("abc"), 12, StringRef("abc"),
                                                             StringRef("def"));
}

TEST_F(UdfIRBuilderTest, date_to_timestamp_test_0) {
    CheckUdf<Nullable<Timestamp>, Nullable<Date>>(
        "timestamp", Timestamp(1589904000000L),
        Date(2020, 05, 20));
}
TEST_F(UdfIRBuilderTest, date_to_timestamp_test_null_0) {
    //    Invalid year
    CheckUdf<Nullable<Timestamp>, Nullable<Date>>("timestamp", nullptr,
                                                  Date(1899, 05, 20));
}
TEST_F(UdfIRBuilderTest, date_to_timestamp_test_null_1) {
    //    Invalid month
    CheckUdf<Nullable<Timestamp>, Nullable<Date>>("timestamp", nullptr,
                                                  Date(2029, 13, 20));
}
TEST_F(UdfIRBuilderTest, date_to_timestamp_test_null_2) {
    //    Invalid day
    CheckUdf<Nullable<Timestamp>, Nullable<Date>>("timestamp", nullptr,
                                                  Date(2029, 05, 32));
}
TEST_F(UdfIRBuilderTest, date_to_timestamp_test_null_3) {
    CheckUdf<Nullable<Timestamp>, Nullable<Date>>("timestamp", nullptr,
                                                  nullptr);
}

TEST_F(UdfIRBuilderTest, string_to_timestamp_test_0) {
    CheckUdf<Nullable<Timestamp>, Nullable<StringRef>>(
        "timestamp", Timestamp(1589907723000),
        StringRef("2020-05-20 01:02:03"));
}
TEST_F(UdfIRBuilderTest, string_to_timestamp_test_1) {
    CheckUdf<Nullable<Timestamp>, Nullable<StringRef>>(
        "timestamp", Timestamp(1589904000000L),
        StringRef("2020-05-20"));
}
TEST_F(UdfIRBuilderTest, string_to_timestamp_test_2) {
    CheckUdf<Nullable<Timestamp>, Nullable<StringRef>>(
        "timestamp", nullptr, StringRef("1899-05-20"));
}
TEST_F(UdfIRBuilderTest, string_to_timestamp_test_3) {
    CheckUdf<Nullable<Timestamp>, Nullable<StringRef>>(
        "timestamp", Timestamp(1589904000000L),
        StringRef("20200520"));
}

TEST_F(UdfIRBuilderTest, timestamp_to_date_test_0) {
    CheckUdf<Nullable<Date>, Nullable<Timestamp>>(
        "date", Date(2020, 05, 20), Timestamp(1589958000000L));
}
TEST_F(UdfIRBuilderTest, timestamp_to_date_test_null_0) {
    CheckUdf<Nullable<Date>, Nullable<Timestamp>>("date", nullptr, nullptr);
}

TEST_F(UdfIRBuilderTest, string_to_date_test_0) {
    CheckUdf<Nullable<Date>, Nullable<StringRef>>(
        "date", Date(2020, 05, 20),
        StringRef("2020-05-20 01:02:03"));
}
TEST_F(UdfIRBuilderTest, string_to_date_test_1) {
    CheckUdf<Nullable<Date>, Nullable<StringRef>>(
        "date", Date(2020, 05, 20), StringRef("2020-05-20"));
}
TEST_F(UdfIRBuilderTest, string_to_date_test_2) {
    CheckUdf<Nullable<Date>, Nullable<StringRef>>(
        "date", nullptr, StringRef("1899-05-20"));
}
TEST_F(UdfIRBuilderTest, string_to_date_test_3) {
    CheckUdf<Nullable<Date>, Nullable<StringRef>>(
        "date", Date(2020, 05, 20), StringRef("20200520"));
}
TEST_F(UdfIRBuilderTest, string_to_smallint_0) {
    CheckUdf<Nullable<int16_t>, Nullable<StringRef>>("int16", 1,
                                                     StringRef("1"));
}
TEST_F(UdfIRBuilderTest, string_to_smallint_1) {
    CheckUdf<Nullable<int16_t>, Nullable<StringRef>>("int16", -1,
                                                     StringRef("-1"));
}
TEST_F(UdfIRBuilderTest, string_to_smallint_2) {
    CheckUdf<Nullable<int16_t>, Nullable<StringRef>>("int16", nullptr,
                                                     StringRef("abc"));
}
TEST_F(UdfIRBuilderTest, string_to_int_0) {
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>>("int32", 1,
                                                     StringRef("1"));
}
TEST_F(UdfIRBuilderTest, string_to_int_1) {
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>>("int32", -1,
                                                     StringRef("-1"));
}
TEST_F(UdfIRBuilderTest, string_to_int_2) {
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>>("int32", nullptr,
                                                     StringRef("abc"));
}
TEST_F(UdfIRBuilderTest, string_to_bigint_0) {
    CheckUdf<Nullable<int64_t>, Nullable<StringRef>>(
        "int64", 1589904000000L, StringRef("1589904000000"));
}
TEST_F(UdfIRBuilderTest, string_to_bigint_1) {
    CheckUdf<Nullable<int64_t>, Nullable<StringRef>>(
        "int64", -1589904000000L, StringRef("-1589904000000"));
}
TEST_F(UdfIRBuilderTest, string_to_bigint_2) {
    CheckUdf<Nullable<int64_t>, Nullable<StringRef>>("int64", nullptr,
                                                     StringRef("abc"));
}
TEST_F(UdfIRBuilderTest, string_to_double_0) {
    CheckUdf<Nullable<double>, Nullable<StringRef>>("double", 1.0,
                                                    StringRef("1.0"));
}
TEST_F(UdfIRBuilderTest, string_to_double_1) {
    CheckUdf<Nullable<double>, Nullable<StringRef>>("double", -1.0,
                                                    StringRef("-1.0"));
}
TEST_F(UdfIRBuilderTest, string_to_double_2) {
    CheckUdf<Nullable<double>, Nullable<StringRef>>("double", nullptr,
                                                    StringRef("abc"));
}
TEST_F(UdfIRBuilderTest, string_to_float_0) {
    CheckUdf<Nullable<float>, Nullable<StringRef>>("float", 1.0f,
                                                   StringRef("1.0"));
}
TEST_F(UdfIRBuilderTest, string_to_float_1) {
    CheckUdf<Nullable<float>, Nullable<StringRef>>("float", -1.0f,
                                                   StringRef("-1.0"));
}
TEST_F(UdfIRBuilderTest, string_to_float_2) {
    CheckUdf<Nullable<float>, Nullable<StringRef>>("float", nullptr,
                                                   StringRef("abc"));
}
TEST_F(UdfIRBuilderTest, like_match) {
    auto udf_name = "like_match";
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("a_b"), StringRef("a%b%"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("a_b"), StringRef("a%b%%"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, false, StringRef("a_b"), StringRef("a%b%%"), StringRef("%"));

    // target is null, return null
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, nullptr, nullptr, StringRef("Mi_e"), StringRef("\\"));
    // pattern is null, return null
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, nullptr, StringRef("Mike"), nullptr, StringRef("\\"));
    // escape is null, disable escape
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, nullptr, StringRef("Mike"), StringRef("Mi_e"), nullptr);

    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("Mike"), StringRef("Mi_e"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("Mike"), StringRef("Mi_e"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, false, StringRef("Mike"), StringRef("Mi\\_e"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("Mi_e"), StringRef("Mi\\_e"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("Mi\\ke"), StringRef("Mi\\_e"), StringRef(""));
}
TEST_F(UdfIRBuilderTest, ilike_match) {
    auto udf_name = "ilike_match";
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("a_b"), StringRef("a%b%"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("a_b"), StringRef("a%b%%"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, false, StringRef("a_b"), StringRef("a%b%%"), StringRef("%"));

    // target is null, return null
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, nullptr, nullptr, StringRef("Mi_e"), StringRef("\\"));
    // pattern is null, return null
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, nullptr, StringRef("mike"), nullptr, StringRef("\\"));
    // escape is null, disable escape
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, nullptr, StringRef("mike"), StringRef("Mi_e"), nullptr);

    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("mike"), StringRef("Mi_e"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, false, StringRef("mike"), StringRef("Mi\\_e"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("mi_e"), StringRef("Mi\\_e"), StringRef("\\"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("mi\\ke"), StringRef("Mi\\_e"), StringRef(""));
}
TEST_F(UdfIRBuilderTest, reverse) {
    auto udf_name = "reverse";
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, StringRef("SQL"), StringRef("LQS"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, StringRef("abc"), StringRef("cba"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, StringRef("a"), StringRef("a"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, StringRef("123456789"), StringRef("987654321"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, StringRef(""), StringRef(""));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, nullptr, nullptr);
}
}  // namespace codegen
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
