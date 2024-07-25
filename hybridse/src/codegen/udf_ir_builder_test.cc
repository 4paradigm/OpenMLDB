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
#include <cstdint>
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
// hex(int) normal check
TEST_F(UdfIRBuilderTest, HexIntUdfTest) {
    CheckUdf<StringRef, int16_t>("hex", "11", static_cast<int16_t>(17));
    CheckUdf<StringRef, int16_t>("hex", "0", static_cast<int16_t>(0));
    CheckUdf<StringRef, int32_t>("hex", "76ADF1", static_cast<int32_t>(7777777));
    CheckUdf<StringRef, int64_t>("hex", "8000000000000000", LLONG_MIN);
    CheckUdf<StringRef, int64_t>("hex", "7FFFFFFFFFFFFFFF", LLONG_MAX);
}
// hex(double) normal check
TEST_F(UdfIRBuilderTest, HexDoubleUdfTest) {
    CheckUdf<StringRef, double>("hex", "11", 17.4);
    CheckUdf<StringRef, double>("hex", "12", 17.5);
    CheckUdf<StringRef, double>("hex", "FFFFFFFFFFFFFFEE", -17.5);
    CheckUdf<StringRef, double>("hex", "FFFFFFFFFFFFFFEF", -17.4);
}
// hex(float) normal check
TEST_F(UdfIRBuilderTest, HexFloatUdfTest) {
    CheckUdf<StringRef, float>("hex", "11", 17.0);
}
// hex(string) normal check
TEST_F(UdfIRBuilderTest, HexStringUdfTest) {
    CheckUdf<StringRef, StringRef>("hex", "537061726B2053514C", StringRef("Spark SQL"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("hex", nullptr, nullptr);
}

TEST_F(UdfIRBuilderTest, UnhexTest) {
    // The following are normal tests.
    CheckUdf<StringRef, StringRef>("unhex", "Spark SQL", StringRef("537061726B2053514C"));
    CheckUdf<StringRef, StringRef>("unhex", "OpenMLDB", StringRef("4F70656E4D4C4442"));
    CheckUdf<StringRef, StringRef>("unhex", "OpenMLDB", StringRef("4f70656e4d4c4442"));
    // The following are valid character but not string unhex tests and the length of
    // some tests cases are odd.
    CheckUdf<StringRef, StringRef>("unhex", "", StringRef("4"));
    CheckUdf<StringRef, StringRef>("unhex", "{", StringRef("7B"));
    CheckUdf<StringRef, StringRef>("unhex", "{", StringRef("47B"));
    CheckUdf<StringRef, StringRef>("unhex", "7&", StringRef("537061726"));
    CheckUdf<StringRef, StringRef>("unhex", "\x8a", StringRef("8a")); // NOLINT
    // The following are invalid tests that contain the non-hex characters, the 'NULL' should
    // be returned.
    CheckUdf<StringRef, StringRef>("unhex", nullptr, StringRef("Z"));
    CheckUdf<StringRef, StringRef>("unhex", nullptr, StringRef("Zzzz"));
    CheckUdf<StringRef, StringRef>("unhex", nullptr, StringRef("zfk"));
    CheckUdf<StringRef, StringRef>("unhex", nullptr, StringRef("zf"));
    CheckUdf<StringRef, StringRef>("unhex", nullptr, StringRef("fk"));
    CheckUdf<StringRef, StringRef>("unhex", nullptr, StringRef("3k"));
    CheckUdf<StringRef, StringRef>("unhex", nullptr, StringRef("4k"));
    CheckUdf<StringRef, StringRef>("unhex", nullptr, StringRef("6k"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("unhex", nullptr, nullptr);
}

TEST_F(UdfIRBuilderTest, DayofmonthDateUdfTest) {
    CheckUdf<int32_t, Date>("dayofmonth", 22, Date(2020, 05, 22));
    CheckUdf<Nullable<int32_t>, Nullable<Date>>("dayofmonth", nullptr, nullptr);
}
TEST_F(UdfIRBuilderTest, MonthDateUdfTest) {
    CheckUdf<int32_t, Date>("month", 5, Date(2020, 05, 22));
    CheckUdf<Nullable<int32_t>, Nullable<Date>>("month", nullptr, nullptr);
}
TEST_F(UdfIRBuilderTest, YearDateUdfTest) {
    CheckUdf<int32_t, Date>("year", 2020, Date(2020, 05, 22));
    CheckUdf<Nullable<int32_t>, Nullable<Date>>("year", nullptr, nullptr);
}
TEST_F(UdfIRBuilderTest, DayofweekDateUdfTest) {
    Date date(2020, 05, 22);
    CheckUdf<int32_t, Date>("dayofweek", 6, date);
}
TEST_F(UdfIRBuilderTest, DayofyearDateUdfTest) {
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
        CheckUdf<Nullable<int32_t>, Date>("dayofyear", nullptr, date);
    }
    {
        Date date(2021, 0, 31);
        CheckUdf<Nullable<int32_t>, Date>("dayofyear", nullptr, date);
    }
    {
        Date date(2021, -1, 31);
        CheckUdf<Nullable<int32_t>, Date>("dayofyear", nullptr, date);
    }
    {
        Date date(2021, 12, 32);
        CheckUdf<Nullable<int32_t>, Date>("dayofyear", nullptr, date);
    }
    {
        Date date(2021, 12, 0);
        CheckUdf<Nullable<int32_t>, Date>("dayofyear", nullptr, date);
    }
    {
        Date date(2021, 12, -10);
        CheckUdf<Nullable<int32_t>, Date>("dayofyear", nullptr, date);
    }
    {
        Date date(2021, 2, 29);
        CheckUdf<Nullable<int32_t>, Date>("dayofyear", nullptr, date);
    }
}
TEST_F(UdfIRBuilderTest, WeekofyearDateUdfTest) {
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
TEST_F(UdfIRBuilderTest, LastdayDateUdfTest) {
    CheckUdf<Nullable<Date>, Nullable<Date>>("last_day", nullptr,
                                             nullptr);
    CheckUdf<Nullable<Date>, Nullable<Date>>("last_day", nullptr,
                                             Date(2022, 02, 31));
    CheckUdf<Nullable<Date>, Nullable<Date>>("last_day", Date(2022, 02, 28),
                                             Date(2022, 02, 10));
    CheckUdf<Nullable<Date>, Nullable<Date>>("last_day", Date(2020, 02, 29),
                                             Date(2020, 02, 10));
    CheckUdf<Nullable<Date>, Nullable<Date>>("last_day", Date(2021, 01, 31),
                                             Date(2021, 01, 01));
}

TEST_F(UdfIRBuilderTest, MinuteTimestampUdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("minute", 43, time);
}
TEST_F(UdfIRBuilderTest, SecondTimestampUdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("second", 40, time);
}
TEST_F(UdfIRBuilderTest, HourTimestampUdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("hour", 10, time);
}
TEST_F(UdfIRBuilderTest, DayofmonthTimestampUdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("dayofmonth", 22, time);
}

TEST_F(UdfIRBuilderTest, DayofweekTimestampUdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("dayofweek", 6, time);
}
TEST_F(UdfIRBuilderTest, DayofyearTimestampUdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("dayofyear", 143, time);
}
TEST_F(UdfIRBuilderTest, WeekofyearTimestampUdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("weekofyear", 21, time);
}
TEST_F(UdfIRBuilderTest, LastdayTimestampUdfTest) {
    // NOTE: last_day will always return a Date for not null Timestamp input
    CheckUdf<Nullable<Date>, Timestamp>("last_day", Date(2022, 8, 31),
                                        Timestamp(1659312000000L));  // 2022-08-01 00:00:00 GMT
    CheckUdf<Nullable<Date>, Timestamp>("last_day", Date(2022, 8, 31),
                                        Timestamp(1659311999000L));  // 2022-07-31 23:59:59 GMT, 08-01 07:59:59 UTC+8
}

TEST_F(UdfIRBuilderTest, MonthTimestampUdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("month", 5, time);
}
TEST_F(UdfIRBuilderTest, YearTimestampUdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, Timestamp>("year", 2020, time);
}

TEST_F(UdfIRBuilderTest, MinuteInt64UdfTest) {
    CheckUdf<int32_t, int64_t>("minute", 43, 1590115420000L);
}
TEST_F(UdfIRBuilderTest, SecondInt64UdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, int64_t>("second", 40, 1590115420000L);
}
TEST_F(UdfIRBuilderTest, HourInt64UdfTest) {
    Timestamp time(1590115420000L);
    CheckUdf<int32_t, int64_t>("hour", 10, 1590115420000L);
}

TEST_F(UdfIRBuilderTest, DayofmonthInt64UdfTest) {
    CheckUdf<int32_t, int64_t>("dayofmonth", 22, 1590115420000L);
}
TEST_F(UdfIRBuilderTest, MonthInt64UdfTest) {
    CheckUdf<int32_t, int64_t>("month", 5, 1590115420000L);
}
TEST_F(UdfIRBuilderTest, YearInt64UdfTest) {
    CheckUdf<int32_t, int64_t>("year", 2020, 1590115420000L);
}
TEST_F(UdfIRBuilderTest, DayofweekInt64UdfTest) {
    CheckUdf<int32_t, int64_t>("dayofweek", 6, 1590115420000L);
    CheckUdf<int32_t, int64_t>("dayofweek", 7, 1590115420000L + 86400000L);

    // Sunday
    CheckUdf<int32_t, int64_t>("dayofweek", 1, 1590115420000L + 2 * 86400000L);
    CheckUdf<int32_t, int64_t>("dayofweek", 2, 1590115420000L + 3 * 86400000L);
}
TEST_F(UdfIRBuilderTest, DayofyearInt64UdfTest) {
    CheckUdf<int32_t, int64_t>("dayofyear", 143, 1590115420000L);
    CheckUdf<int32_t, int64_t>("dayofyear", 144, 1590115420000L + 86400000L);
    CheckUdf<int32_t, int64_t>("dayofyear", 145, 1590115420000L + 2 * 86400000L);
    CheckUdf<int32_t, int64_t>("dayofyear", 146, 1590115420000L + 3 * 86400000L);

    CheckUdf<Nullable<int32_t>, int64_t>("dayofyear", nullptr, -1);
}
TEST_F(UdfIRBuilderTest, WeekofyearInt64UdfTest) {
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
TEST_F(UdfIRBuilderTest, LastdayInt64UdfTest) {
    CheckUdf<Nullable<Date>, int64_t>("last_day", Date(2020, 05, 31),
                                      1589958000000L);  // 2020-05-22
    CheckUdf<Nullable<Date>, int64_t>("last_day", Date(2022, 07, 31),
                                      1658966400000L);  // 2022-07-28
    CheckUdf<Nullable<Date>, int64_t>("last_day", Date(2022, 02, 28),
                                      1644451200000L);  // 2022-02-10
    CheckUdf<Nullable<Date>, int64_t>("last_day", Date(2020, 02, 29),
                                      1581292800000L);  // 2020-02-10
    CheckUdf<Nullable<Date>, int64_t>("last_day", nullptr,
                                      -1);
}
TEST_F(UdfIRBuilderTest, IncInt32UdfTest) {
    CheckUdf<int32_t, int32_t>("inc", 2021, 2020);
}
TEST_F(UdfIRBuilderTest, DistinctCountUdfTest) {
    std::vector<int32_t> vec = {1, 1, 3, 3, 5, 5, 7, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);

    CheckUdf<int64_t, codec::ListRef<int32_t>>("count", 9, list_ref);
    CheckUdf<int64_t, codec::ListRef<int32_t>>("distinct_count", 5, list_ref);
}

TEST_F(UdfIRBuilderTest, MinUdfTest) {
    std::vector<int32_t> vec = {10, 8, 6, 4, 2, 1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckUdf<int32_t, codec::ListRef<int32_t>>("min", 1, list_ref);
}
TEST_F(UdfIRBuilderTest, MaxUdfTest) {
    std::vector<int32_t> vec = {10, 8, 6, 4, 2, 1, 3, 5, 7, 9};
    codec::ArrayListV<int32_t> list(&vec);
    codec::ListRef<int32_t> list_ref;
    list_ref.list = reinterpret_cast<int8_t *>(&list);
    CheckUdf<int32_t, codec::ListRef<int32_t>>("max", 10, list_ref);
}

TEST_F(UdfIRBuilderTest, MaxTimestampUdfTest) {
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
TEST_F(UdfIRBuilderTest, MinTimestampUdfTest) {
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

TEST_F(UdfIRBuilderTest, LogUdfTest) {
    CheckUdf<float, float>("log", log(2.0f), 2.0f);
    CheckUdf<double, double>("log", log(2.0), 2.0);
    CheckUdf<float, float>("ln", log(2.0f), 2.0f);
    CheckUdf<double, double>("ln", log(2.0), 2.0);
    CheckUdf<double, int32_t>("log2", log2(65536), 65536);
    CheckUdf<double, double>("log2", log2(2.0), 2.0);
    CheckUdf<double, int32_t>("log10", log10(65536), 65536);
    CheckUdf<double, double>("log10", log10(2.0), 2.0);
}

TEST_F(UdfIRBuilderTest, AbsUdfTest) {
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

TEST_F(UdfIRBuilderTest, AcosUdfTest) {
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

TEST_F(UdfIRBuilderTest, AsinUdfTest) {
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

TEST_F(UdfIRBuilderTest, AtanUdfTest0) {
    CheckUdf<double, int16_t>("atan", 0, 0);
}
TEST_F(UdfIRBuilderTest, AtanUdfTest1) {
    CheckUdf<double, int16_t>("atan", 1.1071487177940904, 2);
}
TEST_F(UdfIRBuilderTest, AtanUdfTest2) {
    CheckUdf<double, int32_t>("atan", -1.1071487177940904, -2);
}
TEST_F(UdfIRBuilderTest, AtanUdfTest3) {
    CheckUdf<double, int32_t>("atan", 1.1071487177940904, 2);
}
TEST_F(UdfIRBuilderTest, AtanUdfTest4) {
    CheckUdf<double, int64_t>("atan", 0, 0);
}
TEST_F(UdfIRBuilderTest, AtanUdfTest5) {
    CheckUdf<double, int64_t>("atan", -1.1071487177940904, -2);
}
TEST_F(UdfIRBuilderTest, AtanUdfTest6) {
    CheckUdf<float, float>("atan", atan(-45.01f), -45.01f);
}
TEST_F(UdfIRBuilderTest, AtanUdfTest7) {
    CheckUdf<double, double>("atan", 0.1462226769376524, 0.1472738);
}
TEST_F(UdfIRBuilderTest, AtanUdfTest8) {
    CheckUdf<double, int16_t, int32_t>("atan", 2.3561944901923448, 2, -2);
}
TEST_F(UdfIRBuilderTest, AtanUdfTest9) {
    CheckUdf<double, int64_t, int32_t>("atan", 2.3561944901923448, 2, -2);
}
TEST_F(UdfIRBuilderTest, AtanUdfTest10) {
    CheckUdf<double, int64_t, float>("atan", 2.3561944901923448, 2, -2);
}

TEST_F(UdfIRBuilderTest, Atan2UdfTest15) {
    CheckUdf<double, double, int32_t>("atan2", 2.3561944901923448, 2, -2);
}

TEST_F(UdfIRBuilderTest, CeilUdfTest) {
    CheckUdf<int64_t, int16_t>("ceil", 5, 5);
    CheckUdf<int64_t, int32_t>("ceil", 32769, 32769);
    CheckUdf<int64_t, int64_t>("ceil", 2147483649, 2147483649);
    CheckUdf<double, float>("ceil", 0, -0.1f);
    CheckUdf<double, float>("ceil", 2, 1.23f);
    CheckUdf<double, double>("ceil", -1, -1.23);
    CheckUdf<double, double>("ceil", 0, 0);
}

TEST_F(UdfIRBuilderTest, CeilingUdfTest) {
    CheckUdf<int64_t, int16_t>("ceiling", 5, 5);
    CheckUdf<int64_t, int32_t>("ceiling", 32769, 32769);
    CheckUdf<int64_t, int64_t>("ceiling", 2147483649, 2147483649);
    CheckUdf<double, float>("ceiling", 0, -0.1f);
    CheckUdf<double, float>("ceiling", 2, 1.23f);
    CheckUdf<double, double>("ceiling", -1, -1.23);
    CheckUdf<double, double>("ceiling", 0, 0);
}

TEST_F(UdfIRBuilderTest, CosUdfTest) {
    CheckUdf<double, int16_t>("cos", cos(5), 5);
    CheckUdf<double, int32_t>("cos", cos(65536), 65536);
    CheckUdf<double, int64_t>("cos", cos(2147483648), 2147483648);
    CheckUdf<float, float>("cos", cosf(0.5f), 0.5f);
    CheckUdf<double, double>("cos", cos(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, CotUdfTest) {
    CheckUdf<double, int16_t>("cot", cos(5) / sin(5), 5);
    CheckUdf<double, int32_t>("cot", cos(65536) / sin(65536), 65536);
    CheckUdf<double, int64_t>("cot", cos(2147483648) / sin(2147483648),
                              2147483648);
    CheckUdf<float, float>("cot", cosf(0.5f) / sin(0.5f), 0.5f);
    CheckUdf<double, double>("cot", cos(0.5) / sin(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, ExpUdfTest) {
    CheckUdf<double, int16_t>("exp", exp(5), 5);
    CheckUdf<double, int32_t>("exp", exp(65536), 65536);
    CheckUdf<double, int64_t>("exp", exp(2147483648), 2147483648);
    CheckUdf<float, float>("exp", expf(0.5f), 0.5f);
    CheckUdf<double, double>("exp", exp(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, FloorUdfTest) {
    CheckUdf<int64_t, int16_t>("floor", 5, 5);
    CheckUdf<int64_t, int32_t>("floor", 32769, 32769);
    CheckUdf<int64_t, int64_t>("floor", 2147483649, 2147483649);
    CheckUdf<double, float>("floor", -1, -0.1f);
    CheckUdf<double, float>("floor", 1, 1.23f);
    CheckUdf<double, double>("floor", -2, -1.23);
    CheckUdf<double, double>("floor", 0, 0);
}

TEST_F(UdfIRBuilderTest, PowUdfTest) {
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

TEST_F(UdfIRBuilderTest, PowerUdfTest) {
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

TEST_F(UdfIRBuilderTest, RoundWithPositiveD) {
    // We use string as expet result in case the inaccuracy of flaot points
    std::initializer_list<std::pair<double, std::string>> cases = {
        // before decimal position = 0
        {0.5, "0.50"}, {0.12, "0.12"}, {0.123, "0.12"}, {0.1478, "0.15"},
        {0, "0.00"}, {0.012, "0.01"}, {0.0012, "0.00"}, {0.0078, "0.01"},
        // before decimal position > 0
        {1.1, "1.10" }, {1.14, "1.14"}, {1.177, "1.18"}, {1.171, "1.17"},
        {21.1, "21.10" }, {21.14, "21.14"}, {21.177, "21.18"}, {21.171, "21.17"},
        {1889, "1889.00"},
    };

    for (auto& val : cases) {
        // non-negative value
        CheckUdf<double, double, int32_t>("round", std::stod(val.second), val.first, 2);
        // negative value
        std::string expect = "-" + val.second;
        CheckUdf<double, double, int32_t>("round", std::stod(expect), -val.first, 2);
    }

    for (auto c :  {1, 2, 3, 4, 5, 6}) {
        CheckUdf<int64_t, int64_t, int32_t>("round", c, c, 2);
    }
}

TEST_F(UdfIRBuilderTest, RoundWithNegD) {
    // We use string as expet result in case the inaccuracy of flaot points
    std::initializer_list<std::pair<double, std::string>> cases = {
        {0.0, "0.0"}, {1.23, "0"}, {100.12, "100"}, {3712.55, "3700"}, {4488, "4500"},
        {88, "100"}, {175.4, "200"}
    };

    for (auto& val : cases) {
        // non-negative value
        CheckUdf<double, double, int32_t>("round", std::stod(val.second), val.first, -2);
        // negative value
        std::string expect = "-" + val.second;
        CheckUdf<double, double, int32_t>("round", std::stod(expect), -val.first, -2);
    }

    std::initializer_list<std::pair<int32_t, int32_t>> icases = {{0, 0},     {1, 0},     {55, 100},     {100, 100},
                                                                 {145, 100}, {199, 200}, {2312, 2300}};
    for (auto c : icases) {
        CheckUdf<int32_t, int32_t, int32_t>("round", c.second, c.first, -2);
        CheckUdf<int32_t, int32_t, int32_t>("round", -c.second, -c.first, -2);
    }
}

TEST_F(UdfIRBuilderTest, RoundWithZeroD) {
    std::initializer_list<std::pair<double, std::string>> cases = {
        {1.12, "1"}, {1.5, "2"}, {1.77, "2"}, {0.0, "0"}, {88, "88"}
    };
    for (auto& val : cases) {
        // non-negative value
        CheckUdf<double, double, int32_t>("round", std::stod(val.second), val.first, 0);
        CheckUdf<double, double>("round", std::stod(val.second), val.first);
        // negative value
        std::string expect = "-" + val.second;
        CheckUdf<double, double, int32_t>("round", std::stod(expect), -val.first, 0);
        CheckUdf<double, double>("round", std::stod(expect), -val.first);
    }

    std::initializer_list<int32_t> icases = {1, 2, 3, 4, 5, 100, 88};
    for (auto& val : icases) {
        // non-negative value
        CheckUdf<int32_t, int32_t, int32_t>("round", val, val, 0);
        CheckUdf<int32_t, int32_t>("round", val, val);
        // negative value
        CheckUdf<int32_t, int32_t, int32_t>("round", -val, -val, 0);
        CheckUdf<int32_t, int32_t>("round", -val, -val);
    }
}

TEST_F(UdfIRBuilderTest, RoundFail) {
    // first param: only one of [intXX, double, float]
    CheckUdfFail<double, StringRef, int32_t>("round", 12.3, "12.34", 1);

    // second param: only one of [intXX, double, float]
    CheckUdfFail<double, double, StringRef>("round", 12.3, 12.34, "1");
    CheckUdfFail<double, double, Timestamp>("round", 12.3, 12.34, Timestamp(12));

    // no third or more params
    CheckUdfFail<double, double, int32_t, int32_t>("round", 12.3, 12.34, 1, 1);
}

TEST_F(UdfIRBuilderTest, SinUdfTest) {
    CheckUdf<double, int16_t>("sin", sin(5), 5);
    CheckUdf<double, int32_t>("sin", sin(65536), 65536);
    CheckUdf<double, int64_t>("sin", sin(2147483648), 2147483648);
    CheckUdf<float, float>("sin", sinf(0.5f), 0.5f);
    CheckUdf<double, double>("sin", sin(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, SqrtUdfTest) {
    CheckUdf<double, int16_t>("sqrt", sqrt(5), 5);
    CheckUdf<double, int32_t>("sqrt", sqrt(65536), 65536);
    CheckUdf<double, int64_t>("sqrt", sqrt(2147483648), 2147483648);
    CheckUdf<float, float>("sqrt", sqrtf(0.5f), 0.5f);
    CheckUdf<double, double>("sqrt", sqrt(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, TanUdfTest) {
    CheckUdf<double, int16_t>("tan", tan(5), 5);
    CheckUdf<double, int32_t>("tan", tan(65536), 65536);
    CheckUdf<double, int64_t>("tan", tan(2147483648), 2147483648);
    CheckUdf<float, float>("tan", tanf(0.5f), 0.5f);
    CheckUdf<double, double>("tan", tan(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, TruncUdfTest) {
    CheckUdf<int32_t, int16_t>("truncate", trunc(5), 5);
    CheckUdf<int32_t, int32_t>("truncate", trunc(65536), 65536);
    CheckUdf<int64_t, int64_t>("truncate", trunc(2147483648), 2147483648);
    CheckUdf<double, float>("truncate", truncf(0.5f), 0.5f);
    CheckUdf<double, double>("truncate", trunc(0.5), 0.5);
}

TEST_F(UdfIRBuilderTest, SubstringPosLenUdfTest) {
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

TEST_F(UdfIRBuilderTest, SubstringPosUdfTest) {
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

TEST_F(UdfIRBuilderTest, LocateUdfTest) {
    CheckUdf<int32_t, StringRef, StringRef>("locate", 1, StringRef("ab"), StringRef("abcab"));
    CheckUdf<int32_t, StringRef, StringRef>("locate", 3, StringRef("ab"), StringRef("bcab"));
    CheckUdf<int32_t, StringRef, StringRef>("locate", 0, StringRef("ab"), StringRef("bcAb"));
    CheckUdf<int32_t, StringRef, StringRef>("locate", 1, StringRef(""), StringRef(""));
}

TEST_F(UdfIRBuilderTest, LocatePosUdfTest) {
    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 0, StringRef("ab"), StringRef("ab"), -1);
    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 0, StringRef("ab"), StringRef("Ab"), 1);

    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 4, StringRef("ab"), StringRef("abcab"), 2);
    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 0, StringRef("ab"), StringRef("abcAb"), 2);
    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 4, StringRef("ab"), StringRef("abcab"), 2);
    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 0, StringRef("ab"), StringRef("abcab"), 6);

    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 5, StringRef(""), StringRef("abcab"), 5);
    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 6, StringRef(""), StringRef("abcab"), 6);
    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 0, StringRef(""), StringRef("abcab"), 7);

    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 1, StringRef(""), StringRef(""), 1);
    CheckUdf<int32_t, StringRef, StringRef, int32_t>("locate", 0, StringRef(""), StringRef(""), 2);
}

TEST_F(UdfIRBuilderTest, UpperUcase) {
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("upper", StringRef("SQL"), StringRef("Sql"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("ucase", StringRef("SQL"), StringRef("Sql"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("ucase", StringRef("!ABC?"), StringRef("!Abc?"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("ucase", StringRef(""), StringRef(""));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("upper", StringRef(""), StringRef(""));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("ucase", nullptr, nullptr);
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>("upper", nullptr, nullptr);
}

TEST_F(UdfIRBuilderTest, LowerLcase) {
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

TEST_F(UdfIRBuilderTest, ConcatStrUdfTest) {
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
TEST_F(UdfIRBuilderTest, ConcatAnytypeUdfTest) {
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

TEST_F(UdfIRBuilderTest, ConcatWsAnytypeUdfTest) {
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

TEST_F(UdfIRBuilderTest, ToStringTest) {
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

TEST_F(UdfIRBuilderTest, TimestampFormatTest) {
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

TEST_F(UdfIRBuilderTest, DateFormatTest) {
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

TEST_F(UdfIRBuilderTest, StrcmpUdfTest) {
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

TEST_F(UdfIRBuilderTest, IfNull) {
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
}

TEST_F(UdfIRBuilderTest, Nvl2) {
    CheckUdf<double, Nullable<double>, double, double>("nvl2", 2.0, nullptr, 1.0, 2.0);
    CheckUdf<double, Nullable<double>, double, double>("nvl2", 1.0, 12.0, 1.0, 2.0);
    CheckUdf<StringRef, Nullable<int>, StringRef, StringRef>("nvl2", StringRef("abc"), 12, StringRef("abc"),
                                                             StringRef("def"));
}

// a cond expr (nvl/nvl2) returns compatiable type if left and right not same
TEST_F(UdfIRBuilderTest, CondCompatiableType) {
    CheckUdf<int64_t, int64_t, int32_t>("if_null", 12, 12, 1);
    CheckUdf<int64_t, Nullable<int32_t>, int64_t>("if_null", 1, nullptr, 1);

    CheckUdf<double, float, int64_t>("if_null", 20.0, 20.0, 9);
    CheckUdf<double, Nullable<float>, int64_t>("if_null", 20.0, nullptr, 20);

    CheckUdf<StringRef, Nullable<int16_t>, StringRef>("if_null", "20", nullptr, "20");

    CheckUdf<StringRef, bool, int64_t, StringRef>("nvl2", "100", true, 100, "88");
}

TEST_F(UdfIRBuilderTest, DateToTimestampTest0) {
    CheckUdf<Nullable<Timestamp>, Nullable<Date>>(
        "timestamp", Timestamp(1589904000000L),
        Date(2020, 05, 20));
}
TEST_F(UdfIRBuilderTest, DateToTimestampTestNull0) {
    //    Invalid year
    CheckUdf<Nullable<Timestamp>, Nullable<Date>>("timestamp", nullptr,
                                                  Date(1899, 05, 20));
}
TEST_F(UdfIRBuilderTest, DateToTimestampTestNull1) {
    //    Invalid month
    CheckUdf<Nullable<Timestamp>, Nullable<Date>>("timestamp", nullptr,
                                                  Date(2029, 13, 20));
}
TEST_F(UdfIRBuilderTest, DateToTimestampTestNull2) {
    //    Invalid day
    CheckUdf<Nullable<Timestamp>, Nullable<Date>>("timestamp", nullptr,
                                                  Date(2029, 05, 32));
}
TEST_F(UdfIRBuilderTest, DateToTimestampTestNull3) {
    CheckUdf<Nullable<Timestamp>, Nullable<Date>>("timestamp", nullptr,
                                                  nullptr);
}

TEST_F(UdfIRBuilderTest, StringToTimestampTest0) {
    CheckUdf<Nullable<Timestamp>, Nullable<StringRef>>(
        "timestamp", Timestamp(1589907723000),
        StringRef("2020-05-20 01:02:03"));
}
TEST_F(UdfIRBuilderTest, StringToTimestampTest1) {
    CheckUdf<Nullable<Timestamp>, Nullable<StringRef>>(
        "timestamp", Timestamp(1589904000000L),
        StringRef("2020-05-20"));
}
TEST_F(UdfIRBuilderTest, StringToTimestampTest2) {
    CheckUdf<Nullable<Timestamp>, Nullable<StringRef>>(
        "timestamp", nullptr, StringRef("1899-05-20"));
}
TEST_F(UdfIRBuilderTest, StringToTimestampTest3) {
    CheckUdf<Nullable<Timestamp>, Nullable<StringRef>>(
        "timestamp", Timestamp(1589904000000L),
        StringRef("20200520"));
}

TEST_F(UdfIRBuilderTest, UnixTimestampTest) {
    auto func_name = "unix_timestamp";
    CheckUdf<Nullable<int64_t>, Nullable<Date>>(func_name, 1589904000LL, Date(2020, 05, 20));

    //    Invalid year
    CheckUdf<Nullable<int64_t>, Nullable<Date>>(func_name, nullptr, Date(1899, 05, 20));
    //    Invalid month
    CheckUdf<Nullable<int64_t>, Nullable<Date>>(func_name, nullptr, Date(2029, 13, 20));
    //    Invalid day
    CheckUdf<Nullable<int64_t>, Nullable<Date>>(func_name, nullptr, Date(2029, 05, 32));
    CheckUdf<Nullable<int64_t>, Nullable<Date>>(func_name, nullptr, nullptr);
    CheckUdf<Nullable<int64_t>, Nullable<StringRef>>(func_name, 1589907723LL,
                                                       StringRef("2020-05-20 01:02:03"));
    CheckUdf<Nullable<int64_t>, Nullable<StringRef>>(func_name, 1589904000LL, StringRef("2020-05-20"));
    CheckUdf<Nullable<int64_t>, Nullable<StringRef>>(func_name, nullptr, StringRef("1899-05-20"));
    CheckUdf<Nullable<int64_t>, Nullable<StringRef>>(func_name, 1589904000LL, StringRef("20200520"));

    std::time_t result = std::time(nullptr);
    CheckUdf<Nullable<int64_t>, Nullable<StringRef>>(func_name, static_cast<int64_t>(result), "");
}

TEST_F(UdfIRBuilderTest, TimestampToDateTest0) {
    CheckUdf<Nullable<Date>, Nullable<Timestamp>>(
        "date", Date(2020, 05, 20), Timestamp(1589958000000L));
}
TEST_F(UdfIRBuilderTest, TimestampToDateTestNull0) {
    CheckUdf<Nullable<Date>, Nullable<Timestamp>>("date", nullptr, nullptr);
}

TEST_F(UdfIRBuilderTest, StringToDateTest0) {
    CheckUdf<Nullable<Date>, Nullable<StringRef>>(
        "date", Date(2020, 05, 20),
        StringRef("2020-05-20 01:02:03"));
}
TEST_F(UdfIRBuilderTest, StringToDateTest1) {
    CheckUdf<Nullable<Date>, Nullable<StringRef>>(
        "date", Date(2020, 05, 20), StringRef("2020-05-20"));
}
TEST_F(UdfIRBuilderTest, StringToDateTest2) {
    CheckUdf<Nullable<Date>, Nullable<StringRef>>(
        "date", nullptr, StringRef("1899-05-20"));
}
TEST_F(UdfIRBuilderTest, StringToDateTest3) {
    CheckUdf<Nullable<Date>, Nullable<StringRef>>(
        "date", Date(2020, 05, 20), StringRef("20200520"));
}

TEST_F(UdfIRBuilderTest, DateDiff) {
    auto func_name = "datediff";
    // date as input
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<Date>>(func_name, -19, Date(2022, 5, 1), Date(2022, 5, 20));
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<Date>>(func_name, 19, Date(2022, 5, 20), Date(2022, 5, 1));
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<Date>>(func_name, 0, Date(2022, 5, 1), Date(2022, 5, 1));
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<Date>>(func_name, nullptr, Date(1899, 5, 1), Date(2022, 5, 1));
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<Date>>(func_name, nullptr, Date(2022, 5, 1), Date(1899, 5, 1));
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<Date>>(func_name, nullptr, nullptr, Date(2022, 5, 1));
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<Date>>(func_name, nullptr, Date(2022, 5, 1), nullptr);
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<Date>>(func_name, nullptr, nullptr, nullptr);

    // string as input
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, -19, "2022-05-01", "20220520");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, 729, "20221231", "2021-01-01");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, 8400, "2022-12-31", "2000-01-01");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, 44924, "2022-12-31", "1900-01-01");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, 50, "20220620",
                                                                          "2022-05-01 11:11:11");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, 0,
                                                                          "2022-05-01", "20220501");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "2022-02-29", "20220501");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, 9, "1899-05-20", "1899-05-11");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "2022-05-40",
                                                                          "2020-05-20");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, -30, "1199-10-12", "1199-11-11");
    // rfc3399 full format
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(
        func_name, 20, "2000-01-01t00:12:00.1+08:00", "1999-12-12T12:12:12+08:00");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(
        func_name, 19, "2000-01-01t00:12:00.1+08:00", "1999-12-12T20:12:12Z");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(
        func_name, 20, "2000-01-01t06:12:00.1+08:00", "1999-12-12T12:12:12Z");

    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, nullptr, "20220501");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "2022-05-01", nullptr);
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, nullptr, nullptr);

    // mix types
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<Date>>(func_name, -19, "2022-05-01", Date(2022, 5, 20));
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<StringRef>>(func_name, 19, Date(2022, 5, 20), "2022-05-01");
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<StringRef>>(func_name, 3, Date(1900, 1, 1), "1899-12-29");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<Date>>(func_name, -3, "1899-12-29", Date(1900, 1, 1));
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<StringRef>>(func_name, nullptr, nullptr, "2022-05-01");
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<StringRef>>(func_name, nullptr, Date(2022, 5, 20), nullptr);
    CheckUdf<Nullable<int32_t>, Nullable<Date>, Nullable<StringRef>>(func_name, nullptr, nullptr, nullptr);
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<Date>>(func_name, nullptr, nullptr, Date(2022, 5, 20));
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<Date>>(func_name, nullptr, "2022-05-01", nullptr);
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<Date>>(func_name, nullptr, nullptr, nullptr);
}

TEST_F(UdfIRBuilderTest, DateDiffNull) {
    auto func_name = "datediff";

    // out-of-range format
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "1900-01-00",
                                                                          "1999-12-12T12:12:12Z");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "1977-13-01",
                                                                          "1999-12-12T12:12:12Z");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "19771232",
                                                                          "1999-12-12T12:12:12Z");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "1999-12-12T25:12:12Z",
                                                                          "1999-12-12T12:12:12Z");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "1999-12-12T12:66:12Z",
                                                                          "1999-12-12T12:12:12Z");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "1999-12-12T12:00:61Z",
                                                                          "1999-12-12T12:12:12Z");

    // invalid format
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "1999-12-12T12:12:12Z",
                                                                          "202 2-12-2 9");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>, Nullable<StringRef>>(func_name, nullptr, "1999-12-12T12:12:12Z",
                                                                          "12:30:30");
}

class UdfIRCastTest : public ::testing::TestWithParam<std::pair<absl::string_view, Nullable<int64_t>>> {};

static const std::vector<std::pair<absl::string_view, Nullable<int64_t>>>& GetCastCases() {
    static const std::string& INT32_MAX_S = *new auto(std::to_string(INT32_MAX));
    static const std::string& INT16_MIN_S = *new auto(std::to_string(INT16_MIN));
    static const std::vector<std::pair<absl::string_view, Nullable<int64_t>>> cs{
        {"12", 12},
        {"012", 12},
        {"0", 0},
        {" 9", 9},
        {" +100 ", 100},
        {"-999\t", -999},
        {"+0", 0},
        {"-0", 0},
        {"-1", -1},
        {"-88", -88},
        {"0x12", 18},
        {"+0X2a", 42},
        {"-0X2b", -43},
        {"\t -0X2b", -43},
        {"9223372036854775807", INT64_MAX},
        {"0x7FFFFFFFFFFFFFFF", INT64_MAX},
        {"-9223372036854775808", INT64_MIN},
        // int32 overflows
        {"21474770944", 0x4ffff0000},
        {"-21474770944", -21474770944},
        {INT32_MAX_S, INT32_MAX},
        // int16 overflows
        {"0x4eeee", 323310},
        {"-323310", -323310},
        {INT16_MIN_S, INT16_MIN},
        {"", {}},
        {"-", {}},
        {"+", {}},
        {"8g", {}},
        {"80x99", {}},
        {"0x12k", {}},
        {"8l", {}},
        {"8 l", {}},
        {"gg", {}},
        {"89223372036854775807", {}},
        {"-19223372036854775807", {}},
    };
    return cs;
}

INSTANTIATE_TEST_SUITE_P(StrCastNumeric, UdfIRCastTest, ::testing::ValuesIn(GetCastCases()));
TEST_P(UdfIRCastTest, ToI64) {
    auto [in, out] = GetParam();
    CheckUdf<Nullable<int64_t>, StringRef>("int64", out, in);
    CheckUdf<Nullable<int64_t>, StringRef>("bigint", out, in);
}

TEST_P(UdfIRCastTest, ToI32) {
    auto [in, out] = GetParam();
    Nullable<int32_t> expect;
    if (out.is_null() || out.value() > INT32_MAX || out.value() < INT32_MIN) {
        expect = {};
    } else {
        expect = static_cast<int32_t>(out.value());
    }
    CheckUdf<Nullable<int32_t>, StringRef>("int32", expect, in);
    CheckUdf<Nullable<int32_t>, StringRef>("int", expect, in);
}

TEST_P(UdfIRCastTest, ToI16) {
    auto [in, out] = GetParam();
    Nullable<int16_t> expect;
    if (out.is_null() || out.value() > INT16_MAX || out.value() < INT16_MIN) {
        expect = {};
    } else {
        expect = static_cast<int16_t>(out.value());
    }
    CheckUdf<Nullable<int16_t>, StringRef>("int16", expect, in);
    CheckUdf<Nullable<int16_t>, StringRef>("smallint", expect, in);
}

TEST_F(UdfIRBuilderTest, StringToDouble0) {
    CheckUdf<Nullable<double>, Nullable<StringRef>>("double", 1.0,
                                                    StringRef("1.0"));
}
TEST_F(UdfIRBuilderTest, StringToDouble1) {
    CheckUdf<Nullable<double>, Nullable<StringRef>>("double", -1.0,
                                                    StringRef("-1.0"));
}
TEST_F(UdfIRBuilderTest, StringToDouble2) {
    CheckUdf<Nullable<double>, Nullable<StringRef>>("double", nullptr,
                                                    StringRef("abc"));
}
TEST_F(UdfIRBuilderTest, StringToFloat0) {
    CheckUdf<Nullable<float>, Nullable<StringRef>>("float", 1.0f,
                                                   StringRef("1.0"));
}
TEST_F(UdfIRBuilderTest, StringToFloat1) {
    CheckUdf<Nullable<float>, Nullable<StringRef>>("float", -1.0f,
                                                   StringRef("-1.0"));
}
TEST_F(UdfIRBuilderTest, StringToFloat2) {
    CheckUdf<Nullable<float>, Nullable<StringRef>>("float", nullptr,
                                                   StringRef("abc"));
}
TEST_F(UdfIRBuilderTest, LikeMatch) {
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
TEST_F(UdfIRBuilderTest, IlikeMatch) {
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
TEST_F(UdfIRBuilderTest, rlike_match) {
    auto udf_name = "regexp_like";
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("The Lord of the Rings"), StringRef("The Lord .f the Rings"), StringRef(""));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, false, StringRef("The Lord of the Rings"), StringRef("the L.rd .f the Rings"), StringRef(""));

    // target is null, return null
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, nullptr, nullptr, StringRef("The Lord .f the Rings"), StringRef(""));
    // pattern is null, return null
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, nullptr, StringRef("The Lord of the Rings"), nullptr, StringRef(""));
    // flags is null
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, nullptr, StringRef("The Lord of the Rings"), StringRef("The Lord .f the Rings"), nullptr);

    // single flag
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, false, StringRef("The Lord of the Rings"), StringRef("the L.rd .f the Rings"), StringRef("c"));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("The Lord of the Rings"), StringRef("the L.rd .f the Rings"), StringRef("i"));

    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, false, StringRef("The Lord of the Rings\nJ. R. R. Tolkien"),
    StringRef("The Lord of the Rings.J\\. R\\. R\\. Tolkien"), StringRef(""));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("The Lord of the Rings\nJ. R. R. Tolkien"),
    StringRef("The Lord of the Rings.J\\. R\\. R\\. Tolkien"), StringRef("s"));

    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, false, StringRef("The Lord of the Rings\nJ. R. R. Tolkien"),
    StringRef("^The Lord of the Rings$\nJ\\. R\\. R\\. Tolkien"), StringRef(""));
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("The Lord of the Rings\nJ. R. R. Tolkien"),
    StringRef("^The Lord of the Rings$\nJ\\. R\\. R\\. Tolkien"), StringRef("m"));

    // multiple flags
    CheckUdf<Nullable<bool>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        udf_name, true, StringRef("The Lord of the Rings\nJ. R. R. Tolkien"),
    StringRef("^the Lord of the Rings$.J\\. R\\. R\\. Tolkien"), StringRef("mis"));
}
TEST_F(UdfIRBuilderTest, Reverse) {
    auto udf_name = "reverse";
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, StringRef("SQL"), StringRef("LQS"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, StringRef("abc"), StringRef("cba"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, StringRef("a"), StringRef("a"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, StringRef("123456789"), StringRef("987654321"));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, StringRef(""), StringRef(""));
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>>(udf_name, nullptr, nullptr);
}
TEST_F(UdfIRBuilderTest, Degrees) {
    auto udf_name = "degrees";
    constexpr double pi = 3.141592653589793238463L;
    CheckUdf<double, double>(udf_name, 180.0, pi);
    CheckUdf<double, double>(udf_name, 90.0, pi/2);
    CheckUdf<double, double>(udf_name, 0.0, 0.0);
    CheckUdf<double, double>(udf_name, -180.0, -pi);
    CheckUdf<double, double>(udf_name, -90.0, -pi/2);
    CheckUdf<Nullable<double>, Nullable<double>>(udf_name, nullptr, nullptr);
}
TEST_F(UdfIRBuilderTest, CharTest) {
    auto udf_name = "char";
    CheckUdf<StringRef, int32_t>(udf_name, StringRef("A"), 65);
    CheckUdf<StringRef, int32_t>(udf_name, StringRef("B"), 322);
    CheckUdf<StringRef, int32_t>(udf_name, StringRef("N"), -178);
    CheckUdf<StringRef, int32_t>(udf_name, StringRef(1, "\0"), 256);
    CheckUdf<StringRef, int32_t>(udf_name, StringRef(1, "\0"), -256);
    CheckUdf<Nullable<StringRef>, Nullable<int32_t>>(udf_name, nullptr, nullptr);
}
TEST_F(UdfIRBuilderTest, CharLengthUdfTest) {
    auto udf_name = "char_length";
    CheckUdf<int32_t, StringRef>(udf_name, 10, StringRef("Spark SQL "));
    CheckUdf<int32_t, StringRef>(udf_name, 10, StringRef("Spark SQL\n"));
    CheckUdf<int32_t, Nullable<StringRef>>(udf_name, 0, StringRef(""));
    CheckUdf<int32_t, Nullable<StringRef>>(udf_name, 0, nullptr);
}
TEST_F(UdfIRBuilderTest, DegreeToRadiusCheck) {
    auto udf_name = "radians";
    CheckUdf<double, double>(udf_name, M_PI, 180);
    CheckUdf<double, double>(udf_name, M_PI / 2 , 90);
    CheckUdf<double, int16_t>(udf_name, M_PI / 2 , 90);
    CheckUdf<double, int32_t>(udf_name, M_PI / 2 , 90);
    CheckUdf<double, int64_t>(udf_name, M_PI / 2 , 90);
    CheckUdf<double, float>(udf_name, M_PI / 2 , 90);
    CheckUdf<double, double>(udf_name, M_PI / 2 , 90);
    CheckUdf<double, double>(udf_name, 0, 0);
    CheckUdf<Nullable<double>, Nullable<double>>(udf_name, nullptr, nullptr);
}


TEST_F(UdfIRBuilderTest, DegreeToRadiusFail) {
    auto udf_name = "radians";
    CheckUdfFail<double, StringRef>(udf_name, 0, "0");
    CheckUdfFail<double, Timestamp>(udf_name, 0, Timestamp(12000));
    CheckUdfFail<double, Date>(udf_name, 0, Date(2012, 12, 12));
    CheckUdfFail<double, bool>(udf_name, 0, false);
}

TEST_F(UdfIRBuilderTest, Replace) {
    auto fn_name = "replace";

    CheckUdf<StringRef, StringRef, StringRef, StringRef>(fn_name, "ABCDEF", "ABCabc", "abc", "DEF");
    CheckUdf<StringRef, StringRef, StringRef, StringRef>(fn_name, "ABCabc", "ABCabc", "def", "DEF");
    CheckUdf<StringRef, StringRef, StringRef, StringRef>(fn_name, "AABACA", "AaBaCa", "a", "A");
    CheckUdf<StringRef, StringRef, StringRef, StringRef>(fn_name, "Hello Bob Hi Bob Be",
                                                                   "Hello Ben Hi Ben Be", "Ben", "Bob");
}
TEST_F(UdfIRBuilderTest, ReplaceWithoutReplaceStr) {
    auto fn_name = "replace";

    CheckUdf<StringRef, StringRef, StringRef>(fn_name, "ABC", "ABCabc", "abc");
    CheckUdf<StringRef, StringRef, StringRef>(fn_name, "ABCabc", "ABCabc", "def");
    CheckUdf<StringRef, StringRef, StringRef>(fn_name, "ABC", "AaBaCa", "a");
    CheckUdf<StringRef, StringRef, StringRef>(fn_name, "Hello  Hi  Be", "Hello Ben Hi Ben Be", "Ben");
}

TEST_F(UdfIRBuilderTest, ReplaceNullable) {
    auto fn_name = "replace";

    CheckUdf<Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(fn_name, nullptr,
                                                                                                 nullptr, "abc", "ABC");
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        fn_name, nullptr, "ABCabc", nullptr, "ABC");
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        fn_name, nullptr, "ABCabc", "ABC", nullptr);
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        fn_name, nullptr, "ABCabc", nullptr, nullptr);
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        fn_name, nullptr, nullptr, "ABCabc", nullptr);
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(
        fn_name, nullptr, nullptr, nullptr, nullptr);

    CheckUdf<Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(fn_name, nullptr, nullptr, "abc");
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(fn_name, nullptr, "abc", nullptr);
    CheckUdf<Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>(fn_name, nullptr, nullptr, nullptr);
}

TEST_F(UdfIRBuilderTest, TestPMod) {
    auto fn_name = "pmod";

    // int32_t
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(fn_name, 2, -10, 3);
    // both negative will get negative result, which is consistent with SparkSQL
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(fn_name, -1, -10, -3);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(fn_name, 1, 10, 3);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(fn_name, 0, -9, 3);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(fn_name, 0, 9, 3);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(fn_name, 1, 10, -3);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(fn_name, nullptr, 10, 0);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(fn_name, nullptr, 10, nullptr);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(fn_name, nullptr, nullptr, 0);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(fn_name, nullptr, nullptr, nullptr);

    // int64_t
    CheckUdf<Nullable<int64_t>, Nullable<int64_t>, Nullable<int64_t>>(fn_name, 2, -10, 3);
    CheckUdf<Nullable<int64_t>, Nullable<int64_t>, Nullable<int64_t>>(fn_name, nullptr, -10, static_cast<int64_t>(0));
    // int16_t
    CheckUdf<Nullable<int16_t>, Nullable<int16_t>, Nullable<int16_t>>(fn_name, 2, -10, 3);
    CheckUdf<Nullable<int16_t>, Nullable<int16_t>, Nullable<int16_t>>(fn_name, nullptr, -10, static_cast<int16_t>(0));

    // float
    CheckUdf<float, Nullable<float>, Nullable<float>>(fn_name, 2.7f, -10.1f, 3.2f);
    CheckUdf<Nullable<float>, Nullable<float>, Nullable<float>>(fn_name, nullptr, -10.1f, 0.0f);

    // double
    CheckUdf<double, Nullable<double>, Nullable<double>>(fn_name, 2.7, -10.1, 3.2);
    CheckUdf<Nullable<double>, Nullable<double>, Nullable<double>>(fn_name, nullptr, -10.1, 0.0);

    // mix types
    CheckUdf<float, Nullable<float>, Nullable<int32_t>>(fn_name, 1.9f, -10.1f, 3);
    CheckUdf<float, Nullable<int32_t>, Nullable<float>>(fn_name, 2.4f, -10, 3.1f);
    CheckUdf<float, Nullable<float>, Nullable<int64_t>>(fn_name, 1.9f, -10.1f, 3);
    CheckUdf<float, Nullable<int64_t>, Nullable<float>>(fn_name, 2.4f, -10, 3.1f);
    CheckUdf<double, Nullable<double>, Nullable<int64_t>>(fn_name, 1.9, -10.1, 3);
    CheckUdf<double, Nullable<int64_t>, Nullable<double>>(fn_name, 2.4, -10, 3.1);
    CheckUdf<double, Nullable<double>, Nullable<int16_t>>(fn_name, 1.9, -10.1, 3);
    CheckUdf<double, Nullable<int16_t>, Nullable<double>>(fn_name, 2.4, -10, 3.1);
    CheckUdf<Nullable<int64_t>, Nullable<int32_t>, Nullable<int64_t>>(fn_name, 2, -10, 3);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int16_t>>(fn_name, 2, -10, 3);
    CheckUdf<Nullable<int64_t>, Nullable<int64_t>, Nullable<int16_t>>(fn_name, 2, -10, 3);
}

TEST_F(UdfIRBuilderTest, EarthDistanceError) {
    CheckUdf<Nullable<double>, double, double, double, double>("earth_distance", nullptr, 100, 44, 44, 44);
    CheckUdf<Nullable<double>, double, double, double, double>("earth_distance", nullptr, 77, 181, 44, 44);
    CheckUdf<Nullable<double>, double, double, double, double>("earth_distance", nullptr, 77, 99, -91, -184);
}

TEST_F(UdfIRBuilderTest, AddMonths) {
    CheckUdf<Nullable<Date>, Date, int16_t>("add_months", Date(2022, 5, 5), Date(2022, 4, 5), 1);
    CheckUdf<Nullable<Date>, Date, int32_t>("add_months", Date(2022, 4, 5), Date(2022, 4, 5), 0);
    CheckUdf<Nullable<Date>, Date, int32_t>("add_months", Date(2022, 3, 5), Date(2022, 4, 5), -1);

    CheckUdf<Nullable<Date>, Date, int32_t>("add_months", Date(2016, 9, 30), Date(2016, 8, 31), 1);
    CheckUdf<Nullable<Date>, Date, int32_t>("add_months", Date(2011, 8, 31), Date(2012, 1, 31), -5);
    CheckUdf<Nullable<Date>, Date, int32_t>("add_months", Date(2012, 2, 29), Date(2012, 1, 31), 1);
    CheckUdf<Nullable<Date>, Date, int32_t>("add_months", Date(2011, 2, 28), Date(2011, 1, 31), 1);
    CheckUdf<Nullable<Date>, Date, int64_t>("add_months", Date(2010, 11, 30), Date(2012, 1, 31), -14);
    CheckUdf<Nullable<Date>, Date, int64_t>("add_months", Date(2013, 3, 31), Date(2012, 1, 31), 14);
}

// ========================================================================= //
//              JSON functions
// ========================================================================= //
TEST_F(UdfIRBuilderTest, JsonArrayLength) {
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>>("json_array_length", 0, "[]");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>>("json_array_length", 3, "[1,2,3]");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>>("json_array_length", 5, R"([1,2,3,{"f1":1,"f2":[5,6]},4])");

    CheckUdf<Nullable<int32_t>, Nullable<StringRef>>("json_array_length", nullptr, R"({})");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>>("json_array_length", nullptr, "[1,2,3");
    CheckUdf<Nullable<int32_t>, Nullable<StringRef>>("json_array_length", nullptr, nullptr);
}

TEST_F(UdfIRBuilderTest, GetJsonObject) {
    std::string_view json = R"(
        {
          "foo": ["bar", "baz"],
          "": 0,
          "a/b": 1,
          "c%d": 2,
          "e^f": 3,
          "g|h": 4,
          "i\\j": 5,
          "k\"l": 6,
          " ": 7,
          "m~n": 8,
          "o\p": 9,
        })";
    std::initializer_list<std::vector<Nullable<StringRef>>> cases = {
        // empty json path evaluate to whole document
        {"[]", "[]", ""},
        {R"({"k": "val"})", R"({"k": "val"})", ""},

        {absl::StripAsciiWhitespace(json), json, ""},
        {R"(["bar", "baz"])", json, "/foo"},
        {"bar", json, "/foo/0"},
        {"baz", json, "/foo/1"},
        {nullptr, json, "/foo/2"},
        {"0", json, "/"},
        {"1", json, "/a~1b"},  // '/' encoded as '~1'
        {"2", json, "/c%d"},
        {"3", json, "/e^f"},
        {"4", json, "/g|h"},
        {"5", json, R"(/i\\j)"},
        {"6", json, R"(/k\"l)"},
        {"7", json, "/ "},
        {"8", json, "/m~0n"},  // '~' encoded as '~0'
        {"9", json, R"(/o\p)"},  // any character can be escaped
        {nullptr, json, "/bar"},
        {nullptr, json, "/bar/0"},

        {"", R"({"a": ""})", "/a"},
        {"str", R"({"a": "str"})", "/a"},
        {"1", R"({"a": 1})", "/a"},
        {"null", R"({"a": null})", "/a"},
        {"true", R"({"a": true})", "/a"},
        {"false", R"({"a": false})", "/a"},
        {R"({"c": "d"})", R"({"a": {"c": "d"}})", "/a"},

        {nullptr, "{", ""},
        {nullptr, R"({"a"})", "/a"},

        // get_json_object do not fully valid the querying object
        {"flase", R"({"a": flase})", "/a"},
        {"ni", R"({"a": ni})", "/a"},
        {"9n", R"({"a": 9n})", "/a"},
        {"-x", R"({"a": -x})", "/a"},
        {"[nx]", R"({"a": [nx]})", "/a"},
        {R"({"g": trx})", R"({"a": {"g": trx}})", "/a"},

        // invalid array/object part result in strange behavior, won't assert in tests
        // {R"({"g":}})", R"({"a": {"g":}})", "/a"},
        // {"[xxy}", R"({"a": [xxy})", "/a"},
    };

    for (auto cs : cases) {
        CheckUdf<Nullable<StringRef>, Nullable<StringRef>, Nullable<StringRef>>("get_json_object", cs[0], cs[1], cs[2]);
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
