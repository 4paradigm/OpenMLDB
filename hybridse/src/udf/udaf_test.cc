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

#include "udf/udf_test.h"

namespace hybridse {
namespace udf {

using openmldb::base::Date;
using codec::ListRef;
using openmldb::base::StringRef;
using openmldb::base::Timestamp;
using udf::Nullable;

class UdafTest : public ::testing::Test {
 public:
    UdafTest() {}
    ~UdafTest() {}
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

template <class T, class... Args>
void CheckUdfFail(const std::string &name, T expect, Args... args) {
    auto function = udf::UdfFunctionBuilder(name)
                        .args<Args...>()
                        .template returns<T>()
                        .build();
    ASSERT_FALSE(function.valid());
}

// simple udaf check, applied when the udaf function accept only one parameter
template <class Ret, class Arg = Ret>
void CheckUdafOneParam(const std::string &fn, Ret expect, std::initializer_list<Arg> cols) {
    CheckUdf(fn, expect, MakeList(cols));
}

TEST_F(UdafTest, MaxTest) {
    CheckUdafOneParam<Nullable<int32_t>>("max", nullptr, {});
    CheckUdafOneParam<Nullable<int32_t>, Nullable<int32_t>>("max", nullptr, {nullptr});
    CheckUdafOneParam<Nullable<int32_t>, Nullable<int32_t>>("max", nullptr, {nullptr, nullptr});
    CheckUdafOneParam<Nullable<int64_t>, Nullable<int64_t>>("max", 5, { 5, 1});
    CheckUdafOneParam<Nullable<int64_t>, Nullable<int64_t>>("max", 5, {nullptr, 5, 1});
    CheckUdafOneParam<Nullable<float>, Nullable<float>>("max", 5.0, {nullptr, 5.0, -1.0});
    CheckUdafOneParam<Nullable<double>, Nullable<double>>("max", 5.0, {nullptr, 5.0, -1.0});
    CheckUdafOneParam<Nullable<StringRef>, Nullable<StringRef>>("max", nullptr, {nullptr});
    CheckUdafOneParam<Nullable<StringRef>, Nullable<StringRef>>("max", StringRef("abc"), {nullptr, StringRef("abc")});
    CheckUdafOneParam<Nullable<StringRef>, Nullable<StringRef>>("max", StringRef("abc"),
                                                                {nullptr, StringRef("abc"), StringRef("aaa")});
}

TEST_F(UdafTest, MinTest) {
    CheckUdafOneParam<Nullable<int32_t>>("min", nullptr, {});
    CheckUdafOneParam<Nullable<int32_t>, Nullable<int32_t>>("min", nullptr, {nullptr});
    CheckUdafOneParam<Nullable<int32_t>, Nullable<int32_t>>("min", nullptr, {nullptr, nullptr});
    CheckUdafOneParam<Nullable<int64_t>, Nullable<int64_t>>("min", 1, {5, 1});
    CheckUdafOneParam<Nullable<int64_t>, Nullable<int64_t>>("min", 1, {nullptr, 5, 1});
    CheckUdafOneParam<Nullable<float>, Nullable<float>>("min", -1.0, {nullptr, 5.0, -1.0});
    CheckUdafOneParam<Nullable<double>, Nullable<double>>("min", -1.0, {nullptr, 5.0, -1.0});
    CheckUdafOneParam<Nullable<StringRef>, Nullable<StringRef>>("min", nullptr, {nullptr});
    CheckUdafOneParam<Nullable<StringRef>, Nullable<StringRef>>("min", StringRef("abc"), {nullptr, StringRef("abc")});
    CheckUdafOneParam<Nullable<StringRef>, Nullable<StringRef>>("min", StringRef("aaa"),
                                                                {nullptr, StringRef("abc"), StringRef("aaa")});
}

TEST_F(UdafTest, CountTest) {
    CheckUdafOneParam<int64_t, Nullable<int32_t>>("count", 0LL, {nullptr});
    CheckUdafOneParam<int64_t, Nullable<int32_t>>("count", 0LL, {});
    CheckUdafOneParam<int64_t, Nullable<int32_t>>("count", 2, {1, 2});
    CheckUdafOneParam<int64_t, Nullable<int32_t>>("count", 2, {1, 2, nullptr});

    CheckUdafOneParam<int64_t, Nullable<int64_t>>("count", 1, {5, nullptr});
    CheckUdafOneParam<int64_t, Nullable<int64_t>>("count", 1, {5});

    CheckUdafOneParam<int64_t, Nullable<double>>("count", 3, {5.0, 1.0, 2.0});

    CheckUdafOneParam<int64_t, Nullable<StringRef>>("count", 3, {StringRef("c"), StringRef("abc"), StringRef("gc")});
    CheckUdafOneParam<int64_t, Nullable<StringRef>>("count", 2, {nullptr, StringRef("abc"), StringRef("gc")});
}

// TODO(aceforeverd): add test for distinct_count


TEST_F(UdafTest, MedianTest) {
    CheckUdafOneParam<Nullable<double>, Nullable<int32_t>>("median", nullptr, {});
    CheckUdafOneParam<Nullable<double>, Nullable<int32_t>>("median", nullptr, {nullptr});
    CheckUdafOneParam<Nullable<double>, Nullable<int32_t>>("median", nullptr, {nullptr, nullptr});

    CheckUdafOneParam<Nullable<double>, Nullable<int32_t>>("median", 1, {0, 1, 2});
    CheckUdafOneParam<Nullable<double>, Nullable<int32_t>>("median", 1.5, {0, 1, 2, 3});
    CheckUdafOneParam<Nullable<double>, Nullable<int32_t>>("median", 1, {0, 1, 2, nullptr});
    CheckUdafOneParam<Nullable<double>, Nullable<int32_t>>("median", 3, {1, 5, 2, 4, 3});
    CheckUdafOneParam<Nullable<double>, Nullable<int32_t>>("median", 2.5, {1, 2, 4, 3});
    CheckUdafOneParam<Nullable<double>, Nullable<int32_t>>("median", -0.5, {-1, -2, 0, 1});

    CheckUdafOneParam<Nullable<double>, Nullable<double>>("median", 2.0, {1.0, 2.0, 3.0});
    CheckUdafOneParam<Nullable<double>, Nullable<double>>("median", 2.5, {1.0, 2.0, 3.0, 4.0});
    CheckUdafOneParam<Nullable<double>, Nullable<double>>("median", 2.0, {1.0, 2.0, 3.0, nullptr});
    CheckUdafOneParam<Nullable<double>, Nullable<double>>("median", 3.0, {1.0, 5.0, 2.0, 4.0, 3.0});
}

TEST_F(UdafTest, SumWhereTest) {
    CheckUdf<int32_t, ListRef<int32_t>, ListRef<bool>>(
        "sum_where", 10, MakeList<int32_t>({4, 5, 6}),
        MakeBoolList({true, false, true}));

    CheckUdf<int32_t, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "sum_where", 9, MakeList<Nullable<int32_t>>({4, 5, 6, nullptr}),
        MakeList<Nullable<bool>>({true, true, nullptr, false}));

    CheckUdf<Nullable<int32_t>, ListRef<int32_t>, ListRef<bool>>(
        "sum_where", nullptr, MakeList<int32_t>({}), MakeBoolList({}));

    CheckUdf<Nullable<int32_t>, ListRef<Nullable<int32_t>>, ListRef<bool>>(
        "sum_where", nullptr, MakeList<Nullable<int32_t>>({ nullptr }), MakeBoolList({ true }));

    // only non-null & cond elems are sumed
    CheckUdf<int32_t, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "sum_where", 4, MakeList<Nullable<int32_t>>({4, 5, 6, nullptr}),
        MakeList<Nullable<bool>>({true, false, nullptr, true}));
}

TEST_F(UdafTest, CountWhereTest) {
    CheckUdf<int64_t, ListRef<int32_t>, ListRef<bool>>(
        "count_where", 2, MakeList<int32_t>({4, 5, 6}),
        MakeBoolList({true, false, true}));

    CheckUdf<int64_t, ListRef<StringRef>, ListRef<bool>>(
        "count_where", 2,
        MakeList({StringRef("1"), StringRef("2"), StringRef("3")}),
        MakeBoolList({true, false, true}));

    CheckUdf<int64_t, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "count_where", 1, MakeList<Nullable<int32_t>>({4, 5, 6, nullptr}),
        MakeList<Nullable<bool>>({true, false, nullptr, true}));

    CheckUdf<int64_t, ListRef<int32_t>, ListRef<bool>>(
        "count_where", 0, MakeList<int32_t>({}), MakeBoolList({}));
}

TEST_F(UdafTest, AvgWhereTest0) {
    CheckUdf<double, ListRef<int32_t>, ListRef<bool>>(
        "avg_where", 5.0, MakeList<int32_t>({4, 5, 6}),
        MakeBoolList({true, false, true}));
}
TEST_F(UdafTest, AvgWhereTest1) {
    CheckUdf<double, ListRef<int32_t>, ListRef<bool>>(
        "avg_where", 5.0, MakeList<int32_t>({4, 5, 6}),
        MakeBoolList({true, false, true}));
    // TODO(someone): Timestamp arithmetic
    // CheckUdf<double, ListRef<Timestamp>, ListRef<bool>>(
    //    "avg_where", 5.0, MakeList<Timestamp>({Timestamp(4), Timestamp(5),
    //    Timestamp(6)}), MakeBoolList({true, false, true}));
}
TEST_F(UdafTest, AvgWhereTest2) {
    CheckUdf<double, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "avg_where", 5.5, MakeList<Nullable<int32_t>>({4, 5, 6, 7}),
        MakeList<Nullable<bool>>({true, false, nullptr, true}));
}

// nullable test
TEST_F(UdafTest, AvgWhereTest3) {
    CheckUdf<Nullable<double>, ListRef<int32_t>, ListRef<bool>>(
        "avg_where", nullptr, MakeList<int32_t>({}), MakeBoolList({}));

    CheckUdf<Nullable<double>, ListRef<Nullable<int32_t>>, ListRef<bool>>(
        "avg_where", nullptr, MakeList<Nullable<int32_t>>({ nullptr }), MakeBoolList({ true }));

    // only non-null & cond elems are accumed
    CheckUdf<double, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "avg_where", 6.0, MakeList<Nullable<int32_t>>({4, 5, 6, nullptr, 8}),
        MakeList<Nullable<bool>>({true, false, nullptr, true, true}));
}

TEST_F(UdafTest, MinWhereTest) {
    CheckUdf<int32_t, ListRef<int32_t>, ListRef<bool>>(
        "min_where", 4, MakeList<int32_t>({4, 5, 6}),
        MakeBoolList({true, false, true}));

    CheckUdf<int32_t, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "min_where", 7, MakeList<Nullable<int32_t>>({7, 5, 4, 8}),
        MakeList<Nullable<bool>>({true, false, nullptr, true}));

    // NULL if no data
    CheckUdf<Nullable<int32_t>, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "min_where", nullptr, MakeList<Nullable<int32_t>>({}), MakeList<Nullable<bool>>({}));

    // NULL if no matches
    CheckUdf<Nullable<int32_t>, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "min_where", nullptr, MakeList<Nullable<int32_t>>({1, 9, 3}), MakeList<Nullable<bool>>({false, false, false}));

    // NULL if only NULL value matched
    CheckUdf<Nullable<int32_t>, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "min_where", nullptr, MakeList<Nullable<int32_t>>({nullptr, 3, nullptr}),
        MakeList<Nullable<bool>>({true, false, true}));

    // value is NULL => skiped, only pickup not null
    CheckUdf<Nullable<int32_t>, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "min_where", 1, MakeList<Nullable<int32_t>>({1, nullptr, 3}), MakeList<Nullable<bool>>({true, true, true}));
}

TEST_F(UdafTest, MaxWhereTest) {
    CheckUdf<int32_t, ListRef<int32_t>, ListRef<bool>>(
        "max_where", 7, MakeList<int32_t>({7, 5, 6}),
        MakeBoolList({true, false, true}));

    // cond is false or NULL => skiped
    CheckUdf<int32_t, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "max_where", 1, MakeList<Nullable<int32_t>>({1, 5, 4, 0}),
        MakeList<Nullable<bool>>({true, false, nullptr, true}));

    // NULL if no data
    CheckUdf<Nullable<int32_t>, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "max_where", nullptr, MakeList<Nullable<int32_t>>({}), MakeList<Nullable<bool>>({}));

    // NULL if no matches
    CheckUdf<Nullable<int32_t>, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "max_where", nullptr, MakeList<Nullable<int32_t>>({1, 9, 3}), MakeList<Nullable<bool>>({false, false, false}));

    // NULL if only NULL value matched
    CheckUdf<Nullable<int32_t>, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "max_where", nullptr, MakeList<Nullable<int32_t>>({nullptr, 3}), MakeList<Nullable<bool>>({true, false}));

    // value is NULL => skiped, only pickup not null
    CheckUdf<Nullable<int32_t>, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>>(
        "max_where", 3, MakeList<Nullable<int32_t>>({1, nullptr, 3}), MakeList<Nullable<bool>>({true, true, true}));
}

TEST_F(UdafTest, AvgTest) {
    CheckUdf<double, ListRef<int16_t>>("avg", 2.5,
                                       MakeList<int16_t>({1, 2, 3, 4}));
    CheckUdf<double, ListRef<int32_t>>("avg", 2.5,
                                       MakeList<int32_t>({1, 2, 3, 4}));
    CheckUdf<double, ListRef<int64_t>>("avg", 2.5,
                                       MakeList<int64_t>({1, 2, 3, 4}));
    CheckUdf<double, ListRef<float>>("avg", 2.5, MakeList<float>({1, 2, 3, 4}));
    CheckUdf<double, ListRef<Nullable<double>>>("avg", 2.5,
                                      MakeList<Nullable<double>>({1, 2, nullptr, 3, 4}));
    // nullable
    CheckUdf<Nullable<double>, ListRef<double>>("avg", nullptr, MakeList<double>({}));
    CheckUdf<Nullable<double>, ListRef<Nullable<double>>>("avg", nullptr, MakeList<Nullable<double>>({nullptr}));
}

TEST_F(UdafTest, SumTest) {
    CheckUdf<int16_t, ListRef<int16_t>>("sum", 10,
                                       MakeList<int16_t>({1, 2, 3, 4}));
    CheckUdf<int32_t, ListRef<int32_t>>("sum", 10,
                                       MakeList<int32_t>({1, 2, 3, 4}));
    CheckUdf<int64_t, ListRef<int64_t>>("sum", 10,
                                       MakeList<int64_t>({1, 2, 3, 4}));
    CheckUdf<float, ListRef<float>>("sum", 10, MakeList<float>({1, 2, 3, 4}));
    CheckUdf<double, ListRef<Nullable<double>>>("sum", 10,
                                      MakeList<Nullable<double>>({1, 2, nullptr, 3, 4}));
    // nullable
    CheckUdf<Nullable<double>, ListRef<double>>("sum", nullptr, MakeList<double>({}));
    CheckUdf<Nullable<double>, ListRef<Nullable<double>>>("sum", nullptr, MakeList<Nullable<double>>({nullptr}));
}

TEST_F(UdafTest, TopkTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "top", StringRef("6,6,5,4"), MakeList<int32_t>({1, 6, 3, 4, 5, 2, 6}),
        MakeList<int32_t>({4, 4, 4, 4, 4, 4, 4}));

    CheckUdf<StringRef, ListRef<float>, ListRef<int32_t>>(
        "top", StringRef("6.600000,6.600000,5.500000,4.400000"),
        MakeList<float>({1.1, 6.6, 3.3, 4.4, 5.5, 2.2, 6.6}),
        MakeList<int32_t>({4, 4, 4, 4, 4, 4, 4}));

    CheckUdf<StringRef, ListRef<Date>, ListRef<int32_t>>(
        "top", StringRef("1900-01-06,1900-01-06,1900-01-05,1900-01-04"),
        MakeList<Date>(
            {Date(1), Date(6), Date(3), Date(4), Date(5), Date(2), Date(6)}),
        MakeList<int32_t>({4, 4, 4, 4, 4, 4, 4}));

    CheckUdf<StringRef, ListRef<Timestamp>, ListRef<int32_t>>(
        "top",
        StringRef("1970-01-01 08:00:06,1970-01-01 08:00:06,1970-01-01 "
                  "08:00:05,1970-01-01 08:00:04"),
        MakeList<Timestamp>({Timestamp(1000), Timestamp(6000), Timestamp(3000),
                             Timestamp(4000), Timestamp(5000), Timestamp(2000),
                             Timestamp(6000)}),
        MakeList<int32_t>({4, 4, 4, 4, 4, 4, 4}));

    CheckUdf<StringRef, ListRef<StringRef>, ListRef<int32_t>>(
        "top", StringRef("6,6,5,4"),
        MakeList<StringRef>({StringRef("1"), StringRef("6"), StringRef("3"),
                             StringRef("4"), StringRef("5"), StringRef("2"),
                             StringRef("6")}),
        MakeList<int32_t>({4, 4, 4, 4, 4, 4, 4}));

    // null and not enough inputs
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<int32_t>>(
        "top", StringRef("5,3,1"),
        MakeList<Nullable<int32_t>>({1, nullptr, 3, nullptr, 5}),
        MakeList<int32_t>({4, 4, 4, 4, 4}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "top", StringRef(""), MakeList<int32_t>({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, SumCateTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "sum_cate", StringRef("1:4,2:6"), MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<int32_t>({1, 2, 1, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<Date>>(
        "sum_cate", StringRef("1900-01-01:4,1900-01-02:6"),
        MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<Date>({Date(1), Date(2), Date(1), Date(2)}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<StringRef>>(
        "sum_cate", StringRef("x:4,y:6"), MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<StringRef>(
            {StringRef("x"), StringRef("y"), StringRef("x"), StringRef("y")}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>,
             ListRef<Nullable<StringRef>>>(
        "sum_cate", StringRef("x:4,y:6"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, nullptr}),
        MakeList<Nullable<StringRef>>({StringRef("x"), StringRef("y"),
                                       StringRef("x"), StringRef("y"), nullptr,
                                       StringRef("x")}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "sum_cate", StringRef(""), MakeList<int32_t>({}),
        MakeList<int32_t>({}));
}

TEST_F(UdafTest, CountCateTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "count_cate", StringRef("1:2,2:2"), MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<int32_t>({1, 2, 1, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<Date>>(
        "count_cate", StringRef("1900-01-01:2,1900-01-02:2"),
        MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<Date>({Date(1), Date(2), Date(1), Date(2)}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<StringRef>>(
        "count_cate", StringRef("x:1,y:3"), MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<StringRef>(
            {StringRef("x"), StringRef("y"), StringRef("y"), StringRef("y")}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>,
             ListRef<Nullable<StringRef>>>(
        "count_cate", StringRef("x:2,y:2"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, nullptr}),
        MakeList<Nullable<StringRef>>({StringRef("x"), StringRef("y"),
                                       StringRef("x"), StringRef("y"), nullptr,
                                       StringRef("x")}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "count_cate", StringRef(""), MakeList<int32_t>({}),
        MakeList<int32_t>({}));
}

TEST_F(UdafTest, MinCateTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "min_cate", StringRef("1:1,2:2"), MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<int32_t>({1, 2, 1, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<Date>>(
        "min_cate", StringRef("1900-01-01:1,1900-01-02:2"),
        MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<Date>({Date(1), Date(2), Date(1), Date(2)}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<StringRef>>(
        "min_cate", StringRef("x:1,y:2"), MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<StringRef>(
            {StringRef("x"), StringRef("y"), StringRef("x"), StringRef("y")}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>,
             ListRef<Nullable<StringRef>>>(
        "min_cate", StringRef("x:1,y:2"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, nullptr}),
        MakeList<Nullable<StringRef>>({StringRef("x"), StringRef("y"),
                                       StringRef("x"), StringRef("y"), nullptr,
                                       StringRef("x")}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "min_cate", StringRef(""), MakeList<int32_t>({}),
        MakeList<int32_t>({}));
}

TEST_F(UdafTest, MaxCateTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "max_cate", StringRef("1:3,2:4"), MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<int32_t>({1, 2, 1, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<Date>>(
        "max_cate", StringRef("1900-01-01:3,1900-01-02:4"),
        MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<Date>({Date(1), Date(2), Date(1), Date(2)}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<StringRef>>(
        "max_cate", StringRef("x:3,y:4"), MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<StringRef>(
            {StringRef("x"), StringRef("y"), StringRef("x"), StringRef("y")}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>,
             ListRef<Nullable<StringRef>>>(
        "max_cate", StringRef("x:3,y:4"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, nullptr}),
        MakeList<Nullable<StringRef>>({StringRef("x"), StringRef("y"),
                                       StringRef("x"), StringRef("y"), nullptr,
                                       StringRef("x")}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "max_cate", StringRef(""), MakeList<int32_t>({}),
        MakeList<int32_t>({}));
}

TEST_F(UdafTest, AvgCateTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "avg_cate", StringRef("1:2.000000,2:3.000000"),
        MakeList<int32_t>({1, 2, 3, 4}), MakeList<int32_t>({1, 2, 1, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<Date>>(
        "avg_cate", StringRef("1900-01-01:2.000000,1900-01-02:3.000000"),
        MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<Date>({Date(1), Date(2), Date(1), Date(2)}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<StringRef>>(
        "avg_cate", StringRef("x:2.000000,y:3.000000"),
        MakeList<int32_t>({1, 2, 3, 4}),
        MakeList<StringRef>(
            {StringRef("x"), StringRef("y"), StringRef("x"), StringRef("y")}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>,
             ListRef<Nullable<StringRef>>>(
        "avg_cate", StringRef("x:2.000000,y:3.000000"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, nullptr}),
        MakeList<Nullable<StringRef>>({StringRef("x"), StringRef("y"),
                                       StringRef("x"), StringRef("y"), nullptr,
                                       StringRef("x")}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<int32_t>>(
        "avg_cate", StringRef(""), MakeList<int32_t>({}),
        MakeList<int32_t>({}));
}

TEST_F(UdafTest, SumCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>>(
        "sum_cate_where", StringRef("1:4,2:6"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<int32_t>({1, 2, 1, 2, 1, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<Date>>(
        "sum_cate_where", StringRef("1900-01-01:4,1900-01-02:6"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<Date>({Date(1), Date(2), Date(1), Date(2), Date(1), Date(2)}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>>(
        "sum_cate_where", StringRef("x:4,y:6"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<StringRef>({StringRef("x"), StringRef("y"), StringRef("x"),
                             StringRef("y"), StringRef("x"), StringRef("y")}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>,
             ListRef<Nullable<StringRef>>>(
        "sum_cate_where", StringRef("x:3,y:4"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, nullptr}),
        MakeList<Nullable<bool>>({false, nullptr, true, true, true, true}),
        MakeList<Nullable<StringRef>>({StringRef("x"), StringRef("y"),
                                       StringRef("x"), StringRef("y"), nullptr,
                                       StringRef("x")}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>>(
        "sum_cate_where", StringRef(""), MakeList<int32_t>({}),
        MakeBoolList({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, CountCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>>(
        "count_cate_where", StringRef("1:2,2:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<int32_t>({1, 2, 1, 2, 1, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<Date>>(
        "count_cate_where", StringRef("1900-01-01:2,1900-01-02:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<Date>({Date(1), Date(2), Date(1), Date(2), Date(1), Date(2)}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>>(
        "count_cate_where", StringRef("x:2,y:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<StringRef>({StringRef("x"), StringRef("y"), StringRef("x"),
                             StringRef("y"), StringRef("x"), StringRef("y")}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>,
             ListRef<Nullable<StringRef>>>(
        "count_cate_where", StringRef("x:1,y:1"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, nullptr}),
        MakeList<Nullable<bool>>({false, nullptr, true, true, true, true}),
        MakeList<Nullable<StringRef>>({StringRef("x"), StringRef("y"),
                                       StringRef("x"), StringRef("y"), nullptr,
                                       StringRef("x")}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>>(
        "count_cate_where", StringRef(""), MakeList<int32_t>({}),
        MakeBoolList({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, MaxCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>>(
        "max_cate_where", StringRef("1:3,2:4"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<int32_t>({1, 2, 1, 2, 1, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<Date>>(
        "max_cate_where", StringRef("1900-01-01:3,1900-01-02:4"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<Date>({Date(1), Date(2), Date(1), Date(2), Date(1), Date(2)}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>>(
        "max_cate_where", StringRef("x:3,y:4"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<StringRef>({StringRef("x"), StringRef("y"), StringRef("x"),
                             StringRef("y"), StringRef("x"), StringRef("y")}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>,
             ListRef<Nullable<StringRef>>>(
        "max_cate_where", StringRef("x:3,y:4"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, nullptr}),
        MakeList<Nullable<bool>>({false, nullptr, true, true, true, true}),
        MakeList<Nullable<StringRef>>({StringRef("x"), StringRef("y"),
                                       StringRef("x"), StringRef("y"), nullptr,
                                       StringRef("x")}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>>(
        "max_cate_where", StringRef(""), MakeList<int32_t>({}),
        MakeBoolList({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, MinCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>>(
        "min_cate_where", StringRef("1:1,2:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<int32_t>({1, 2, 1, 2, 1, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<Date>>(
        "min_cate_where", StringRef("1900-01-01:1,1900-01-02:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<Date>({Date(1), Date(2), Date(1), Date(2), Date(1), Date(2)}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>>(
        "min_cate_where", StringRef("x:1,y:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<StringRef>({StringRef("x"), StringRef("y"), StringRef("x"),
                             StringRef("y"), StringRef("x"), StringRef("y")}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>,
             ListRef<Nullable<StringRef>>>(
        "min_cate_where", StringRef("x:3,y:4"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, nullptr}),
        MakeList<Nullable<bool>>({false, nullptr, true, true, true, true}),
        MakeList<Nullable<StringRef>>({StringRef("x"), StringRef("y"),
                                       StringRef("x"), StringRef("y"), nullptr,
                                       StringRef("x")}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>>(
        "min_cate_where", StringRef(""), MakeList<int32_t>({}),
        MakeBoolList({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, AvgCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>>(
        "avg_cate_where", StringRef("1:2.000000,2:3.000000"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<int32_t>({1, 2, 1, 2, 1, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<Date>>(
        "avg_cate_where", StringRef("1900-01-01:2.000000,1900-01-02:3.000000"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<Date>({Date(1), Date(2), Date(1), Date(2), Date(1), Date(2)}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>>(
        "avg_cate_where", StringRef("x:2.000000,y:3.000000"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6}),
        MakeBoolList({true, true, true, true, false, false}),
        MakeList<StringRef>({StringRef("x"), StringRef("y"), StringRef("x"),
                             StringRef("y"), StringRef("x"), StringRef("y")}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>,
             ListRef<Nullable<StringRef>>>(
        "avg_cate_where", StringRef("x:3.000000,y:4.000000"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, nullptr}),
        MakeList<Nullable<bool>>({false, nullptr, true, true, true, true}),
        MakeList<Nullable<StringRef>>({StringRef("x"), StringRef("y"),
                                       StringRef("x"), StringRef("y"), nullptr,
                                       StringRef("x")}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>>(
        "avg_cate_where", StringRef(""), MakeList<int32_t>({}),
        MakeBoolList({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, TopNKeyCountCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>,
             ListRef<int32_t>>(
        "top_n_key_count_cate_where", StringRef("2:2,1:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<int32_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<Date>,
             ListRef<int32_t>>(
        "top_n_key_count_cate_where", StringRef("1900-01-02:2,1900-01-01:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<Date>({Date(0), Date(1), Date(2), Date(0), Date(1), Date(2),
                        Date(0), Date(1), Date(2)}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>,
             ListRef<int32_t>>(
        "top_n_key_count_cate_where", StringRef("z:2,y:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<StringRef>({StringRef("x"), StringRef("y"), StringRef("z"),
                             StringRef("x"), StringRef("y"), StringRef("z"),
                             StringRef("x"), StringRef("y"), StringRef("z")}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>,
             ListRef<Nullable<StringRef>>, ListRef<int32_t>>(
        "top_n_key_count_cate_where", StringRef("z:1,y:1"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, 6, nullptr}),
        MakeList<Nullable<bool>>(
            {false, nullptr, true, true, true, true, true}),
        MakeList<Nullable<StringRef>>(
            {StringRef("x"), StringRef("y"), StringRef("z"), StringRef("x"),
             StringRef("y"), nullptr, StringRef("x")}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>,
             ListRef<int32_t>>("top_n_key_count_cate_where", StringRef(""),
                               MakeList<int32_t>({}), MakeBoolList({}),
                               MakeList<int32_t>({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, TopNKeySumCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>,
             ListRef<int32_t>>(
        "top_n_key_sum_cate_where", StringRef("2:9,1:7"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<int32_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<Date>,
             ListRef<int32_t>>(
        "top_n_key_sum_cate_where", StringRef("1900-01-02:9,1900-01-01:7"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<Date>({Date(0), Date(1), Date(2), Date(0), Date(1), Date(2),
                        Date(0), Date(1), Date(2)}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>,
             ListRef<int32_t>>(
        "top_n_key_sum_cate_where", StringRef("z:9,y:7"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<StringRef>({StringRef("x"), StringRef("y"), StringRef("z"),
                             StringRef("x"), StringRef("y"), StringRef("z"),
                             StringRef("x"), StringRef("y"), StringRef("z")}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>,
             ListRef<Nullable<StringRef>>, ListRef<int32_t>>(
        "top_n_key_sum_cate_where", StringRef("z:3,y:5"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, 6, nullptr}),
        MakeList<Nullable<bool>>(
            {false, nullptr, true, true, true, true, true}),
        MakeList<Nullable<StringRef>>(
            {StringRef("x"), StringRef("y"), StringRef("z"), StringRef("x"),
             StringRef("y"), nullptr, StringRef("x")}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>,
             ListRef<int32_t>>("top_n_key_sum_cate_where", StringRef(""),
                               MakeList<int32_t>({}), MakeBoolList({}),
                               MakeList<int32_t>({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, TopNKeyMinCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>,
             ListRef<int32_t>>(
        "top_n_key_min_cate_where", StringRef("2:3,1:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<int32_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<Date>,
             ListRef<int32_t>>(
        "top_n_key_min_cate_where", StringRef("1900-01-02:3,1900-01-01:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<Date>({Date(0), Date(1), Date(2), Date(0), Date(1), Date(2),
                        Date(0), Date(1), Date(2)}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>,
             ListRef<int32_t>>(
        "top_n_key_min_cate_where", StringRef("z:3,y:2"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<StringRef>({StringRef("x"), StringRef("y"), StringRef("z"),
                             StringRef("x"), StringRef("y"), StringRef("z"),
                             StringRef("x"), StringRef("y"), StringRef("z")}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>,
             ListRef<Nullable<StringRef>>, ListRef<int32_t>>(
        "top_n_key_min_cate_where", StringRef("z:3,y:5"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, 6, nullptr}),
        MakeList<Nullable<bool>>(
            {false, nullptr, true, true, true, true, true}),
        MakeList<Nullable<StringRef>>(
            {StringRef("x"), StringRef("y"), StringRef("z"), StringRef("x"),
             StringRef("y"), nullptr, StringRef("x")}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>,
             ListRef<int32_t>>("top_n_key_min_cate_where", StringRef(""),
                               MakeList<int32_t>({}), MakeBoolList({}),
                               MakeList<int32_t>({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, TopNKeyMaxCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>,
             ListRef<int32_t>>(
        "top_n_key_max_cate_where", StringRef("2:6,1:5"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<int32_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<Date>,
             ListRef<int32_t>>(
        "top_n_key_max_cate_where", StringRef("1900-01-02:6,1900-01-01:5"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<Date>({Date(0), Date(1), Date(2), Date(0), Date(1), Date(2),
                        Date(0), Date(1), Date(2)}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>,
             ListRef<int32_t>>(
        "top_n_key_max_cate_where", StringRef("z:6,y:5"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<StringRef>({StringRef("x"), StringRef("y"), StringRef("z"),
                             StringRef("x"), StringRef("y"), StringRef("z"),
                             StringRef("x"), StringRef("y"), StringRef("z")}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>,
             ListRef<Nullable<StringRef>>, ListRef<int32_t>>(
        "top_n_key_max_cate_where", StringRef("z:3,y:5"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, 6, nullptr}),
        MakeList<Nullable<bool>>(
            {false, nullptr, true, true, true, true, true}),
        MakeList<Nullable<StringRef>>(
            {StringRef("x"), StringRef("y"), StringRef("z"), StringRef("x"),
             StringRef("y"), nullptr, StringRef("x")}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>,
             ListRef<int32_t>>("top_n_key_max_cate_where", StringRef(""),
                               MakeList<int32_t>({}), MakeBoolList({}),
                               MakeList<int32_t>({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, TopNKeyAvgCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>,
             ListRef<int32_t>>(
        "top_n_key_avg_cate_where", StringRef("2:4.500000,1:3.500000"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<int32_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<Date>,
             ListRef<int32_t>>(
        "top_n_key_avg_cate_where",
        StringRef("1900-01-02:4.500000,1900-01-01:3.500000"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<Date>({Date(0), Date(1), Date(2), Date(0), Date(1), Date(2),
                        Date(0), Date(1), Date(2)}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>,
             ListRef<int32_t>>(
        "top_n_key_avg_cate_where", StringRef("z:4.500000,y:3.500000"),
        MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<StringRef>({StringRef("x"), StringRef("y"), StringRef("z"),
                             StringRef("x"), StringRef("y"), StringRef("z"),
                             StringRef("x"), StringRef("y"), StringRef("z")}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>,
             ListRef<Nullable<StringRef>>, ListRef<int32_t>>(
        "top_n_key_avg_cate_where", StringRef("z:3.000000,y:5.000000"),
        MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, 6, nullptr}),
        MakeList<Nullable<bool>>(
            {false, nullptr, true, true, true, true, true}),
        MakeList<Nullable<StringRef>>(
            {StringRef("x"), StringRef("y"), StringRef("z"), StringRef("x"),
             StringRef("y"), nullptr, StringRef("x")}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>,
             ListRef<int32_t>>("top_n_key_avg_cate_where", StringRef(""),
                               MakeList<int32_t>({}), MakeBoolList({}),
                               MakeList<int32_t>({}), MakeList<int32_t>({}));
}

TEST_F(UdafTest, TopNValueCountCateWhereTest) {
    CheckUdf<StringRef, ListRef<StringRef>, ListRef<bool>, ListRef<int32_t>, ListRef<int32_t>>(
        "top_n_value_count_cate_where", "2:2,1:2", MakeList<StringRef>({"1", "2", "3", "4", "5", "6", "7", "8", "9"}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<int32_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}), MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int64_t>, ListRef<bool>, ListRef<Date>, ListRef<int32_t>>(
        "top_n_value_count_cate_where", "1900-01-01:3,1900-01-02:2", MakeList<int64_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({false, true, true, true, true, true, false, true, false}),
        MakeList<Date>({Date(0), Date(1), Date(2), Date(0), Date(1), Date(2), Date(0), Date(1), Date(2)}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>, ListRef<int32_t>>(
        "top_n_value_count_cate_where", "x:3,y:1", MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, false, true, false, false, true, false, false}),
        MakeList<StringRef>({"x", "y", "z", "x", "y", "z", "x", "y", "z"}),
        MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>, ListRef<Nullable<StringRef>>,
             ListRef<int64_t>>("top_n_value_count_cate_where", "y:2,z:1",
                               MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, 6, nullptr, 7}),
                               MakeList<Nullable<bool>>({false, nullptr, true, true, true, true, true, true}),
                               MakeList<Nullable<StringRef>>({"x", "y", "z", "x", "y", nullptr, "x", "y"}),
                               MakeList<int64_t>({2, 2, 2, 2, 2, 2, 2, 2}));

    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>, ListRef<int32_t>>(
        "top_n_value_count_cate_where", "", MakeList<int32_t>({}), MakeBoolList({}), MakeList<int32_t>({}),
        MakeList<int32_t>({}));
}

TEST_F(UdafTest, TopNValueMaxCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>, ListRef<int32_t>>(
        "top_n_value_max_cate_where", "0:7,2:6", MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, true, false, false}),
        MakeList<int32_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}), MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<StringRef>, ListRef<int64_t>>(
        "top_n_value_max_cate_where", "z:6", MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, false, false, false}),
        MakeList<StringRef>({"x", "y", "z", "x", "y", "z", "x", "y", "z"}),
        MakeList<int64_t>({1, 1, 1, 1, 1, 1, 1, 1, 1}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>, ListRef<Nullable<StringRef>>,
             ListRef<int32_t>>("top_n_value_max_cate_where", "y:5,x:4",
                               MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, 6, nullptr}),
                               MakeList<Nullable<bool>>({false, nullptr, true, true, true, true, true}),
                               MakeList<Nullable<StringRef>>({"x", "y", "z", "x", "y", nullptr, "x"}),
                               MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2}));
    //
    // empty
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>, ListRef<int32_t>>(
        "top_n_value_max_cate_where", "", MakeList<int32_t>({}), MakeBoolList({}), MakeList<int32_t>({}),
        MakeList<int32_t>({}));
}

TEST_F(UdafTest, TopNValueMinCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>, ListRef<int32_t>>(
        "top_n_value_min_cate_where", "2:3,1:2", MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, true, false, false}),
        MakeList<int32_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}), MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>, ListRef<Nullable<StringRef>>,
             ListRef<int32_t>>("top_n_value_min_cate_where", "y:5,x:4",
                               MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, 6, nullptr}),
                               MakeList<Nullable<bool>>({false, nullptr, true, true, true, true, true}),
                               MakeList<Nullable<StringRef>>({"x", "y", "z", "x", "y", nullptr, "x"}),
                               MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2}));
}

TEST_F(UdafTest, TopNValueSumCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>, ListRef<int32_t>>(
        "top_n_value_sum_cate_where", "0:12,2:9", MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, true, false, false}),
        MakeList<int32_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}), MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>, ListRef<Nullable<StringRef>>,
             ListRef<int32_t>>("top_n_value_sum_cate_where", "y:5,x:4",
                               MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, 6, nullptr}),
                               MakeList<Nullable<bool>>({false, nullptr, true, true, true, true, true}),
                               MakeList<Nullable<StringRef>>({"x", "y", "z", "x", "y", nullptr, "x"}),
                               MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2}));
}

TEST_F(UdafTest, TopNValueAvgCateWhereTest) {
    CheckUdf<StringRef, ListRef<int32_t>, ListRef<bool>, ListRef<int32_t>, ListRef<int32_t>>(
        "top_n_value_avg_cate_where", "2:4.500000,0:4.000000", MakeList<int32_t>({1, 2, 3, 4, 5, 6, 7, 8, 9}),
        MakeBoolList({true, true, true, true, true, true, true, false, false}),
        MakeList<int32_t>({0, 1, 2, 0, 1, 2, 0, 1, 2}), MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2, 2, 2}));

    // null key and values
    CheckUdf<StringRef, ListRef<Nullable<int32_t>>, ListRef<Nullable<bool>>, ListRef<Nullable<StringRef>>,
             ListRef<int32_t>>("top_n_value_avg_cate_where", "y:5.000000,x:4.000000",
                               MakeList<Nullable<int32_t>>({1, 2, 3, 4, 5, 6, nullptr}),
                               MakeList<Nullable<bool>>({false, nullptr, true, true, true, true, true}),
                               MakeList<Nullable<StringRef>>({"x", "y", "z", "x", "y", nullptr, "x"}),
                               MakeList<int32_t>({2, 2, 2, 2, 2, 2, 2}));
}

TEST_F(UdafTest, DrawdownTest) {
    double expected = 0.75;
    CheckUdf<double, ListRef<int16_t>>("drawdown", expected, MakeList<int16_t>({1, 8, 5, 2, 10, 4}));
    CheckUdf<double, ListRef<int32_t>>("drawdown", expected, MakeList<int32_t>({1, 8, 5, 2, 10, 4}));
    CheckUdf<double, ListRef<int64_t>>("drawdown", expected, MakeList<int64_t>({1, 8, 5, 2, 10, 4}));
    CheckUdf<double, ListRef<float>>("drawdown", expected, MakeList<float>({1, 8, 5, 2, 10, 4}));
    CheckUdf<double, ListRef<Nullable<double>>>("drawdown", expected,
                                                MakeList<Nullable<double>>({1, 8, 5, 2, nullptr, 10, 4}));

    CheckUdf<double, ListRef<Nullable<double>>>("drawdown", 0, MakeList<Nullable<double>>({1}));
    CheckUdf<double, ListRef<Nullable<double>>>("drawdown", 0, MakeList<Nullable<double>>({1, 8}));
    CheckUdf<double, ListRef<Nullable<double>>>("drawdown", 1, MakeList<Nullable<double>>({1, 0.0}));
    CheckUdf<double, ListRef<Nullable<double>>>("drawdown", 0, MakeList<Nullable<double>>({0.0, 0.0}));
    CheckUdf<double, ListRef<Nullable<double>>>("drawdown", 0, MakeList<Nullable<double>>({1, 1}));
    CheckUdf<double, ListRef<Nullable<double>>>("drawdown", 1, MakeList<Nullable<double>>({10, 1, 0.0, 20, 20, 10}));
    CheckUdf<double, ListRef<Nullable<double>>>("drawdown", 0.8, MakeList<Nullable<double>>({10, 4, 5, 20, 50, 10}));

    // nullable
    CheckUdf<Nullable<double>, ListRef<double>>("drawdown", nullptr, MakeList<double>({}));
    CheckUdf<Nullable<double>, ListRef<Nullable<double>>>("drawdown", nullptr, MakeList<Nullable<double>>({nullptr}));

    // negative value will be skipped
    CheckUdf<double, ListRef<Nullable<double>>>("drawdown", 0.5, MakeList<Nullable<double>>({-1, 2, -2, 1}));
    CheckUdf<Nullable<double>, ListRef<Nullable<double>>>("drawdown", nullptr,
                                                          MakeList<Nullable<double>>({-1, -2, -1}));
}

}  // namespace udf
}  // namespace hybridse

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::llvm::InitializeNativeTarget();
    ::llvm::InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
