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

#include <dlfcn.h>
#include <gtest/gtest.h>
#include <stdint.h>

#include <algorithm>
#include <tuple>
#include <utility>
#include <vector>

#include "absl/strings/substitute.h"
#include "base/fe_slice.h"
#include "case/sql_case.h"
#include "codec/list_iterator_codec.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"
#include "vm/mem_catalog.h"
namespace hybridse {
namespace udf {
using hybridse::codec::ArrayListV;
using hybridse::codec::ColumnImpl;
using hybridse::codec::ListRef;
using hybridse::codec::Row;
using hybridse::sqlcase::SqlCase;
using openmldb::base::Date;
using openmldb::base::Timestamp;

class UdfTest : public ::testing::Test {
 public:
    UdfTest() { InitData(); }
    ~UdfTest() {}

    void InitData() {
        rows.clear();
        // prepare row buf
        {
            int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
            *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
            *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
            *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
            *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
            *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
            rows.push_back(Row(base::RefCountedSlice::Create(ptr, 28)));
        }

        {
            int8_t* ptr = static_cast<int8_t*>(malloc(28));
            *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
            *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
            *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
            *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
            *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
            rows.push_back(Row(base::RefCountedSlice::Create(ptr, 28)));
        }

        {
            int8_t* ptr = static_cast<int8_t*>(malloc(28));
            *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
            *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
            *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
            *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
            *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
            rows.push_back(Row(base::RefCountedSlice::Create(ptr, 28)));
        }
    }

 protected:
    std::vector<Row> rows;
};

template <typename V>
bool FetchColList(vm::ListV<Row>* table, size_t col_idx, size_t offset,
                  ListRef<V>* res) {
    const uint32_t size = sizeof(ColumnImpl<V>);
    int8_t* buf = reinterpret_cast<int8_t*>(malloc(size));
    auto datatype = DataTypeTrait<V>::codec_type_enum();

    codec::ListRef<Row> table_ref;
    table_ref.list = reinterpret_cast<int8_t*>(table);

    if (0 !=
        ::hybridse::codec::v1::GetCol(reinterpret_cast<int8_t*>(&table_ref), 0,
                                      col_idx, offset, datatype, buf)) {
        return false;
    }
    res->list = buf;
    return true;
}

void SumTest(vm::ListV<Row>* table) {
    {
        ListRef<int32_t> list;
        ASSERT_TRUE(FetchColList(table, 0, 2, &list));

        auto sum = UdfFunctionBuilder("sum")
                       .args<ListRef<int32_t>>()
                       .returns<int32_t>()
                       .build();
        ASSERT_TRUE(sum.valid());
        ASSERT_EQ(1 + 11 + 111, sum(list));
    }

    {
        ListRef<int16_t> list;
        ASSERT_TRUE(FetchColList(table, 1, 2 + 4, &list));

        auto sum = UdfFunctionBuilder("sum")
                       .args<ListRef<int16_t>>()
                       .returns<int16_t>()
                       .build();
        ASSERT_TRUE(sum.valid());
        ASSERT_EQ(2 + 22 + 222, sum(list));
    }

    {
        ListRef<float> list;
        ASSERT_TRUE(FetchColList(table, 2, 2 + 4 + 2, &list));

        auto sum = UdfFunctionBuilder("sum")
                       .args<ListRef<float>>()
                       .returns<float>()
                       .build();
        ASSERT_TRUE(sum.valid());
        ASSERT_EQ(3.1f + 33.1f + 333.1f, sum(list));
    }

    {
        ListRef<double> list;
        ASSERT_TRUE(FetchColList(table, 3, 2 + 4 + 2 + 4, &list));

        auto sum = UdfFunctionBuilder("sum")
                       .args<ListRef<double>>()
                       .returns<double>()
                       .build();
        ASSERT_TRUE(sum.valid());
        ASSERT_EQ(4.1 + 44.1 + 444.1, sum(list));
    }

    {
        ListRef<int64_t> list;
        ASSERT_TRUE(FetchColList(table, 4, 2 + 4 + 2 + 4 + 8, &list));

        auto sum = UdfFunctionBuilder("sum")
                       .args<ListRef<int64_t>>()
                       .returns<int64_t>()
                       .build();
        ASSERT_TRUE(sum.valid());
        ASSERT_EQ(5L + 55L + 555L, sum(list));
    }
}

TEST_F(UdfTest, udf_mem_table_handler_sum_test) {
    vm::MemTableHandler window;
    for (auto row : rows) {
        window.AddRow(row);
    }
    SumTest(&window);
}

TEST_F(UdfTest, udf_mem_time_table_handler_sum_test) {
    vm::MemTimeTableHandler window;
    uint64_t ts = 1000;
    for (auto row : rows) {
        window.AddRow(ts++, row);
    }
    SumTest(&window);
}
TEST_F(UdfTest, udf_mem_table_handler_large_size_sum_test) {
    vm::MemTableHandler window;
    int64_t w_size = 4000000;
    for (int i = 0; i < w_size; i++) {
        window.AddRow(rows[0]);
    }
    {
        ListRef<int32_t> list;
        ASSERT_TRUE(FetchColList(&window, 0, 2, &list));

        auto sum = UdfFunctionBuilder("sum")
                       .args<ListRef<int32_t>>()
                       .returns<int32_t>()
                       .build();
        ASSERT_TRUE(sum.valid());
        ASSERT_EQ(w_size, sum(list));
    }
}
TEST_F(UdfTest, udf_mem_time_table_handler_large_size_sum_test) {
    vm::MemTimeTableHandler window;
    uint64_t ts = 1000;
    int64_t w_size = 4000000;
    for (int i = 0; i < w_size; i++) {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
        window.AddRow(ts++, Row(base::RefCountedSlice::Create(ptr, 28)));
    }
    {
        ListRef<int32_t> list;
        ASSERT_TRUE(FetchColList(&window, 0, 2, &list));

        auto sum = UdfFunctionBuilder("sum")
                       .args<ListRef<int32_t>>()
                       .returns<int32_t>()
                       .build();
        ASSERT_TRUE(sum.valid());
        ASSERT_EQ(w_size, sum(list));
    }
}

TEST_F(UdfTest, udf_sum_test) {
    ArrayListV<Row> window(&rows);
    SumTest(&window);
}

TEST_F(UdfTest, GetColTest) {
    ArrayListV<Row> impl(&rows);
    const uint32_t size = sizeof(ColumnImpl<int16_t>);
    codec::ListRef<Row> impl_ref;
    impl_ref.list = reinterpret_cast<int8_t*>(&impl);

    for (int i = 0; i < 10; ++i) {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::hybridse::codec::ListRef<> list_ref;
        list_ref.list = buf;
        ASSERT_EQ(0, ::hybridse::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&impl_ref), 0, 0, 2,
                         hybridse::type::kInt32, buf));
        ::hybridse::codec::ColumnImpl<int16_t>* col =
            reinterpret_cast<::hybridse::codec::ColumnImpl<int16_t>*>(
                list_ref.list);
        auto col_iterator = col->GetIterator();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(1, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(11, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(111, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_FALSE(col_iterator->Valid());
    }
}

TEST_F(UdfTest, GetWindowColRangeTest) {
    // w[0:20s]
    vm::CurrentHistoryWindow table(vm::Window::kFrameRowsRange, -36000000, 0);
    std::string schema = "col1:int,col2:timestamp";
    std::string data =
        "0, 1590115410000\n"
        "1, 1590115420000\n"
        "2, 1590115430000\n"
        "3, 1590115440000\n"
        "4, 1590115450000\n"
        "5, 1590115460000\n"
        "6, 1590115470000\n"
        "7, 1590115480000\n"
        "8, 1590115490000\n"
        "9, 1590115500000";
    type::TableDef table_def;
    std::vector<Row> rows;
    ASSERT_TRUE(hybridse::sqlcase::SqlCase::ExtractSchema(schema, table_def));
    ASSERT_TRUE(hybridse::sqlcase::SqlCase::ExtractRows(table_def.columns(),
                                                        data, rows));
    codec::RowView row_view(table_def.columns());
    for (auto row : rows) {
        row_view.Reset(row.buf());
        table.BufferData(row_view.GetTimestampUnsafe(1), row);
    }

    const uint32_t inner_list_size = sizeof(codec::InnerRangeList<Row>);
    int8_t* inner_list_buf = reinterpret_cast<int8_t*>(alloca(inner_list_size));
    ASSERT_EQ(0, ::hybridse::codec::v1::GetInnerRangeList(
                     reinterpret_cast<int8_t*>(&table), 1590115500000, -20000,
                     -50000, inner_list_buf));
    int32_t offset = row_view.GetPrimaryFieldOffset(0);
    hybridse::type::Type type = hybridse::type::kInt32;
    const uint32_t size = sizeof(ColumnImpl<int32_t>);
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
    ListRef<> inner_list_ref;
    inner_list_ref.list = inner_list_buf;

    for (int i = 0; i < 100000; ++i) {
        ASSERT_EQ(0, ::hybridse::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&inner_list_ref), 0, 0,
                         offset, type, buf));
        ::hybridse::codec::ColumnImpl<int32_t>* col =
            reinterpret_cast<::hybridse::codec::ColumnImpl<int32_t>*>(buf);
        auto col_iterator = col->GetIterator();
        col_iterator->SeekToFirst();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(7, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(6, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_EQ(5, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_EQ(4, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_FALSE(col_iterator->Valid());
    }
}

TEST_F(UdfTest, GetWindowColRowsTest) {
    // w[0:20s]
    vm::CurrentHistoryWindow table(vm::Window::kFrameRowsRange, -36000000, 0);
    std::string schema = "col1:int,col2:timestamp";
    std::string data =
        "0, 1590115410000\n"
        "1, 1590115420000\n"
        "2, 1590115430000\n"
        "3, 1590115440000\n"
        "4, 1590115450000\n"
        "5, 1590115460000\n"
        "6, 1590115470000\n"
        "7, 1590115480000\n"
        "8, 1590115490000\n"
        "9, 1590115500000";
    type::TableDef table_def;
    std::vector<Row> rows;
    ASSERT_TRUE(hybridse::sqlcase::SqlCase::ExtractSchema(schema, table_def));
    ASSERT_TRUE(hybridse::sqlcase::SqlCase::ExtractRows(table_def.columns(),
                                                        data, rows));
    codec::RowView row_view(table_def.columns());
    for (auto row : rows) {
        row_view.Reset(row.buf());
        table.BufferData(row_view.GetTimestampUnsafe(1), row);
    }

    const uint32_t inner_list_size = sizeof(codec::InnerRowsList<Row>);
    int8_t* inner_list_buf = reinterpret_cast<int8_t*>(alloca(inner_list_size));
    ASSERT_EQ(0, ::hybridse::codec::v1::GetInnerRowsList(
                     reinterpret_cast<int8_t*>(&table), 3, 8, inner_list_buf));
    int32_t offset = row_view.GetPrimaryFieldOffset(0);
    hybridse::type::Type type = hybridse::type::kInt32;
    const uint32_t size = sizeof(ColumnImpl<int32_t>);
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));

    ListRef<> inner_list_ref;
    inner_list_ref.list = inner_list_buf;

    for (int i = 0; i < 100000; ++i) {
        ASSERT_EQ(0, ::hybridse::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&inner_list_ref), 0, 0,
                         offset, type, buf));
        ::hybridse::codec::ColumnImpl<int32_t>* col =
            reinterpret_cast<::hybridse::codec::ColumnImpl<int32_t>*>(buf);
        auto col_iterator = col->GetIterator();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(6, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(5, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_EQ(4, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_EQ(3, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_EQ(2, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_EQ(1, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_FALSE(col_iterator->Valid());
    }
}

TEST_F(UdfTest, GetWindowColTest) {
    vm::CurrentHistoryWindow table(vm::Window::kFrameRowsRange, -2, 0);
    uint64_t ts = 1000;
    for (auto row : rows) {
        table.BufferData(ts++, row);
    }

    ListRef<> table_ref;
    table_ref.list = reinterpret_cast<int8_t*>(&table);

    const uint32_t size = sizeof(ColumnImpl<int32_t>);
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
    for (int i = 0; i < 100000; ++i) {
        ASSERT_EQ(0, ::hybridse::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&table_ref), 0, 0, 2,
                         hybridse::type::kInt32, buf));
        ::hybridse::codec::ColumnImpl<int32_t>* col =
            reinterpret_cast<::hybridse::codec::ColumnImpl<int32_t>*>(buf);
        auto col_iterator = col->GetIterator();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(111, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(11, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_EQ(1, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_FALSE(col_iterator->Valid());
    }
}
TEST_F(UdfTest, GetTimeMemColTest) {
    vm::MemTimeTableHandler table;
    uint64_t ts = 1000;
    for (auto row : rows) {
        table.AddRow(ts++, row);
    }
    ListRef<> table_ref;
    table_ref.list = reinterpret_cast<int8_t*>(&table);

    const uint32_t size = sizeof(ColumnImpl<int32_t>);
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
    for (int i = 0; i < 1000000; ++i) {
        ASSERT_EQ(0, ::hybridse::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&table_ref), 0, 0, 2,
                         hybridse::type::kInt32, buf));
        ColumnImpl<int32_t>* col = reinterpret_cast<ColumnImpl<int32_t>*>(buf);
        auto col_iterator = col->GetIterator();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(1, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(11, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(111, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_FALSE(col_iterator->Valid());
    }
}
TEST_F(UdfTest, GetColHeapTest) {
    ArrayListV<Row> impl(&rows);
    ListRef<> impl_ref;
    impl_ref.list = reinterpret_cast<int8_t*>(&impl);

    const uint32_t size = sizeof(ColumnImpl<int16_t>);
    for (int i = 0; i < 1000; ++i) {
        int8_t buf[size];  // NOLINT
        ::hybridse::codec::ListRef<> list_ref;
        list_ref.list = buf;
        ASSERT_EQ(0, ::hybridse::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&impl_ref), 0, 0, 2,
                         hybridse::type::kInt32, buf));
        ::hybridse::codec::ColumnImpl<int16_t>* impl =
            reinterpret_cast<::hybridse::codec::ColumnImpl<int16_t>*>(
                list_ref.list);
        auto iter = impl->GetIterator();
        ASSERT_TRUE(iter->Valid());
        iter->Next();
        ASSERT_TRUE(iter->Valid());
        iter->Next();
        ASSERT_TRUE(iter->Valid());
        iter->Next();
        ASSERT_FALSE(iter->Valid());
    }
}

TEST_F(UdfTest, DateToString) {
    {
        codec::StringRef str;
        Date date(2020, 5, 22);
        udf::v1::date_to_string(&date, &str);
        ASSERT_EQ(codec::StringRef("2020-05-22"), str);
    }
}

TEST_F(UdfTest, TimestampToString) {
    {
        codec::StringRef str;
        Timestamp ts(1590115420000L);
        udf::v1::timestamp_to_string(&ts, &str);
        ASSERT_EQ(codec::StringRef("2020-05-22 10:43:40"), str);
    }
    {
        codec::StringRef str;
        Timestamp ts(1590115421000L);
        udf::v1::timestamp_to_string(&ts, &str);
        ASSERT_EQ(codec::StringRef("2020-05-22 10:43:41"), str);
    }
}

template <class Ret, class... Args>
void CheckUdf(UdfLibrary* library, const std::string& name, Ret&& expect,
              Args&&... args) {
    auto function = udf::UdfFunctionBuilder(name)
                        .args<Args...>()
                        .template returns<Ret>()
                        .library(library)
                        .build();
    ASSERT_TRUE(function.valid());
    auto result = function(std::forward<Args>(args)...);
    ASSERT_EQ(std::forward<Ret>(expect), result);
}

class ExternUdfTest : public ::testing::Test {
 public:
    static int32_t IfNull(int32_t in, bool is_null, int32_t default_val) {
        return is_null ? default_val : in;
    }

    static int32_t AddOne(int32_t in) { return in + 1; }

    static int32_t AddTwo(int32_t x, int32_t y) { return x + y; }

    static int32_t AddTwoOneNullable(int32_t x, bool x_is_null, int32_t y) {
        return x_is_null ? y : x + y;
    }

    static void IfStringNull(codec::StringRef* in, bool is_null,
                             codec::StringRef* default_val,
                             codec::StringRef* output) {
        *output = is_null ? *default_val : *in;
    }

    static void NewDate(int64_t in, bool is_null, Date* out,
                        bool* is_null_addr) {
        *is_null_addr = is_null;
        if (!is_null) {
            out->date_ = in;
        }
    }

    static double SumTuple(float x1, bool x1_is_null, float x2, double x3,
                           double x4, bool x4_is_null) {
        double res = 0;
        if (!x1_is_null) {
            res += x1;
        }
        res += x2;
        res += x3;
        if (!x4_is_null) {
            res += x4;
        }
        return res;
    }

    static void MakeTuple(int16_t x, int32_t y, bool y_is_null, int64_t z,
                          int16_t* t1, int32_t* t2, bool* t2_is_null,
                          int64_t* t3) {
        *t1 = x;
        *t2 = y;
        *t2_is_null = y_is_null;
        *t3 = z;
    }
};

TEST_F(ExternUdfTest, TestCompoundTypedExternalCall) {
    UdfLibrary library;
    library.RegisterExternal("if_null")
        .args<Nullable<int32_t>, int32_t>(ExternUdfTest::IfNull)
        .args<Nullable<codec::StringRef>, codec::StringRef>(
            ExternUdfTest::IfStringNull);

    library.RegisterExternal("add_one").args<int32_t>(ExternUdfTest::AddOne);

    library.RegisterExternal("add_two").args<int32_t, int32_t>(
        ExternUdfTest::AddTwo);

    library.RegisterExternal("add_two_one_nullable")
        .args<Nullable<int32_t>, int32_t>(ExternUdfTest::AddTwoOneNullable);

    library.RegisterExternal("new_date")
        .args<Nullable<int64_t>>(
            TypeAnnotatedFuncPtrImpl<std::tuple<Nullable<int64_t>>>::RBA<
                Nullable<Date>>(ExternUdfTest::NewDate));

    library.RegisterExternal("sum_tuple")
        .args<Tuple<Nullable<float>, float>, Tuple<double, Nullable<double>>>(
            ExternUdfTest::SumTuple)
        .args<Tuple<Nullable<float>, Tuple<float, double, Nullable<double>>>>(
            ExternUdfTest::SumTuple);

    library.RegisterExternal("make_tuple")
        .args<int16_t, Nullable<int32_t>, int64_t>(
            TypeAnnotatedFuncPtrImpl<
                std::tuple<int16_t, Nullable<int32_t>, int64_t>>::
                RBA<Tuple<int16_t, Nullable<int32_t>, int64_t>>(
                    ExternUdfTest::MakeTuple));

    // pass null to primitive
    CheckUdf<int32_t, Nullable<int32_t>, int32_t>(&library, "if_null", 1, 1, 3);
    CheckUdf<int32_t, Nullable<int32_t>, int32_t>(&library, "if_null", 3,
                                                  nullptr, 3);

    // pass null to struct
    CheckUdf<codec::StringRef, Nullable<codec::StringRef>, codec::StringRef>(
        &library, "if_null", codec::StringRef("1"), codec::StringRef("1"),
        codec::StringRef("3"));
    CheckUdf<codec::StringRef, Nullable<codec::StringRef>, codec::StringRef>(
        &library, "if_null", codec::StringRef("3"), nullptr,
        codec::StringRef("3"));

    // pass null to non-null arg
    CheckUdf<Nullable<int32_t>, int32_t>(&library, "add_one", 2, 1);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>>(&library, "add_one", 2, 1);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>>(&library, "add_one", nullptr,
                                                   nullptr);

    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        &library, "add_two", nullptr, 1, nullptr);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        &library, "add_two_one_nullable", 3, 2, 1);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        &library, "add_two_one_nullable", nullptr, 1, nullptr);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        &library, "add_two_one_nullable", 1, nullptr, 1);
    CheckUdf<Nullable<int32_t>, Nullable<int32_t>, Nullable<int32_t>>(
        &library, "add_two_one_nullable", nullptr, nullptr, nullptr);

    // nullable return
    CheckUdf<Nullable<Date>, Nullable<int64_t>>(&library, "new_date",
                                                       Date(1), 1);
    CheckUdf<Nullable<Date>, Nullable<int64_t>>(&library, "new_date",
                                                       nullptr, nullptr);

    // pass tuple
    CheckUdf<double, Tuple<Nullable<float>, float>,
             Tuple<double, Nullable<double>>>(
        &library, "sum_tuple", 10.0, Tuple<Nullable<float>, float>(1.0f, 2.0f),
        Tuple<double, Nullable<double>>(3.0, 4.0));
    CheckUdf<double, Tuple<Nullable<float>, float>,
             Tuple<double, Nullable<double>>>(
        &library, "sum_tuple", 5.0,
        Tuple<Nullable<float>, float>(nullptr, 2.0f),
        Tuple<double, Nullable<double>>(3.0, nullptr));
    CheckUdf<Nullable<double>, Tuple<float, Nullable<float>>,
             Tuple<double, double>>(
        &library, "sum_tuple", nullptr,
        Tuple<float, Nullable<float>>(1.0f, nullptr),
        Tuple<double, double>(3.0, 4.0));

    // nested tuple
    CheckUdf<double,
             Tuple<Nullable<float>, Tuple<float, double, Nullable<double>>>>(
        &library, "sum_tuple", 10.0,
        Tuple<Nullable<float>, Tuple<float, double, Nullable<double>>>(
            1.0f, Tuple<float, double, Nullable<double>>(2.0f, 3.0, 4.0)));
    CheckUdf<double,
             Tuple<Nullable<float>, Tuple<float, double, Nullable<double>>>>(
        &library, "sum_tuple", 5.0,
        Tuple<Nullable<float>, Tuple<float, double, Nullable<double>>>(
            nullptr,
            Tuple<float, double, Nullable<double>>(2.0f, 3.0, nullptr)));
    CheckUdf<Nullable<double>,
             Tuple<float, Tuple<Nullable<float>, double, double>>>(
        &library, "sum_tuple", nullptr,
        Tuple<float, Tuple<Nullable<float>, double, double>>(
            1.0f, Tuple<Nullable<float>, double, double>(nullptr, 3.0, 4.0)));

    // return tuple
    using TupleResT = Tuple<int16_t, Nullable<int32_t>, int64_t>;
    CheckUdf<TupleResT, int16_t, Nullable<int32_t>, int64_t>(
        &library, "make_tuple", TupleResT(1, 2, 3), 1, 2, 3);
    CheckUdf<TupleResT, int16_t, Nullable<int32_t>, int64_t>(
        &library, "make_tuple", TupleResT(1, nullptr, 3), 1, nullptr, 3);
}

TEST_F(ExternUdfTest, LikeMatchTest) {
    auto check_like = [](bool match, bool is_null, const std::string_view name, const std::string_view pattern,
                         const std::string_view esc) -> void {
        codec::StringRef name_ref(name.size(), name.data());
        codec::StringRef pattern_ref(pattern.size(), pattern.data());
        codec::StringRef escape_ref(esc.size(), esc.data());
        bool ret = false;
        bool ret_null = false;
        v1::like(&name_ref, &pattern_ref, &escape_ref, &ret, &ret_null);
        EXPECT_EQ(match, ret) << "like(" << name << ", " << pattern << ", " << esc << ")";
        EXPECT_EQ(is_null, ret_null) << "like(" << name << ", " << pattern << ", " << esc << ")";

        if (esc == "\\") {
            // also check like(x, x)
            bool ret = false;
            bool ret_null = false;
            v1::like(&name_ref, &pattern_ref, &ret, &ret_null);
            EXPECT_EQ(match, ret) << "like(" << name << ", " << pattern << ")";
            EXPECT_EQ(is_null, ret_null) << "like(" << name << ", " << pattern << ")";
        }
    };
    check_like(true, false, "abc", "abc", "\\");
    check_like(true, false, "Mary", "M%", "\\");
    // case sensitive
    check_like(false, false, "Mary", "m%", "\\");
    check_like(false, false, "mary", "MARy", "\\");

    check_like(true, false, "Mary", "M_ry", "\\");
    check_like(false, false, "Mary", "m_ry", "\\");

    // match empty
    check_like(true, false, "", "", "\\");
    check_like(false, false, "abc", "", "\\");
    check_like(false, false, "", "abc", "\\");

    // escape character
    check_like(true, false, "Evan_W", R"r(%\_%)r", "\\");
    check_like(false, false, "EvansW", R"r(%\_%)r", "\\");
    check_like(true, false, "Evan_W", "%$_%", "$");
    check_like(false, false, "EvansW", "%$_%", "$");
    check_like(true, false, "Evan%w", "Eva_p%w", "p");
    check_like(false, false, "Evan%W", "eva_p%w", "P");

    // not match
    check_like(false, false, "Mike", "mary", "\\");
    check_like(false, false, "Evan_W", "%_x", "\\");
    // it is exact match
    check_like(false, false, "Evan_W", "Evan", "\\");
    check_like(false, false, "Super", "SupberGlob", "\\");

    // disable escape by provide empty string, (_) and (%) always has special meaning
    // this is raw string so double backslash can avoided
    check_like(true, false, R"r(Evan\%w)r", R"r(Evan\%w)r", "");
    check_like(false, false, R"r(Evan%w)r", R"r(Evan\%w)r", "");
    check_like(true, false, R"r(Evan%w)r", R"r(Evan\%w)r", "\\");
    check_like(false, false, R"r(Evan%aw)r", R"r(Evan\%w)r", "\\");
    check_like(true, false, R"r(Evan\kkbw)r", R"r(Evan\%w)r", "");
    check_like(true, false, R"r(Evan\no\sw)r", R"r(Evan\%\_w)r", "");

    // special check if pattern end with special character <percent>
    check_like(true, false, "a_b", "a%b%", "\\");
    check_like(true, false, "a_b", "a%b%%", "\\");
    check_like(true, false, "a_b", "a_b%%%", "\\");
    // escape <percent>, it is not special character any more
    check_like(false, false, "a_b", "a_b\\%", "\\");
    check_like(false, false, "a_b", "a_b%%", "%");
    // NOTE: it is a invalid pattern sequence here: end with escape character
    //  currently we return false, but might change to throw exception in the further
    check_like(false, false, "a_b", "a_b%", "%");

    // invalid match pattern
    check_like(false, false, R"r(Evan_w\)r", R"r(Evan_w\)r", "\\");
}

TEST_F(ExternUdfTest, LikeMatchNullable) {
    auto check_null = [](bool expect, bool is_null, codec::StringRef* name_ref, codec::StringRef* pattern_ref,
                         codec::StringRef* escape) -> void {
        bool ret = false;
        bool ret_null = false;
        v1::like(name_ref, pattern_ref, escape, &ret, &ret_null);
        EXPECT_EQ(is_null, ret_null) << "'" << (name_ref ? name_ref->ToString() : "<null>") << "' LIKE '"
                                     << (pattern_ref ? pattern_ref->ToString() : "<null>") << "' ESCAPE '"
                                     << (escape ? escape->ToString() : "<null>") << "'";
        if (!is_null) {
            EXPECT_EQ(expect, ret) << "'" << (name_ref ? name_ref->ToString() : "<null>") << "' LIKE '"
                                   << (pattern_ref ? pattern_ref->ToString() : "<null>") << "' ESCAPE '"
                                   << (escape ? escape->ToString() : "<null>") << "'";
        }
    };
    char escape = '\\';
    codec::StringRef escape_ref(1, &escape);
    codec::StringRef name(R"s(Ma_y)s");
    codec::StringRef pattern(R"s(Ma\_y)s");

    // any of name or pattern is null, return null
    check_null(false, true, nullptr, &pattern, &escape_ref);
    check_null(false, true, &name, nullptr, &escape_ref);
    check_null(false, true, nullptr, nullptr, &escape_ref);
    check_null(false, true, nullptr, &pattern, nullptr);
    check_null(false, true, &name, nullptr, nullptr);
    check_null(false, true, nullptr, nullptr, nullptr);
    check_null(false, true, &name, &pattern, nullptr);
    check_null(true, false, &name, &pattern, &escape_ref);
}

TEST_F(ExternUdfTest, ILikeMatchTest) {
    auto check_ilike = [](bool match, bool is_null, const std::string_view name, const std::string_view pattern,
                         const std::string_view esc) -> void {
        codec::StringRef name_ref(name.size(), name.data());
        codec::StringRef pattern_ref(pattern.size(), pattern.data());
        codec::StringRef escape_ref(esc.size(), esc.data());
        bool ret = false;
        bool ret_null = false;
        v1::ilike(&name_ref, &pattern_ref, &escape_ref, &ret, &ret_null);
        EXPECT_EQ(match, ret) << "like(" << name << ", " << pattern << ", " << esc << ")";
        EXPECT_EQ(is_null, ret_null) << "like(" << name << ", " << pattern << ", " << esc << ")";

        if (esc == "\\") {
            // also check like(x, x)
            bool ret = false;
            bool ret_null = false;
            v1::ilike(&name_ref, &pattern_ref, &ret, &ret_null);
            EXPECT_EQ(match, ret) << "like(" << name << ", " << pattern << ")";
            EXPECT_EQ(is_null, ret_null) << "like(" << name << ", " << pattern << ")";
        }
    };
    check_ilike(true, false, "abc", "abc", "\\");
    check_ilike(true, false, "Mary", "M%", "\\");
    // case insensitive
    check_ilike(true, false, "Mary", "m%", "\\");
    check_ilike(true, false, "mary", "MARy", "\\");

    check_ilike(true, false, "Mary", "M_ry", "\\");
    check_ilike(true, false, "Mary", "m_ry", "\\");

    // match empty
    check_ilike(true, false, "", "", "\\");
    check_ilike(false, false, "abc", "", "\\");
    check_ilike(false, false, "", "abc", "\\");

    // escape character
    check_ilike(true, false, "Evan_W", R"r(%\_%)r", "\\");
    check_ilike(false, false, "EvansW", R"r(%\_%)r", "\\");
    check_ilike(true, false, "Evan_W", "%$_%", "$");
    check_ilike(false, false, "EvansW", "%$_%", "$");
    check_ilike(true, false, "Evan%W", "eva_p%w", "p");
    check_ilike(false, false, "Evan%W", "eva_P%w", "p");  // escape character do not ignore case
    check_ilike(false, false, "Evan%W", "eva_p%w", "P");  // escape character do not ignore case

    // not match
    check_ilike(false, false, "Mike", "mary", "\\");
    check_ilike(false, false, "Evan_W", "%_x", "\\");
    // it is exact match
    check_ilike(false, false, "Evan_W", "evan", "\\");
    check_ilike(false, false, "Super", "supberGlob", "\\");

    // disable escape by provide empty string, (_) and (%) always has special meaning
    // this is raw string so double backslash can avoided
    check_ilike(true, false, R"r(Evan\%w)r", R"r(evan\%w)r", "");
    check_ilike(false, false, R"r(Evan%w)r", R"r(evan\%w)r", "");
    check_ilike(true, false, R"r(Evan%w)r", R"r(evan\%w)r", "\\");
    check_ilike(false, false, R"r(Evan%aw)r", R"r(evan\%w)r", "\\");
    check_ilike(true, false, R"r(Evan\kkbw)r", R"r(evan\%w)r", "");
    check_ilike(true, false, R"r(Evan\no\sw)r", R"r(evan\%\_w)r", "");

    // special check if pattern end with special character <percent>
    check_ilike(true, false, "A_b", "a%b%", "\\");
    check_ilike(true, false, "A_b", "a%b%%", "\\");
    check_ilike(true, false, "a_b", "a_b%%%", "\\");
    // escape <percent>, it is not special character any more
    check_ilike(false, false, "A_b", "a_b\\%", "\\");
    check_ilike(false, false, "A_b", "a_b%%", "%");
    // NOTE: it is a invalid pattern sequence here: end with escape character
    //  currently we return false, but might change to throw exception in the further
    check_ilike(false, false, "A_b", "a_b%", "%");

    // might better to throw a exception
    // invalid match pattern
    check_ilike(false, false, R"r(Evan_w\)r", R"r(evan_w\)r", "\\");
    // invalid escape sequence
    check_ilike(false, false, "Mary", "M_ry", "ab");
}

TEST_F(ExternUdfTest, ILikeMatchNullable) {
    auto check_null = [](bool expect, bool is_null, codec::StringRef* name_ref, codec::StringRef* pattern_ref,
                         codec::StringRef* escape) -> void {
        bool ret = false;
        bool ret_null = false;
        v1::ilike(name_ref, pattern_ref, escape, &ret, &ret_null);
        EXPECT_EQ(is_null, ret_null) << (name_ref ? name_ref->ToString() : "<null>") << " LIKE "
                                     << (pattern_ref ? pattern_ref->ToString() : "<null>") << " ESCAPE "
                                     << (escape ? escape->ToString() : "<null>");
        if (!is_null) {
            EXPECT_EQ(expect, ret) << (name_ref ? name_ref->ToString() : "<null>") << " LIKE "
                                   << (pattern_ref ? pattern_ref->ToString() : "<null>") << " ESCAPE "
                                   << (escape ? escape->ToString() : "<null>");
        }
    };
    char escape = '\\';
    codec::StringRef escape_ref(1, &escape);
    codec::StringRef name(R"s(Ma_y)s");
    codec::StringRef pattern(R"s(ma\_y)s");

    check_null(false, true, nullptr, &pattern, &escape_ref);
    check_null(false, true, &name, nullptr, &escape_ref);
    check_null(false, true, nullptr, nullptr, &escape_ref);

    check_null(false, true, nullptr, &pattern, nullptr);
    check_null(false, true, &name, nullptr, nullptr);
    check_null(false, true, nullptr, nullptr, nullptr);
    check_null(false, true, &name, &pattern, nullptr);

    check_null(true, false, &name, &pattern, &escape_ref);
}

TEST_F(ExternUdfTest, RLikeMatchTest) {
    auto check_rlike = [](bool match, bool is_null, const std::string_view name, const std::string_view pattern,
                         const std::string_view flags) -> void {
        codec::StringRef name_ref(name.size(), name.data());
        codec::StringRef pattern_ref(pattern.size(), pattern.data());
        codec::StringRef flags_ref(flags.size(), flags.data());
        bool ret = false;
        bool ret_null = false;
        v1::regexp_like(&name_ref, &pattern_ref, &flags_ref, &ret, &ret_null);
        EXPECT_EQ(match, ret) << "rlike(" << name << ", " << pattern << ", " << flags << ")";
        EXPECT_EQ(is_null, ret_null) << "rlike(" << name << ", " << pattern << ", " << flags << ")";

        if (flags == "") {
            // also check regexp_like(x, x)
            bool ret = false;
            bool ret_null = false;
            v1::regexp_like(&name_ref, &pattern_ref, &ret, &ret_null);
            EXPECT_EQ(match, ret) << "rlike(" << name << ", " << pattern << ")";
            EXPECT_EQ(is_null, ret_null) << "rlike(" << name << ", " << pattern << ")";
        }
    };

    check_rlike(true, false, "The Lord of the Rings", "The Lord of the Rings", "");

    // case sensitive
    check_rlike(true, false, "The Lord of the Rings", "The L.rd .f the Rings", "");
    check_rlike(false, false, "The Lord of the Rings", "the L.rd .f the Rings", "");

    // match empty
    check_rlike(true, false, "", "", "");
    check_rlike(false, false, "The Lord of the Rings", "", "");
    check_rlike(false, false, "", "The Lord of the Rings", "");

    // single flag
    check_rlike(false, false, "The Lord of the Rings", "the L.rd .f the Rings", "c");
    check_rlike(true, false, "The Lord of the Rings", "the L.rd .f the Rings", "i");

    check_rlike(false, false, "The Lord of the Rings\nJ. R. R. Tolkien",
                "The Lord of the Rings.J\\. R\\. R\\. Tolkien", "");
    check_rlike(true, false, "The Lord of the Rings\nJ. R. R. Tolkien",
                "The Lord of the Rings.J\\. R\\. R\\. Tolkien", "s");

    check_rlike(false, false, "The Lord of the Rings\nJ. R. R. Tolkien",
                "^The Lord of the Rings$\nJ\\. R\\. R\\. Tolkien", "");
    check_rlike(true, false, "The Lord of the Rings\nJ. R. R. Tolkien",
                "^The Lord of the Rings$\nJ\\. R\\. R\\. Tolkien", "m");

    // multiple flags
    check_rlike(true, false, "The Lord of the Rings\nJ. R. R. Tolkien",
                "^the Lord of the Rings$.J\\. R\\. R\\. Tolkien", "mis");
}

TEST_F(ExternUdfTest, RLikeMatchNullable) {
    auto check_null = [](bool expect, bool is_null, codec::StringRef* name_ref, codec::StringRef* pattern_ref,
                         codec::StringRef* flags) -> void {
        bool ret = false;
        bool ret_null = false;
        v1::regexp_like(name_ref, pattern_ref, flags, &ret, &ret_null);
        EXPECT_EQ(is_null, ret_null) << (name_ref ? name_ref->ToString() : "<null>") << " RLIKE "
                                     << (pattern_ref ? pattern_ref->ToString() : "<null>") << " FLAGS "
                                     << (flags ? flags->ToString() : "<null>");
        if (!is_null) {
            EXPECT_EQ(expect, ret) << (name_ref ? name_ref->ToString() : "<null>") << " RLIKE "
                                   << (pattern_ref ? pattern_ref->ToString() : "<null>") << " FLAGS "
                                   << (flags ? flags->ToString() : "<null>");
        }
    };
    char flags[] = "";
    codec::StringRef flags_ref(flags);
    codec::StringRef name("The Lord of the Rings");
    codec::StringRef pattern("The Lord .f the Rings");

    check_null(false, true, nullptr, &pattern, &flags_ref);
    check_null(false, true, &name, nullptr, &flags_ref);
    check_null(false, true, nullptr, nullptr, &flags_ref);

    check_null(false, true, nullptr, &pattern, nullptr);
    check_null(false, true, &name, nullptr, nullptr);
    check_null(false, true, nullptr, nullptr, nullptr);
    check_null(false, true, &name, &pattern, nullptr);

    check_null(true, false, &name, &pattern, &flags_ref);
}

TEST_F(ExternUdfTest, Replace) {
    auto check = [](bool is_null, StringRef expect, StringRef str, StringRef search, StringRef replace) {
        StringRef out;
        bool out_null = false;
        auto expr_str = absl::Substitute("replace($0, $1, $2) != $3", str.DebugString(), search.DebugString(),
                                         replace.DebugString(), expect.DebugString());
        v1::replace(&str, &search, &replace, &out, &out_null);
        EXPECT_EQ(is_null, out_null) << expr_str;
        if (!is_null) {
            EXPECT_EQ(expect, out) << expr_str;
        }
    };

    // replace one
    check(false, "ABCDEF", "ABCabc", "abc", "DEF");
    // replace nothing
    check(false, "ABCabc", "ABCabc", "def", "DEF");
    // replace multiple chars
    check(false, "AABACA", "AaBaCa", "a", "A");
    // replace multiple words
    check(false, "Hello Bob Hi Bob Be", "Hello Ben Hi Ben Be", "Ben", "Bob");

    // empty replace removes the search
    // replace one
    check(false, "ABC", "ABCabc", "abc", "");
    // replace nothing
    check(false, "ABCabc", "ABCabc", "def", "");
    // replace multiple chars
    check(false, "ABC", "AaBaCa", "a", "");
    // replace multiple words
    check(false, "Hello  Hi  Be", "Hello Ben Hi Ben Be", "Ben", "");
}

TEST_F(ExternUdfTest, ReplaceWithoutReplaceStr) {
    auto check = [](bool is_null, StringRef expect, StringRef str, StringRef search) {
        StringRef out;
        bool out_null = false;
        auto expr_str =
            absl::Substitute("replace($0, $1) != $2", str.DebugString(), search.DebugString(), expect.DebugString());
        v1::replace(&str, &search, &out, &out_null);
        EXPECT_EQ(is_null, out_null) << expr_str;
        if (!is_null) {
            EXPECT_EQ(expect, out) << expr_str;
        }
    };

    // replace one
    check(false, "ABC", "ABCabc", "abc");
    // replace nothing
    check(false, "ABCabc", "ABCabc", "def");
    // replace multiple chars
    check(false, "ABC", "AaBaCa", "a");
    // replace multiple words
    check(false, "Hello  Hi  Be", "Hello Ben Hi Ben Be", "Ben");
}

TEST_F(ExternUdfTest, ReplaceNullable) {
    auto debug_str = [](StringRef* ref) -> std::string { return ref == nullptr ? "NULL" : ref->DebugString(); };
    auto check3 = [&debug_str](bool is_null, StringRef expect, StringRef* str, StringRef* search, StringRef* replace) {
        StringRef out;
        bool out_null = false;
        auto expr_str = absl::Substitute("replace($0, $1, $2) != $3", debug_str(str), debug_str(search),
                                         debug_str(replace), expect.DebugString());
        v1::replace(str, search, replace, &out, &out_null);
        EXPECT_EQ(is_null, out_null) << expr_str;
        if (!is_null) {
            EXPECT_EQ(expect, out) << expr_str;
        }
    };

    auto check2 = [&debug_str](bool is_null, StringRef expect, StringRef* str, StringRef* search) {
        StringRef out;
        bool out_null = false;
        auto expr_str =
            absl::Substitute("replace($0, $1) != $2", debug_str(str), debug_str(search), expect.DebugString());
        v1::replace(str, search, &out, &out_null);
        EXPECT_EQ(is_null, out_null) << expr_str;
        if (!is_null) {
            EXPECT_EQ(expect, out) << expr_str;
        }
    };

    StringRef str("ABCabc");
    StringRef search("abc");
    StringRef replace("ABC");

    check3(true, nullptr, nullptr, &search, &replace);
    check3(true, nullptr, &str, nullptr, &replace);
    check3(true, nullptr, &str, &search, nullptr);
    check3(true, nullptr, &str, nullptr, nullptr);
    check3(true, nullptr, nullptr, &search, nullptr);
    check3(true, nullptr, nullptr, nullptr, nullptr);

    check2(true, nullptr, &str, nullptr);
    check2(true, nullptr, nullptr, &search);
    check2(true, nullptr, nullptr, nullptr);
}

}  // namespace udf
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
