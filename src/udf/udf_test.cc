/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf.h"
#include <dlfcn.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include <algorithm>
#include <vector>
#include "base/fe_slice.h"
#include "codec/list_iterator_codec.h"
#include "vm/mem_catalog.h"
namespace fesql {
namespace udf {
using fesql::codec::ArrayListV;
using fesql::codec::ColumnImpl;
using fesql::codec::Row;

class UDFTest : public ::testing::Test {
 public:
    UDFTest() { InitData(); }
    ~UDFTest() {}

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

TEST_F(UDFTest, UDF_mem_table_handler_sum_test) {
    vm::MemTableHandler window;
    for (auto row : rows) {
        window.AddRow(row);
    }
    const uint32_t size = sizeof(ColumnImpl<int16_t>);
    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;

        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window), 0,
                                          2, fesql::type::kInt32, buf));
        ASSERT_EQ(1 + 11 + 111, fesql::udf::v1::sum_list<int32_t>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window), 0,
                                          2 + 4, fesql::type::kInt16, buf));
        ASSERT_EQ(2 + 22 + 222, fesql::udf::v1::sum_list<int16_t>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window), 0,
                                          2 + 4 + 2, fesql::type::kFloat, buf));
        ASSERT_EQ(3.1f + 33.1f + 333.1f, fesql::udf::v1::sum_list<float>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&window), 0, 2 + 4 + 2 + 4,
                         fesql::type::kDouble, buf));
        ASSERT_EQ(4.1 + 44.1 + 444.1, fesql::udf::v1::sum_list<double>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&window), 0,
                         2 + 4 + 2 + 4 + 8, fesql::type::kInt64, buf));
        ASSERT_EQ(5L + 55L + 555L, fesql::udf::v1::sum_list<int64_t>(col));
    }
}

TEST_F(UDFTest, UDF_mem_time_table_handler_sum_test) {
    vm::MemTimeTableHandler window;
    uint64_t ts = 1000;
    for (auto row : rows) {
        window.AddRow(ts++, row);
    }
    const uint32_t size = sizeof(ColumnImpl<int16_t>);
    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;

        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window), 0,
                                          2, fesql::type::kInt32, buf));
        ASSERT_EQ(1 + 11 + 111, fesql::udf::v1::sum_list<int32_t>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window), 0,
                                          2 + 4, fesql::type::kInt16, buf));
        ASSERT_EQ(2 + 22 + 222, fesql::udf::v1::sum_list<int16_t>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window), 0,
                                          2 + 4 + 2, fesql::type::kFloat, buf));
        ASSERT_EQ(3.1f + 33.1f + 333.1f, fesql::udf::v1::sum_list<float>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&window), 0, 2 + 4 + 2 + 4,
                         fesql::type::kDouble, buf));
        ASSERT_EQ(4.1 + 44.1 + 444.1, fesql::udf::v1::sum_list<double>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&window), 0,
                         2 + 4 + 2 + 4 + 8, fesql::type::kInt64, buf));
        ASSERT_EQ(5L + 55L + 555L, fesql::udf::v1::sum_list<int64_t>(col));
    }
}

TEST_F(UDFTest, UDF_sum_test) {
    ArrayListV<Row> window(&rows);
    const uint32_t size = sizeof(::fesql::codec::ColumnImpl<int16_t>);
    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window), 0,
                                          2, fesql::type::kInt32, buf));
        ASSERT_EQ(1 + 11 + 111, fesql::udf::v1::sum_list<int32_t>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window), 0,
                                          2 + 4, fesql::type::kInt16, buf));
        ASSERT_EQ(2 + 22 + 222, fesql::udf::v1::sum_list<int16_t>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&window), 0,
                                          2 + 4 + 2, fesql::type::kFloat, buf));
        ASSERT_EQ(3.1f + 33.1f + 333.1f, fesql::udf::v1::sum_list<float>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&window), 0, 2 + 4 + 2 + 4,
                         fesql::type::kDouble, buf));
        ASSERT_EQ(4.1 + 44.1 + 444.1, fesql::udf::v1::sum_list<double>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&window), 0,
                         2 + 4 + 2 + 4 + 8, fesql::type::kInt64, buf));
        ASSERT_EQ(5L + 55L + 555L, fesql::udf::v1::sum_list<int64_t>(col));
    }
}

TEST_F(UDFTest, UDF_max_test) {
    ArrayListV<Row> impl(&rows);
    const uint32_t size = sizeof(::fesql::codec::ColumnImpl<int16_t>);
    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 0,
                                          2, fesql::type::kInt32, buf));
        ASSERT_EQ(111, fesql::udf::v1::max_list<int32_t>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 0,
                                          2 + 4, fesql::type::kInt16, buf));
        ASSERT_EQ(222, fesql::udf::v1::max_list<int16_t>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 0,
                                          2 + 4 + 2, fesql::type::kFloat, buf));
        ASSERT_EQ(333.1f, fesql::udf::v1::max_list<float>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&impl), 0, 2 + 4 + 2 + 4,
                         fesql::type::kDouble, buf));
        ASSERT_EQ(444.1, fesql::udf::v1::max_list<double>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&impl), 0, 2 + 4 + 2 + 4 + 8,
                         fesql::type::kInt64, buf));
        ASSERT_EQ(555L, fesql::udf::v1::max_list<int64_t>(col));
    }
}

TEST_F(UDFTest, UDF_min_test) {
    ArrayListV<Row> impl(&rows);
    const uint32_t size = sizeof(ColumnImpl<int16_t>);
    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 0,
                                          2, fesql::type::kInt32, buf));
        ASSERT_EQ(1, fesql::udf::v1::min_list<int32_t>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 0,
                                          2 + 4, fesql::type::kInt16, buf));
        ASSERT_EQ(2, fesql::udf::v1::min_list<int16_t>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 0,
                                          2 + 4 + 2, fesql::type::kFloat, buf));
        ASSERT_EQ(3.1f, fesql::udf::v1::min_list<float>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&impl), 0, 2 + 4 + 2 + 4,
                         fesql::type::kDouble, buf));
        ASSERT_EQ(4.1, fesql::udf::v1::min_list<double>(col));
    }

    {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(0, ::fesql::codec::v1::GetCol(
                         reinterpret_cast<int8_t*>(&impl), 0, 2 + 4 + 2 + 4 + 8,
                         fesql::type::kInt64, buf));
        ASSERT_EQ(5L, fesql::udf::v1::min_list<int64_t>(col));
    }
}
TEST_F(UDFTest, GetColTest) {
    ArrayListV<Row> impl(&rows);
    const uint32_t size = sizeof(ColumnImpl<int16_t>);
    for (int i = 0; i < 10; ++i) {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 0,
                                          2, fesql::type::kInt32, buf));
        ::fesql::codec::ColumnImpl<int16_t>* col =
            reinterpret_cast<::fesql::codec::ColumnImpl<int16_t>*>(
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
TEST_F(UDFTest, GetWindowColTest) {
    vm::CurrentHistoryWindow table(-2);
    uint64_t ts = 1000;
    for (auto row : rows) {
        table.BufferData(ts++, row);
    }

    const uint32_t size = sizeof(ColumnImpl<int32_t>);
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
    for (int i = 0; i < 100000; ++i) {
        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&table), 0,
                                          2, fesql::type::kInt32, buf));
        ::fesql::codec::ColumnImpl<int32_t>* col =
            reinterpret_cast<::fesql::codec::ColumnImpl<int32_t>*>(buf);
        auto col_iterator = col->GetIterator();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(11, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(111, col_iterator->GetValue());
        col_iterator->Next();
        ASSERT_FALSE(col_iterator->Valid());
    }
}
TEST_F(UDFTest, GetTimeMemColTest) {
    vm::MemTimeTableHandler table;
    uint64_t ts = 1000;
    for (auto row : rows) {
        table.AddRow(ts++, row);
    }
    const uint32_t size = sizeof(ColumnImpl<int32_t>);
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
    for (int i = 0; i < 1000000; ++i) {
        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&table), 0,
                                          2, fesql::type::kInt32, buf));
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
TEST_F(UDFTest, GetColHeapTest) {
    ArrayListV<Row> impl(&rows);
    const uint32_t size = sizeof(ColumnImpl<int16_t>);
    for (int i = 0; i < 1000; ++i) {
        int8_t buf[size];  // NOLINT
        ::fesql::codec::ListRef list_ref;
        list_ref.list = buf;
        ASSERT_EQ(
            0, ::fesql::codec::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 0,
                                          2, fesql::type::kInt32, buf));
        ::fesql::codec::ColumnImpl<int16_t>* impl =
            reinterpret_cast<::fesql::codec::ColumnImpl<int16_t>*>(
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

}  // namespace udf
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
