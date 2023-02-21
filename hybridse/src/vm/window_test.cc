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

#include <utility>
#include "codec/list_iterator_codec.h"
#include "gtest/gtest.h"
#include "proto/fe_type.pb.h"
#include "vm/mem_catalog.h"
#include "vm/runner.h"
namespace hybridse {
namespace vm {
using codec::ArrayListIterator;
using codec::ArrayListV;
using codec::ColumnImpl;
using codec::InnerRangeIterator;
using codec::Row;
using codec::v1::GetCol;

class WindowIteratorTest : public ::testing::Test {
 public:
    WindowIteratorTest() {}
    ~WindowIteratorTest() {}
};

TEST_F(WindowIteratorTest, ArrayListIteratorImplTest) {
    std::vector<int> int_vec({1, 2, 3, 4, 5});
    ArrayListV<int> list(&int_vec);
    auto impl = list.GetIterator();

    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(1, impl->GetValue());
    impl->Next();

    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(2, impl->GetValue());
    impl->Next();

    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(3, impl->GetValue());
    impl->Next();

    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(4, impl->GetValue());
    impl->Next();

    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(5, impl->GetValue());
    impl->Next();
    ASSERT_FALSE(impl->Valid());
}

TEST_F(WindowIteratorTest, MemTableIteratorImplTest) {
    // prepare row buf
    std::vector<Row> rows;
    MemTableHandler table;
    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
        table.AddRow(Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        table.AddRow(Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        table.AddRow(Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    auto impl = table.GetIterator();
    ASSERT_TRUE(impl->Valid());
    impl->Next();
    ASSERT_TRUE(impl->Valid());
    impl->Next();
    ASSERT_TRUE(impl->Valid());
    impl->Next();
    ASSERT_FALSE(impl->Valid());
}

TEST_F(WindowIteratorTest, MemSegmentIteratorImplTest) {
    // prepare row buf
    std::vector<Row> rows;
    MemTimeTableHandler table;
    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
        table.AddRow(1, Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        table.AddRow(2, Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        table.AddRow(3, Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    auto impl = table.GetIterator();
    ASSERT_TRUE(impl->Valid());
    impl->Next();
    ASSERT_TRUE(impl->Valid());
    impl->Next();
    ASSERT_TRUE(impl->Valid());
    impl->Next();
    ASSERT_FALSE(impl->Valid());
}

TEST_F(WindowIteratorTest, MemColumnIteratorImplTest) {
    // prepare row buf
    MemTimeTableHandler table;
    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
        table.AddRow(0, Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        table.AddRow(1, Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        table.AddRow(2, Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    auto column = new ColumnImpl<int32_t>(&table, 0, 0, 2);
    auto impl = column->GetIterator();
    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(1, impl->GetValue());
    impl->Next();
    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(11, impl->GetValue());
    impl->Next();
    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(111, impl->GetValue());
    impl->Next();
    ASSERT_FALSE(impl->Valid());
    delete (column);
}

TEST_F(WindowIteratorTest, MemGetColTest) {
    // prepare row buf
    MemTimeTableHandler table;
    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
        table.AddRow(0, Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        table.AddRow(1, Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        table.AddRow(2, Row(base::RefCountedSlice::Create(ptr, 28)));
    }

    const uint32_t size = sizeof(ColumnImpl<int32_t>);
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
    ASSERT_EQ(0, GetCol(reinterpret_cast<int8_t*>(&table), 0, 0, 2,
                        type::kInt32, buf));

    ListV<Row>* list = reinterpret_cast<ListV<Row>*>(&table);
    new (buf) ColumnImpl<int32_t>(list, 0, 0, 2);
    auto column = reinterpret_cast<ColumnImpl<int32_t>*>(buf);
    auto impl = column->GetIterator();
    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(1, impl->GetValue());
    impl->Next();
    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(11, impl->GetValue());
    impl->Next();
    ASSERT_TRUE(impl->Valid());
    ASSERT_EQ(111, impl->GetValue());
    impl->Next();
    ASSERT_FALSE(impl->Valid());
    //    delete (column);
}

TEST_F(WindowIteratorTest, CurrentHistoryWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));

    // history current_ts -1000 ~ current_ts
    {
        vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -1000L, 0);

        window.BufferData(1L, row);
        ASSERT_EQ(1u, window.GetCount());
        window.BufferData(2L, row);
        ASSERT_EQ(2u, window.GetCount());
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.GetCount());
        window.BufferData(40L, row);
        ASSERT_EQ(4u, window.GetCount());
        window.BufferData(500L, row);
        ASSERT_EQ(5u, window.GetCount());
        window.BufferData(1000L, row);
        ASSERT_EQ(6u, window.GetCount());
        window.BufferData(1001L, row);
        ASSERT_EQ(7u, window.GetCount());
        window.BufferData(1002L, row);
        ASSERT_EQ(7u, window.GetCount());
        window.BufferData(1003L, row);
        ASSERT_EQ(7u, window.GetCount());
        window.BufferData(1004L, row);
        ASSERT_EQ(7u, window.GetCount());
        window.BufferData(1005L, row);
        ASSERT_EQ(8u, window.GetCount());
        window.BufferData(1500L, row);
        ASSERT_EQ(8u, window.GetCount());
        window.BufferData(2004L, row);
        ASSERT_EQ(4u, window.GetCount());
        window.BufferData(3000L, row);
        ASSERT_EQ(2u, window.GetCount());
        window.BufferData(5000L, row);
        ASSERT_EQ(1u, window.GetCount());
        window.BufferData(6000L, row);
        ASSERT_EQ(2u, window.GetCount());
    }

    // history current_ts -1000 ~ current_ts max_size = 5
    {
        vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -1000L, 0,
                                        5);
        window.BufferData(1L, row);
        ASSERT_EQ(1u, window.GetCount());
        window.BufferData(2L, row);
        ASSERT_EQ(2u, window.GetCount());
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.GetCount());
        window.BufferData(40L, row);
        ASSERT_EQ(4u, window.GetCount());
        window.BufferData(500L, row);
        ASSERT_EQ(5u, window.GetCount());
        window.BufferData(1000L, row);
        ASSERT_EQ(5u, window.GetCount());
        window.BufferData(1001L, row);
        ASSERT_EQ(5u, window.GetCount());
        window.BufferData(1500L, row);
        ASSERT_EQ(4u, window.GetCount());
        window.BufferData(2004L, row);
        ASSERT_EQ(2u, window.GetCount());
        window.BufferData(3000L, row);
        ASSERT_EQ(2u, window.GetCount());
        window.BufferData(5000L, row);
        ASSERT_EQ(1u, window.GetCount());
        window.BufferData(6000L, row);
        ASSERT_EQ(2u, window.GetCount());
    }

    // history buffer error
    {
        vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -400L, 0,
                                        5);
        ASSERT_TRUE(window.BufferData(1L, row));
        ASSERT_EQ(1u, window.GetCount());
        ASSERT_TRUE(window.BufferData(2L, row));
        ASSERT_EQ(2u, window.GetCount());
        ASSERT_TRUE(window.BufferData(3L, row));
        ASSERT_EQ(3u, window.GetCount());
        ASSERT_TRUE(window.BufferData(400L, row));
        ASSERT_EQ(4u, window.GetCount());
        ASSERT_TRUE(window.BufferData(500L, row));
        ASSERT_EQ(2u, window.GetCount());
        ASSERT_FALSE(window.BufferData(100L, row));
    }
}

void Check_Next_N(RowIterator* iter, int n) {
    int i = 0;
    while (i++ < n) {
        ASSERT_TRUE(iter->Valid());
        iter->Next();
    }
}

void Check_Key(RowIterator* iter, uint64_t key) {
    ASSERT_TRUE(iter->Valid());
    ASSERT_EQ(key, iter->GetKey());
}

TEST_F(WindowIteratorTest, InnerRangeWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -3600000L, 0);
    window.BufferData(1590115410000, row);
    window.BufferData(1590115420000, row);
    window.BufferData(1590115430000, row);
    window.BufferData(1590115440000, row);
    window.BufferData(1590115450000, row);
    window.BufferData(1590115460000, row);
    window.BufferData(1590115470000, row);
    window.BufferData(1590115480000, row);
    window.BufferData(1590115490000, row);
    window.BufferData(1590115500000, row);

    // check w[30s:80s]
    {
        uint64_t start = 1590115500000 - 30000L;
        uint64_t end = 1590115500000 - 80000L;
        auto inner_window = std::unique_ptr<codec::InnerRangeList<Row>>(
            new codec::InnerRangeList<Row>(&window, start, end));
        auto iter = inner_window->GetIterator();
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115470000, iter->GetKey());
        Check_Next_N(iter.get(), 4);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115430000, iter->GetKey());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115420000, iter->GetKey());
        iter->Next();
        ASSERT_FALSE(iter->Valid());
    }
}
TEST_F(WindowIteratorTest, InnerRangeIteratorTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -3600000L, 0);
    window.BufferData(1590115410000, row);
    window.BufferData(1590115420000, row);
    window.BufferData(1590115430000, row);
    window.BufferData(1590115440000, row);
    window.BufferData(1590115450000, row);
    window.BufferData(1590115460000, row);
    window.BufferData(1590115470000, row);
    window.BufferData(1590115480000, row);
    window.BufferData(1590115490000, row);
    window.BufferData(1590115500000, row);

    // normal iterator check
    {
        auto iter = window.GetIterator();
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115500000, iter->GetKey());
        Check_Next_N(iter.get(), 9);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115410000, iter->GetKey());
    }

    // check w[30s:80s]
    {
        uint64_t start = 1590115500000 - 30000L;
        uint64_t end = 1590115500000 - 80000L;
        auto iter = std::unique_ptr<RowIterator>(
            new InnerRangeIterator<Row>(&window, start, end));
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115470000, iter->GetKey());
        Check_Next_N(iter.get(), 4);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115430000, iter->GetKey());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115420000, iter->GetKey());
        iter->Next();
        ASSERT_FALSE(iter->Valid());
    }
}

TEST_F(WindowIteratorTest, InnerRowsWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -3600000L, 0);
    window.BufferData(1590115410000, row);
    window.BufferData(1590115420000, row);
    window.BufferData(1590115430000, row);
    window.BufferData(1590115440000, row);
    window.BufferData(1590115450000, row);
    window.BufferData(1590115460000, row);
    window.BufferData(1590115470000, row);
    window.BufferData(1590115480000, row);
    window.BufferData(1590115490000, row);
    window.BufferData(1590115500000, row);
    // check w[3:8]
    {
        uint64_t start = 3;
        uint64_t end = 8;

        auto inner_window = std::unique_ptr<codec::InnerRowsList<Row>>(
            new codec::InnerRowsList<Row>(&window, start, end));
        auto iter = inner_window->GetIterator();
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115470000, iter->GetKey());
        Check_Next_N(iter.get(), 4);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115430000, iter->GetKey());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115420000, iter->GetKey());
        iter->Next();
        ASSERT_FALSE(iter->Valid());
    }
}

TEST_F(WindowIteratorTest, InnerRowsIteratorTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -3600000L, 0);
    window.BufferData(1590115410000, row);
    window.BufferData(1590115420000, row);
    window.BufferData(1590115430000, row);
    window.BufferData(1590115440000, row);
    window.BufferData(1590115450000, row);
    window.BufferData(1590115460000, row);
    window.BufferData(1590115470000, row);
    window.BufferData(1590115480000, row);
    window.BufferData(1590115490000, row);
    window.BufferData(1590115500000, row);

    // normal iterator check
    {
        auto iter = window.GetIterator();
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115500000, iter->GetKey());
        Check_Next_N(iter.get(), 9);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115410000, iter->GetKey());
    }

    // check w[3:8]
    {
        uint64_t start = 3;
        uint64_t end = 8;
        auto iter = std::unique_ptr<RowIterator>(
            new codec::InnerRowsIterator<Row>(&window, start, end));
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115470000, iter->GetKey());
        Check_Next_N(iter.get(), 4);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115430000, iter->GetKey());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115420000, iter->GetKey());
        iter->Next();
        ASSERT_FALSE(iter->Valid());
    }
}

TEST_F(WindowIteratorTest, CurrentHistoryRowsWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    vm::CurrentHistoryWindow window(vm::Window::kFrameRowsMergeRowsRange,
                                    -20000, 9, 0);
    window.BufferData(1590115400000, row);
    window.BufferData(1590115410000, row);
    window.BufferData(1590115420000, row);
    window.BufferData(1590115430000, row);
    window.BufferData(1590115440000, row);
    window.BufferData(1590115450000, row);
    window.BufferData(1590115460000, row);
    window.BufferData(1590115470000, row);
    window.BufferData(1590115480000, row);
    window.BufferData(1590115490000, row);
    window.BufferData(1590115500000, row);

    // normal iterator check
    {
        auto iter = window.GetIterator();
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115500000, iter->GetKey());
        Check_Next_N(iter.get(), 9);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115410000, iter->GetKey());
        iter->Next();
        ASSERT_FALSE(iter->Valid());
    }

    // check w[3:8]
    {
        uint64_t start = 3;
        uint64_t end = 8;
        auto iter = std::unique_ptr<RowIterator>(
            new codec::InnerRowsIterator<Row>(&window, start, end));
        iter->SeekToFirst();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115470000, iter->GetKey());
        Check_Next_N(iter.get(), 4);
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115430000, iter->GetKey());

        iter->Next();
        ASSERT_TRUE(iter->Valid());
        ASSERT_EQ(1590115420000, iter->GetKey());
        iter->Next();
        ASSERT_FALSE(iter->Valid());
    }
}

TEST_F(WindowIteratorTest, PureHistoryWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    // Window
    // RowsRange between 3s preceding and 1s preceding MAXSIZE 2
    vm::HistoryWindow window(
        WindowRange(vm::Window::kFrameRowsRange, -3000, -1000, 0, 0));
    ASSERT_TRUE(window.BufferData(1590738990000, row));
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590738991000, row);
    ASSERT_EQ(1, window.GetCount());
    window.BufferData(1590738992000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738993000, row);
    ASSERT_EQ(3, window.GetCount());
    window.BufferData(1590738994000, row);
    ASSERT_EQ(3, window.GetCount());
    window.BufferData(1590738995000, row);
    ASSERT_EQ(3, window.GetCount());
    window.BufferData(1590738999000, row);
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590738999000, row);
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590738999000, row);
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590739000000, row);
    ASSERT_EQ(3, window.GetCount());
    window.BufferData(1590739001000, row);
    ASSERT_EQ(4, window.GetCount());
    window.BufferData(1590739001000, row);
    window.BufferData(1590115480000, row);
    window.BufferData(1590115490000, row);
    window.BufferData(1590739001000, row);
    window.BufferData(1590739002000, row);
}
TEST_F(WindowIteratorTest, PureHistoryWindowWithMaxSizeTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    // Window
    // RowsRange between 3s preceding and 1s preceding MAXSIZE 2
    vm::HistoryWindow window(
        WindowRange(vm::Window::kFrameRowsRange, -3000, -1000, 0, 2));
    ASSERT_TRUE(window.BufferData(1590738990000, row));
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590738991000, row);
    ASSERT_EQ(1, window.GetCount());
    window.BufferData(1590738992000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738993000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738994000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738995000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738999000, row);
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590739001000, row);
    window.BufferData(1590115480000, row);
    window.BufferData(1590115490000, row);
    window.BufferData(1590739001000, row);
    window.BufferData(1590739002000, row);
}

TEST_F(WindowIteratorTest, PureHistoryWindowRowsMergeRowsRangeWithMaxSizeTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    // Window
    // RowsRange between 3s preceding and 1s preceding MAXSIZE 2
    vm::HistoryWindow window(
        WindowRange(vm::Window::kFrameRowsRange, -3000, -1000, 0, 2));
    ASSERT_TRUE(window.BufferData(1590738990000, row));
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590738991000, row);
    ASSERT_EQ(1, window.GetCount());
    window.BufferData(1590738992000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738993000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738994000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738995000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738999000, row);
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590739001000, row);
    ASSERT_EQ(1, window.GetCount());
    window.BufferData(1590739002000, row);
    ASSERT_EQ(2, window.GetCount());
}

TEST_F(WindowIteratorTest, CurrentHistoryWindowExcludeCurrentTimeTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));

    // history current_ts -1000 ~ current_ts
    {
        vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -1000L, 0);
        window.set_exclude_current_time(true);

        // [1], buffer: 1
        window.BufferData(1L, row);
        ASSERT_EQ(1u, window.GetCount());

        // [1, 2], buffer: 2
        window.BufferData(2L, row);
        ASSERT_EQ(2u, window.GetCount());

        // [1, 2, 3], buffer: 3
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.GetCount());

        // [1, 2, 3] buffer: 3, 3
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.GetCount());

        // [1, 2, 3] buffer: 3, 3, 3
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.GetCount());

        // [1, 2, 3] buffer: 3, 3, 3, 3
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.GetCount());

        // [1, 2, 3, 3, 3, 3, 4] buffer: 4
        window.BufferData(4L, row);
        ASSERT_EQ(7u, window.GetCount());

        // [1, 2, 3, 3, 3, 3, 4, 40] buffer: 40
        window.BufferData(40L, row);
        ASSERT_EQ(8u, window.GetCount());

        // [1, 2, 3, 3, 3, 3, 4, 40, 500] buffer: 500
        window.BufferData(500L, row);
        ASSERT_EQ(9u, window.GetCount());

        // [1, 2, 3, 3, 3, 3, 4, 40, 500, 1000] buffer: 1000
        window.BufferData(1000L, row);
        ASSERT_EQ(10u, window.GetCount());

        // [1, 2, 3, 3, 3, 3, 4, 40, 500, 1000, 1001] buffer: 1001
        window.BufferData(1001L, row);
        ASSERT_EQ(11u, window.GetCount());

        // [3, 3, 3, 3, 4, 40, 500, 1000, 1001, 1003] buffer: 1003
        window.BufferData(1003L, row);
        ASSERT_EQ(10u, window.GetCount());

        // [4, 40, 500, 1000, 1001, 1003, 1004] buffer: 1004
        window.BufferData(1004L, row);
        ASSERT_EQ(7u, window.GetCount());
    }

    // history current_ts -1000 ~ current_ts
    {
        vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -1000L, 5);
        window.set_exclude_current_time(true);

        // [1], buffer: 1
        window.BufferData(1L, row);
        ASSERT_EQ(1u, window.GetCount());

        // [1, 2], buffer: 2
        window.BufferData(2L, row);
        ASSERT_EQ(2u, window.GetCount());

        // [1, 2, 3], buffer: 3
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.GetCount());

        // [1, 2, 3] buffer: 3, 3
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.GetCount());

        // [1, 2, 3] buffer: 3, 3, 3
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.GetCount());

        // [1, 2, 3] buffer: 3, 3, 3, 3
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.GetCount());

        // [3, 3, 3, 3, 4] buffer: 4
        window.BufferData(4L, row);
        ASSERT_EQ(5u, window.GetCount());

        // [3, 3, 3, 4, 40] buffer: 40
        window.BufferData(40L, row);
        ASSERT_EQ(5u, window.GetCount());

        // [3, 3, 4, 40, 500] buffer: 500
        window.BufferData(500L, row);
        ASSERT_EQ(5u, window.GetCount());

        // [3, 4, 40, 500, 1000] buffer: 1000
        window.BufferData(1000L, row);
        ASSERT_EQ(5u, window.GetCount());

        // [4, 40, 500, 1000, 1001] buffer: 1001
        window.BufferData(1001L, row);
        ASSERT_EQ(5u, window.GetCount());

        // [40, 500, 1000, 1001, 1003] buffer: 1003
        window.BufferData(1003L, row);
        ASSERT_EQ(5u, window.GetCount());

        // [500, 1000, 1001, 1003, 1004] buffer: 1004
        window.BufferData(1004L, row);
        ASSERT_EQ(5u, window.GetCount());
    }
    // history buffer error
    {
        vm::CurrentHistoryWindow window(vm::Window::kFrameRowsRange, -400L, 0,
                                        5);
        window.set_exclude_current_time(true);
        ASSERT_TRUE(window.BufferData(1L, row));
        ASSERT_EQ(1u, window.GetCount());
        ASSERT_TRUE(window.BufferData(2L, row));
        ASSERT_EQ(2u, window.GetCount());
        ASSERT_TRUE(window.BufferData(3L, row));
        ASSERT_EQ(3u, window.GetCount());
        ASSERT_TRUE(window.BufferData(400L, row));
        ASSERT_EQ(4u, window.GetCount());
        ASSERT_TRUE(window.BufferData(500L, row));
        ASSERT_EQ(2u, window.GetCount());
        ASSERT_FALSE(window.BufferData(100L, row));
    }
}

TEST_F(WindowIteratorTest, PureHistoryWindowExcludeCurrentTimeTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    // Window
    // RowsRange between 3s preceding and 1s preceding MAXSIZE 2
    vm::HistoryWindow window(
        WindowRange(vm::Window::kFrameRowsRange, -3000, -1000, 0, 0));
    window.set_exclude_current_time(true);
    ASSERT_TRUE(window.BufferData(1590738990000, row));
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590738991000, row);
    ASSERT_EQ(1, window.GetCount());
    window.BufferData(1590738992000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738993000, row);
    ASSERT_EQ(3, window.GetCount());
    window.BufferData(1590738994000, row);
    ASSERT_EQ(3, window.GetCount());
    window.BufferData(1590738995000, row);
    ASSERT_EQ(3, window.GetCount());

    window.BufferData(1590738999000, row);
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590738999000, row);
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590738999000, row);
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590739000000, row);
    ASSERT_EQ(3, window.GetCount());
    window.BufferData(1590739001000, row);
    ASSERT_EQ(4, window.GetCount());
    window.BufferData(1590115480000, row);
    window.BufferData(1590115490000, row);
    window.BufferData(1590739001000, row);
    window.BufferData(1590739002000, row);
}
TEST_F(WindowIteratorTest, PureHistoryWindowWithMaxSizeExcludeCurrentTimeTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    // Window
    // RowsRange between 3s preceding and 1s preceding MAXSIZE 2
    vm::HistoryWindow window(
        WindowRange(vm::Window::kFrameRowsRange, -3000, -1000, 0, 2));
    window.set_exclude_current_time(true);
    ASSERT_TRUE(window.BufferData(1590738990000, row));
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590738991000, row);
    ASSERT_EQ(1, window.GetCount());
    window.BufferData(1590738992000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738993000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738994000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738995000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738999000, row);
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590739001000, row);
    window.BufferData(1590115480000, row);
    window.BufferData(1590115490000, row);
    window.BufferData(1590739001000, row);
    window.BufferData(1590739002000, row);
}

TEST_F(WindowIteratorTest,
       PureHistoryWindowRowsMergeRowsRangeWithMaxSizeExcludeCurrentTimeTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    // Window
    // RowsRange between 3s preceding and 1s preceding MAXSIZE 2
    vm::HistoryWindow window(
        WindowRange(vm::Window::kFrameRowsRange, -3000, -1000, 0, 2));
    window.set_exclude_current_time(true);
    ASSERT_TRUE(window.BufferData(1590738990000, row));
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590738991000, row);
    ASSERT_EQ(1, window.GetCount());
    window.BufferData(1590738992000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738993000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738994000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738995000, row);
    ASSERT_EQ(2, window.GetCount());
    window.BufferData(1590738999000, row);
    ASSERT_EQ(0, window.GetCount());
    window.BufferData(1590739001000, row);
    ASSERT_EQ(1, window.GetCount());
    window.BufferData(1590739002000, row);
    ASSERT_EQ(2, window.GetCount());
}
TEST_F(WindowIteratorTest,
       CurrentHistoryRowsAndRowRangeWindowExcludeCurrentTimeTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));
    {
        vm::CurrentHistoryWindow window(vm::Window::kFrameRowsMergeRowsRange,
                                        -20000, 9, 0);
        window.set_exclude_current_time(false);
        window.BufferData(1590115400000, row);
        ASSERT_EQ(1L, window.GetCount());

        window.BufferData(1590115410000, row);
        ASSERT_EQ(2L, window.GetCount());
        window.BufferData(1590115410000, row);
        ASSERT_EQ(3L, window.GetCount());
        window.BufferData(1590115410000, row);
        ASSERT_EQ(4L, window.GetCount());

        window.BufferData(1590115450000, row);
        ASSERT_EQ(5L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(6L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(7L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(8L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(9L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(10L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(10L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(10L, window.GetCount());
    }

    {
        vm::CurrentHistoryWindow window(vm::Window::kFrameRowsMergeRowsRange,
                                        -20000, 9, 0);
        window.set_exclude_current_time(true);
        window.BufferData(1590115400000, row);
        ASSERT_EQ(1L, window.GetCount());
        window.BufferData(1590115410000, row);
        ASSERT_EQ(2L, window.GetCount());
        window.BufferData(1590115410000, row);
        ASSERT_EQ(2L, window.GetCount());
        window.BufferData(1590115410000, row);
        ASSERT_EQ(2L, window.GetCount());

        window.BufferData(1590115450000, row);
        ASSERT_EQ(5L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(5L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(5L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(5L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(5L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(5L, window.GetCount());
        window.BufferData(1590115450000, row);
        ASSERT_EQ(5L, window.GetCount());

        window.BufferData(1590115460000, row);
        ASSERT_EQ(10L, window.GetCount());
        window.BufferData(1590115470000, row);
        ASSERT_EQ(10L, window.GetCount());
        window.BufferData(1590115470000, row);
        ASSERT_EQ(10L, window.GetCount());
        window.BufferData(1590115470000, row);
        ASSERT_EQ(10L, window.GetCount());
        window.BufferData(1590115470000, row);
        ASSERT_EQ(10L, window.GetCount());

        window.BufferData(1590115480000, row);
        ASSERT_EQ(10L, window.GetCount());
    }
}
class RequestUnionWindowTest : public ::testing::Test {
 public:
    RequestUnionWindowTest() {}
    ~RequestUnionWindowTest() {}
};

void CHECK_TABLE_KEY(std::shared_ptr<TableHandler> table,
                     std::vector<uint64_t> keys) {
    if (!table) {
        ASSERT_TRUE(keys.empty());
        return;
    }
    auto iter = table->GetIterator();
    if (!iter) {
        ASSERT_TRUE(keys.empty());
        return;
    }

    iter->SeekToFirst();
    std::vector<uint64_t> real_keys;
    while (iter->Valid()) {
        real_keys.push_back(iter->GetKey());
        iter->Next();
    }
    ASSERT_EQ(keys, real_keys);
}
void CHECK_REQUEST_UNION_WINDOW(const WindowRange& window_range,
                                const std::vector<uint64_t>& buffered_keys,
                                uint64_t current_key,
                                const std::vector<uint64_t>& exp_keys,
                                const bool exclude_current_time = false) {
    Row row;
    auto table = std::make_shared<MemTimeTableHandler>();
    for (uint64_t key : buffered_keys) {
        table->AddRow(key, row);
    }
    auto union_table =
        RequestUnionRunner::RequestUnionWindow(row, std::vector<std::shared_ptr<TableHandler>>({table}), current_key,
                                               window_range, true, exclude_current_time);
    CHECK_TABLE_KEY(union_table, exp_keys);
}
void CHECK_BUFFER_WINDOW(const WindowRange& window_range,
                         const std::vector<uint64_t>& keys_for_buffer,
                         uint64_t current_key,
                         const std::vector<uint64_t>& exp_keys,
                         const bool exclude_current_time = false) {
    std::shared_ptr<HistoryWindow> history_window =
        std::make_shared<HistoryWindow>(window_range);
    history_window->set_exclude_current_time(exclude_current_time);

    for (auto iter = keys_for_buffer.rbegin(); iter != keys_for_buffer.rend();
         iter++) {
        // Skip buffering future key-row
        if (*iter > current_key) {
            break;
        }
        history_window->BufferData(*iter, Row());
    }
    ASSERT_TRUE(history_window->BufferData(current_key, Row()));
    CHECK_TABLE_KEY(history_window, exp_keys);
}
TEST_F(RequestUnionWindowTest, RequestRowsWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));

    auto table = std::make_shared<MemTimeTableHandler>();
    std::vector<uint64_t> keys({10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L});
    for (uint64_t key : keys) {
        table->AddRow(key, row);
    }

    {
        // current row: 11
        WindowRange window_range = WindowRange::CreateRowsWindow(5);
        uint64_t current_key = 11L;
        std::vector<uint64_t> exp_keys({11L, 10L, 9L, 8L, 7L, 6L});

        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys));
        ASSERT_NO_FATAL_FAILURE(
            CHECK_BUFFER_WINDOW(window_range, keys, current_key, exp_keys));
    }

    {
        // current row: 8L
        WindowRange window_range = WindowRange::CreateRowsWindow(5);
        uint64_t current_key = 8L;
        std::vector<uint64_t> exp_keys({8L, 8L, 7L, 6L, 5L, 4L});

        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys));
        ASSERT_NO_FATAL_FAILURE(
            CHECK_BUFFER_WINDOW(window_range, keys, current_key, exp_keys));
    }

    {
        // current row: 3L
        WindowRange window_range = WindowRange::CreateRowsWindow(5);
        uint64_t current_key = 3L;
        std::vector<uint64_t> exp_keys({3L, 3L, 2L});
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys));
        ASSERT_NO_FATAL_FAILURE(
            CHECK_BUFFER_WINDOW(window_range, keys, current_key, exp_keys));
    }

    {
        // current row: 1L
        WindowRange window_range = WindowRange::CreateRowsWindow(5);
        uint64_t current_key = 1L;
        std::vector<uint64_t> exp_keys({1L});
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys));
        ASSERT_NO_FATAL_FAILURE(
            CHECK_BUFFER_WINDOW(window_range, keys, current_key, exp_keys));
    }
}
TEST_F(RequestUnionWindowTest, RequestRowsRangeWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));

    auto table = std::make_shared<MemTimeTableHandler>();
    std::vector<uint64_t> keys({10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L});
    for (uint64_t key : keys) {
        table->AddRow(key, row);
    }

    {
        // current row: 11
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0);
        uint64_t current_key = 11L;
        std::vector<uint64_t> exp_keys({11L, 10L, 9L, 8L, 7L, 6L});
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys));
        ASSERT_NO_FATAL_FAILURE(
            CHECK_BUFFER_WINDOW(window_range, keys, current_key, exp_keys));
    }
    {
        // current row: 11 exclude current time
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0);
        uint64_t current_key = 11L;
        std::vector<uint64_t> exp_keys({11L, 10L, 9L, 8L, 7L, 6L});
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys));
        ASSERT_NO_FATAL_FAILURE(
            CHECK_BUFFER_WINDOW(window_range, keys, current_key, exp_keys));
    }

    {
        // current row: 11 maxsize 3
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0, 3);
        uint64_t current_key = 11L;
        std::vector<uint64_t> exp_keys({11L, 10L, 9L});
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys));
        ASSERT_NO_FATAL_FAILURE(
            CHECK_BUFFER_WINDOW(window_range, keys, current_key, exp_keys));
    }
    {
        // current row: 11 maxsize 3 exclude current time
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0, 3);
        uint64_t current_key = 11L;
        std::vector<uint64_t> exp_keys({11L, 10L, 9L});
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys));
        ASSERT_NO_FATAL_FAILURE(
            CHECK_BUFFER_WINDOW(window_range, keys, current_key, exp_keys));
    }

    {
        // current row: 8L
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0);
        uint64_t current_key = 8L;
        std::vector<uint64_t> exp_keys({8L, 8L, 7L, 6L, 5L, 4L, 3L});
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys));
        ASSERT_NO_FATAL_FAILURE(
            CHECK_BUFFER_WINDOW(window_range, keys, current_key, exp_keys));
    }
    {
        // current row: 8L maxsize 3
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0, 3);
        uint64_t current_key = 8L;
        std::vector<uint64_t> exp_keys({8L, 8L, 7L});
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys));
        ASSERT_NO_FATAL_FAILURE(
            CHECK_BUFFER_WINDOW(window_range, keys, current_key, exp_keys));
    }

    {
        // current row: 8L exclude current time
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0);
        uint64_t current_key = 8L;
        std::vector<uint64_t> exp_keys({8L, 7L, 6L, 5L, 4L, 3L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 1L
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0);
        uint64_t current_key = 1L;
        std::vector<uint64_t> exp_keys({1L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 1L exclude current time
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0);
        uint64_t current_key = 1L;
        std::vector<uint64_t> exp_keys({1L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 13L
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L, 8L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 13L maxsize
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0, 3);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 13L exclude current time
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L, 8L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 13L maxsize
        WindowRange window_range = WindowRange::CreateRowsRangeWindow(-5, 0, 3);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
}

TEST_F(RequestUnionWindowTest, RequestRowsMergeRowsRangeWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(base::RefCountedSlice::Create(ptr, 28));

    auto table = std::make_shared<MemTimeTableHandler>();
    std::vector<uint64_t> keys({10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L, 2L});
    for (uint64_t key : keys) {
        table->AddRow(key, row);
    }

    {
        // current row: 11
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 2);
        uint64_t current_key = 11L;
        std::vector<uint64_t> exp_keys({11L, 10L, 9L, 8L, 7L, 6L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 11 exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 2);
        uint64_t current_key = 11L;
        std::vector<uint64_t> exp_keys({11L, 10L, 9L, 8L, 7L, 6L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 11 maxsize 3
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 2, 3);
        uint64_t current_key = 11L;
        std::vector<uint64_t> exp_keys({11L, 10L, 9L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 11 maxsize 3 exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 2, 3);
        uint64_t current_key = 11L;
        std::vector<uint64_t> exp_keys({11L, 10L, 9L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 8L
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 2);
        uint64_t current_key = 8L;
        std::vector<uint64_t> exp_keys({8L, 8L, 7L, 6L, 5L, 4L, 3L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 8L maxsize 3
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 2, 3);
        uint64_t current_key = 8L;
        std::vector<uint64_t> exp_keys({8L, 8L, 7L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 8L exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 2);
        uint64_t current_key = 8L;
        std::vector<uint64_t> exp_keys({8L, 7L, 6L, 5L, 4L, 3L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 8L maxsize 3 exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 2, 3);
        uint64_t current_key = 8L;
        std::vector<uint64_t> exp_keys({8L, 7L, 6L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 1L
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 2);
        uint64_t current_key = 1L;
        std::vector<uint64_t> exp_keys({1L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 1L exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 2);
        uint64_t current_key = 1L;
        std::vector<uint64_t> exp_keys({1L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 13L
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 5);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L, 8L, 7L, 6L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 13L maxsize
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 5, 7);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L, 8L, 7L, 6L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 13L exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 5);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L, 8L, 7L, 6L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
    {
        // current row: 13L maxsize
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 5, 7);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L, 8L, 7L, 6L});
        const bool exclude_current_time = false;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 13L maxsize exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-5, 5, 7);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L, 8L, 7L, 6L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 13L exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-10, 5);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 13L exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-10, 5, 7);
        uint64_t current_key = 13L;
        std::vector<uint64_t> exp_keys({13L, 10L, 9L, 8L, 7L, 6L, 5L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 10L exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-7, 5);
        uint64_t current_key = 10L;
        std::vector<uint64_t> exp_keys({10L, 9L, 8L, 7L, 6L, 5L, 4L, 3L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }

    {
        // current row: 10L exclude current time
        WindowRange window_range =
            WindowRange::CreateRowsMergeRowsRangeWindow(-7, 5, 7);
        uint64_t current_key = 10L;
        std::vector<uint64_t> exp_keys({10L, 9L, 8L, 7L, 6L, 5L, 4L});
        const bool exclude_current_time = true;
        ASSERT_NO_FATAL_FAILURE(CHECK_REQUEST_UNION_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
        ASSERT_NO_FATAL_FAILURE(CHECK_BUFFER_WINDOW(
            window_range, keys, current_key, exp_keys, exclude_current_time));
    }
}
}  // namespace vm
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
