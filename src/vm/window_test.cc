/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/
#include <utility>
#include "codec/list_iterator_codec.h"
#include "gtest/gtest.h"
#include "proto/type.pb.h"
#include "vm/mem_catalog.h"
namespace fesql {
namespace vm {
using codec::ArrayListIterator;
using codec::ArrayListV;
using codec::ColumnImpl;
using codec::v1::GetCol;
using codec::Row;

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
        table.AddRow(Row(ptr, 28));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        table.AddRow(Row(ptr, 28));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        table.AddRow(Row(ptr, 28));
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
        table.AddRow(1, Row(ptr, 28));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        table.AddRow(2, Row(ptr, 28));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        table.AddRow(3, Row(ptr, 28));
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
        table.AddRow(Row(ptr, 28));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        table.AddRow(Row(ptr, 28));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        table.AddRow(Row(ptr, 28));
    }

    auto column = new ColumnImpl<int32_t>(&table, 2);
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
        table.AddRow(Row(ptr, 28));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        table.AddRow(Row(ptr, 28));
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        table.AddRow(Row(ptr, 28));
    }

    const uint32_t size = sizeof(ColumnImpl<int32_t>);
    int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
    ASSERT_EQ(0, GetCol(reinterpret_cast<int8_t*>(&table),
                                            2, type::kInt32, buf));

    ListV<Row> * list =
        reinterpret_cast<ListV<Row>*>(&table);
    new (buf) ColumnImpl<int32_t >(list, 2);
    auto column = reinterpret_cast<ColumnImpl<int32_t >*>(buf);
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
    Row row(ptr, 28);

    // history current_ts -1000 ~ current_ts
    {
        CurrentHistoryWindow window(-1000L);
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
        ASSERT_EQ(6u, window.GetCount());
        window.BufferData(1002L, row);
        ASSERT_EQ(6u, window.GetCount());
        window.BufferData(1003L, row);
        ASSERT_EQ(6u, window.GetCount());
        window.BufferData(1004L, row);
        ASSERT_EQ(7u, window.GetCount());
        window.BufferData(1005L, row);
        ASSERT_EQ(8u, window.GetCount());
        window.BufferData(1500L, row);
        ASSERT_EQ(7u, window.GetCount());
        window.BufferData(2004L, row);
        ASSERT_EQ(3u, window.GetCount());
        window.BufferData(3000L, row);
        ASSERT_EQ(2u, window.GetCount());
        window.BufferData(5000L, row);
        ASSERT_EQ(1u, window.GetCount());
        window.BufferData(6000L, row);
        ASSERT_EQ(1u, window.GetCount());
    }

    // history current_ts -1000 ~ current_ts max_size = 5
    {
        CurrentHistoryWindow window(-1000L, 5);
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
        ASSERT_EQ(3u, window.GetCount());
        window.BufferData(2004L, row);
        ASSERT_EQ(2u, window.GetCount());
        window.BufferData(3000L, row);
        ASSERT_EQ(2u, window.GetCount());
        window.BufferData(5000L, row);
        ASSERT_EQ(1u, window.GetCount());
        window.BufferData(6000L, row);
        ASSERT_EQ(1u, window.GetCount());
    }
}

TEST_F(WindowIteratorTest, CurrentHistoryUnboundWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row(ptr, 28);

    // history current_ts -1000 ~ current_ts
    CurrentHistoryUnboundWindow window;
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
    ASSERT_EQ(8u, window.GetCount());
    window.BufferData(1003L, row);
    ASSERT_EQ(9u, window.GetCount());
    window.BufferData(1004L, row);
    ASSERT_EQ(10u, window.GetCount());
    window.BufferData(1005L, row);
    ASSERT_EQ(11u, window.GetCount());
    window.BufferData(1500L, row);
    ASSERT_EQ(12u, window.GetCount());
    window.BufferData(2004L, row);
    ASSERT_EQ(13u, window.GetCount());
    window.BufferData(3000L, row);
    ASSERT_EQ(14u, window.GetCount());
    window.BufferData(5000L, row);
    ASSERT_EQ(15u, window.GetCount());
    window.BufferData(6000L, row);
    ASSERT_EQ(16u, window.GetCount());
}

}  // namespace vm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
