/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/
#include "storage/window.h"
#include <utility>
#include "gtest/gtest.h"
#include "proto/type.pb.h"
namespace fesql {
namespace storage {
class WindowIteratorTest : public ::testing::Test {
 public:
    WindowIteratorTest() {}
    ~WindowIteratorTest() {}
};

TEST_F(WindowIteratorTest, IteratorImplTest) {
    std::vector<int> int_vec({1, 2, 3, 4, 5});
    ListV<int> list(&int_vec);
    IteratorImpl<int> impl(list);

    ASSERT_TRUE(impl.Valid());
    ASSERT_EQ(1, impl.Next());

    ASSERT_TRUE(impl.Valid());
    ASSERT_EQ(2, impl.Next());

    ASSERT_TRUE(impl.Valid());
    ASSERT_EQ(3, impl.Next());

    ASSERT_TRUE(impl.Valid());
    ASSERT_EQ(4, impl.Next());

    ASSERT_TRUE(impl.Valid());
    ASSERT_EQ(5, impl.Next());

    IteratorImpl<int>* subImpl = impl.range(2, 4);
    ASSERT_TRUE(subImpl->Valid());
    ASSERT_EQ(3, subImpl->Next());

    ASSERT_TRUE(subImpl->Valid());
    ASSERT_EQ(4, subImpl->Next());

    IteratorImpl<int>* subImpl2 = impl.range(0, 2);
    ASSERT_TRUE(subImpl2->Valid());
    ASSERT_EQ(1, subImpl2->Next());

    ASSERT_TRUE(subImpl2->Valid());
    ASSERT_EQ(2, subImpl2->Next());

    IteratorImpl<int>* subImpl3 = impl.range(3, 2);
    ASSERT_FALSE(subImpl3->Valid());
}

TEST_F(WindowIteratorTest, WindowIteratorImplTest) {
    // prepare row buf
    std::vector<Row> rows;
    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 2;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 3.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 4.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
        rows.push_back(Row{.buf = ptr});
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
        rows.push_back(Row{.buf = ptr});
    }

    {
        int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
        *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
        *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
        *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
        *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
        *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
        rows.push_back(Row{.buf = ptr});
    }

    ListV<Row> list(&rows);
    IteratorImpl<Row> impl(list);
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
}

TEST_F(WindowIteratorTest, CurrentHistoryWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row({.buf = ptr});

    // history current_ts -1000 ~ current_ts
    {
        CurrentHistoryWindow window(-1000L);
        window.BufferData(1L, row);
        ASSERT_EQ(1u, window.Count());
        window.BufferData(2L, row);
        ASSERT_EQ(2u, window.Count());
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.Count());
        window.BufferData(40L, row);
        ASSERT_EQ(4u, window.Count());
        window.BufferData(500L, row);
        ASSERT_EQ(5u, window.Count());
        window.BufferData(1000L, row);
        ASSERT_EQ(6u, window.Count());
        window.BufferData(1001L, row);
        ASSERT_EQ(6u, window.Count());
        window.BufferData(1002L, row);
        ASSERT_EQ(6u, window.Count());
        window.BufferData(1003L, row);
        ASSERT_EQ(6u, window.Count());
        window.BufferData(1004L, row);
        ASSERT_EQ(7u, window.Count());
        window.BufferData(1005L, row);
        ASSERT_EQ(8u, window.Count());
        window.BufferData(1500L, row);
        ASSERT_EQ(7u, window.Count());
        window.BufferData(2004L, row);
        ASSERT_EQ(3u, window.Count());
        window.BufferData(3000L, row);
        ASSERT_EQ(2u, window.Count());
        window.BufferData(5000L, row);
        ASSERT_EQ(1u, window.Count());
        window.BufferData(6000L, row);
        ASSERT_EQ(1u, window.Count());
    }

    // history current_ts -1000 ~ current_ts max_size = 5
    {
        CurrentHistoryWindow window(-1000L, 5);
        window.BufferData(1L, row);
        ASSERT_EQ(1u, window.Count());
        window.BufferData(2L, row);
        ASSERT_EQ(2u, window.Count());
        window.BufferData(3L, row);
        ASSERT_EQ(3u, window.Count());
        window.BufferData(40L, row);
        ASSERT_EQ(4u, window.Count());
        window.BufferData(500L, row);
        ASSERT_EQ(5u, window.Count());
        window.BufferData(1000L, row);
        ASSERT_EQ(5u, window.Count());
        window.BufferData(1001L, row);
        ASSERT_EQ(5u, window.Count());
        window.BufferData(1500L, row);
        ASSERT_EQ(3u, window.Count());
        window.BufferData(2004L, row);
        ASSERT_EQ(2u, window.Count());
        window.BufferData(3000L, row);
        ASSERT_EQ(2u, window.Count());
        window.BufferData(5000L, row);
        ASSERT_EQ(1u, window.Count());
        window.BufferData(6000L, row);
        ASSERT_EQ(1u, window.Count());
    }
}

TEST_F(WindowIteratorTest, CurrentHistoryUnboundWindowTest) {
    std::vector<std::pair<uint64_t, Row>> rows;
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row({.buf = ptr});

    // history current_ts -1000 ~ current_ts
    CurrentHistoryUnboundWindow window;
    window.BufferData(1L, row);
    ASSERT_EQ(1u, window.Count());
    window.BufferData(2L, row);
    ASSERT_EQ(2u, window.Count());
    window.BufferData(3L, row);
    ASSERT_EQ(3u, window.Count());
    window.BufferData(40L, row);
    ASSERT_EQ(4u, window.Count());
    window.BufferData(500L, row);
    ASSERT_EQ(5u, window.Count());
    window.BufferData(1000L, row);
    ASSERT_EQ(6u, window.Count());
    window.BufferData(1001L, row);
    ASSERT_EQ(7u, window.Count());
    window.BufferData(1002L, row);
    ASSERT_EQ(8u, window.Count());
    window.BufferData(1003L, row);
    ASSERT_EQ(9u, window.Count());
    window.BufferData(1004L, row);
    ASSERT_EQ(10u, window.Count());
    window.BufferData(1005L, row);
    ASSERT_EQ(11u, window.Count());
    window.BufferData(1500L, row);
    ASSERT_EQ(12u, window.Count());
    window.BufferData(2004L, row);
    ASSERT_EQ(13u, window.Count());
    window.BufferData(3000L, row);
    ASSERT_EQ(14u, window.Count());
    window.BufferData(5000L, row);
    ASSERT_EQ(15u, window.Count());
    window.BufferData(6000L, row);
    ASSERT_EQ(16u, window.Count());
}

TEST_F(WindowIteratorTest, CurrentHistorySlideWindowTest) {
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row({.buf = ptr});

//     history current_ts -1000 ~ current_ts
    {
        std::vector<Row> rows;
        std::vector<uint64_t> keys;
        rows.push_back(row);
        keys.push_back(1L);
        rows.push_back(row);
        keys.push_back(2L);
        rows.push_back(row);
        keys.push_back(3L);
        rows.push_back(row);
        keys.push_back(40L);
        rows.push_back(row);
        keys.push_back(500L);
        rows.push_back(row);
        keys.push_back(1000L);
        rows.push_back(row);
        keys.push_back(1001L);
        rows.push_back(row);
        keys.push_back(1002L);
        rows.push_back(row);
        keys.push_back(1003L);
        rows.push_back(row);
        keys.push_back(1004L);
        rows.push_back(row);
        keys.push_back(1005L);
        rows.push_back(row);
        keys.push_back(1500L);
        rows.push_back(row);
        keys.push_back(2004L);
        rows.push_back(row);
        keys.push_back(3000L);
        rows.push_back(row);
        keys.push_back(5000L);
        rows.push_back(row);
        keys.push_back(6000L);
        ASSERT_EQ(rows.size(), keys.size());
        CurrentHistorySlideWindow window(-1000L, &rows, &keys, 0);
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(1u, window.Count());

        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(2u, window.Count());

        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(3u, window.Count());

        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(4u, window.Count());

        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(5u, window.Count());

        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(6u, window.Count());

        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(6u, window.Count());

        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(6u, window.Count());

        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(6u, window.Count());

        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(7u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(8u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(7u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(3u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(2u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(1u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(1u, window.Count());
        ASSERT_FALSE(window.Slide());
    }

    // history current_ts -1000 ~ current_ts max_size = 5
    {
        std::vector<Row> rows;
        std::vector<uint64_t> keys;
        rows.push_back(row);
        keys.push_back(1L);
        rows.push_back(row);
        keys.push_back(2L);
        rows.push_back(row);
        keys.push_back(3L);
        rows.push_back(row);
        keys.push_back(40L);
        rows.push_back(row);
        keys.push_back(500L);
        rows.push_back(row);
        keys.push_back(1000L);
        rows.push_back(row);
        keys.push_back(1001L);
        rows.push_back(row);
        keys.push_back(1500L);
        rows.push_back(row);
        keys.push_back(2004L);
        rows.push_back(row);
        keys.push_back(3000L);
        rows.push_back(row);
        keys.push_back(5000L);
        rows.push_back(row);
        keys.push_back(6000L);
        ASSERT_EQ(rows.size(), keys.size());
        CurrentHistorySlideWindow window(-1000L, 5, &rows, &keys, 0);
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(1u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(2u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(3u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(4u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(5u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(5u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(5u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(3u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(2u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(2u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(1u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(1u, window.Count());
        ASSERT_FALSE(window.Slide());
    }

    // history current_ts -1000 ~ current_ts max_size = 3
    {
        std::vector<Row> rows;
        std::vector<uint64_t> keys;
        rows.reserve(7);
        keys.reserve(7);

        // reserve rows
        rows.push_back(row);
        keys.push_back(1);
        rows.push_back(row);
        keys.push_back(1L);
        rows.push_back(row);
        keys.push_back(1);
        rows.push_back(row);
        keys.push_back(1L);

        rows.push_back(row);
        keys.push_back(1L);
        rows.push_back(row);
        keys.push_back(2L);
        rows.push_back(row);
        keys.push_back(3L);
        ASSERT_EQ(rows.size(), keys.size());
        CurrentHistorySlideWindow window(-1000L, 2, &rows, &keys, 4);
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(1u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(2u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(2u, window.Count());
        ASSERT_FALSE(window.Slide());
    }
}

TEST_F(WindowIteratorTest, CurrentHistoryUnboundSlideWindowTest) {
    int8_t* ptr = reinterpret_cast<int8_t*>(malloc(28));
    *(reinterpret_cast<int32_t*>(ptr + 2)) = 1;
    *(reinterpret_cast<int64_t*>(ptr + 2 + 4)) = 1;
    Row row({.buf = ptr});

    {
        std::vector<Row> rows;
        std::vector<uint64_t> keys;
        rows.push_back(row);
        keys.push_back(1L);
        rows.push_back(row);
        keys.push_back(2L);
        rows.push_back(row);
        keys.push_back(3L);
        rows.push_back(row);
        keys.push_back(40L);
        rows.push_back(row);
        keys.push_back(500L);
        rows.push_back(row);
        keys.push_back(1000L);
        rows.push_back(row);
        keys.push_back(1001L);
        rows.push_back(row);
        keys.push_back(1002L);
        rows.push_back(row);
        keys.push_back(1003L);
        rows.push_back(row);
        keys.push_back(1004L);
        rows.push_back(row);
        keys.push_back(1005L);
        rows.push_back(row);
        keys.push_back(1500L);
        rows.push_back(row);
        keys.push_back(2004L);
        rows.push_back(row);
        keys.push_back(3000L);
        rows.push_back(row);
        keys.push_back(5000L);
        rows.push_back(row);
        keys.push_back(6000L);
        ASSERT_EQ(rows.size(), keys.size());
        //        std::reverse(rows.begin(), rows.end());
        //        std::reverse(keys.begin(), keys.end());

        // history current_ts -1000 ~ current_ts
        CurrentHistoryUnboundSlideWindow window(&rows, &keys, 0);
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(1u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(2u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(3u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(4u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(5u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(6u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(7u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(8u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(9u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(10u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(11u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(12u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(13u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(14u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(15u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(16u, window.Count());
        ASSERT_FALSE(window.Slide());
    }

    {
        std::vector<Row> rows;
        std::vector<uint64_t> keys;
        rows.push_back(row);
        keys.push_back(1L);
        rows.push_back(row);
        keys.push_back(2L);
        rows.push_back(row);
        keys.push_back(3L);
        rows.push_back(row);
        keys.push_back(40L);
        rows.push_back(row);
        keys.push_back(500L);
        rows.push_back(row);
        keys.push_back(1000L);
        rows.push_back(row);
        keys.push_back(1001L);
        rows.push_back(row);
        keys.push_back(1002L);
        rows.push_back(row);
        keys.push_back(1003L);
        rows.push_back(row);
        keys.push_back(1004L);
        rows.push_back(row);
        keys.push_back(1005L);
        rows.push_back(row);
        keys.push_back(1500L);
        rows.push_back(row);
        keys.push_back(2004L);
        rows.push_back(row);
        keys.push_back(3000L);
        rows.push_back(row);
        keys.push_back(5000L);
        rows.push_back(row);
        keys.push_back(6000L);

        //        std::reverse(rows.begin(), rows.end());
        //        std::reverse(keys.begin(), keys.end());
        ASSERT_EQ(rows.size(), keys.size());

        // history current_ts -1000 ~ current_ts
        CurrentHistoryUnboundSlideWindow window(10, &rows, &keys, 0);
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(1u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(2u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(3u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(4u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(5u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(6u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(7u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(8u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(9u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(10u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(10u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(10u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(10u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(10u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(10u, window.Count());
        ASSERT_TRUE(window.Slide());
        ASSERT_EQ(10u, window.Count());
        ASSERT_FALSE(window.Slide());
    }
}

}  // namespace storage
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
