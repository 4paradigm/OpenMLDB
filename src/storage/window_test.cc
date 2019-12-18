/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/25
 *--------------------------------------------------------------------------
 **/
#include "storage/window.h"
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
    IteratorImpl<int> impl(int_vec);

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

    WindowIteratorImpl impl(rows);
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
    ASSERT_TRUE(impl.Valid());
}

}  // namespace storage
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
