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

#include "storage/window.h"
namespace fesql {
namespace udf {
using storage::ColumnIteratorImpl;
using storage::WindowIteratorImpl;

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
            rows.push_back(storage::Row{.buf = ptr});
        }

        {
            int8_t* ptr = static_cast<int8_t*>(malloc(28));
            *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
            *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
            *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
            *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
            *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
            rows.push_back(storage::Row{.buf = ptr});
        }

        {
            int8_t* ptr = static_cast<int8_t*>(malloc(28));
            *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
            *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
            *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
            *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
            *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
            rows.push_back(storage::Row{.buf = ptr});
        }
    }

 protected:
    std::vector<storage::Row> rows;
};

TEST_F(UDFTest, UDF_sum_test) {
    WindowIteratorImpl impl(rows);
    const uint32_t size = sizeof(::fesql::storage::ColumnIteratorImpl<int16_t>);
    for (int i = 0; i < 10; ++i) {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::storage::ListRef list_ref;
        list_ref.iterator = buf;
        int8_t* col = reinterpret_cast<int8_t*>(&list_ref);

        ASSERT_EQ(
            0, ::fesql::storage::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 2,
                                            fesql::type::kInt32, buf));
        ASSERT_EQ(1 + 11 + 111, fesql::udf::v1::sum_int32(col));
    }
}

TEST_F(UDFTest, GetColTest) {
    WindowIteratorImpl impl(rows);
    const uint32_t size = sizeof(::fesql::storage::ColumnIteratorImpl<int16_t>);
    for (int i = 0; i < 10; ++i) {
        int8_t* buf = reinterpret_cast<int8_t*>(alloca(size));
        ::fesql::storage::ListRef list_ref;
        list_ref.iterator = buf;
        ASSERT_EQ(
            0, ::fesql::storage::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 2,
                                            fesql::type::kInt32, buf));
        ::fesql::storage::ColumnIteratorImpl<int16_t>* col_iterator =
            reinterpret_cast< ::fesql::storage::ColumnIteratorImpl<int16_t>*>(
                list_ref.iterator);
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(1u, col_iterator->Next());
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(11u, col_iterator->Next());
        ASSERT_TRUE(col_iterator->Valid());
        ASSERT_EQ(111u, col_iterator->Next());
        ASSERT_FALSE(col_iterator->Valid());
    }
}

TEST_F(UDFTest, GetColHeapTest) {
    WindowIteratorImpl impl(rows);
    const uint32_t size = sizeof(::fesql::storage::ColumnIteratorImpl<int16_t>);
    for (int i = 0; i < 100000000; ++i) {
        int8_t buf[size];  // NOLINT
        ::fesql::storage::ListRef list_ref;
        list_ref.iterator = buf;
        ASSERT_EQ(
            0, ::fesql::storage::v1::GetCol(reinterpret_cast<int8_t*>(&impl), 2,
                                            fesql::type::kInt32, buf));
    }
}

}  // namespace udf
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
