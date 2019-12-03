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
#include "base/window.h"
namespace fesql {
namespace udf {
using base::ColumnIteratorImpl;
using base::WindowIteratorImpl;

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
            rows.push_back(base::Row{.buf = ptr});
        }

        {
            int8_t* ptr = static_cast<int8_t*>(malloc(28));
            *(reinterpret_cast<int32_t*>(ptr + 2)) = 11;
            *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 22;
            *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 33.1f;
            *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 44.1;
            *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
            rows.push_back(base::Row{.buf = ptr});
        }

        {
            int8_t* ptr = static_cast<int8_t*>(malloc(28));
            *(reinterpret_cast<int32_t*>(ptr + 2)) = 111;
            *(reinterpret_cast<int16_t*>(ptr + 2 + 4)) = 222;
            *(reinterpret_cast<float*>(ptr + 2 + 4 + 2)) = 333.1f;
            *(reinterpret_cast<double*>(ptr + 2 + 4 + 2 + 4)) = 444.1;
            *(reinterpret_cast<int64_t*>(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
            rows.push_back(base::Row{.buf = ptr});
        }
    }

 protected:
    std::vector<base::Row> rows;
};

TEST_F(UDFTest, UDF_sum_test) {
    WindowIteratorImpl impl(rows);
    int8_t* col = reinterpret_cast<int8_t*>(
        ::fesql::udf::col(
        reinterpret_cast<int8_t*>(&impl), 2, fesql::type::kInt32));
    ASSERT_EQ(1 + 11 + 111, fesql::udf::sum_int32(col));
}

}  // namespace udf
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
