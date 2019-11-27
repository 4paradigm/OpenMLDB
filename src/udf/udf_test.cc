/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf.h"
#include <dlfcn.h>
#include <gtest/gtest.h>
#include <stdint.h>
#include <algorithm>
#include <iostream>
#include "base/window.cc"
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
            int8_t* ptr = static_cast<int8_t*>(malloc(28));
            *((int32_t*)(ptr + 2)) = 1;
            *((int16_t*)(ptr + 2 + 4)) = 2;
            *((float*)(ptr + 2 + 4 + 2)) = 3.1f;
            *((double*)(ptr + 2 + 4 + 2 + 4)) = 4.1;
            *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 5;
            rows.push_back(base::Row{.buf = ptr});
        }

        {
            int8_t* ptr = static_cast<int8_t*>(malloc(28));
            *((int32_t*)(ptr + 2)) = 11;
            *((int16_t*)(ptr + 2 + 4)) = 22;
            *((float*)(ptr + 2 + 4 + 2)) = 33.1f;
            *((double*)(ptr + 2 + 4 + 2 + 4)) = 44.1;
            *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 55;
            rows.push_back(base::Row{.buf = ptr});
        }

        {
            int8_t* ptr = static_cast<int8_t*>(malloc(28));
            *((int32_t*)(ptr + 2)) = 111;
            *((int16_t*)(ptr + 2 + 4)) = 222;
            *((float*)(ptr + 2 + 4 + 2)) = 333.1f;
            *((double*)(ptr + 2 + 4 + 2 + 4)) = 444.1;
            *((int64_t*)(ptr + 2 + 4 + 2 + 4 + 8)) = 555;
            rows.push_back(base::Row{.buf = ptr});
        }
    }

 protected:
    std::vector<base::Row> rows;
};

TEST_F(UDFTest, UDF_sum_test) {
    WindowIteratorImpl impl(rows);
    ColumnIteratorImpl<int32_t>* col_iter =
        new ColumnIteratorImpl<int32_t>(impl, 2);
    ASSERT_EQ(1 + 11 + 111, fesql_sum(((int8_t*)(col_iter))));
}

TEST_F(UDFTest, UDF_sum_ddl_find_test) {
    void* dllHandle = NULL;
    dllHandle = dlopen("./libfesql_udf.so", RTLD_LAZY);
    ASSERT_TRUE(nullptr != dllHandle);
    int32_t (*udf)(int8_t*) = NULL;
    udf = (int32_t(*)(int8_t*))dlsym(dllHandle, "fesql_sum");
    ASSERT_TRUE(nullptr != udf);

    WindowIteratorImpl impl(rows);
    ColumnIteratorImpl<int32_t>* col_iter =
        new ColumnIteratorImpl<int32_t>(impl, 2);
    ASSERT_EQ(1 + 11 + 111, udf(((int8_t*)(col_iter))));
}

TEST_F(UDFTest, UDF_llvm_invoke_udf_test) {
    void* dllHandle = NULL;
    dllHandle = dlopen("./libfesql_udf.so", RTLD_LAZY);
    ASSERT_TRUE(nullptr != dllHandle);
    int32_t (*udf)()= NULL;
    udf = (int32_t(*)())dlsym(dllHandle, "fesql_get5");
    ASSERT_TRUE(nullptr != udf);
    ASSERT_EQ(5, udf());
}

}  // namespace udf
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}