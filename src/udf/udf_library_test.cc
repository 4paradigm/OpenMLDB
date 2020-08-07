/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_library_test.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf_library.h"
#include <gtest/gtest.h>
#include <unordered_set>
#include <vector>
#include "udf/udf_registry.h"

namespace fesql {
namespace udf {

class UDFLibraryTest : public ::testing::Test {};

TEST_F(UDFLibraryTest, test_check_is_udaf_by_name) {
    UDFLibrary library;
    node::NodeManager nm;

    library.RegisterUDAF("sum")
        .templates<int32_t, int32_t, int32_t>()
        .const_init(0)
        .update("add", reinterpret_cast<void*>(0))
        .output("identity", reinterpret_cast<void*>(1));

    ASSERT_TRUE(library.IsUDAF("sum", 1));
    ASSERT_TRUE(!library.IsUDAF("sum", 2));
    ASSERT_TRUE(!library.IsUDAF("sum2", 1));
}

}  // namespace udf
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::GTEST_FLAG(color) = "yes";
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
