/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_bm_case_test.cc
 *
 * Author: chenjing
 * Date: 2020/4/8
 *--------------------------------------------------------------------------
 **/
#include "bm/storage_bm_case.h"
#include "gtest/gtest.h"
namespace fesql {
namespace bm {
class StorageBMCaseTest : public ::testing::Test {
 public:
    StorageBMCaseTest() {}
    ~StorageBMCaseTest() {}
};

TEST_F(StorageBMCaseTest, ArrayListIterate_TEST) {
    ArrayListIterate(nullptr, TEST, 100L);
    ArrayListIterate(nullptr, TEST, 1000L);
    ArrayListIterate(nullptr, TEST, 10000L);
}
}  // namespace bm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
