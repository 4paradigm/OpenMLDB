/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_bm_case_test.cc
 *
 * Author: chenjing
 * Date: 2020/4/8
 *--------------------------------------------------------------------------
 **/
#include "bm/udf_bm_case.h"
#include "gtest/gtest.h"
namespace fesql {
namespace bm {
class UDFBMCaseTest : public ::testing::Test {
 public:
    UDFBMCaseTest() {}
    ~UDFBMCaseTest() {}
};

TEST_F(UDFBMCaseTest, SumCol1_TEST) {
    SumCol1(nullptr, TEST, 2L);
    SumCol1(nullptr, TEST, 10L);
    SumCol1(nullptr, TEST, 100L);
    SumCol1(nullptr, TEST, 1000L);
    SumCol1(nullptr, TEST, 10000L);
}
}  // namespace bm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
