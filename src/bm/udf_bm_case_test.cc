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
    SumCol(nullptr, TEST, 2L, "col1");
    SumCol(nullptr, TEST, 10L, "col1");
    SumCol(nullptr, TEST, 100L, "col1");
    SumCol(nullptr, TEST, 1000L, "col1");
    SumCol(nullptr, TEST, 10000L, "col1");
}


TEST_F(UDFBMCaseTest, CopyMemTable_TEST) {
    CopyMemTable(nullptr, TEST, 2L);
    CopyMemTable(nullptr, TEST, 10L);
    CopyMemTable(nullptr, TEST, 100L);
    CopyMemTable(nullptr, TEST, 1000L);
}
}  // namespace bm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
