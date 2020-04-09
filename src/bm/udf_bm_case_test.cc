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

TEST_F(UDFBMCaseTest, SumArrayListCol1_TEST) {
    SumArrayListCol(nullptr, TEST, 10L, "col1");
    SumArrayListCol(nullptr, TEST, 100L, "col1");
    SumArrayListCol(nullptr, TEST, 1000L, "col1");
    SumArrayListCol(nullptr, TEST, 10000L, "col1");
}

TEST_F(UDFBMCaseTest, SumMemTableCol1_TEST) {
    SumMemTableCol(nullptr, TEST, 10L, "col1");
    SumMemTableCol(nullptr, TEST, 100L, "col1");
    SumMemTableCol(nullptr, TEST, 1000L, "col1");
    SumMemTableCol(nullptr, TEST, 10000L, "col1");
}

TEST_F(UDFBMCaseTest, CopyMemSegment_TEST) {
    CopyMemSegment(nullptr, TEST, 10L);
    CopyMemSegment(nullptr, TEST, 100L);
    CopyMemSegment(nullptr, TEST, 1000L);
}

TEST_F(UDFBMCaseTest, CopyMemTable_TEST) {
    CopyMemTable(nullptr, TEST, 10L);
    CopyMemTable(nullptr, TEST, 100L);
    CopyMemTable(nullptr, TEST, 1000L);
}


TEST_F(UDFBMCaseTest, CopyArrayList_TEST) {
    CopyArrayList(nullptr, TEST, 10L);
    CopyArrayList(nullptr, TEST, 100L);
    CopyArrayList(nullptr, TEST, 1000L);
}

TEST_F(UDFBMCaseTest, TabletTableIterate_TEST) {
    TabletFullIterate(nullptr, TEST, 10L);
    TabletFullIterate(nullptr, TEST, 100L);
    TabletFullIterate(nullptr, TEST, 1000L);
}

TEST_F(UDFBMCaseTest, TabletWindowIterate_TEST) {
    TabletWindowIterate(nullptr, TEST, 10L);
    TabletWindowIterate(nullptr, TEST, 100L);
    TabletWindowIterate(nullptr, TEST, 1000L);
}

TEST_F(UDFBMCaseTest, MemSegmentIterate_TEST) {
    MemSegmentIterate(nullptr, TEST, 10L);
    MemSegmentIterate(nullptr, TEST, 100L);
    MemSegmentIterate(nullptr, TEST, 1000L);
}

TEST_F(UDFBMCaseTest, MemTableIterate_TEST) {
    MemTableIterate(nullptr, TEST, 10L);
    MemTableIterate(nullptr, TEST, 100L);
    MemTableIterate(nullptr, TEST, 1000L);
}
}  // namespace bm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
