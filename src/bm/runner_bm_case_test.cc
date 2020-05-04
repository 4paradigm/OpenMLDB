/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * runner_bm_case_test.cc
 *
 * Author: chenjing
 * Date: 2020/4/8
 *--------------------------------------------------------------------------
 **/
#include "bm/runner_bm_case.h"
#include "gtest/gtest.h"
namespace fesql {
namespace bm {
class RunnerBMCaseTest : public ::testing::Test {
 public:
    RunnerBMCaseTest() {}
    ~RunnerBMCaseTest() {}
};

TEST_F(RunnerBMCaseTest, AggRunnerCase_TEST) {
    WindowSumFeature1_Aggregation(nullptr, TEST, 1, 100L);
    WindowSumFeature1_Aggregation(nullptr, TEST, 1, 1000L);
    WindowSumFeature1_Aggregation(nullptr, TEST, 1, 10000L);
}
TEST_F(RunnerBMCaseTest, RequestUnionRunnerCase_TEST) {
    WindowSumFeature1_RequestUnion(nullptr, TEST, 1, 100L);
    WindowSumFeature1_RequestUnion(nullptr, TEST, 1, 1000L);
    WindowSumFeature1_RequestUnion(nullptr, TEST, 1, 10000L);
}

}  // namespace bm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
