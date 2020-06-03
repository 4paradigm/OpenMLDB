/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * fesql_case_test.cc
 *
 * Author: chenjing
 * Date: 2019/12/25
 *--------------------------------------------------------------------------
 **/
#include "bm/engine_bm_case.h"
#include "gtest/gtest.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
namespace fesql {
namespace bm {
class EngineBMCaseTest : public ::testing::Test {
 public:
    EngineBMCaseTest() {}
    ~EngineBMCaseTest() {}
};

TEST_F(EngineBMCaseTest, EngineWindowSumFeature1_TEST) {
    EngineWindowSumFeature1(nullptr, TEST, 1L, 2L);
    EngineWindowSumFeature1(nullptr, TEST, 1L, 10L);
    EngineWindowSumFeature1(nullptr, TEST, 1L, 100L);
    EngineWindowSumFeature1(nullptr, TEST, 1L, 1000L);
    EngineWindowSumFeature1(nullptr, TEST, 100L, 100L);
    EngineWindowSumFeature1(nullptr, TEST, 1000L, 1000L);
}

TEST_F(EngineBMCaseTest, EngineWindowSumFeature5_TEST) {
    EngineWindowSumFeature5(nullptr, TEST, 1L, 2L);
    EngineWindowSumFeature5(nullptr, TEST, 1L, 10L);
    EngineWindowSumFeature5(nullptr, TEST, 1L, 100L);
    EngineWindowSumFeature5(nullptr, TEST, 1L, 1000L);
    EngineWindowSumFeature5(nullptr, TEST, 100L, 100L);
    EngineWindowSumFeature5(nullptr, TEST, 1000L, 1000L);
}

TEST_F(EngineBMCaseTest, EngineRunBatchWindowSumFeature1_TEST) {
    EngineRunBatchWindowSumFeature1(nullptr, TEST, 1L, 2L);
    EngineRunBatchWindowSumFeature1(nullptr, TEST, 1L, 10L);
    EngineRunBatchWindowSumFeature1(nullptr, TEST, 1L, 100L);
    EngineRunBatchWindowSumFeature1(nullptr, TEST, 100L, 100L);
    EngineRunBatchWindowSumFeature1(nullptr, TEST, 1000L, 1000L);
}

TEST_F(EngineBMCaseTest, WINDOW_CASE0_QUERY_BATCH_TEST) {
    EngineRunBatchWindowSumFeature5(nullptr, TEST, 1L, 2L);
    EngineRunBatchWindowSumFeature5(nullptr, TEST, 1L, 10L);
    EngineRunBatchWindowSumFeature5(nullptr, TEST, 1L, 100L);
    EngineRunBatchWindowSumFeature5(nullptr, TEST, 100L, 100L);
    EngineRunBatchWindowSumFeature5(nullptr, TEST, 1000L, 1000L);
}

TEST_F(EngineBMCaseTest, EngineRequestSimpleSelectDouble_TEST) {
    EngineRequestSimpleSelectDouble(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineRequestSimpleSelectInt32_TEST) {
    EngineRequestSimpleSelectInt32(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineRequestSimpleSelectVarchar_TEST) {
    EngineRequestSimpleSelectVarchar(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineRequestSimpleSelectDate_TEST) {
    EngineRequestSimpleSelectDate(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineRequestSimpleSelectTimestamp_TEST) {
    EngineRequestSimpleSelectTimestamp(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineRequestSimpleUDF_TEST) {
    EngineRequestSimpleUDF(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineSimpleSelectDouble_TEST) {
    EngineSimpleSelectDouble(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineSimpleSelectInt32_TEST) {
    EngineSimpleSelectInt32(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineSimpleSelectVarchar_TEST) {
    EngineSimpleSelectVarchar(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineSimpleSelectTimestamp_TEST) {
    EngineSimpleSelectTimestamp(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineSimpleSelectDate_TEST) {
    EngineSimpleSelectDate(nullptr, TEST);
}

TEST_F(EngineBMCaseTest, EngineSimpleUDF_TEST) {
    EngineSimpleUDF(nullptr, TEST);
}
}  // namespace bm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
