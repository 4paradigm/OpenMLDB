/*
 * src/bm/engine_bm_case_test.cc
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
TEST_F(EngineBMCaseTest, EngineWindowSumFeature5Window5_TEST) {
    EngineWindowSumFeature5Window5(nullptr, TEST, 1L, 100L);
    EngineWindowSumFeature5Window5(nullptr, TEST, 1L, 1000L);
    EngineWindowSumFeature5Window5(nullptr, TEST, 100L, 100L);
    EngineWindowSumFeature5Window5(nullptr, TEST, 1000L, 1000L);
}
TEST_F(EngineBMCaseTest, EngineWindowMultiAggWindow25Feature25_TEST) {
    EngineWindowMultiAggWindow25Feature25(nullptr, TEST, 1L, 100L);
    EngineWindowMultiAggWindow25Feature25(nullptr, TEST, 1L, 1000L);
    EngineWindowMultiAggWindow25Feature25(nullptr, TEST, 100L, 100L);
    EngineWindowMultiAggWindow25Feature25(nullptr, TEST, 1000L, 1000L);
}
TEST_F(EngineBMCaseTest, EngineRunBatchWindowMultiAggWindow25Feature25_TEST) {
    EngineRunBatchWindowMultiAggWindow25Feature25(nullptr, TEST, 1L, 100L);
    EngineRunBatchWindowMultiAggWindow25Feature25(nullptr, TEST, 1L, 1000L);
    EngineRunBatchWindowMultiAggWindow25Feature25(nullptr, TEST, 100L, 100L);
    EngineRunBatchWindowMultiAggWindow25Feature25(nullptr, TEST, 1000L, 1000L);
}
TEST_F(EngineBMCaseTest, EngineRunBatchWindowSumFeature1_TEST) {
    EngineRunBatchWindowSumFeature1(nullptr, TEST, 1L, 2L);
    EngineRunBatchWindowSumFeature1(nullptr, TEST, 1L, 10L);
    EngineRunBatchWindowSumFeature1(nullptr, TEST, 1L, 100L);
    EngineRunBatchWindowSumFeature1(nullptr, TEST, 100L, 100L);
    EngineRunBatchWindowSumFeature1(nullptr, TEST, 1000L, 1000L);
}
TEST_F(EngineBMCaseTest, EngineRunBatchWindowSumFeature5Window5_TEST) {
    EngineRunBatchWindowSumFeature5Window5(nullptr, TEST, 100L, 100L);
}
TEST_F(EngineBMCaseTest, EngineWindowMultiAggFeature5_TEST) {
    EngineWindowMultiAggFeature5(nullptr, TEST, 100L, 100L);
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

// TODO(xxx): udf script fix
// TEST_F(EngineBMCaseTest, EngineRequestSimpleUDF_TEST) {
//    EngineRequestSimpleUDF(nullptr, TEST);
// }

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

// TODO(xxx): udf script fix
// TEST_F(EngineBMCaseTest, EngineSimpleUDF_TEST) {
//    EngineSimpleUDF(nullptr, TEST);
// }
}  // namespace bm
}  // namespace fesql
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
