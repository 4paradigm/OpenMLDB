/*
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

#include "bm/client_bm_case.h"
#include "gperftools/heap-profiler.h"
#include "gtest/gtest.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
using namespace llvm;  // NOLINT
namespace hybridse {
namespace bm {
class HybridSE_CASE_Test : public ::testing::Test {
 public:
    HybridSE_CASE_Test() {}
    ~HybridSE_CASE_Test() {}
};

TEST_F(HybridSE_CASE_Test, SIMPLE_QUERY_CASE1_TEST) {
    SIMPLE_CASE1_QUERY(nullptr, TEST, false, 1, 1000);
    SIMPLE_CASE1_QUERY(nullptr, TEST, false, 10, 10);
    SIMPLE_CASE1_QUERY(nullptr, TEST, false, 2, 1000);
}

TEST_F(HybridSE_CASE_Test, SIMPLE_QUERY_CASE1_BATCH_TEST) {
    SIMPLE_CASE1_QUERY(nullptr, TEST, true, 10, 10);
    SIMPLE_CASE1_QUERY(nullptr, TEST, true, 2, 1000);
}

TEST_F(HybridSE_CASE_Test, WINDOW_CASE0_QUERY_TEST) {
    WINDOW_CASE0_QUERY(nullptr, TEST, false, 10, 10);
    WINDOW_CASE0_QUERY(nullptr, TEST, false, 2, 1000);
}

TEST_F(HybridSE_CASE_Test, WINDOW_CASE0_QUERY_BATCH_TEST) {
    WINDOW_CASE0_QUERY(nullptr, TEST, true, 10, 10);
    WINDOW_CASE0_QUERY(nullptr, TEST, true, 2, 1000);
}

TEST_F(HybridSE_CASE_Test, WINDOW_CASE1_QUERY_TEST) {
    WINDOW_CASE1_QUERY(nullptr, TEST, false, 10, 10);
    WINDOW_CASE1_QUERY(nullptr, TEST, false, 2, 1000);
}

TEST_F(HybridSE_CASE_Test, WINDOW_CASE1_QUERY_BATCH_TEST) {
    WINDOW_CASE1_QUERY(nullptr, TEST, true, 10, 10);
    WINDOW_CASE1_QUERY(nullptr, TEST, true, 2, 1000);
}

TEST_F(HybridSE_CASE_Test, WINDOW_CASE2_QUERY_BATCH_TEST) {
    WINDOW_CASE2_QUERY(nullptr, TEST, true, 10, 10);
    WINDOW_CASE2_QUERY(nullptr, TEST, true, 2, 1000);
}
}  // namespace bm
}  // namespace hybridse
int main(int argc, char** argv) {
    InitLLVM X(argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
