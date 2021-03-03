/*
 * storage_bm_case_test.cc
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
