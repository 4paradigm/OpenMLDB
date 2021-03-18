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

#include "bm/storage_bm_case.h"
#include "gtest/gtest.h"
namespace hybridse {
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

TEST_F(StorageBMCaseTest, TabletTableIterate_TEST) {
    TabletFullIterate(nullptr, TEST, 10L);
    TabletFullIterate(nullptr, TEST, 100L);
    TabletFullIterate(nullptr, TEST, 1000L);
}

TEST_F(StorageBMCaseTest, TabletWindowIterate_TEST) {
//    TabletWindowIterate(nullptr, TEST, 10L);
    TabletWindowIterate(nullptr, TEST, 100L);
//    TabletWindowIterate(nullptr, TEST, 1000L);
}

TEST_F(StorageBMCaseTest, MemSegmentIterate_TEST) {
    MemSegmentIterate(nullptr, TEST, 10L);
    MemSegmentIterate(nullptr, TEST, 100L);
    MemSegmentIterate(nullptr, TEST, 1000L);
}

TEST_F(StorageBMCaseTest, MemTableIterate_TEST) {
    MemTableIterate(nullptr, TEST, 10L);
    MemTableIterate(nullptr, TEST, 100L);
    MemTableIterate(nullptr, TEST, 1000L);
}

TEST_F(StorageBMCaseTest, RequestUnionTableIterate_TEST) {
    RequestUnionTableIterate(nullptr, TEST, 10L);
    RequestUnionTableIterate(nullptr, TEST, 100L);
    RequestUnionTableIterate(nullptr, TEST, 1000L);
}

}  // namespace bm
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
