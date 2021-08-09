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

#include "benchmark/udf_bm_case.h"
#include "gtest/gtest.h"
namespace hybridse {
namespace bm {
class UdfBMCaseTest : public ::testing::Test {
 public:
    UdfBMCaseTest() {}
    ~UdfBMCaseTest() {}
};

TEST_F(UdfBMCaseTest, SumArrayListCol1_TEST) {
    SumArrayListCol(nullptr, TEST, 10L, "col1");
    SumArrayListCol(nullptr, TEST, 100L, "col1");
    SumArrayListCol(nullptr, TEST, 1000L, "col1");
    SumArrayListCol(nullptr, TEST, 10000L, "col1");
}

TEST_F(UdfBMCaseTest, SumMemTableCol1_TEST) {
    SumMemTableCol(nullptr, TEST, 10L, "col1");
    SumMemTableCol(nullptr, TEST, 100L, "col1");
    SumMemTableCol(nullptr, TEST, 1000L, "col1");
    SumMemTableCol(nullptr, TEST, 10000L, "col1");
}

TEST_F(UdfBMCaseTest, SumRequestUnionTableCol1_TEST) {
    SumRequestUnionTableCol(nullptr, TEST, 10L, "col1");
    SumRequestUnionTableCol(nullptr, TEST, 100L, "col1");
    SumRequestUnionTableCol(nullptr, TEST, 1000L, "col1");
    SumRequestUnionTableCol(nullptr, TEST, 10000L, "col1");
}

TEST_F(UdfBMCaseTest, CopyMemSegment_TEST) {
    CopyMemSegment(nullptr, TEST, 10L);
    CopyMemSegment(nullptr, TEST, 100L);
    CopyMemSegment(nullptr, TEST, 1000L);
}

TEST_F(UdfBMCaseTest, CopyMemTable_TEST) {
    CopyMemTable(nullptr, TEST, 10L);
    CopyMemTable(nullptr, TEST, 100L);
    CopyMemTable(nullptr, TEST, 1000L);
}

TEST_F(UdfBMCaseTest, CopyArrayList_TEST) {
    CopyArrayList(nullptr, TEST, 10L);
    CopyArrayList(nullptr, TEST, 100L);
    CopyArrayList(nullptr, TEST, 1000L);
}

TEST_F(UdfBMCaseTest, CTimeDay_TEST) { CTimeDay(nullptr, TEST, 1); }
TEST_F(UdfBMCaseTest, CTimeMonth) { CTimeMonth(nullptr, TEST, 1); }
TEST_F(UdfBMCaseTest, CTimeYear_TEST) { CTimeYear(nullptr, TEST, 1); }
TEST_F(UdfBMCaseTest, ByteMemPoolAlloc1000_TEST) {
    ByteMemPoolAlloc1000(nullptr, TEST, 10);
    ByteMemPoolAlloc1000(nullptr, TEST, 100);
    ByteMemPoolAlloc1000(nullptr, TEST, 1000);
    ByteMemPoolAlloc1000(nullptr, TEST, 10000);
}
TEST_F(UdfBMCaseTest, TimestampToString_TEST) {
    TimestampToString(nullptr, TEST);
}
TEST_F(UdfBMCaseTest, TimestampFormat_TEST) { TimestampFormat(nullptr, TEST); }

TEST_F(UdfBMCaseTest, DateToString_TEST) { DateToString(nullptr, TEST); }
TEST_F(UdfBMCaseTest, DateFormat_TEST) { DateFormat(nullptr, TEST); }

}  // namespace bm
}  // namespace hybridse
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
