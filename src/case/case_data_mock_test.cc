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

#include "case/case_data_mock.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
namespace hybridse {
namespace sqlcase {

class RepeaterTest : public ::testing::Test {
 public:
    RepeaterTest() {}
    ~RepeaterTest() {}
};

TEST_F(RepeaterTest, Int16_Repeater) {
    {
        IntRepeater<int16_t> repeater;
        repeater.Range(0, 100, 1);
        ASSERT_EQ(repeater.GetValue(), 0);
        ASSERT_EQ(repeater.GetValue(), 1);
        ASSERT_EQ(repeater.GetValue(), 2);
    }
    {
        IntRepeater<int16_t> repeater;
        repeater.Range(0, 100, 20);
        ASSERT_EQ(repeater.GetValue(), 0);
        ASSERT_EQ(repeater.GetValue(), 20);
        ASSERT_EQ(repeater.GetValue(), 40);
    }
}

}  // namespace sqlcase
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
