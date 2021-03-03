/*
 * src/bm/base_bm_test.cc
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
 * base_bm_test.cc
 *
 * Author: chenjing
 * Date: 2019/12/24
 *--------------------------------------------------------------------------
 **/
#include "bm/base_bm.h"
#include "glog/logging.h"
#include "gtest/gtest.h"
namespace fesql {
namespace bm {

class BMBaseTest : public ::testing::Test {
 public:
    BMBaseTest() {}
    ~BMBaseTest() {}
};

TEST_F(BMBaseTest, Int16_Repeater) {
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

}  // namespace bm
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
