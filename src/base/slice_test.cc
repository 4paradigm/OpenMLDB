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

#include "base/slice.h"

#include "gtest/gtest.h"

namespace openmldb {
namespace base {

class SliceTest : public ::testing::Test {
 public:
    SliceTest() {}
    ~SliceTest() {}
};

TEST_F(SliceTest, Compare) {
    Slice a("test1");
    Slice b("test1");
    ASSERT_EQ(0, a.compare(b));
    ASSERT_TRUE(a == b);
    ASSERT_EQ(sizeof(a), 16u);
}

TEST_F(SliceTest, Assign) {
    {
        char* data = new char[2];
        Slice a(data, 2, true);
        Slice b = a;
    }
    ASSERT_TRUE(true);
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
