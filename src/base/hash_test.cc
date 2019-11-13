/*
 * hash_test.cc
 * Copyright (C) 2019 wangtaize <wangtaize@wangtaizedeMacBook-Pro-2.local>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "base/hash.h"

#include "gtest/gtest.h"

namespace fesql {
namespace base {

class HashTest : public ::testing::Test {
 public:
    HashTest() {}
    ~HashTest() {}
};

TEST_F(HashTest, Int64_Hash) {
    int32_t i = -1;
    int64_t output = MurmurHash64A(&i, 4, 0xe17a1465);
    ASSERT_EQ(output, -2087233940855511134);
}

TEST_F(HashTest, CharPtr_Hash) {
    std::string i = "hello";
    int64_t output = MurmurHash64A(i.c_str(), 5, 0xe17a1465);
    ASSERT_EQ(output, -4155090522938856779);
}

}  // namespace base
}  // namespace fesql

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
