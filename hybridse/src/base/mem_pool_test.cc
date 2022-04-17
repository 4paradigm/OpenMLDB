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

#include "base/mem_pool.h"
#include <string>
#include "gtest/gtest.h"

namespace hybridse {
namespace base {

class MemPoolTest : public ::testing::Test {
 public:
    MemPoolTest() {}
    ~MemPoolTest() {}
};

TEST_F(MemPoolTest, ByteMemoryPoolTest) {
    {
        ::openmldb::base::ByteMemoryPool mem_pool;
        char* s1 = mem_pool.Alloc(10);
        memcpy(s1, "helloworld", 10);
        ASSERT_EQ("helloworld", std::string(s1, 10));
    }

    {
        ::openmldb::base::ByteMemoryPool mem_pool;
        const char src1[] = "helloworld";
        char* s1 = mem_pool.Alloc(10);
        memcpy(s1, src1, strlen(src1));

        const char src2[] = "hybridse";
        char* s2 = mem_pool.Alloc(5);
        memcpy(s2, src2, strlen(src2));

        char* s3 = mem_pool.Alloc(15);
        memcpy(s3, s1, 10);
        memcpy(s3 + 10, s2, 5);
        ASSERT_EQ("helloworld", std::string(s1, 10));
        ASSERT_EQ("hybri", std::string(s2, 5));
        ASSERT_EQ("helloworldhybri", std::string(s3, 15));
    }
}
}  // namespace base
}  // namespace hybridse

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
