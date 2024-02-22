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

#include "gtest/gtest.h"
#include "base/sys_info.h"

namespace openmldb {
namespace base {

class SystemInfoTest : public ::testing::Test {
 public:
    SystemInfoTest() {}
    ~SystemInfoTest() {}
};

TEST_F(SystemInfoTest, GetMemory) {
    base::SysInfo info;
    auto status = base::GetSysMem(&info);
    ASSERT_TRUE(status.OK());
    ASSERT_GT(info.mem_total, 0);
    ASSERT_GT(info.mem_used, 0);
    ASSERT_GT(info.mem_free, 0);
    ASSERT_EQ(info.mem_total, info.mem_used + info.mem_buffers + info.mem_free + info.mem_cached);
    /*printf("total:%lu\n", info.mem_total);
    printf("used:%lu\n", info.mem_used);
    printf("free:%lu\n", info.mem_free);
    printf("buffers:%lu\n", info.mem_buffers);
    printf("cached:%lu\n", info.mem_cached);*/
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
