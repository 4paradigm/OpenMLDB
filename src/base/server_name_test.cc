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

#include "base/server_name.h"

#include "gtest/gtest.h"

DECLARE_string(data_dir);

namespace openmldb {
namespace base {

class ServerNameTest : public ::testing::Test {
 public:
    ServerNameTest() {}
    ~ServerNameTest() {}
};

TEST_F(ServerNameTest, GetName) {
    std::string restore_dir = "/tmp/data";
    std::string server_name;
    ASSERT_TRUE(GetNameFromTxt(restore_dir, &server_name));
    std::string server_name_2;
    ASSERT_TRUE(GetNameFromTxt(restore_dir, &server_name_2));
    ASSERT_EQ(server_name, server_name_2);
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    ::openmldb::base::SetLogLevel(INFO);
    ::openmldb::base::RemoveDirRecursive("/tmp/data");
    int ret = RUN_ALL_TESTS();
    return ret;
}
