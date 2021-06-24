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

#include "base/glog_wapper.h"

#include <iostream>
#include <string>

#include "gtest/gtest.h"

namespace openmldb {
namespace base {

class GlogWapperTest : public ::testing::Test {
 public:
    GlogWapperTest() {}
    ~GlogWapperTest() {}
};

TEST_F(GlogWapperTest, Log) {
    ::openmldb::base::SetLogLevel(DEBUG);
    std::string path = "hello";
    ::openmldb::base::SetLogFile(path);
    PDLOG(INFO, "hello %d %f", 290, 3.1);
    std::string s = "word";
    PDLOG(INFO, "hello %s", s);
    PDLOG(WARNING, "this is a warning %s", "hello");
    DEBUGLOG("hello %d", 233);
    uint64_t time = 123456;
    DEBUGLOG("[Gc4TTL] segment gc with key %lu, consumed %lu, count %lu", time, time + 100, time - 100);
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
