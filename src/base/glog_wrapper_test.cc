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

#include "base/glog_wrapper.h"

#include <iostream>
#include <string>

#include "gtest/gtest.h"

namespace fs = boost::filesystem;

namespace openmldb {
namespace base {

class GlogWrapperTest : public ::testing::Test {
 public:
    GlogWrapperTest() {}
    ~GlogWrapperTest() {}
};

TEST_F(GlogWrapperTest, UnprotectedSetGlog) {
    ::openmldb::base::SetLogLevel(DEBUG);
    std::string path = "hello";
    FLAGS_openmldb_log_dir = "/tmp/glog_wrapper_test1";
    FLAGS_role = "tester1";
    fs::remove_all(FLAGS_openmldb_log_dir);
    UnprotectedSetupGlog();
    PDLOG(INFO, "hello %d %f", 290, 3.1);
    std::string s = "word";
    PDLOG(INFO, "hello %s", s);
    PDLOG(WARNING, "this is a warning %s", "hello");
    DEBUGLOG("hello %d", 233);
    uint64_t time = 123456;
    DEBUGLOG("[Gc4TTL] segment gc with key %lu, consumed %lu, count %lu", time, time + 100, time - 100);
    ASSERT_TRUE(fs::exists(FLAGS_openmldb_log_dir) && fs::is_directory(FLAGS_openmldb_log_dir));
    fs::remove_all(FLAGS_openmldb_log_dir);
    ASSERT_TRUE(!fs::exists(FLAGS_openmldb_log_dir));
    // to avoid effecting other tests
    google::ShutdownGoogleLogging();

    LOG(INFO) << "you can see me(without init glog)";
    // set to empty, won't write to dir
    FLAGS_openmldb_log_dir = "";
    UnprotectedSetupGlog();
    LOG(INFO) << "you can see me(set empty log dir)";
}

TEST_F(GlogWrapperTest, ProtectedSetGlog) {
    std::string log_path = "/tmp/glog_wrapper_test2";
    FLAGS_openmldb_log_dir = log_path;
    FLAGS_role = "";
    fs::remove_all(log_path);
    ASSERT_TRUE(!fs::exists(log_path));
    SetupGLog();
    // haven't write log
    ASSERT_TRUE(fs::is_empty(log_path));
    FLAGS_openmldb_log_dir = "";
    SetupGLog();  // won't work, still write to log_path
    LOG(INFO) << "test";
    ASSERT_TRUE(fs::exists(log_path) && fs::is_directory(log_path));
    fs::remove_all(log_path);
    ASSERT_TRUE(!fs::exists(log_path));
    google::ShutdownGoogleLogging();
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
