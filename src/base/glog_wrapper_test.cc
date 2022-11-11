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

namespace openmldb {
namespace base {

namespace fs = boost::filesystem;

class GlogWrapperTest : public ::testing::Test {
 public:
    GlogWrapperTest() {}
    ~GlogWrapperTest() {}

    void CheckDirAndDel(const std::string& dir) {
        ASSERT_TRUE(fs::exists(dir) && fs::is_directory(dir));
        fs::remove_all(dir);
        ASSERT_TRUE(!fs::exists(dir));
    }
};

TEST_F(GlogWrapperTest, UnprotectedSetGlog) {
    ::openmldb::base::SetLogLevel(DEBUG);
    std::string path = "hello";
    FLAGS_openmldb_log_dir = "/tmp/glog_wrapper_test1";
    FLAGS_role = "tester1";  // role != empty/sql_client, use openmldb_log_dir
    fs::remove_all(FLAGS_openmldb_log_dir);
    UnprotectedSetupGlog();
    PDLOG(INFO, "hello %d %f", 290, 3.1);
    std::string s = "word";
    PDLOG(INFO, "hello %s", s);
    PDLOG(WARNING, "this is a warning %s", "hello");
    DEBUGLOG("hello %d", 233);
    uint64_t time = 123456;
    DEBUGLOG("[Gc4TTL] segment gc with key %lu, consumed %lu, count %lu", time, time + 100, time - 100);
    CheckDirAndDel(FLAGS_openmldb_log_dir);
    // to avoid effecting other tests
    google::ShutdownGoogleLogging();

    LOG(INFO) << "you can see me(without init glog)";
    // set to empty, won't write to dir
    FLAGS_openmldb_log_dir = "";
    FLAGS_role = "tablet";
    ASSERT_FALSE(FLAGS_logtostderr);
    // glog dir is empty, so we'll set FLAGS_logtostderr to true
    UnprotectedSetupGlog();
    ASSERT_TRUE(FLAGS_logtostderr);
    FLAGS_logtostderr = false;  // reset it
    LOG(INFO) << "you can see me(set empty log dir)";
    google::ShutdownGoogleLogging();
}

TEST_F(GlogWrapperTest, changeLevelAfterInit) {
    FLAGS_role = "";
    FLAGS_glog_dir = "";
    FLAGS_glog_level = 0;
    ASSERT_FALSE(FLAGS_logtostderr);
    UnprotectedSetupGlog();
    ASSERT_TRUE(FLAGS_logtostderr);
    FLAGS_logtostderr = false;  // reset it
    LOG(INFO) << "you can see me in console";
    FLAGS_minloglevel = 1;
    LOG(INFO) << "you can't see me, log level can be changed in runtime";
    google::ShutdownGoogleLogging();

    FLAGS_role = "";
    FLAGS_glog_level = 1;
    std::string log_path = "/tmp/foo";
    FLAGS_glog_dir = log_path;
    // do InitGoogleLogging, shutdown later
    UnprotectedSetupGlog();
    LOG(INFO) << "you can't see me";
    LOG(WARNING) << "you can't see me, i'm in log";

    // minloglevel can be changed in runtime
    FLAGS_minloglevel = 0;
    LOG(INFO) << "you can see me in log, log level can be changed in runtime";

    CheckDirAndDel(log_path);
    // to avoid effecting other tests
    google::ShutdownGoogleLogging();
}

TEST_F(GlogWrapperTest, useOriginFlags) {
    FLAGS_role = "tablet";
    FLAGS_openmldb_log_dir = "/tmp/wont_use";
    FLAGS_logtostderr = false;
    ASSERT_FALSE(FLAGS_logtostderr);
    UnprotectedSetupGlog(true);
    ASSERT_TRUE(FLAGS_logtostderr);
    LOG(INFO) << "you can see me cause logtostderr";
    FLAGS_logtostderr = false;
    google::ShutdownGoogleLogging();

    std::string log_path = "/tmp/glog_wrapper_test2";
    FLAGS_log_dir = log_path;
    UnprotectedSetupGlog(true);
    LOG(INFO) << "you can see me in log";
    CheckDirAndDel(log_path);
    google::ShutdownGoogleLogging();
}

// DO NOT test protected SetupGlog outside of this test
TEST_F(GlogWrapperTest, ProtectedSetGlog) {
    std::string log_path = "/tmp/glog_wrapper_test3";
    FLAGS_openmldb_log_dir = "/tmp/wont_work";
    FLAGS_glog_dir = log_path;
    FLAGS_role = "";  // role is empty, use glog_dir, not openmldb_log_dir
    fs::remove_all(log_path);
    ASSERT_TRUE(!fs::exists(log_path));
    ASSERT_TRUE(SetupGlog());
    // haven't write log
    ASSERT_TRUE(fs::is_empty(log_path));
    FLAGS_glog_dir = "";
    FLAGS_glog_level = 3;
    ASSERT_FALSE(SetupGlog());  // won't work, still write to log_path, info level
    LOG(INFO) << "you can see me in log";
    CheckDirAndDel(log_path);
    google::ShutdownGoogleLogging();
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
