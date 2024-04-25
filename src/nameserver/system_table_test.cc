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

#include "nameserver/system_table.h"

#include <gflags/gflags.h>
#include <sched.h>
#include <unistd.h>

#include <string>
#include <vector>

#include "absl/time/clock.h"
#include "absl/time/time.h"
#include "base/glog_wrapper.h"
#include "brpc/server.h"
#include "client/ns_client.h"
#include "common/timer.h"
#include "gtest/gtest.h"
#include "rpc/rpc_client.h"
#include "test/util.h"

DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);

namespace openmldb {
namespace nameserver {

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};
class SystemTableTest : public ::testing::Test {
 public:
    SystemTableTest() { FLAGS_skip_grant_tables = false; }
    ~SystemTableTest() { FLAGS_skip_grant_tables = true; }
};

TEST_F(SystemTableTest, SystemTable) {
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/system_table" + ::openmldb::test::GenRand();
    brpc::Server tablet;
    ASSERT_TRUE(::openmldb::test::StartTablet("127.0.0.1:9530", &tablet));
    brpc::Server ns;
    ASSERT_TRUE(::openmldb::test::StartNS("127.0.0.1:6530", &ns));
    ::openmldb::client::NsClient ns_client("127.0.0.1:6530", "");
    ns_client.Init();

    // wait a while for that ns become leader
    absl::SleepFor(absl::Seconds(5));

    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::string msg;
    ASSERT_TRUE(ns_client.ShowTable("", INTERNAL_DB, false, tables, msg));
    ASSERT_EQ(3, tables.size());
    ASSERT_EQ("JOB_INFO", tables[0].name());
    ASSERT_EQ("PRE_AGG_META_INFO", tables[1].name());
    ASSERT_EQ("USER", tables[2].name());
    tables.clear();
    // deny drop system table
    ASSERT_FALSE(ns_client.DropDatabase(INTERNAL_DB, msg));

    ASSERT_TRUE(ns_client.ShowTable(JOB_INFO_NAME, INTERNAL_DB, false, tables, msg));
    ASSERT_EQ(1, tables.size());
    ASSERT_STREQ("JOB_INFO", tables[0].name().c_str());
    tables.clear();

    ASSERT_TRUE(ns_client.ShowTable(PRE_AGG_META_NAME, INTERNAL_DB, false, tables, msg));
    ASSERT_EQ(1, tables.size());
    ASSERT_STREQ("PRE_AGG_META_INFO", tables[0].name().c_str());
    tables.clear();

    ASSERT_TRUE(ns_client.ShowTable("", INFORMATION_SCHEMA_DB, false, tables, msg));
    ASSERT_EQ(2, tables.size());
    tables.clear();
    ASSERT_FALSE(ns_client.DropDatabase(INFORMATION_SCHEMA_DB, msg));

    ASSERT_TRUE(ns_client.ShowTable(GLOBAL_VARIABLES, INFORMATION_SCHEMA_DB, false, tables, msg));
    ASSERT_EQ(1, tables.size());
    ASSERT_STREQ("GLOBAL_VARIABLES", tables[0].name().c_str());
    tables.clear();
}

}  // namespace nameserver
}  // namespace openmldb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::test::InitRandomDiskFlags("system_table_test");
    return RUN_ALL_TESTS();
}
