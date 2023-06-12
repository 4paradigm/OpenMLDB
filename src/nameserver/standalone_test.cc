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

#include <brpc/server.h>
#include <gflags/gflags.h>
#include <sched.h>
#include <unistd.h>

#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "client/ns_client.h"
#include "gtest/gtest.h"
#include "nameserver/name_server_impl.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "proto/type.pb.h"
#include "rpc/rpc_client.h"
#include "tablet/tablet_impl.h"
#include "test/util.h"

DECLARE_string(endpoint);
DECLARE_string(db_root_path);
DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(request_timeout_ms);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_int32(make_snapshot_threshold_offset);
DECLARE_uint32(name_server_task_max_concurrency);
DECLARE_bool(auto_failover);

using brpc::Server;
using openmldb::tablet::TabletImpl;
using ::openmldb::zk::ZkClient;
using std::shared_ptr;
using std::string;
using std::tuple;
using std::vector;

namespace openmldb {
namespace nameserver {

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};
class StandaloneTest : public ::testing::Test {
 public:
    StandaloneTest() {}
    ~StandaloneTest() {}
};

TEST_F(StandaloneTest, smoketest) {
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();
    brpc::Server tablet;
    ASSERT_TRUE(::openmldb::test::StartTablet("127.0.0.1:9530", &tablet));

    brpc::Server server;
    ASSERT_TRUE(::openmldb::test::StartNS("127.0.0.1:9631", "127.0.0.1:9530", &server));
    ::openmldb::client::NsClient client("127.0.0.1:9631", "");
    ASSERT_EQ(client.Init(), 0);

    TableInfo table_info;
    std::string name = "test" + ::openmldb::test::GenRand();
    table_info.set_name(name);
    ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, &table_info);
    std::string msg;
    ASSERT_TRUE(client.CreateTable(table_info, false, msg));
}

TEST_F(StandaloneTest, smoketestdisk) {
    FLAGS_zk_root_path = "/rtidb3" + ::openmldb::test::GenRand();
    brpc::Server tablet;
    ASSERT_TRUE(::openmldb::test::StartTablet("127.0.0.1:9530", &tablet));

    brpc::Server server;
    ASSERT_TRUE(::openmldb::test::StartNS("127.0.0.1:9631", "127.0.0.1:9530", &server));
    ::openmldb::client::NsClient client("127.0.0.1:9631", "");
    ASSERT_EQ(client.Init(), 0);

    TableInfo table_info;
    table_info.set_storage_mode(openmldb::common::kHDD);
    std::string name = "test" + ::openmldb::test::GenRand();
    table_info.set_name(name);
    ::openmldb::test::AddDefaultSchema(0, 0, ::openmldb::type::kAbsoluteTime, &table_info);
    std::string msg;
    ASSERT_TRUE(client.CreateTable(table_info, false, msg));

    openmldb::base::RemoveDirRecursive(FLAGS_hdd_root_path);
}


}  // namespace nameserver
}  // namespace openmldb

int main(int argc, char** argv) {
    FLAGS_zk_session_timeout = 100000;
    ::testing::InitGoogleTest(&argc, argv);
    ::openmldb::base::SetLogLevel(INFO);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::test::TempPath tmp_path;
    FLAGS_db_root_path = tmp_path.GetTempPath();
    FLAGS_hdd_root_path = tmp_path.GetTempPath();
    return RUN_ALL_TESTS();
}
