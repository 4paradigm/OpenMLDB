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

#include <sched.h>
#include <unistd.h>
#include <string>

#include "boost/bind.hpp"
#include "gflags/gflags.h"
#include "gtest/gtest.h"
#include "tablet/tablet_impl.h"
#include "test/util.h"

DECLARE_string(endpoint);
DECLARE_string(db_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

using ::openmldb::zk::ZkClient;

namespace openmldb {
namespace tablet {

uint32_t counter = 10;
static bool call_invoked = false;
static size_t endpoint_size = 1;

void WatchCallback(const std::vector<std::string>& endpoints) {
    if (call_invoked) {
        return;
    }
    ASSERT_EQ(endpoint_size, endpoints.size());
    ASSERT_EQ("127.0.0.1:9527", endpoints[0]);
    call_invoked = true;
}

class MockClosure : public ::google::protobuf::Closure {
 public:
    MockClosure() {}
    ~MockClosure() {}
    void Run() {}
};

class TabletImplTest : public ::testing::Test {
 public:
    TabletImplTest() {}
    ~TabletImplTest() {}
};

TEST_F(TabletImplTest, KeepAlive) {
    FLAGS_endpoint = "127.0.0.1:9527";
    FLAGS_zk_cluster = "127.0.0.1:6181";
    FLAGS_zk_root_path = "/rtidb2";
    ZkClient zk_client(FLAGS_zk_cluster, "", 1000, "test1", FLAGS_zk_root_path, "", "");
    bool ok = zk_client.Init();
    ASSERT_TRUE(ok);
    ok = zk_client.Mkdir("/rtidb2/nodes");
    ASSERT_TRUE(ok);
    zk_client.WatchNodes(boost::bind(&WatchCallback, _1));
    ok = zk_client.WatchNodes();
    ASSERT_TRUE(ok);
    TabletImpl tablet;
    ok = tablet.Init("");
    ASSERT_TRUE(ok);
    ok = tablet.RegisterZK();
    ASSERT_TRUE(ok);
    sleep(5);
    ASSERT_TRUE(call_invoked);
}

}  // namespace tablet
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand(time(NULL));
    ::openmldb::base::SetLogLevel(DEBUG);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::test::InitRandomDiskFlags("tablet_impl_keep_alive_test");
    return RUN_ALL_TESTS();
}
