//
// tablet_impl_test.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-05
//

#include "tablet/tablet_impl.h"
#include "proto/tablet.pb.h"
#include "base/kv_iterator.h"
#include "gtest/gtest.h"
#include "logging.h"
#include "timer.h"
#include <gflags/gflags.h>
#include <sched.h>
#include <unistd.h>


DECLARE_string(endpoint);
DECLARE_string(db_root_path);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

using ::rtidb::zk::ZkClient;



namespace rtidb {
namespace tablet {

uint32_t counter = 10;
static bool call_invoked = false;
static int32_t endpoint_size = 1;


inline std::string GenRand() {
    return std::to_string(rand() % 10000000 + 1);
}

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
    FLAGS_endpoint="127.0.0.1:9527";
    FLAGS_zk_cluster="127.0.0.1:6181";
    FLAGS_zk_root_path="/rtidb2";
    ZkClient zk_client(FLAGS_zk_cluster, 1000, "test1", FLAGS_zk_root_path);
    bool ok = zk_client.Init();
    ASSERT_TRUE(ok);
    ok = zk_client.Mkdir("/rtidb2/nodes");
    ASSERT_TRUE(ok);
    zk_client.WatchNodes(boost::bind(&WatchCallback, _1));
    ok = zk_client.WatchNodes();
    ASSERT_TRUE(ok);
    TabletImpl tablet;
    ok = tablet.Init();
    ASSERT_TRUE(ok);
    sleep(5);
    ASSERT_TRUE(call_invoked);
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    srand (time(NULL));
    ::baidu::common::SetLogLevel(::baidu::common::DEBUG);
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    FLAGS_db_root_path = "/tmp/" + ::rtidb::tablet::GenRand();
    return RUN_ALL_TESTS();
}



