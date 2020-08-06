//
// zk_client_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-21
//

#include "zk/zk_client.h"
#include <gtest/gtest.h>
#include <sched.h>
#include <unistd.h>
#include <boost/bind.hpp>
#include "base/glog_wapper.h" // NOLINT
extern "C" {
#include "zookeeper/zookeeper.h"
}



namespace rtidb {
namespace zk {

static bool call_invoked = false;
static int32_t endpoint_size = 2;
class ZkClientTest : public ::testing::Test {
 public:
    ZkClientTest() {}

    ~ZkClientTest() {}
};

inline std::string GenRand() { return std::to_string(rand() % 10000000 + 1); } // NOLINT

void WatchCallback(const std::vector<std::string>& endpoints) {
    PDLOG(INFO, "call back with endpoints size %d", endpoints.size());
    ASSERT_EQ(endpoint_size, endpoints.size());
    call_invoked = true;
}

TEST_F(ZkClientTest, BadZk) {
    ZkClient client("127.0.0.1:13181", "", 1000, "127.0.0.1:9527", "/rtidb");
    bool ok = client.Init();
    ASSERT_FALSE(ok);
}

TEST_F(ZkClientTest, Init) {
    ZkClient client("127.0.0.1:6181", "", 1000, "127.0.0.1:9527", "/rtidb");
    bool ok = client.Init();
    ASSERT_TRUE(ok);
    ok = client.Register();
    ASSERT_TRUE(ok);
    std::vector<std::string> endpoints;
    ok = client.GetNodes(endpoints);
    ASSERT_TRUE(ok);
    uint32_t size = 1;
    ASSERT_EQ(size, endpoints.size());
    ASSERT_EQ("127.0.0.1:9527", endpoints[0]);
    client.WatchNodes(boost::bind(&WatchCallback, _1));
    // trigger watch
    ok = client.WatchNodes();
    ASSERT_TRUE(ok);
    {
        ZkClient client2(
                "127.0.0.1:6181", "", 1000, "127.0.0.1:9528", "/rtidb");
        ok = client2.Init();
        client2.Register();
        ASSERT_TRUE(ok);
        sleep(5);
        ASSERT_TRUE(call_invoked);
        endpoint_size = 1;
    }
    sleep(5);
}

TEST_F(ZkClientTest, CreateNode) {
    ZkClient client("127.0.0.1:6181", "", 1000, "127.0.0.1:9527", "/rtidb1");
    bool ok = client.Init();
    ASSERT_TRUE(ok);

    std::string assigned_path;
    ok = client.CreateNode("/rtidb1/lock/request", "",
                           ZOO_EPHEMERAL | ZOO_SEQUENCE, assigned_path);
    ASSERT_TRUE(ok);

    std::string node = "/rtidb1/test/node" + GenRand();
    int ret = client.IsExistNode(node);
    ASSERT_EQ(ret, 1);
    ok = client.CreateNode(node, "value");
    ASSERT_TRUE(ok);
    ret = client.IsExistNode(node);
    ASSERT_EQ(ret, 0);

    ZkClient client2("127.0.0.1:6181", "", 1000, "127.0.0.1:9527", "/rtidb1");
    ok = client2.Init();
    ASSERT_TRUE(ok);

    std::string assigned_path1;
    ok = client2.CreateNode("/rtidb1/lock/request", "",
                            ZOO_EPHEMERAL | ZOO_SEQUENCE, assigned_path1);
    ASSERT_TRUE(ok);
}

TEST_F(ZkClientTest, ZkNodeChange) {
    ZkClient client("127.0.0.1:6181", "", 1000, "127.0.0.1:9527", "/rtidb1");
    bool ok = client.Init();
    ASSERT_TRUE(ok);

    std::string node = "/rtidb1/test/node" + GenRand();
    int ret = client.IsExistNode(node);
    ASSERT_EQ(ret, 1);
    ok = client.CreateNode(node, "1");
    ASSERT_TRUE(ok);
    ret = client.IsExistNode(node);
    ASSERT_EQ(ret, 0);

    ZkClient client2("127.0.0.1:6181", "", 1000, "127.0.0.1:9527", "/rtidb1");
    ok = client2.Init();
    ASSERT_TRUE(ok);
    bool detect = false;
    ok = client2.WatchItem(node, [&detect]{ detect = true; });
    ASSERT_TRUE(ok);
    ok = client.SetNodeValue(node, "2");
    ASSERT_TRUE(ok);
    for (int i = 0 ; i < 10; i++) {
        if (detect) {
            break;
        }
        sleep(1);
    }
    ASSERT_TRUE(detect);
    detect = false;
    ok = client.SetNodeValue(node, "3");
    ASSERT_TRUE(ok);
    for (int i = 0 ; i < 10; i++) {
        if (detect) {
            break;
        }
        sleep(1);
    }
    ASSERT_TRUE(detect);
}

}  // namespace zk
}  // namespace rtidb

int main(int argc, char** argv) {
    srand(time(NULL));
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
