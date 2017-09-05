//
// zk_client_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-21
//

#include "zk/zk_client.h"
#include <gtest/gtest.h>
#include <boost/bind.hpp>
#include <sched.h>
#include <unistd.h>
#include "logging.h"

using ::baidu::common::INFO;

namespace rtidb {
namespace zk {

static bool call_invoked = false;
class ZkClientTest : public ::testing::Test {

public:
    ZkClientTest() {}

    ~ZkClientTest() {}
};

void WatchCallback(const std::vector<std::string>& endpoints) {
    LOG(INFO, "call back with endpoints size %d", endpoints.size());
    ASSERT_EQ(2, endpoints.size());
    call_invoked = true;
}

TEST_F(ZkClientTest, Init) {
    ZkClient client("127.0.0.1:2181", 1000, "127.0.0.1:9527", "/rtidb");
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
    client.WatchNodes();

    ZkClient client2("127.0.0.1:2181", 1000, "127.0.0.1:9528", "/rtidb");
    ok = client2.Init();
    client2.Register();
    ASSERT_TRUE(ok);
    sleep(10);
    ASSERT_TRUE(call_invoked);
}
}
}

int main(int argc, char** argv) {
    srand (time(NULL));
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

