//
// zk_client_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-21
//

#include "zk/zk_client.h"
#include "zk/dist_lock.h"
#include <gtest/gtest.h>
#include <boost/bind.hpp>
#include <sched.h>
#include <unistd.h>
#include "logging.h"
extern "C" {
#include "zookeeper/zookeeper.h"
} 

using ::baidu::common::INFO;

namespace rtidb {
namespace zk {

static bool call_invoked = false;
class DistLockTest : public ::testing::Test {

public:
    DistLockTest() {}

    ~DistLockTest() {}
};

void OnLockedCallback() {
    call_invoked = true;
}

void OnLostCallback() {}


TEST_F(DistLockTest, Lock) {
    ZkClient client("127.0.0.1:12181", 1000, "127.0.0.1:9527", "/rtidb_lock");
    bool ok = client.Init();
    ASSERT_TRUE(ok);
    DistLock lock("/rtidb_lock/nameserver_lock", &client, boost::bind(&OnLockedCallback), boost::bind(&OnLostCallback), "endpoint1");
    lock.Lock();
    sleep(5);
    if (!call_invoked) {
        lock.Stop();
        ASSERT_TRUE(false);
    }
    ASSERT_TRUE(lock.IsLocked());
    std::string current_lock;
    lock.CurrentLockValue(current_lock);
    ASSERT_EQ("endpoint1", current_lock);
    call_invoked = false;
    ZkClient client2("127.0.0.1:12181", 1000, "127.0.0.1:9527", "/rtidb_lock");
    ok = client2.Init();
    if (!ok) {
        lock.Stop();
        ASSERT_TRUE(false);
    }
    DistLock lock2("/rtidb_lock/nameserver_lock", &client2, boost::bind(&OnLockedCallback), boost::bind(&OnLostCallback), "endpoint2");
    lock2.Lock();
    sleep(5);
    if (!call_invoked) {
        lock.Stop();
        ASSERT_TRUE(false);
    }
    lock2.CurrentLockValue(current_lock);
    ASSERT_EQ("endpoint1", current_lock);
    lock.Stop();
    lock2.Stop();
}

}
}

int main(int argc, char** argv) {
    srand (time(NULL));
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

