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

#include "zk/dist_lock.h"

#include <gtest/gtest.h>
#include <sched.h>
#include <unistd.h>

#include <boost/bind.hpp>

#include "zk/zk_client.h"
extern "C" {
#include "zookeeper/zookeeper.h"
}

namespace openmldb {
namespace zk {

static bool call_invoked = false;
class DistLockTest : public ::testing::Test {
 public:
    DistLockTest() {}

    ~DistLockTest() {}
};

void OnLockedCallback() { call_invoked = true; }

void OnLostCallback() {}

TEST_F(DistLockTest, Lock) {
    ZkClient client("127.0.0.1:6181", "", 10000, "127.0.0.1:9527", "/openmldb_lock");
    bool ok = client.Init();
    ASSERT_TRUE(ok);
    DistLock lock("/openmldb_lock/nameserver_lock", &client, boost::bind(&OnLockedCallback),
                  boost::bind(&OnLostCallback), "endpoint1");
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
    ZkClient client2("127.0.0.1:6181", "", 10000, "127.0.0.1:9527", "/openmldb_lock");
    ok = client2.Init();
    if (!ok) {
        lock.Stop();
        ASSERT_TRUE(false);
    }
    DistLock lock2("/openmldb_lock/nameserver_lock", &client2, boost::bind(&OnLockedCallback),
                   boost::bind(&OnLostCallback), "endpoint2");
    lock2.Lock();
    sleep(5);
    ASSERT_FALSE(call_invoked);
    lock2.CurrentLockValue(current_lock);
    ASSERT_EQ("endpoint1", current_lock);
    lock.Stop();
    lock2.Stop();
}

}  // namespace zk
}  // namespace openmldb

int main(int argc, char** argv) {
    srand(time(NULL));
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
