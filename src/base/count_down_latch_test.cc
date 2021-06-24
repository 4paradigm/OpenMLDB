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

#include "base/count_down_latch.h"

#include "boost/bind.hpp"
#include "common/thread_pool.h"
#include "gtest/gtest.h"

using ::baidu::common::ThreadPool;

namespace openmldb {
namespace base {

class CountDownLatchTest : public ::testing::Test {
 public:
    CountDownLatchTest() {}
    ~CountDownLatchTest() {}
};

void DoCountDown(CountDownLatch* cdl) { cdl->CountDown(); }

TEST_F(CountDownLatchTest, IsDone) {
    CountDownLatch latch(3);
    latch.CountDown();
    latch.CountDown();
    ASSERT_FALSE(latch.IsDone());
    latch.CountDown();
    ASSERT_TRUE(latch.IsDone());
}

TEST_F(CountDownLatchTest, Invalid) {
    CountDownLatch latch(-1);
    ASSERT_TRUE(latch.IsDone());
}

TEST_F(CountDownLatchTest, MultiIsDone) {
    CountDownLatch latch(3);
    ThreadPool pool(1);
    pool.AddTask(boost::bind(&DoCountDown, &latch));
    latch.TimeWait(10);
    ASSERT_FALSE(latch.IsDone());
    ASSERT_EQ(2u, latch.GetCount());
    pool.AddTask(boost::bind(&DoCountDown, &latch));
    pool.AddTask(boost::bind(&DoCountDown, &latch));
    latch.Wait();
    ASSERT_TRUE(latch.IsDone());
    ASSERT_EQ(0u, latch.GetCount());
}

}  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
