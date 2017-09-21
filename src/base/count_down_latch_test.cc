//
// count_down_latch_test.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-21
//

#include "count_down_latch.h"
#include "boost/bind.hpp"
#include "gtest/gtest.h"
#include "thread_pool.h"

using ::baidu::common::ThreadPool;

namespace rtidb {
namespace base {

class CountDownLatchTest : public ::testing::Test {

public:
    CountDownLatchTest() {}
    ~CountDownLatchTest() {}
};


void DoCountDown(CountDownLatch* cdl) {
    cdl->CountDown();
}

TEST_F(CountDownLatchTest, IsDone) {
    CountDownLatch latch(3);
    latch.CountDown();
    latch.CountDown();
    ASSERT_FALSE(latch.IsDone());
    latch.CountDown();
    ASSERT_TRUE(latch.IsDone());
}

TEST_F(CountDownLatchTest, MultiIsDone) {
    CountDownLatch latch(3);
    ThreadPool pool(1);
    pool.AddTask(boost::bind(&DoCountDown, &latch));
    latch.TimeWait(10);
    ASSERT_FALSE(latch.IsDone());
    ASSERT_EQ(2, latch.GetCount());
    pool.AddTask(boost::bind(&DoCountDown, &latch));
    pool.AddTask(boost::bind(&DoCountDown, &latch));
    latch.Wait();
    ASSERT_TRUE(latch.IsDone());
    ASSERT_EQ(0, latch.GetCount());
}

}
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



