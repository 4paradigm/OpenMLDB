//
// Created by kongsys on 8/20/19.
//

#include "ringqueue.h"
#include "gtest/gtest.h"


namespace  rtidb {
namespace base {

class RingQueueTest : public ::testing::Test {

public:
    RingQueueTest() {}
    ~RingQueueTest() {}
};

TEST_F(RingQueueTest, full) {
    uint32_t size = 10;
    RingQueue<uint32_t> rq(size);
    for (uint32_t i = 0; i < size; i++) {
        rq.put(i);
    }
    ASSERT_TRUE(rq.full());
    rq.pop();
    ASSERT_FALSE(rq.full());

}

TEST_F(RingQueueTest, empty) {
    uint32_t  size = 10;
    RingQueue<uint32_t> rq(size);
    ASSERT_TRUE(rq.empty());
    rq.put(size);
    ASSERT_FALSE(rq.empty());
}

TEST_F(RingQueueTest, capacity) {
    uint32_t  size = 10;
    RingQueue<uint32_t> rq(size);
    ASSERT_EQ(size, rq.capacity());
}

TEST_F(RingQueueTest, size) {
    uint32_t size = 10;
    RingQueue<uint32_t> rq(size);
    for (uint32_t i = 1; i <= size; i++) {
        rq.put(i);
        ASSERT_EQ(i, rq.size());
    }
    ASSERT_TRUE(rq.full());
    for (uint32_t i = 1; i <= size/5; i++) {
	rq.pop();
    }
    for (uint32_t i = 1; i <= size/5; i++) {
	rq.put(size + i);
    }
    ASSERT_EQ(size, rq.size());
}

TEST_F(RingQueueTest, pop) {
    uint32_t size = 10;
    RingQueue<uint32_t> rq(size);
    for (uint32_t i = 1; i <= size; i++) {
        rq.put(i);
        ASSERT_EQ(i, rq.pop());
    }
}

};
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
