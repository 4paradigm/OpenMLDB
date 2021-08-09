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

#include "base/ringqueue.h"

#include "gtest/gtest.h"

namespace openmldb {
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
    uint32_t size = 10;
    RingQueue<uint32_t> rq(size);
    ASSERT_TRUE(rq.empty());
    rq.put(size);
    ASSERT_FALSE(rq.empty());
}

TEST_F(RingQueueTest, capacity) {
    uint32_t size = 10;
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
    for (uint32_t i = 1; i <= size / 5; i++) {
        rq.pop();
    }
    for (uint32_t i = 1; i <= size / 5; i++) {
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

};  // namespace base
}  // namespace openmldb

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
