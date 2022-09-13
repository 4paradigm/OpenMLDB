/*
 *  Copyright 2021 4Paradigm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <gtest/gtest.h>
#include <unistd.h>

#include <thread>
#include <unordered_map>

#include "common/ring_buffer.h"
#include "common/switch_buffer.h"

namespace streaming {
namespace interval_join {

class BufferTest : public ::testing::Test {};

TEST_F(BufferTest, RingBufferSmokeTest) {
    int buf_size = 1024;
    RingBuffer buffer(buf_size);
    EXPECT_FALSE(buffer.Avail());
    EXPECT_FALSE(buffer.Full());

    Element ele;
    EXPECT_TRUE(buffer.Put(&ele));
    EXPECT_TRUE(buffer.Avail());
    EXPECT_FALSE(buffer.Full());
    EXPECT_EQ(1, buffer.count());

    auto p = buffer.Get();
    EXPECT_EQ(p, &ele);
    EXPECT_FALSE(buffer.Avail());
    EXPECT_FALSE(buffer.Full());
    EXPECT_EQ(0, buffer.count());

    Element eles[buf_size];  // NOLINT
    for (int i = 0; i < buf_size; i++) {
        eles[i] = Element();
    }

    for (int i = 0; i < buf_size / 2; i++) {
        EXPECT_TRUE(buffer.Put(&eles[i]));
    }
    EXPECT_EQ(buf_size / 2, buffer.count());
    EXPECT_FALSE(buffer.Full());
    EXPECT_TRUE(buffer.Avail());

    for (int i = 0; i < buf_size / 2; i++) {
        auto ele = buffer.Get();
        EXPECT_EQ(&eles[i], ele);
    }
    EXPECT_EQ(nullptr, buffer.Get());

    for (int i = 0; i < buf_size; i++) {
        EXPECT_TRUE(buffer.Put(&eles[i]));
    }
    EXPECT_FALSE(buffer.Put(&eles[0]));
    EXPECT_EQ(buf_size, buffer.count());
    EXPECT_TRUE(buffer.Full());
    EXPECT_TRUE(buffer.Avail());

    for (int i = 0; i < buf_size; i++) {
        auto ele = buffer.Get();
        EXPECT_EQ(&eles[i], ele);
    }
    EXPECT_EQ(nullptr, buffer.Get());
}

TEST_F(BufferTest, RingBufferBlockingTest) {
    int buf_size = 1;
    RingBuffer buffer(buf_size);
    EXPECT_FALSE(buffer.Avail());
    EXPECT_FALSE(buffer.Full());

    Element ele;
    EXPECT_TRUE(buffer.Put(&ele, true));
    EXPECT_TRUE(buffer.Avail());
    EXPECT_TRUE(buffer.Full());
    EXPECT_EQ(1, buffer.count());

    Element ele1;
    EXPECT_FALSE(buffer.Put(&ele1, false));
    EXPECT_FALSE(buffer.Put(&ele1, true, 100));

    auto p = buffer.Get(true, 100);
    EXPECT_EQ(p, &ele);
    EXPECT_FALSE(buffer.Avail());
    EXPECT_FALSE(buffer.Full());
    EXPECT_EQ(0, buffer.count());

    p = buffer.Get(true, 100);
    EXPECT_EQ(nullptr, p);
}

TEST_F(BufferTest, RingBufferThreadTest) {
    int buf_size = 102400;
    int producer = 8;
    RingBuffer buffer(buf_size, producer);
    std::unordered_map<Element*, int> ele_map;
    Element* eles[buf_size * producer];
    for (int i = 0; i < buf_size * producer; i++) {
        eles[i] = new Element();
        ele_map[eles[i]] = 0;
    }

    auto produce = [&](int pid) {
        int partition_num = buf_size;
        for (int i = pid * partition_num; i < (pid + 1) * partition_num; i++) {
            EXPECT_TRUE(buffer.Put(eles[i], true));
        }
    };

    auto consume = [&]() {
        for (int i = 0; i < buf_size * producer; i++) {
            auto ele = buffer.Get(true);
            EXPECT_NE(nullptr, ele) << "ele " << i << " failed";
            EXPECT_TRUE(ele_map.count(ele));
            ele_map[ele]++;
            EXPECT_EQ(1, ele_map[ele]);
        }
    };

    std::vector<std::thread> producers;
    for (int i = 0; i < producer; i++) {
        producers.emplace_back(produce, i);
    }
    auto consumer = std::thread(consume);
    for (int i = 0; i < producer; i++) {
        producers[i].join();
    }
    consumer.join();

    for (int i = 0; i < buf_size * producer; i++) {
        delete eles[i];
    }
}

TEST_F(BufferTest, SingleBufferFullTest) {
    SingleBuffer buffer(10);
    EXPECT_FALSE(buffer.Full());
    Element ele;
    EXPECT_TRUE(buffer.Put(&ele));
    buffer.MarkDone();
    EXPECT_TRUE(buffer.Full());
    EXPECT_TRUE(buffer.Avail());
    EXPECT_EQ(&ele, buffer.Get());
    EXPECT_FALSE(buffer.Avail());
    EXPECT_FALSE(buffer.Full());

    EXPECT_TRUE(buffer.Put(&ele));
    EXPECT_FALSE(buffer.Avail());
    EXPECT_FALSE(buffer.Full());
}

TEST_F(BufferTest, SingleBufferTest) {
    int buf_size = 10;
    SingleBuffer buffer(buf_size);
    EXPECT_FALSE(buffer.Full());
    EXPECT_FALSE(buffer.Avail());

    Element* eles[buf_size];
    for (int i = 0; i < buf_size; i++) {
        eles[i] = new Element();
    }

    for (int i = 0; i < buf_size; i++) {
        EXPECT_EQ(0, buffer.count());
        EXPECT_TRUE(buffer.Put(eles[i]));
    }
    EXPECT_EQ(buf_size, buffer.count());

    for (int i = 0; i < buf_size; i++) {
        EXPECT_EQ(eles[i], buffer.Get());
        EXPECT_EQ(buf_size - 1 - i, buffer.count());
    }

    for (int i = 0; i < buf_size; i++) {
        delete eles[i];
    }
}

TEST_F(BufferTest, SingleBufferThreadTest) {
    int buf_size = 1024000;
    SingleBuffer buffer(buf_size);
    EXPECT_FALSE(buffer.Full());
    EXPECT_FALSE(buffer.Avail());

    Element* eles[buf_size];
    for (int i = 0; i < buf_size; i++) {
        eles[i] = new Element();
    }

    auto produce = [&]() {
        for (int i = 0; i < buf_size; i++) {
            EXPECT_TRUE(buffer.Put(eles[i]));
        }
    };

    auto consume = [&]() {
        while (!buffer.Avail()) {}

        for (int i = 0; i < buf_size; i++) {
            EXPECT_EQ(eles[i], buffer.Get());
        }
    };

    std::thread consumer(consume);
    std::thread producer(produce);

    producer.join();
    consumer.join();

    for (int i = 0; i < buf_size; i++) {
        delete eles[i];
    }
}

TEST_F(BufferTest, MultiBufferCountTest) {
    int buf_size = 10;
    Element* eles[buf_size];
    for (int i = 0; i < buf_size; i++) {
        eles[i] = new Element();
    }

    {
        int buf_size = 8;
        MultiBuffer buffer(buf_size);
        EXPECT_FALSE(buffer.Full());
        EXPECT_FALSE(buffer.Avail());
        EXPECT_EQ(0, buffer.count());

        for (int i = 0; i < buf_size; i++) {
            EXPECT_TRUE(buffer.Put(eles[i]));
        }
        EXPECT_EQ(buf_size, buffer.count());

        EXPECT_FALSE(buffer.Put(eles[0]));
    }

    {
        int buf_size = 9;
        // real size will be 5 * 2 = 10
        MultiBuffer buffer(buf_size);
        EXPECT_FALSE(buffer.Full());
        EXPECT_FALSE(buffer.Avail());
        EXPECT_EQ(0, buffer.count());

        for (int i = 0; i < buf_size; i++) {
            EXPECT_TRUE(buffer.Put(eles[i]));
        }
        EXPECT_EQ((buf_size + 1) / 2, buffer.count());

        EXPECT_TRUE(buffer.Put(eles[0]));
        EXPECT_EQ((buf_size + 1) / 2 * 2, buffer.count());
    }

    for (int i = 0; i < buf_size; i++) {
        delete eles[i];
    }
}

TEST_F(BufferTest, MultiBufferTest) {
    int buf_size = 1024000;
    MultiBuffer buffer(buf_size);
    EXPECT_FALSE(buffer.Full());
    EXPECT_FALSE(buffer.Avail());

    Element* eles[buf_size];
    for (int i = 0; i < buf_size; i++) {
        eles[i] = new Element();
    }

    auto produce = [&]() {
        for (int i = 0; i < buf_size; i++) {
            EXPECT_TRUE(buffer.Put(eles[i]));
        }
    };

    auto consume = [&]() {
        while (!buffer.Avail()) {}

        for (int i = 0; i < buf_size; i++) {
            EXPECT_EQ(eles[i], buffer.Get(true));
        }
    };

    std::thread consumer(consume);
    std::thread producer(produce);

    producer.join();
    consumer.join();

    for (int i = 0; i < buf_size; i++) {
        delete eles[i];
    }
}

TEST_F(BufferTest, SwitchBufferCountTest) {
    int buf_size = 12;
    int producer_num = 2;
    Element* eles[buf_size];
    for (int i = 0; i < buf_size; i++) {
        eles[i] = new Element();
        eles[i]->set_sid(i % producer_num);
    }

    {
        int buf_size = 8;
        SwitchBuffer buffer(buf_size, producer_num);
        EXPECT_FALSE(buffer.Full());
        EXPECT_FALSE(buffer.Avail());
        EXPECT_EQ(0, buffer.count());

        for (int i = 0; i < buf_size; i++) {
            EXPECT_TRUE(buffer.Put(eles[i]));
        }
        EXPECT_EQ(buf_size, buffer.count());
        EXPECT_FALSE(buffer.Put(eles[8]));
    }

    {
        int buf_size = 9;
        // real size will be (3*2) * 2 = 12
        SwitchBuffer buffer(buf_size, producer_num);
        EXPECT_FALSE(buffer.Full());
        EXPECT_FALSE(buffer.Avail());
        EXPECT_EQ(0, buffer.count());

        for (int i = 0; i < buf_size; i++) {
            EXPECT_TRUE(buffer.Put(eles[i]));
        }
        EXPECT_EQ(3 * 2, buffer.count());

        for (int i = buf_size; i < 12; i++) {
            EXPECT_TRUE(buffer.Put(eles[i]));
        }
        EXPECT_EQ(3 * 2 * 2, buffer.count());
    }

    for (int i = 0; i < buf_size; i++) {
        delete eles[i];
    }
}

TEST_F(BufferTest, SwitchBufferTest) {
    int buf_size = 1024000;
    int producer_num = 8;
    SwitchBuffer buffer(buf_size, producer_num);
    EXPECT_FALSE(buffer.Full());
    EXPECT_FALSE(buffer.Avail());
    int times = 2;

    Element* eles[buf_size];
    for (int i = 0; i < buf_size; i++) {
        eles[i] = new Element();
        eles[i]->set_sid(i % producer_num);
    }

    auto produce = [&](int id) {
        for (int j = 0; j < times; j++) {
            for (int i = id; i < buf_size; i += producer_num) {
                EXPECT_TRUE(buffer.Put(eles[i], true));
            }
        }
    };

    auto consume = [&]() {
        while (!buffer.Avail()) {}

        for (int j = 0; j < times; j++) {
            for (int i = 0; i < buf_size; i++) {
                EXPECT_NE(nullptr, buffer.Get(true));
            }
        }
    };
    std::vector<std::thread> producers;

    for (int i = 0; i < producer_num; i++) {
        producers.emplace_back(produce, i);
    }
    std::thread consumer(consume);

    for (int i = 0; i < producer_num; i++) {
        producers[i].join();
    }
    consumer.join();

    for (int i = 0; i < buf_size; i++) {
        delete eles[i];
    }
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
