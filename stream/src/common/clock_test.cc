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
#include <thread>
#include <vector>

#include "common/clock.h"

namespace streaming {
namespace interval_join {

class ClockTest : public ::testing::Test {};

TEST_F(ClockTest, ClockSmokeTest) {
    {
        Clock clock(1);
        EXPECT_EQ(1, clock.GetTime());

        clock.UpdateTime(0);
        EXPECT_EQ(1, clock.GetTime());
        EXPECT_EQ(1, clock.GetWatermark());

        clock.UpdateTime(100);
        EXPECT_EQ(100, clock.GetTime());
    }

    {
        Clock clock;
        EXPECT_EQ(0, clock.GetTime());

        clock.UpdateTime(0);
        EXPECT_EQ(0, clock.GetTime());

        clock.UpdateTime(UINT64_MAX);
        EXPECT_EQ(UINT64_MAX, clock.GetTime());
    }
}

TEST_F(ClockTest, ClockThreadTest) {
    Clock clock;
    int thread_num = 8;
    size_t ts = 1000000;
    std::vector<std::thread> threads;

    auto update_clock = [&](bool reverse = false) {
        if (reverse) {
            for (size_t i = ts; i > 0; i--) {
                clock.UpdateTime(ts);
                EXPECT_GE(clock.GetTime() - i, 0);
            }
        } else {
            for (size_t i = 0; i < ts; i++) {
                clock.UpdateTime(ts);
                EXPECT_GE(clock.GetTime() - i, 0);
            }
        }
    };
    for (int i = 0; i < thread_num; i++) {
        bool reverse = false;
        if (i % 2 == 0) {
            reverse = true;
        }
        threads.emplace_back(update_clock, reverse);
    }

    for (int i = 0; i < thread_num; i++) {
        threads[i].join();
    }

    EXPECT_EQ(ts, clock.GetTime());
}

}  // namespace interval_join
}  // namespace streaming

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
