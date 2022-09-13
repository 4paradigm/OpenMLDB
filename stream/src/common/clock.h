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
#ifndef STREAM_SRC_COMMON_CLOCK_H_
#define STREAM_SRC_COMMON_CLOCK_H_

#include <common/timer.h>
#include <cstddef>
#include <atomic>

namespace streaming {
namespace interval_join {

// all in microsecond
class Clock {
 public:
    static Clock& GetClock() {
        static Clock clock;
        return clock;
    }

    explicit Clock(size_t init = 0) : curr_ts_(init) {}

    void UpdateTime(size_t t) {
        auto curr_ts = curr_ts_.load(std::memory_order_relaxed);
        if (t > curr_ts) {
            curr_ts_.store(t, std::memory_order_relaxed);
        }
    }

    size_t GetTime() const {
        return curr_ts_.load( std::memory_order_relaxed);;
    }

    static size_t GetSysTime() {
        return ::baidu::common::timer::get_micros();
    }

    // watermark is simply the max of all eventTime arrived in the system
    // == Flink::BoundedOutOfOrdernessTimestampExtractor(maxOutOfOrderness=0)
    size_t GetWatermark() const {
        return GetTime();
    }

 private:
    std::atomic<size_t> curr_ts_ = 0;
};

}  // namespace interval_join
}  // namespace streaming


#endif  // STREAM_SRC_COMMON_CLOCK_H_
