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


#ifndef SRC_BASE_COUNT_DOWN_LATCH_H_
#define SRC_BASE_COUNT_DOWN_LATCH_H_
#include <condition_variable> // NOLINT
#include <mutex> // NOLINT

namespace openmldb {
namespace base {

class CountDownLatch {
 public:
    explicit CountDownLatch(int32_t count) : count_(count), mu_(), cv_() {}
    ~CountDownLatch() {}

    void CountDown() {
        std::lock_guard<std::mutex> lock(mu_);
        if (--count_ <= 0) {
            cv_.notify_all();
        }
    }

    void TimeWait(uint64_t timeout) {
        std::unique_lock<std::mutex> lock(mu_);
        cv_.wait_for(lock, std::chrono::milliseconds(timeout));
    }

    void Wait() {
        std::unique_lock<std::mutex> lock(mu_);
        while (count_ > 0) {
            cv_.wait(lock);
        }
    }

    bool IsDone() {
        std::lock_guard<std::mutex> lock(mu_);
        return count_ <= 0;
    }

    uint32_t GetCount() {
        std::lock_guard<std::mutex> lock(mu_);
        return count_;
    }

 private:
    int32_t count_;
    std::mutex mu_;
    std::condition_variable cv_;
};

}  // namespace base
}  // namespace openmldb
#endif  // SRC_BASE_COUNT_DOWN_LATCH_H_
