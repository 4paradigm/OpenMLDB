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

#ifndef SRC_BASE_RINGQUEUE_H_
#define SRC_BASE_RINGQUEUE_H_

#include <condition_variable>  // NOLINT
#include <cstdint>
#include <mutex>  // NOLINT

namespace openmldb {
namespace base {

template <class T>
class RingQueue {
 public:
    explicit RingQueue(uint32_t size = 100) : max_size_(size), buf_(new T[size]), head_(0), tail_(0), full_(false) {}

    ~RingQueue() { delete[] buf_; }
    bool full() const { return full_; }

    bool empty() const { return (!full_ && (head_ == tail_)); }

    uint32_t capacity() const { return max_size_; }

    uint32_t size() const {
        uint32_t size = max_size_;

        if (!full_) {
            if (head_ >= tail_) {
                size = head_ - tail_;
            } else {
                size = max_size_ + head_ - tail_;
            }
        }
        return size;
    }

    void put(const T& item) {
        buf_[head_] = item;
        head_ = (head_ + 1) % max_size_;
        full_ = head_ == tail_;
    }
    const T& pop() {
        const auto& val = buf_[tail_];

        full_ = false;
        tail_ = (tail_ + 1) % max_size_;

        return val;
    }

 private:
    const uint32_t max_size_;
    T* buf_;
    uint32_t head_;
    uint32_t tail_;
    bool full_;
};

}  // namespace base
}  // namespace openmldb

#endif  // SRC_BASE_RINGQUEUE_H_
