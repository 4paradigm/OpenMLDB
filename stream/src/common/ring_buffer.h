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
#ifndef STREAM_SRC_COMMON_RING_BUFFER_H_
#define STREAM_SRC_COMMON_RING_BUFFER_H_

#include <atomic>
#include <vector>
#include "common/buffer.h"
#include "common/element.h"

namespace streaming {
namespace interval_join {

class RingBuffer : public Buffer {
 public:
    explicit RingBuffer(size_t size, int producers = 1)
        : Buffer(size + producers), producers_(producers), ring_(size_) {}

    ~RingBuffer() override {}

    inline bool Avail() override {
        return LoadProducerPos() != LoadConsumerPos();
    }

    // IfFull may cause false-positive in multi-thread environment
    // which means it is not full actually, but return true in practice.
    // It doesn't affect the correctness.
    // false-negative will affect the correctness,
    // but it is not possible as we reserve the #producers_ slots
    inline bool Full() override {
        int64_t p_pos = LoadProducerPos();
        int64_t c_pos = LoadConsumerPos();
        if (c_pos == p_pos) {
            return false;
        } else if (p_pos > c_pos) {
           return p_pos + producers_ >= size_ && (p_pos + producers_ - size_) >= c_pos;
        } else {
            return (p_pos + producers_) >= c_pos;
        }
    }

    // an estimate in multi-threaded environment
    inline int64_t count() const override {
        int64_t p_pos = LoadProducerPos();
        int64_t c_pos = LoadConsumerPos();
        if (p_pos >= c_pos) {
            return p_pos - c_pos;
        } else {
            return p_pos + size_ - c_pos;
        }
    }

    Element* GetUnblocking() override;
    bool PutUnblocking(Element* ele) override;

 private:
    std::vector<std::atomic<Element*>> ring_;
    // multiple producers
    // current free slot
    std::atomic<uint64_t> p_pos_ = 0;
    // single consumer
    // current avail slot
    std::atomic<uint64_t> c_pos_ = 0;
    // max write parallelism supported (max #producers allowed)
    // #producers_ slots are wasted for high concurrency
    int producers_ = 1;

    inline int64_t FetchAndIncrProducerPos() {
        // UINT64_MAX + 1 -> 0
        uint64_t idx = p_pos_++;
        return idx % size_;
    }

    inline int64_t FetchAndIncrConsumerPos() {
        // UINT64_MAX + 1 -> 0
        uint64_t idx = c_pos_++;
        return idx % size_;
    }

    inline int64_t LoadProducerPos() const {
        return p_pos_.load(std::memory_order_relaxed) % size_;
    }

    inline int64_t LoadConsumerPos() const {
        return c_pos_.load(std::memory_order_relaxed) % size_;
    }

    int64_t GetFreeSlot() {
        if (Full()) {
            return -1;
        }

        return FetchAndIncrProducerPos();
    }
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_COMMON_RING_BUFFER_H_
