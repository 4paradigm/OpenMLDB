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
#ifndef STREAM_SRC_COMMON_SWITCH_BUFFER_H_
#define STREAM_SRC_COMMON_SWITCH_BUFFER_H_

#include <glog/logging.h>
#include <atomic>
#include <memory>
#include <utility>
#include <vector>
#include "common/buffer.h"

namespace streaming {
namespace interval_join {

/*
     * not thread-safe
     * 1. write all data to buffer
     * 2. read all data from buffer
 */
class SingleBuffer : public Buffer {
 public:
    explicit SingleBuffer(size_t size) : Buffer(size), buffer_(size) {}

    // whenever r_idx_ == w_idx_, Reset the buffer
    // so it is one-time consumed only
    Element* GetUnblocking() override;

    bool PutUnblocking(Element* ele) override;

    // write can only occur before read starts
    // if read starts, `Full()` will always return true
    bool Full() override {
        return ready_to_read_ || w_idx_ >= size_;
    }

    bool Avail() override {
        return ready_to_read_;
    }

    int64_t count() const override;

    void MarkDone(bool done = true) override {
        if (r_idx_ < w_idx_) {
            ready_to_read_ = true;
        }
        Buffer::MarkDone(done);
    }

    Element* head() const {
        if (w_idx_ > 0) {
            return buffer_.at(0);
        } else {
            return nullptr;
        }
    }

    Element* tail() const {
        if (w_idx_ > 0) {
            return buffer_.at(w_idx_ - 1);
        } else {
            return nullptr;
        }
    }

 private:
    std::vector<Element*> buffer_;
    int r_idx_ = 0;
    int w_idx_ = 0;
    std::atomic<bool> ready_to_read_ = false;

    void Reset() {
        r_idx_ = 0;
        w_idx_ = 0;
    }
};

class MultiBuffer : public Buffer {
 public:
    static constexpr int kSwitchSize = 2;
    explicit MultiBuffer(size_t size);

    explicit MultiBuffer(MultiBuffer&& buf);

    // will only be called in consumer thread
    Element* GetUnblocking() override;

    bool PutUnblocking(Element* element) override;

    // write can only occur before read starts
    bool Full() override {
        return !Free();
    }

    // will only be called in consumer thread
    bool Avail() override;

    int64_t count() const override;

    void MarkDone(bool done = true) override;

 private:
    std::atomic<int> r_idx_ = -1;
    int r_count_ = 0;
    size_t s_size_;
    std::atomic<int> w_idx_ = -1;
    std::unique_ptr<SingleBuffer> buffers_[kSwitchSize];

    bool Free();
};

class SwitchBuffer : public Buffer {
 public:
    // size is the total size of the buffer
    SwitchBuffer(size_t size, int producers);
    ~SwitchBuffer() {}

    // will only be called in the consumer thread
    bool Avail() override;
    bool Full() override;

    // if blocking is false
    // return the current element if available
    // otherwise return nullptr
    //
    // if blocking is true
    // block until there is an element to return or timeout
    // timeout == -1, means wait forever without timeout
    Element* GetUnblocking() override;

    // use the non-const Element* in case we may update some element-wise stats later
    // if blocking is false
    // insert the element if there is available slot and return true
    // otherwise return false
    //
    // if blocking is true
    // block until there is an element to return true or timeout
    // timeout == -1, means wait forever without timeout
    bool PutUnblocking(Element* element) override;

    int64_t count() const override;

    void MarkDone(bool done = true) override;

 private:
    std::vector<MultiBuffer> buffers_;
    int r_idx_ = 0;
    int producers_;
};

}  // namespace interval_join
}  // namespace streaming
#endif  // STREAM_SRC_COMMON_SWITCH_BUFFER_H_
