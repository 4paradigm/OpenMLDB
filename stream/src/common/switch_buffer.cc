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

#include "common/switch_buffer.h"

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <unistd.h>
#include <limits>

namespace streaming {
namespace interval_join {

SwitchBuffer::SwitchBuffer(size_t size, int producers) : Buffer(size), producers_(producers) {
    size_t per_producer_size = (size + producers - 1) / producers;
    for (int i = 0; i < producers_; i++) {
        buffers_.push_back(MultiBuffer(per_producer_size));
    }
}

Element* SwitchBuffer::GetUnblocking() {
    auto idx = r_idx_;
    auto& buffer = buffers_.at(idx);
    if (buffer.Avail()) {
        return buffer.Get();
    } else {
        idx = (idx + 1) % producers_;
        while (idx != r_idx_) {
            auto& buffer = buffers_.at(idx);
            if (buffer.Avail()) {
                r_idx_ = idx;
                return buffer.Get();
            }

            idx = (idx + 1) % producers_;
        }
    }
    return nullptr;
}

bool SwitchBuffer::Full() {
    bool full = true;
    for (int i = 0; i < producers_; i++) {
        full &= buffers_[i].Full();
    }
    return full;
}

bool SwitchBuffer::Avail() {
    auto idx = r_idx_;
    if (buffers_.at(idx).Avail()) {
        return true;
    } else {
        idx = (idx + 1) % producers_;
        while (idx != r_idx_) {
            auto& buffer = buffers_.at(idx);
            if (buffer.Avail()) {
                r_idx_ = idx;
                return true;
            }

            idx = (idx + 1) % producers_;
        }
    }
    return false;
}

bool SwitchBuffer::PutUnblocking(Element* element) {
    int sid = element->sid();
    return buffers_[sid].Put(element);
}

int64_t SwitchBuffer::count() const {
    int64_t c = 0;
    for (int i = 0; i < producers_; i++) {
        c += buffers_[i].count();
    }
    return c;
}

void SwitchBuffer::MarkDone(bool done) {
    for (int i = 0; i < producers_; i++) {
        buffers_[i].MarkDone(done);
    }
    Buffer::MarkDone(done);
}

MultiBuffer::MultiBuffer(size_t size) : Buffer(size) {
    s_size_ = (size_ + kSwitchSize - 1) / kSwitchSize;
    size_ = s_size_ * kSwitchSize;
    for (int i = 0; i < kSwitchSize; i++) {
        buffers_[i] = std::make_unique<SingleBuffer>(s_size_);
    }
}

MultiBuffer::MultiBuffer(MultiBuffer&& buf)
    : Buffer(buf.size_),
      s_size_(buf.s_size_),
      r_idx_(buf.r_idx_.load()),
      w_idx_(buf.w_idx_.load()),
      r_count_(buf.r_count_) {
    r_idx_ = buf.r_idx_.load();
    w_idx_ = buf.w_idx_.load();
    for (int i = 0; i < kSwitchSize; i++) {
        buffers_[i] = std::move(buf.buffers_[i]);
    }
}

// will only be called in consumer thread
Element* MultiBuffer::GetUnblocking() {
    Element* ele = nullptr;
    if (Avail()) {
        r_count_++;
        ele = buffers_[r_idx_]->Get();
        if (r_count_ == s_size_) {
            r_idx_ = -1;
            r_count_ = 0;
        }
    } else {
        r_idx_ = -1;
    }
    return ele;
}

bool MultiBuffer::PutUnblocking(Element* element) {
    if (Free()) {
        return buffers_[w_idx_]->Put(element);
    } else {
        return false;
    }
}

bool MultiBuffer::Free() {
    if (w_idx_ != -1 && !buffers_[w_idx_]->Full()) {
        return true;
    }

    for (int i = 0; i < kSwitchSize; i++) {
        if (!buffers_[i]->Full()) {
            w_idx_ = i;
            return true;
        }
    }

    return false;
}

// will only be called in consumer thread
bool MultiBuffer::Avail() {
    if (r_idx_ != -1 && buffers_[r_idx_]->Avail()) {
        return true;
    }

    int r_idx = -1;
    int64_t min_ts = std::numeric_limits<int64_t>::max();
    for (int i = 0; i < kSwitchSize; i++) {
        if (buffers_[i]->Avail()) {
            auto ts = buffers_[i]->tail()->ts();
            if (ts < min_ts) {
                r_idx = i;
                min_ts = ts;
            }
        }
    }

    if (r_idx != -1) {
        r_idx_ = r_idx;
        return true;
    } else {
        return false;
    }
}

int64_t MultiBuffer::count() const {
    int64_t c = 0;
    for (int i = 0; i < kSwitchSize; i++) {
        c += buffers_[i]->count();
    }
    return c;
}

void MultiBuffer::MarkDone(bool done) {
    for (int i = 0; i < kSwitchSize; i++) {
        buffers_[i]->MarkDone(done);
    }
    Buffer::MarkDone(done);
}

Element* SingleBuffer::GetUnblocking() {
    if (r_idx_ >= w_idx_) {
        return nullptr;
    }
    auto ele = buffer_[r_idx_++];
    if (r_idx_ == w_idx_) {
        ready_to_read_ = false;
        Reset();
    }
    return ele;
}

bool SingleBuffer::PutUnblocking(Element* ele) {
    if (w_idx_ >= size_) return false;

    buffer_[w_idx_++] = ele;
    if (w_idx_ == size_) {
        ready_to_read_ = true;
    }
    return true;
}

int64_t SingleBuffer::count() const {
    if (ready_to_read_) {
        return w_idx_ - r_idx_;
    } else {
        return 0;
    }
}

}  // namespace interval_join
}  // namespace streaming
