//
// ringqueue.h
// Copyright (C) 2017 4paradigm.com
// Author kongquan
// Date 2019-08-15
//

#ifndef SRC_BASE_RINGQUEUE_H_
#define SRC_BASE_RINGQUEUE_H_

#include <condition_variable> // NOLINT
#include <cstdint>
#include <mutex> // NOLINT

namespace rtidb {
namespace base {

template <class T>
class RingQueue {
 public:
    explicit RingQueue(uint32_t size = 100)
        : max_size_(size),
          buf_(new T[size]),
          head_(0),
          tail_(0),
          full_(false) {}

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
}  // namespace rtidb

#endif  // SRC_BASE_RINGQUEUE_H_
