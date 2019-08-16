//
// Created by kongsys on 8/15/19.
//

#ifndef BASE_RINGQUEUE_H
#define BASE_RINGQUEUE_H

#include <cstdint>
#include <mutex>
#include <condition_variable>

namespace rtidb {
namespace base {

template<class T>
class RingQueue {
public:
    RingQueue(int32_t size = 100):
    max_size_(size),
    buf_(new T[size]),
    head_(0),
    tail_(0),
    full_(false) {};

    ~RingQueue() {
      delete[] buf_;
    };
    bool full() const { return full_; }

    bool empty() const { return (!full_ && (head_ == tail_)); };

    int32_t capacity() const { return max_size_; };

    int32_t size() const {
        int32_t size = max_size_;

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
    const int32_t max_size_;
    T* buf_;
    int32_t head_;
    int32_t tail_;
    bool full_;
};

}
}

#endif //RTIDB_RINGQUEUE_H
