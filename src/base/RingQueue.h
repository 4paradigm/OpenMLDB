//
// Created by kongsys on 8/15/19.
//

#include <cstdint>
#include <bthread/mutex.h>
#include <condition_variable>

#ifndef RTIDB_RINGQUEUE_H
#define RTIDB_RINGQUEUE_H

#endif //RTIDB_RINGQUEUE_H

namespace rtidb {
namespace base {
    template<class T>
    class RingQueue {
    public:
        RingQueue(int32_t size = 100):
        max_size_(size),
        buf_(new T[size]),
        full_(false),
        head_(0),
        tail_(0) {};

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

        void put(T item) {
            std::unique_lock<std::mutex> lock(mutex_);
            if (full_) {
                cv.wait(lock);
            }
            buf_[head_] = item;

            head_ = (head_ + 1) % max_size_;
            full_ = head_ == tail_;
        }
        T get() {
            std::lock_guard<std::mutex> lock(mutex_);
            if (empty()) {
                return T();
            }
            auto val = buf_[tail_];
            if (full_) {
                cv.notify_one();
            }
            full_ = false;
            tail_ = (tail_ + 1) % max_size_;

            return val;
        }
    private:
        std::mutex mutex_;
        const int32_t max_size_;
        T* buf_;
        int32_t head_;
        int32_t tail_;
        bool full_;
        std::condition_variable cv;
    };
}
}