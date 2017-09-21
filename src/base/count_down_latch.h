//
// count_down_latch.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-21
//


#ifndef RTIDB_BASE_COUNT_DOWN_LATCH_H
#define RTIDB_BASE_COUNT_DOWN_LATCH_H
#include "mutex.h"

using ::baidu::common::Mutex;
using ::baidu::common::MutexLock;
using ::baidu::common::CondVar;

namespace rtidb {
namespace base {

class CountDownLatch {

public:
    CountDownLatch(uint32_t count):count_(count),
    mu_(), cv_(&mu_){}
    ~CountDownLatch() {}

    void CountDown() {
        MutexLock lock(&mu_);
        if (--count_ <= 0) {
            cv_.Broadcast();
        }
    }

    void TimeWait(uint64_t timeout) {
        MutexLock lock(&mu_);
        cv_.TimeWait(timeout);
    }

    void Wait() {
        MutexLock lock(&mu_);
        while (count_ > 0) {
            cv_.Wait();
        }
    }

    bool IsDone() {
        MutexLock lock(&mu_);
        return count_ <= 0;
    }

    uint32_t GetCount() {
        MutexLock lock(&mu_);
        return count_;
    }

private:
    uint32_t count_;
    Mutex mu_;
    CondVar cv_;
};

}
}
#endif /* !RTIDB_BASE_COUNT_DOWN_LATCH_H */
