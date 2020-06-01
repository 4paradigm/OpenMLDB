//
// id_generator.h
// Copyright (C) 2017 4paradigm.com
// Author wangbao
// Date 2020-3-20
//
#pragma once

#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include <atomic>

#include "timer.h"  // NOLINT

namespace rtidb {
namespace base {

class IdGenerator {
 public:
    IdGenerator() : id_(0) {}
    IdGenerator(const IdGenerator&) = delete;
    IdGenerator& operator=(const IdGenerator&) = delete;
    ~IdGenerator() {}

    int64_t Next() {
        int64_t ts = ::baidu::common::timer::get_micros();
        pthread_t tid = pthread_self();
        int64_t rd = id_.fetch_add(1, std::memory_order_acq_rel);
#if __linux__
        int64_t res = ((ts << 12) | ((tid & 0xFF) << 12) | (rd & 0xFFF));
#else
        // pthread_t is a pointer on mac
        int64_t res = ((ts << 12) | ((reinterpret_cast<uint64_t>(tid) & 0xFF) << 12) | (rd & 0xFFF));
#endif
        return llabs(res);
    }

 private:
    std::atomic<int64_t> id_;
};

}  // namespace base
}  // namespace rtidb
