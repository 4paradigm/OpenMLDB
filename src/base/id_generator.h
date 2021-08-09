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

#pragma once

#include <pthread.h>
#include <stdlib.h>
#include <unistd.h>

#include <atomic>

#include "common/timer.h"

namespace openmldb {
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
}  // namespace openmldb
