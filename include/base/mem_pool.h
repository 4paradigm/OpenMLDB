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

#ifndef INCLUDE_BASE_MEM_POOL_H_
#define INCLUDE_BASE_MEM_POOL_H_

#include <stddef.h>
#include <stdint.h>
#include <list>
#include <thread>  //NOLINT
namespace openmldb {
namespace base {
class MemoryChunk {
 public:
    MemoryChunk(MemoryChunk* next, size_t request_size)
        : next_(next),
          chuck_size_(request_size > DEFAULT_CHUCK_SIZE ? request_size : DEFAULT_CHUCK_SIZE),
          allocated_size_(0),
          mem_(new char[chuck_size_]) {
    }
    ~MemoryChunk() {
        delete[] mem_;
    }
    inline size_t available_size() const { return chuck_size_ - allocated_size_; }
    char* Alloc(size_t request_size) {
        if (request_size > available_size()) {
            return nullptr;
        }
        char* addr = mem_ + allocated_size_;
        allocated_size_ += request_size;
        return addr;
    }
    inline MemoryChunk* next() { return next_; }
    enum { DEFAULT_CHUCK_SIZE = 4096 };

 private:
    MemoryChunk* next_;
    const size_t chuck_size_;
    size_t allocated_size_;
    char* const mem_;
};
class ByteMemoryPool {
 public:
    explicit ByteMemoryPool(size_t init_size = MemoryChunk::DEFAULT_CHUCK_SIZE)
        : chucks_(nullptr) {
        ExpandStorage(init_size);
    }
    ~ByteMemoryPool() {
        Reset();
    }
    char* Alloc(size_t request_size) {
        if (nullptr == chucks_ || chucks_->available_size() < request_size) {
            ExpandStorage(request_size);
        }
        return chucks_->Alloc(request_size);
    }

    // clear last chuck
    // and delete other chucks
    void Reset() {
        auto chuck = chucks_;
        while (chuck) {
            chucks_ = chuck->next();
            delete chuck;
            chuck = chucks_;
        }
    }
    void ExpandStorage(size_t request_size) {
        chucks_ = new MemoryChunk(chucks_, request_size);
    }

 private:
    MemoryChunk* chucks_;
};
}  // namespace base
}  // namespace openmldb

#endif  // INCLUDE_BASE_MEM_POOL_H_
