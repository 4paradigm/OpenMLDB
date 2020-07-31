/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * mem_pool.h
 *
 * Author: chenjing
 * Date: 2020/7/22
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_BASE_MEM_POOL_H_
#define SRC_BASE_MEM_POOL_H_
#include <stddef.h>
#include <stdint.h>
#include <list>
#include "glog/logging.h"
namespace fesql {
namespace base {
class MemoryChunk {
 public:
    MemoryChunk(MemoryChunk* next, size_t request_size)
        : next_(next),
          chuck_size_(request_size > DEFAULT_CHUCK_SIZE ? request_size
                                                        : DEFAULT_CHUCK_SIZE),
          allocated_size_(0),
          mem_(new char[chuck_size_]) {}
    ~MemoryChunk() { delete[] mem_; }
    inline size_t available_size() { return chuck_size_ - allocated_size_; }
    char* Alloc(size_t request_size) {
        if (request_size > available_size()) {
            return nullptr;
        }
        char* addr = mem_ + allocated_size_;
        allocated_size_ += request_size;
        return addr;
    }
    inline void free() { allocated_size_ = 0; }
    inline MemoryChunk* next() { return next_; }
    enum { DEFAULT_CHUCK_SIZE = 4096 };

 private:
    MemoryChunk* next_;
    size_t chuck_size_;
    size_t allocated_size_;
    char* mem_;
};
class ByteMemoryPool {
 public:
    explicit ByteMemoryPool(size_t init_size = MemoryChunk::DEFAULT_CHUCK_SIZE)
        : chucks_(nullptr) {
        DLOG(INFO) << "ByteMemoryPool";
        ExpandStorage(init_size);
    }
    ~ByteMemoryPool() {
        DLOG(INFO) << "~ByteMemoryPool";
        Reset();
        if (chucks_) {
            delete chucks_;
        }
    }
    char* Alloc(size_t request_size) {
        DLOG(INFO) << "Alloc";
        if (chucks_->available_size() < request_size) {
            ExpandStorage(request_size);
        }
        return chucks_->Alloc(request_size);
    }

    // clear last chuck
    // and delete other chucks
    void Reset() {
        auto chuck = chucks_;
        while (chuck->next()) {
            chucks_ = chuck->next();
            delete chuck;
            chuck = chucks_;
        }
        chuck->free();
    }
    void ExpandStorage(size_t request_size) {
        DLOG(INFO) << "ExpandStorage";
        chucks_ = new MemoryChunk(chucks_, request_size);
    }

 private:
    MemoryChunk* chucks_;
};
}  // namespace base
}  // namespace fesql

#endif  // SRC_BASE_MEM_POOL_H_
