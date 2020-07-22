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
namespace fesql{
namespace base{
class MemoryChuck {
 public:
    MemoryChuck(MemoryChuck* next, size_t request_size)
        : next_(next),
          chuck_size_(request_size > DEFAULT_CHUCK_SIZE ? request_size
                                                        : DEFAULT_CHUCK_SIZE),
          allocated_size_(0),
          mem_(new char[chuck_size_]) {}
    ~MemoryChuck() { delete[] mem_; }
    inline size_t available_size() { return chuck_size_ - allocated_size_; }

    char* Alloc(size_t request_size) {
        if (request_size > available_size()) {
            return nullptr;
        }
        char* addr = mem_ + allocated_size_;
        allocated_size_ += request_size;
        return addr;
    }
    inline MemoryChuck* next() { return next_; }
    enum { DEFAULT_CHUCK_SIZE = 4096 };

 private:
    MemoryChuck* next_;
    size_t chuck_size_;
    size_t allocated_size_;
    char* mem_;
};
class ByteMemoryPool {
 public:
    ByteMemoryPool(size_t init_size = MemoryChuck::DEFAULT_CHUCK_SIZE)
        : chucks_(nullptr) {
        ExpandStorage(init_size);
    }
    ~ByteMemoryPool() {
        auto chuck = chucks_;
        while (chuck) {
            chucks_ = chuck->next();
            delete chuck;
            chuck = chucks_;
        }
    }
    char* Alloc(size_t request_size) {
        if (chucks_->available_size() < request_size) {
            ExpandStorage(request_size);
        }
        return chucks_->Alloc(request_size);
    }

    void ExpandStorage(size_t request_size) {
        chucks_ = new MemoryChuck(chucks_, request_size);
    }

 private:
    MemoryChuck* chucks_;
};
}
}

#endif  // SRC_BASE_MEM_POOL_H_
