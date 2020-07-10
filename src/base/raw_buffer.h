// Copyright (C) 2019, 4paradigm

#pragma once
#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>

#include "glog/logging.h"

namespace fesql {
namespace base {

struct RawBuffer {
    char* addr;
    size_t size;

    bool CopyFrom(const char* buf, size_t buf_size) const {
        if (size < buf_size) {
            LOG(WARNING) << "Buffer size too small" << size
                         << " , require >=" << buf_size;
            return false;
        }
        memcpy(addr, buf, buf_size);
        return true;
    }

    RawBuffer(char* addr, size_t size) : addr(addr), size(size) {}
};

}  // namespace base
}  // namespace fesql
