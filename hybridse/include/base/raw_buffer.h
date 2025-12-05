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

#ifndef HYBRIDSE_INCLUDE_BASE_RAW_BUFFER_H_
#define HYBRIDSE_INCLUDE_BASE_RAW_BUFFER_H_

#include <stddef.h>
#include <string.h>

#include "glog/logging.h"

namespace hybridse {
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
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_BASE_RAW_BUFFER_H_
