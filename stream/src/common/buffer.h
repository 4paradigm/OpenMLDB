/*
 *  Copyright 2021 4Paradigm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef STREAM_SRC_COMMON_BUFFER_H_
#define STREAM_SRC_COMMON_BUFFER_H_

#include <gflags/gflags.h>
#include <stddef.h>
#include <atomic>

#include "common/element.h"

namespace streaming {
namespace interval_join {

DECLARE_int64(buffer_blocking_sleep_us);

class Buffer {
 public:
    explicit Buffer(size_t size) : size_(size) {}
    virtual ~Buffer() {}

    virtual bool Avail() = 0;
    virtual bool Full() = 0;

    // if blocking is false
    // return the current element if available
    // otherwise return nullptr
    //
    // if blocking is true
    // block until there is an element to return or timeout
    // timeout == -1, means wait forever without timeout
    virtual Element* Get(bool blocking = false, int64_t timeout = -1);

    // use the non-const Element* in case we may update some element-wise stats later
    // if blocking is false
    // insert the element if there is available slot and return true
    // otherwise return false
    //
    // if blocking is true
    // block until there is an element to return true or timeout
    // timeout == -1, means wait forever without timeout
    virtual bool Put(Element* element, bool blocking = false, int64_t timeout = -1);

    virtual int64_t count() const = 0;

    virtual void MarkDone(bool done = true) {
        done_ = done;
    }

    virtual bool Done() const {
        return done_;
    }

 protected:
    size_t size_;
    std::atomic<bool> done_ = false;

    virtual Element* GetUnblocking() = 0;
    virtual bool PutUnblocking(Element* element) = 0;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_COMMON_BUFFER_H_
