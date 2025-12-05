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

#include "base/fe_slice.h"
#include <atomic>

namespace hybridse {
namespace base {

RefCountedSlice::~RefCountedSlice() { _release(); }

void RefCountedSlice::_release() {
    if (this->ref_cnt_ != nullptr) {
        if (std::atomic_fetch_add_explicit(ref_cnt_, -1, std::memory_order_release) == 1) {
            std::atomic_thread_fence(std::memory_order_acquire);
            assert(buf());
            free(buf());
            delete this->ref_cnt_;
        }
    }
}

void RefCountedSlice::_copy(const RefCountedSlice& slice) {
    reset(slice.data(), slice.size());
    ref_cnt_ = slice.ref_cnt_;
    if (this->ref_cnt_ != nullptr) {
        std::atomic_fetch_add_explicit(ref_cnt_, 1, std::memory_order_relaxed);
    }
}

void RefCountedSlice::_swap(RefCountedSlice& slice) {
    std::swap(ref_cnt_, slice.ref_cnt_);
    Slice::_swap(slice);
}

RefCountedSlice::RefCountedSlice(const RefCountedSlice& slice) {
    _copy(slice);
}

RefCountedSlice::RefCountedSlice(RefCountedSlice&& slice) { _swap(slice); }

RefCountedSlice& RefCountedSlice::operator=(const RefCountedSlice& slice) {
    if (&slice == this) {
        return *this;
    }
    _release();
    _copy(slice);
    return *this;
}

RefCountedSlice& RefCountedSlice::operator=(RefCountedSlice&& slice) {
    _swap(slice);
    return *this;
}

}  // namespace base
}  // namespace hybridse
