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

namespace hybridse {
namespace base {

RefCountedSlice::~RefCountedSlice() { Release(); }

void RefCountedSlice::Release() {
    if (this->ref_cnt_ != nullptr) {
        auto& cnt = *this->ref_cnt_;
        cnt -= 1;
        if (cnt == 0) {
            // memset in case the buf is still used after free
            memset(buf(), 0, size());
            free(buf());
            delete this->ref_cnt_;
        }
    }
}

void RefCountedSlice::Update(const RefCountedSlice& slice) {
    reset(slice.data(), slice.size());
    this->ref_cnt_ = slice.ref_cnt_;
    if (this->ref_cnt_ != nullptr) {
        (*this->ref_cnt_) += 1;
    }
}

RefCountedSlice::RefCountedSlice(const RefCountedSlice& slice) {
    this->Update(slice);
}

RefCountedSlice::RefCountedSlice(RefCountedSlice&& slice) {
    this->Update(slice);
}

RefCountedSlice& RefCountedSlice::operator=(const RefCountedSlice& slice) {
    if (&slice == this) {
        return *this;
    }
    this->Release();
    this->Update(slice);
    return *this;
}

RefCountedSlice& RefCountedSlice::operator=(RefCountedSlice&& slice) {
    if (&slice == this) {
        return *this;
    }
    this->Release();
    this->Update(slice);
    return *this;
}

}  // namespace base
}  // namespace hybridse
