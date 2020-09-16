// Copyright (C) 2019, 4paradigm
#include "base/fe_slice.h"

namespace fesql {
namespace base {

RefCountedSlice::~RefCountedSlice() { Release(); }

void RefCountedSlice::Release() {
    if (this->ref_cnt_ != nullptr) {
        auto& cnt = *this->ref_cnt_;
        cnt -= 1;
        if (cnt == 0) {
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
}  // namespace fesql
