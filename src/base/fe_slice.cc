// Copyright (C) 2019, 4paradigm
#include "base/fe_slice.h"

namespace fesql {
namespace base {

RefCountedSlice RefCountedSlice::CreateManaged(int8_t* buf, size_t size) {
    return RefCountedSlice(buf, size, true);
}

RefCountedSlice RefCountedSlice::Create(int8_t* buf, size_t size) {
    return RefCountedSlice(buf, size, false);
}

RefCountedSlice RefCountedSlice::CreateEmpty() {
    return RefCountedSlice(nullptr, 0, false);
}

RefCountedSlice::~RefCountedSlice() {
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
    this->Update(slice);
    return *this;
}

RefCountedSlice& RefCountedSlice::operator=(RefCountedSlice&& slice) {
    this->Update(slice);
    return *this;
}

}  // namespace base
}  // namespace fesql
