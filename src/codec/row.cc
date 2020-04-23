/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * row.cc
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/
#include "codec/row.h"
#include "base/slice.h"
namespace fesql {
namespace codec {
using fesql::base::Slice;
int Row::compare(const Row &b) const {
    int r = slice_.compare(b.slice_);
    if (r != 0) {
        return r;
    }
    size_t this_len = slices_.size();
    size_t b_len = b.slices_.size();
    size_t min_len = this_len < b_len ? this_len : b_len;
    for (size_t i = 0; i < min_len; i++) {
        int slice_compared = slices_[i].compare(b.slices_[i]);
        if (0 == slice_compared) {
            continue;
        }
        return slice_compared;
    }

    return this_len < b_len ? -1 : this_len > b_len ? +1 : 0;
}
int8_t **Row::GetRowPtrs() const {
    if (slices_.empty()) {
        return new int8_t *[1] { slice_.buf() };
    } else {
        int8_t **ptrs = new int8_t *[slices_.size() + 1];
        int pos = 0;
        ptrs[pos++] = slice_.buf();
        for (auto slice : slices_) {
            ptrs[pos++] = slice.buf();
        }
        return ptrs;
    }
}
int32_t *Row::GetRowSizes() const {
    if (slices_.empty()) {
        return new int32_t[1]{static_cast<int32_t>(slice_.size())};
    } else {
        int32_t *sizes = new int32_t[slices_.size() + 1];
        int pos = 0;
        sizes[pos++] = slice_.size();
        for (auto slice : slices_) {
            sizes[pos++] = static_cast<int32_t>(slice.size());
        }
        return sizes;
    }
}

inline bool operator==(const Row &x, const Row &y) {
    return x.slice_ == y.slice_ && x.slices_ == y.slices_;
}

inline bool operator!=(const Row &x, const Row &y) { return !(x == y); }

}  // namespace codec
}