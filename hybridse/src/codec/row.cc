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

#include "codec/row.h"

namespace hybridse {
namespace codec {

Row::Row() : slice_() {}

Row::Row(const std::string &str)
    : slice_(RefCountedSlice::Create(
          reinterpret_cast<int8_t *>(const_cast<char *>(str.data())),
          str.length())) {}

Row::Row(const Row &s) : slice_(s.slice_), slices_(s.slices_) {}

Row::Row(size_t major_slices, const Row &major, size_t secondary_slices, const Row &secondary)
    : slice_(major.slice_), slices_(major_slices + secondary_slices - 1) {
    size_t offset = 0;
    for (; offset + 1 < major_slices; ++offset) {
        if (major.slices_.size() > offset) {
            slices_[offset] = major.slices_[offset];
        }
    }
    slices_[offset ++] = secondary.slice_;
    for (size_t off = 0; off + 1 < secondary_slices; ++off) {
        if (secondary.slices_.size() > off) {
            slices_[off + major_slices] = secondary.slices_[off];
        }
    }
}

Row::Row(const hybridse::base::RefCountedSlice &s, size_t secondary_slices, const Row &secondary)
    : slice_(s), slices_(secondary_slices) {
    slices_[0] = secondary.slice_;
    for (size_t offset = 0; offset < secondary_slices - 1; ++offset) {
        if (secondary.slices_.size() > offset) {
            slices_[1 + offset] = secondary.slices_[offset];
        }
    }
}

Row::Row(const RefCountedSlice &s) : slice_(s) {}

Row::~Row() {}

void Row::Append(const std::vector<RefCountedSlice> &slices) {
    if (!slices.empty()) {
        slices_.insert(slices_.end(), slices.begin(), slices.end());
    }
}
void Row::Append(const Row &b) {
    slices_.push_back(b.slice_);
    Append(b.slices_);
}

int32_t Row::GetRowPtrCnt() const { return 1 + slices_.size(); }

// Return a string that contains the copy of the referenced data.
std::string Row::ToString() const { return slice_.ToString(); }

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

}  // namespace codec
}  // namespace hybridse
