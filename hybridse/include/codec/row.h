/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_INCLUDE_CODEC_ROW_H_
#define HYBRIDSE_INCLUDE_CODEC_ROW_H_

#include <cstdint>
#include <string>
#include <vector>
#include "base/fe_slice.h"

namespace hybridse {
namespace codec {

using hybridse::base::RefCountedSlice;
using hybridse::base::Slice;

class Row {
 public:
    Row();
    explicit Row(const std::string &str);
    Row(const Row &s);
    explicit Row(size_t major_slices, const Row &major, size_t secondary_slices,
        const Row &secondary);
    explicit Row(const hybridse::base::RefCountedSlice &s, size_t secondary_slices,
        const Row &secondary);

    explicit Row(const hybridse::base::RefCountedSlice &s);

    virtual ~Row();

    inline int8_t *buf() const { return slice_.buf(); }
    inline int8_t *buf(int32_t pos) const {
        return 0 == pos ? slice_.buf() : slices_[pos - 1].buf();
    }

    inline int32_t size() const { return slice_.size(); }
    inline int32_t size(int32_t pos) const {
        return 0 == pos ? slice_.size() : slices_.at(pos - 1).size();
    }

    // Return true if the length of the referenced data is zero
    inline bool empty() const { return slice_.empty() && slices_.empty(); }

    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare(const Row &b) const;

    friend bool operator<(const Row &lhs, const Row &rhs) { return lhs.compare(rhs) < 0; }
    friend bool operator>(const Row &lhs, const Row &rhs) { return lhs.compare(rhs) > 0; }
    friend bool operator==(const Row &lhs, const Row &rhs) { return lhs.compare(rhs) == 0; }

    int8_t **GetRowPtrs() const;

    int32_t GetRowPtrCnt() const;
    int32_t *GetRowSizes() const;

    hybridse::base::RefCountedSlice GetSlice(uint32_t slice_index) const {
        if (slice_index >= slices_.size() + 1) {
            return RefCountedSlice();
        }
        return 0 == slice_index ? slice_ : slices_[slice_index - 1];
    }
    inline void Append(const hybridse::base::RefCountedSlice &slice) {
        slices_.emplace_back(slice);
    }
    // Return a string that contains the copy of the referenced data.
    std::string ToString() const;

    void Reset(const int8_t *buf, size_t size) {
        slice_.reset(reinterpret_cast<const char *>(buf), size);
    }

    void Reset(const ::hybridse::base::RefCountedSlice& r) {
        slice_ = r;
    }

 private:
    void Append(const std::vector<hybridse::base::RefCountedSlice> &slices);
    void Append(const Row &b);

    RefCountedSlice slice_;
    std::vector<RefCountedSlice> slices_;
};

}  // namespace codec
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_CODEC_ROW_H_
