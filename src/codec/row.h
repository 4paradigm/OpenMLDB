/*
 * row.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#ifndef SRC_CODEC_ROW_H_
#define SRC_CODEC_ROW_H_

#include <cstdint>
#include <map>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/raw_buffer.h"
#include "base/fe_slice.h"
#include "proto/fe_type.pb.h"

namespace fesql {
namespace codec {

using fesql::base::SharedSliceRef;
using fesql::base::Slice;

class Row {
 public:
    Row();
    explicit Row(const std::string& str);
    Row(const Row &s);
    Row(const Row &major, const Row &secondary);

    explicit Row(const SharedSliceRef& s);

    virtual ~Row();

    inline int8_t* buf() const { return slice_->buf(); }
    inline int8_t* buf(int32_t pos) const {
        return 0 == pos ? slice_->buf() : slices_[pos - 1]->buf();
    }

    inline int32_t size() const { return slice_->size(); }
    inline int32_t size(int32_t pos) const {
        return 0 == pos ? slice_->size() : slices_[pos - 1]->size();
    }

    // Return true if the length of the referenced data is zero
    inline bool empty() const { return slice_->empty() && slices_.empty(); }

    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare(const Row &b) const;

    void Append(const std::vector<SharedSliceRef> &slices);
    void Append(const Row &b);

    int8_t **GetRowPtrs() const;

    int32_t GetRowPtrCnt() const;
    int32_t *GetRowSizes() const;
    void AppendEmptyRow();

    // Return a string that contains the copy of the referenced data.
    std::string ToString() const;

    SharedSliceRef slice_;
    std::vector<SharedSliceRef> slices_;
};

}  // namespace codec
}  // namespace fesql
#endif  // SRC_CODEC_ROW_H_
