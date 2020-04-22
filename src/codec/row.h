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

#include <map>
#include <string>
#include <cstdint>
#include <unordered_map>
#include <utility>
#include <vector>
#include "base/raw_buffer.h"
#include "proto/type.pb.h"

namespace fesql {
namespace codec {

using fesql::base::Slice;

class Row {
 public:
    Row() : slice_() {}
    Row(int8_t *d, size_t n) : slice_(d, n, false) {}
    Row(int8_t *d, size_t n, bool need_free) : slice_(d, n, need_free) {}
    Row(const char *d, size_t n) : slice_(d, n, false) {}
    Row(const char *d, size_t n, bool need_free) : slice_(d, n, need_free) {}
    Row(Row &s) : slice_(s.slice_), slices_(s.slices_) {}
    Row(const Row &s) : slice_(s.slice_), slices_(s.slices_) {}
    explicit Row(const base::RawBuffer& buf) : slice_(buf) {}
    explicit Row(const Slice &s) : slice_(s) {}
    explicit Row(const std::string &s) : slice_(s) {}

    explicit Row(const char *s) : slice_(s) {}
    virtual ~Row() {}
    inline int8_t *buf() const { return slice_.buf(); }
    inline const char *data() const { return slice_.data(); }
    inline int32_t size() const { return slice_.size(); }
    // Return true if the length of the referenced data is zero
    inline bool empty() const { return slice_.empty() && slices_.empty(); }
    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare(const Row &b) const;
    Slice slice_;
    std::vector<Slice> slices_;
};

}  // namespace codec
}  // namespace fesql
#endif  // SRC_CODEC_ROW_H_
