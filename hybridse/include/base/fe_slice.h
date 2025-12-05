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

#ifndef HYBRIDSE_INCLUDE_BASE_FE_SLICE_H_
#define HYBRIDSE_INCLUDE_BASE_FE_SLICE_H_

#include <assert.h>
#include <memory.h>
#include <stddef.h>
#include <string.h>

#include <string>
#include <utility>

#include "base/raw_buffer.h"

namespace hybridse {
namespace base {

class Slice {
 public:
    // Return a pointer to the beginning of the referenced data
    inline const char *data() const { return data_; }
    inline int8_t *buf() const {
        return reinterpret_cast<int8_t *>(const_cast<char *>(data_));
    }

    // Return the length (in bytes) of the referenced data
    inline size_t size() const { return size_; }

    // Return true if the length of the referenced data is zero
    inline bool empty() const { return 0 == size_; }

    inline void reset(const char *d, size_t size) {
        data_ = d;
        size_ = size;
    }

    // Create an empty slice.
    Slice() : size_(0), data_("") {}

    // Create a slice that refers to d[0,n-1].
    Slice(const char *d, size_t n) : size_(n), data_(d) {}

    // Create slice from string
    explicit Slice(const std::string &s) : size_(s.size()), data_(s.data()) {}

    // Create slice from buffer
    explicit Slice(const hybridse::base::RawBuffer &buf)
        : size_(buf.size), data_(buf.addr) {}

    // Create slice from c string
    explicit Slice(const char *s) : size_(strlen(s)), data_(s) {}

    ~Slice() {}

    // Return the ith byte in the referenced data.
    // REQUIRES: n < size()
    char operator[](size_t n) const {
        assert(n < size());
        return data_[n];
    }

    // Change this slice to refer to an empty array
    void clear() {
        data_ = "";
        size_ = 0;
    }

    // Drop the first "n" bytes from this slice.
    void remove_prefix(size_t n) {
        assert(n <= size());
        data_ += n;
        size_ -= n;
    }

    // Return a string that contains the copy of the referenced data.
    std::string ToString() const { return std::string(data_, size_); }

    // Three-way comparison.  Returns value:
    //   <  0 iff "*this" <  "b",
    //   == 0 iff "*this" == "b",
    //   >  0 iff "*this" >  "b"
    int compare(const Slice &b) const;

    // Return true iff "x" is a prefix of "*this"
    bool starts_with(const Slice &x) const {
        return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
    }

 protected:
    void _swap(Slice &slice) {
        std::swap(data_, slice.data_);
        std::swap(size_, slice.size_);
    }

 private:
    uint32_t size_;
    const char *data_;
};

inline bool operator==(const Slice &x, const Slice &y) {
    return ((x.size() == y.size()) &&
            (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice &x, const Slice &y) { return !(x == y); }

inline int Slice::compare(const Slice &b) const {
    const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
    int r = memcmp(data_, b.data_, min_len);
    if (r == 0) {
        if (size_ < b.size_) {
            r = -1;
        } else if (size_ > b.size_) {
            r = +1;
        }
    }
    return r;
}

class RefCountedSlice : public Slice {
 public:
    ~RefCountedSlice();

    // Create slice own the buffer
    inline static RefCountedSlice CreateManaged(int8_t *buf, size_t size) {
        return RefCountedSlice(buf, size, true);
    }

    // Create slice without ownership
    inline static RefCountedSlice Create(int8_t *buf, size_t size) {
        return RefCountedSlice(buf, size, false);
    }

    // Create slice without ownership
    inline static RefCountedSlice Create(const char *buf, size_t size) {
        return RefCountedSlice(buf, size, false);
    }

    RefCountedSlice() : Slice(nullptr, 0), ref_cnt_(nullptr) {}

    RefCountedSlice(const RefCountedSlice &slice);
    RefCountedSlice(RefCountedSlice &&);
    RefCountedSlice &operator=(const RefCountedSlice &);
    RefCountedSlice &operator=(RefCountedSlice &&);

 private:
    RefCountedSlice(int8_t *data, size_t size, bool managed)
        : Slice(reinterpret_cast<const char *>(data), size),
          ref_cnt_(managed ? new std::atomic<int32_t>(1) : nullptr) {}

    RefCountedSlice(const char *data, size_t size, bool managed)
        : Slice(data, size), ref_cnt_(managed ? new std::atomic<int32_t>(1) : nullptr) {}

    void _release();

    void _copy(const RefCountedSlice &slice);
    void _swap(RefCountedSlice &slice);

    std::atomic<int32_t> *ref_cnt_;
};

}  // namespace base
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_BASE_FE_SLICE_H_
