// Copyright (C) 2019, 4paradigm

#pragma once
#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <memory.h>
#include <string>
#include <memory>

#include "base/raw_buffer.h"

namespace fesql {
namespace base {

class Slice;
typedef std::shared_ptr<Slice> SharedSliceRef;


class Slice {
 public:
    // Create slice own the buffer
    static SharedSliceRef CreateManaged(const int8_t* buf, size_t size);

    // Create slice without ownership
    static SharedSliceRef Create(const int8_t* buf, size_t size);

    // Create slice from c-str without ownership
    static SharedSliceRef CreateFromCStr(const char* str);
    static SharedSliceRef CreateFromCStr(const std::string& str);

    // Create empty slice
    static SharedSliceRef CreateEmpty();

    // Return a pointer to the beginning of the referenced data
    inline const char* data() const { return data_; }
    inline int8_t* buf() const {
        return reinterpret_cast<int8_t*>(const_cast<char*>(data_));
    }

    // Return the length (in bytes) of the referenced data
    inline size_t size() const { return size_; }

    // Return true if the length of the referenced data is zero
    inline bool empty() const { return 0 == size_; }

    // Create an empty slice.
    Slice() : need_free_(false), size_(0), data_("") {}

    // Create a slice that refers to d[0,n-1].
    Slice(const char* d, size_t n) : need_free_(false), size_(n), data_(d) {}

    // Create slice from string
    explicit Slice(const std::string& s)
        : need_free_(false), size_(s.size()), data_(s.data()) {}

    // Create slice from buffer
    explicit Slice(const fesql::base::RawBuffer& buf)
        : need_free_(false), size_(buf.size), data_(buf.addr) {}

    // Create slice from c string
    explicit Slice(const char* s)
        : need_free_(false), size_(strlen(s)), data_(s) {}

    ~Slice() {
        if (need_free_) {
            free(const_cast<char*>(data_));
        }
    }

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
    int compare(const Slice& b) const;

    // Return true iff "x" is a prefix of "*this"
    bool starts_with(const Slice& x) const {
        return ((size_ >= x.size_) && (memcmp(data_, x.data_, x.size_) == 0));
    }

 private:
    Slice(const char* d, size_t n, bool need_free)
        : need_free_(need_free), size_(n), data_(d) {}

    bool need_free_;
    uint32_t size_;
    const char* data_;
};

inline bool operator==(const Slice& x, const Slice& y) {
    return ((x.size() == y.size()) &&
            (memcmp(x.data(), y.data(), x.size()) == 0));
}

inline bool operator!=(const Slice& x, const Slice& y) { return !(x == y); }

inline int Slice::compare(const Slice& b) const {
    const size_t min_len = (size_ < b.size_) ? size_ : b.size_;
    int r = memcmp(data_, b.data_, min_len);
    if (r == 0) {
        if (size_ < b.size_)
            r = -1;
        else if (size_ > b.size_)
            r = +1;
    }
    return r;
}


}  // namespace base
}  // namespace fesql
