#pragma once
#include <assert.h>
#include <stddef.h>
#include <string.h>
#include <string>

namespace fesql {
namespace storage {

class Slice {
 public:
    // Create an empty slice.
    Slice() : need_free_(false), size_(0), data_("") {}

    // Create a slice that refers to d[0,n-1].
    Slice(const char* d, size_t n) : need_free_(false), size_(n), data_(d) {}

    // Create a slice that refers to the contents of "s"
    Slice(const std::string& s)
        : need_free_(false), size_(s.size()), data_(s.data()) {}

    Slice(const char* s) : need_free_(false), size_(strlen(s)), data_(s) {}
    Slice(const char* d, size_t n, bool need_free)
        : need_free_(need_free), size_(n), data_(d) {}
    // Return a pointer to the beginning of the referenced data
    const char* data() const { return data_; }

    // Return the length (in bytes) of the referenced data
    size_t size() const { return size_; }

    // Return true iff the length of the referenced data is zero
    bool empty() const { return size_ == 0; }

    void reset(const char* d, size_t size) {
        // TODO if need free is true, reset is forbidden
        data_ = d;
        size_ = size;
    }

    ~Slice() {
        if (need_free_) {
            delete[] data_;
        }
    }

    // Return the ith byte in the referenced data.
    // REQUIRES: n < size()
    char operator[](size_t n) const {
        assert(n < size());
        return data_[n];
    }

    Slice(const Slice& s)
        : need_free_(false), size_(s.size()), data_(s.data()) {}

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
    bool need_free_;
    uint32_t size_;
    const char* data_;
    // Intentionally copyable
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

}  // namespace storage
}  // namespace fesql
