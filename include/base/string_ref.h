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

#ifndef INCLUDE_BASE_STRING_REF_H_
#define INCLUDE_BASE_STRING_REF_H_

#include <stdint.h>
#include <cstddef>
#include <cstring>
#include <string>

namespace openmldb {
namespace base {

struct StringRef {
    StringRef() : size_(0), data_(nullptr) {}
    explicit StringRef(const char* str)
        : size_(nullptr == str ? 0 : strlen(str)), data_(str) {}
    StringRef(uint32_t size, const char* data) : size_(size), data_(data) {}

    explicit StringRef(const std::string& str)
        : size_(str.size()), data_(str.data()) {}

    ~StringRef() {}

    const inline bool IsNull() const { return nullptr == data_; }
    const std::string ToString() const {
        return size_ == 0 ? "" : std::string(data_, size_);
    }

    static int compare(const StringRef& a, const StringRef& b) {
        const size_t min_len = (a.size_ < b.size_) ? a.size_ : b.size_;
        int r = memcmp(a.data_, b.data_, min_len);
        if (r == 0) {
            if (a.size_ < b.size_) {
                r = -1;
            } else if (a.size_ > b.size_) {
                r = +1;
            }
        }
        return r;
    }

    uint32_t size_;
    const char* data_;
};

__attribute__((unused)) static const StringRef operator+(const StringRef& a,
                                                         const StringRef& b) {
    StringRef str;
    str.size_ = a.size_ + b.size_;
    char* buffer = static_cast<char*>(malloc(str.size_ + 1));
    str.data_ = buffer;
    if (a.size_ > 0) {
        memcpy(buffer, a.data_, a.size_);
    }
    if (b.size_ > 0) {
        memcpy(buffer + a.size_, b.data_, b.size_);
    }
    buffer[str.size_] = '\0';
    return str;
}

__attribute__((unused)) static std::ostream& operator<<(std::ostream& os,
                                                        const StringRef& a) {
    os << a.ToString();
    return os;
}
__attribute__((unused)) static bool operator==(const StringRef& a,
                                               const StringRef& b) {
    return 0 == StringRef::compare(a, b);
}
__attribute__((unused)) static bool operator!=(const StringRef& a,
                                               const StringRef& b) {
    return 0 != StringRef::compare(a, b);
}
__attribute__((unused)) static bool operator>=(const StringRef& a,
                                               const StringRef& b) {
    return StringRef::compare(a, b) >= 0;
}
__attribute__((unused)) static bool operator>(const StringRef& a,
                                              const StringRef& b) {
    return StringRef::compare(a, b) > 0;
}
__attribute__((unused)) static bool operator<=(const StringRef& a,
                                               const StringRef& b) {
    return StringRef::compare(a, b) <= 0;
}
__attribute__((unused)) static bool operator<(const StringRef& a,
                                              const StringRef& b) {
    return StringRef::compare(a, b) < 0;
}

}  // namespace base
}  // namespace openmldb

#endif  // INCLUDE_BASE_STRING_REF_H_
