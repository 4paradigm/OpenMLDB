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

#ifndef INCLUDE_CODEC_TYPE_CODEC_H_
#define INCLUDE_CODEC_TYPE_CODEC_H_

#include <stdint.h>
#include <cstddef>
#include <string>
#include <vector>
#include "base/fe_hash.h"
#include "base/mem_pool.h"
#include "glog/logging.h"

namespace hybridse {
namespace codec {
static const uint32_t SEED = 0xe17a1465;
struct StringRef {
    StringRef() : size_(0), data_(nullptr) {}
    explicit StringRef(const char* str)
        : size_(nullptr == str ? 0 : strlen(str)), data_(str) {}
    explicit StringRef(const std::string& str)
        : size_(str.size()), data_(str.data()) {}
    StringRef(uint32_t size, const char* data) : size_(size), data_(data) {}
    ~StringRef() {}
    const inline bool IsNull() const { return nullptr == data_; }
    const std::string ToString() const {
        return size_ == 0 ? "" : std::string(data_, size_);
    }
    static int compare(const StringRef& a, const StringRef& b) {
        const size_t min_len = (a.size_ < b.size_) ? a.size_ : b.size_;
        int r = memcmp(a.data_, b.data_, min_len);
        if (r == 0) {
            if (a.size_ < b.size_)
                r = -1;
            else if (a.size_ > b.size_)
                r = +1;
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

struct Timestamp {
    Timestamp() : ts_(0) {}
    explicit Timestamp(int64_t ts) : ts_(ts < 0 ? 0 : ts) {}
    Timestamp& operator+=(const Timestamp& t1) {
        ts_ += t1.ts_;
        return *this;
    }
    Timestamp& operator-=(const Timestamp& t1) {
        ts_ -= t1.ts_;
        return *this;
    }
    int64_t ts_;
};

__attribute__((unused)) static const Timestamp operator+(const Timestamp& a,
                                                         const Timestamp& b) {
    return Timestamp(a.ts_ + b.ts_);
}
__attribute__((unused)) static const Timestamp operator-(const Timestamp& a,
                                                         const Timestamp& b) {
    return Timestamp(a.ts_ - b.ts_);
}
__attribute__((unused)) static const Timestamp operator/(const Timestamp& a,
                                                         const int64_t b) {
    return Timestamp(static_cast<int64_t>(a.ts_ / b));
}
__attribute__((unused)) static bool operator>(const Timestamp& a,
                                              const Timestamp& b) {
    return a.ts_ > b.ts_;
}
__attribute__((unused)) static bool operator<(const Timestamp& a,
                                              const Timestamp& b) {
    return a.ts_ < b.ts_;
}
__attribute__((unused)) static bool operator>=(const Timestamp& a,
                                               const Timestamp& b) {
    return a.ts_ >= b.ts_;
}
__attribute__((unused)) static bool operator<=(const Timestamp& a,
                                               const Timestamp& b) {
    return a.ts_ <= b.ts_;
}
__attribute__((unused)) static bool operator==(const Timestamp& a,
                                               const Timestamp& b) {
    return a.ts_ == b.ts_;
}
__attribute__((unused)) static bool operator!=(const Timestamp& a,
                                               const Timestamp& b) {
    return a.ts_ != b.ts_;
}

struct Date {
    Date() : date_(0) {}
    explicit Date(int32_t date) : date_(date < 0 ? 0 : date) {}
    Date(int32_t year, int32_t month, int32_t day) : date_(0) {
        if (year < 1900 || year > 9999) {
            return;
        }
        if (month < 1 || month > 12) {
            return;
        }
        if (day < 1 || day > 31) {
            return;
        }
        int32_t data = (year - 1900) << 16;
        data = data | ((month - 1) << 8);
        data = data | day;
        date_ = data;
    }
    static bool Decode(int32_t date, int32_t* year, int32_t* month,
                       int32_t* day) {
        if (date < 0) {
            return false;
        }
        *day = date & 0x0000000FF;
        date = date >> 8;
        *month = 1 + (date & 0x0000FF);
        *year = 1900 + (date >> 8);
        return true;
    }
    int32_t date_;
};

__attribute__((unused)) static bool operator>(const Date& a, const Date& b) {
    return a.date_ > b.date_;
}
__attribute__((unused)) static bool operator<(const Date& a, const Date& b) {
    return a.date_ < b.date_;
}
__attribute__((unused)) static bool operator>=(const Date& a, const Date& b) {
    return a.date_ >= b.date_;
}
__attribute__((unused)) static bool operator<=(const Date& a, const Date& b) {
    return a.date_ <= b.date_;
}
__attribute__((unused)) static bool operator==(const Date& a, const Date& b) {
    return a.date_ == b.date_;
}
__attribute__((unused)) static bool operator!=(const Date& a, const Date& b) {
    return a.date_ != b.date_;
}

template <typename V = void>
struct ListRef {
    int8_t* list;
};

struct IteratorRef {
    int8_t* iterator;
};

namespace v1 {

static constexpr uint8_t VERSION_LENGTH = 2;
static constexpr uint8_t SIZE_LENGTH = 4;
static constexpr uint8_t HEADER_LENGTH = VERSION_LENGTH + SIZE_LENGTH;

// calc the total row size with primary_size, str field count and str_size
uint32_t CalcTotalLength(uint32_t primary_size, uint32_t str_field_cnt,
                         uint32_t str_size, uint32_t* str_addr_space);

inline void AppendNullBit(int8_t* buf_ptr, uint32_t col_idx, int8_t is_null) {
    int8_t* ptr = buf_ptr + HEADER_LENGTH + (col_idx >> 3);
    if (is_null) {
        *(reinterpret_cast<uint8_t*>(ptr)) |= 1 << (col_idx & 0x07);
    } else {
        *(reinterpret_cast<uint8_t*>(ptr)) &= ~(1 << (col_idx & 0x07));
    }
}

inline int32_t AppendInt16(int8_t* buf_ptr, uint32_t buf_size, int16_t val,
                           uint32_t field_offset) {
    if (field_offset + 2 > buf_size) {
        LOG(WARNING) << "invalid field offset expect less than " << buf_size
                     << " but " << field_offset + 2;
        return -1;
    }
    *(reinterpret_cast<int16_t*>(buf_ptr + field_offset)) = val;
    return 4;
}

inline int32_t AppendFloat(int8_t* buf_ptr, uint32_t buf_size, float val,
                           uint32_t field_offset) {
    if (field_offset + 4 > buf_size) {
        LOG(WARNING) << "invalid field offset expect less than " << buf_size
                     << " but " << field_offset + 4;
        return -1;
    }
    *(reinterpret_cast<float*>(buf_ptr + field_offset)) = val;
    return 4;
}

inline int32_t AppendInt32(int8_t* buf_ptr, uint32_t buf_size, int32_t val,
                           uint32_t field_offset) {
    if (field_offset + 4 > buf_size) {
        LOG(WARNING) << "invalid field offset expect less than " << buf_size
                     << " but " << field_offset + 4;
        return -1;
    }
    *(reinterpret_cast<int32_t*>(buf_ptr + field_offset)) = val;
    return 4;
}

inline int32_t AppendInt64(int8_t* buf_ptr, uint32_t buf_size, int64_t val,
                           uint32_t field_offset) {
    if (field_offset + 8 > buf_size) {
        LOG(WARNING) << "invalid field offset expect less than " << buf_size
                     << " but " << field_offset + 8;
        return -1;
    }
    *(reinterpret_cast<int64_t*>(buf_ptr + field_offset)) = val;
    return 8;
}

inline int32_t AppendDouble(int8_t* buf_ptr, uint32_t buf_size, double val,
                            uint32_t field_offset) {
    if (field_offset + 8 > buf_size) {
        LOG(WARNING) << "invalid field offset expect less than " << buf_size
                     << " but " << field_offset + 8;
        return -1;
    }

    *(reinterpret_cast<double*>(buf_ptr + field_offset)) = val;
    return 8;
}

int32_t AppendString(int8_t* buf_ptr, uint32_t buf_size, uint32_t col_idx,
                     int8_t* val, uint32_t size, int8_t is_null,
                     uint32_t str_start_offset, uint32_t str_field_offset,
                     uint32_t str_addr_space, uint32_t str_body_offset);

inline int8_t GetAddrSpace(uint32_t size) {
    if (size <= UINT8_MAX) {
        return 1;
    } else if (size <= UINT16_MAX) {
        return 2;
    } else if (size <= 1 << 24) {
        return 3;
    } else {
        return 4;
    }
}

inline bool IsNullAt(const int8_t* row, uint32_t idx) {
    if (row == nullptr) {
        return true;
    }
    const int8_t* ptr = row + HEADER_LENGTH + (idx >> 3);
    return *(reinterpret_cast<const uint8_t*>(ptr)) & (1 << (idx & 0x07));
}

inline int8_t GetBoolFieldUnsafe(const int8_t* row, uint32_t offset) {
    int8_t value = *(row + offset);
    return value;
}

inline int8_t GetBoolField(const int8_t* row, uint32_t idx, uint32_t offset,
                           int8_t* is_null) {
    if (row == nullptr || IsNullAt(row, idx)) {
        *is_null = true;
        return 0;
    } else {
        *is_null = false;
        return GetBoolFieldUnsafe(row, offset);
    }
}

inline int16_t GetInt16FieldUnsafe(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int16_t*>(row + offset));
}

inline int16_t GetInt16Field(const int8_t* row, uint32_t idx, uint32_t offset,
                             int8_t* is_null) {
    if (row == nullptr || IsNullAt(row, idx)) {
        *is_null = true;
        return 0;
    } else {
        *is_null = false;
        return GetInt16FieldUnsafe(row, offset);
    }
}

inline int32_t GetInt32FieldUnsafe(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int32_t*>(row + offset));
}

inline int32_t GetInt32Field(const int8_t* row, uint32_t idx, uint32_t offset,
                             int8_t* is_null) {
    if (row == nullptr || IsNullAt(row, idx)) {
        *is_null = true;
        return 0;
    } else {
        *is_null = false;
        return GetInt32FieldUnsafe(row, offset);
    }
}

inline int64_t GetInt64FieldUnsafe(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int64_t*>(row + offset));
}

inline int64_t GetInt64Field(const int8_t* row, uint32_t idx, uint32_t offset,
                             int8_t* is_null) {
    if (row == nullptr || IsNullAt(row, idx)) {
        *is_null = true;
        return 0;
    } else {
        *is_null = false;
        return GetInt64FieldUnsafe(row, offset);
    }
}

inline float GetFloatFieldUnsafe(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const float*>(row + offset));
}

inline float GetFloatField(const int8_t* row, uint32_t idx, uint32_t offset,
                           int8_t* is_null) {
    if (row == nullptr || IsNullAt(row, idx)) {
        *is_null = true;
        return 0;
    } else {
        *is_null = false;
        return GetFloatFieldUnsafe(row, offset);
    }
}

inline Timestamp GetTimestampFieldUnsafe(const int8_t* row, uint32_t offset) {
    return Timestamp(*(reinterpret_cast<const int64_t*>(row + offset)));
}

inline Timestamp GetTimestampField(const int8_t* row, uint32_t idx,
                                   uint32_t offset, int8_t* is_null) {
    if (row == nullptr || IsNullAt(row, idx)) {
        *is_null = true;
        return Timestamp();
    } else {
        *is_null = false;
        return GetTimestampFieldUnsafe(row, offset);
    }
}

inline double GetDoubleFieldUnsafe(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const double*>(row + offset));
}

inline double GetDoubleField(const int8_t* row, uint32_t idx, uint32_t offset,
                             int8_t* is_null) {
    if (row == nullptr || IsNullAt(row, idx)) {
        *is_null = true;
        return 0.0;
    } else {
        *is_null = false;
        return GetDoubleFieldUnsafe(row, offset);
    }
}

// native get string field method
int32_t GetStrFieldUnsafe(const int8_t* row, uint32_t col_idx,
                          uint32_t str_field_offset,
                          uint32_t next_str_field_offset,
                          uint32_t str_start_offset, uint32_t addr_space,
                          const char** data, uint32_t* size);

int32_t GetStrField(const int8_t* row, uint32_t idx, uint32_t str_field_offset,
                    uint32_t next_str_field_offset, uint32_t str_start_offset,
                    uint32_t addr_space, const char** data, uint32_t* size,
                    int8_t* is_null);

int32_t GetCol(int8_t* input, int32_t row_idx, uint32_t col_idx, int32_t offset,
               int32_t type_id, int8_t* data);
int32_t GetInnerRangeList(int8_t* input, int64_t start_key,
                          int64_t start_offset, int64_t end_offset,
                          int8_t* data);
int32_t GetInnerRowsList(int8_t* input, int64_t start_offset,
                         int64_t end_offset, int8_t* data);

int32_t GetStrCol(int8_t* input, int32_t row_idx, uint32_t col_idx,
                  int32_t str_field_offset, int32_t next_str_field_offset,
                  int32_t str_start_offset, int32_t type_id, int8_t* data);

}  // namespace v1
}  // namespace codec
}  // namespace hybridse

// custom specialization of std::hash for timestamp, date and string
namespace std {
template <>
struct hash<hybridse::codec::Timestamp> {
    std::size_t operator()(const hybridse::codec::Timestamp& t) const {
        return std::hash<int64_t>()(t.ts_);
    }
};

template <>
struct hash<hybridse::codec::Date> {
    std::size_t operator()(const hybridse::codec::Date& t) const {
        return std::hash<int32_t>()(t.date_);
    }
};

template <>
struct hash<hybridse::codec::StringRef> {
    std::size_t operator()(const hybridse::codec::StringRef& t) const {
        return hybridse::base::hash(t.data_, t.size_, hybridse::codec::SEED);
    }
};

}  // namespace std

#endif  // INCLUDE_CODEC_TYPE_CODEC_H_
