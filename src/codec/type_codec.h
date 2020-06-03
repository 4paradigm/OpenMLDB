/*
 * type_codec.h
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

#ifndef SRC_CODEC_TYPE_CODEC_H_
#define SRC_CODEC_TYPE_CODEC_H_

#include <stdint.h>
#include <cstddef>
#include <string>
#include <vector>
#include "butil/iobuf.h"
#include "glog/logging.h"

namespace fesql {
namespace codec {

struct StringRef {
    StringRef() : size_(0), data_(nullptr) {}
    StringRef(uint32_t size, char* data) : size_(size), data_(strdup(data)) {}
    ~StringRef() {}
    const bool IsNull() const { return nullptr == data_; }
    const std::string ToString() const {
        return size_ == 0 ? "" : std::string(data_, size_);
    }
    uint32_t size_;
    char* data_;
};

inline static int compare(const StringRef& a, const StringRef& b) {
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
static const StringRef operator+(const StringRef& a, const StringRef& b) {
    StringRef str;
    str.size_ = a.size_ + b.size_;
    str.data_ = static_cast<char*>(malloc(str.size_ + 1));
    if (a.size_ > 0) {
        memcpy(str.data_, a.data_, a.size_);
    }
    if (b.size_ > 0) {
        memcpy(str.data_ + a.size_, b.data_, b.size_);
    }
    str.data_[str.size_] = '\0';
    return str;
}
static bool operator==(const StringRef& a, const StringRef& b) {
    return 0 == compare(a, b);
}
static bool operator!=(const StringRef& a, const StringRef& b) {
    return 0 != compare(a, b);
}
static bool operator>=(const StringRef& a, const StringRef& b) {
    return compare(a, b) >= 0;
}
static bool operator>(const StringRef& a, const StringRef& b) {
    return compare(a, b) > 0;
}
static bool operator<=(const StringRef& a, const StringRef& b) {
    return compare(a, b) <= 0;
}
static bool operator<(const StringRef& a, const StringRef& b) {
    return compare(a, b) < 0;
}

struct Timestamp {
    Timestamp() : ts_(0) {}
    explicit Timestamp(int64_t ts) : ts_(ts) {}
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

static const Timestamp operator+(const Timestamp& a, const Timestamp& b) {
    return Timestamp(a.ts_ + b.ts_);
}
static const Timestamp operator-(const Timestamp& a, const Timestamp& b) {
    return Timestamp(a.ts_ - b.ts_);
}
static const Timestamp operator/(const Timestamp& a, const int64_t b) {
    return Timestamp(static_cast<int64_t>(a.ts_ / b));
}
static bool operator>(const Timestamp& a, const Timestamp& b) {
    return a.ts_ > b.ts_;
}
static bool operator<(const Timestamp& a, const Timestamp& b) {
    return a.ts_ < b.ts_;
}
static bool operator>=(const Timestamp& a, const Timestamp& b) {
    return a.ts_ >= b.ts_;
}
static bool operator<=(const Timestamp& a, const Timestamp& b) {
    return a.ts_ <= b.ts_;
}
static bool operator==(const Timestamp& a, const Timestamp& b) {
    return a.ts_ == b.ts_;
}
static bool operator!=(const Timestamp& a, const Timestamp& b) {
    return a.ts_ != b.ts_;
}

struct Date {
    Date() : date_(0) {}
    explicit Date(int32_t date) : date_(date) {}
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
    int32_t date_;
};
static bool operator>(const Date& a, const Date& b) {
    return a.date_ > b.date_;
}
static bool operator<(const Date& a, const Date& b) {
    return a.date_ < b.date_;
}
static bool operator>=(const Date& a, const Date& b) {
    return a.date_ >= b.date_;
}
static bool operator<=(const Date& a, const Date& b) {
    return a.date_ <= b.date_;
}
static bool operator==(const Date& a, const Date& b) {
    return a.date_ == b.date_;
}
static bool operator!=(const Date& a, const Date& b) {
    return a.date_ != b.date_;
}

struct ListRef {
    int8_t* list;
};

struct IteratorRef {
    int8_t* iterator;
};

namespace v1 {

static constexpr uint8_t VERSION_LENGTH = 2;
static constexpr uint8_t SIZE_LENGTH = 4;
// calc the total row size with primary_size, str field count and str_size
inline uint32_t CalcTotalLength(uint32_t primary_size, uint32_t str_field_cnt,
                                uint32_t str_size, uint32_t* str_addr_space) {
    uint32_t total_size = primary_size + str_size;
    if (total_size + str_field_cnt <= UINT8_MAX) {
        *str_addr_space = 1;
        return total_size + str_field_cnt;
    } else if (total_size + str_field_cnt * 2 <= UINT16_MAX) {
        *str_addr_space = 2;
        return total_size + str_field_cnt * 2;
    } else if (total_size + str_field_cnt * 3 <= 1 << 24) {
        *str_addr_space = 3;
        return total_size + str_field_cnt * 3;
    } else {
        *str_addr_space = 4;
        return total_size + str_field_cnt * 4;
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

int32_t AppendString(int8_t* buf_ptr, uint32_t buf_size, int8_t* val,
                     uint32_t size, uint32_t str_start_offset,
                     uint32_t str_field_offset, uint32_t str_addr_space,
                     uint32_t str_body_offset);

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

inline int8_t GetBoolField(const butil::IOBuf& row, uint32_t offset) {
    int8_t value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 1, offset);
    return value;
}

inline int8_t GetBoolField(const int8_t* row, uint32_t offset) {
    int8_t value = *(row + offset);
    return value;
}

inline int16_t GetInt16Field(const butil::IOBuf& row, uint32_t offset) {
    int16_t value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 2, offset);
    return value;
}

inline int16_t GetInt16Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int16_t*>(row + offset));
}

inline int32_t GetInt32Field(const butil::IOBuf& row, uint32_t offset) {
    int32_t value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 4, offset);
    return value;
}

inline int32_t GetInt32Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int32_t*>(row + offset));
}

inline int64_t GetInt64Field(const butil::IOBuf& row, uint32_t offset) {
    int64_t value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 8, offset);
    return value;
}

inline int64_t GetInt64Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int64_t*>(row + offset));
}

inline float GetFloatField(const butil::IOBuf& row, uint32_t offset) {
    float value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 4, offset);
    return value;
}

inline float GetFloatField(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const float*>(row + offset));
}
inline Timestamp GetTimestampField(const int8_t* row, uint32_t offset) {
    return Timestamp(*(reinterpret_cast<const int64_t*>(row + offset)));
}

inline double GetDoubleField(const butil::IOBuf& row, uint32_t offset) {
    double value = 0;
    row.copy_to(reinterpret_cast<void*>(&value), 8, offset);
    return value;
}

inline double GetDoubleField(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const double*>(row + offset));
}

// native get string field method
int32_t GetStrField(const int8_t* row, uint32_t str_field_offset,
                    uint32_t next_str_field_offset, uint32_t str_start_offset,
                    uint32_t addr_space, int8_t** data, uint32_t* size);

int32_t GetStrField(const butil::IOBuf& row, uint32_t str_field_offset,
                    uint32_t next_str_field_offset, uint32_t str_start_offset,
                    uint32_t addr_space, butil::IOBuf* output);

int32_t GetCol(int8_t* input, int32_t row_idx, int32_t offset, int32_t type_id,
               int8_t* data);

int32_t GetStrCol(int8_t* input, int32_t row_idx, int32_t str_field_offset,
                  int32_t next_str_field_offset, int32_t str_start_offset,
                  int32_t type_id, int8_t* data);

}  // namespace v1
}  // namespace codec
}  // namespace fesql
#endif  // SRC_CODEC_TYPE_CODEC_H_
