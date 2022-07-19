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

#ifndef HYBRIDSE_INCLUDE_CODEC_TYPE_CODEC_H_
#define HYBRIDSE_INCLUDE_CODEC_TYPE_CODEC_H_

#include <stdint.h>
#include <cstddef>
#include <string>
#include <vector>
#include "base/fe_hash.h"
#include "base/mem_pool.h"
#include "base/string_ref.h"
#include "base/type.h"
#include "glog/logging.h"

namespace hybridse {
namespace codec {

static const uint32_t SEED = 0xe17a1465;
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

inline openmldb::base::Timestamp GetTimestampFieldUnsafe(const int8_t* row, uint32_t offset) {
    return openmldb::base::Timestamp(*(reinterpret_cast<const int64_t*>(row + offset)));
}

inline openmldb::base::Timestamp GetTimestampField(const int8_t* row, uint32_t idx,
                                   uint32_t offset, int8_t* is_null) {
    if (row == nullptr || IsNullAt(row, idx)) {
        *is_null = true;
        return openmldb::base::Timestamp();
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

int32_t GetInnerRowsRangeList(int8_t* input, int64_t start_key, int64_t start_offset_rows, int64_t end_offset_range,
                              int8_t* data);

int32_t GetStrCol(int8_t* input, int32_t row_idx, uint32_t col_idx,
                  int32_t str_field_offset, int32_t next_str_field_offset,
                  int32_t str_start_offset, int32_t type_id, int8_t* data);

}  // namespace v1
}  // namespace codec
}  // namespace hybridse

// custom specialization of std::hash for timestamp, date and string
namespace std {
template <>
struct hash<openmldb::base::Timestamp> {
    std::size_t operator()(const openmldb::base::Timestamp& t) const {
        return std::hash<int64_t>()(t.ts_);
    }
};

template <>
struct hash<openmldb::base::Date> {
    std::size_t operator()(const openmldb::base::Date& t) const {
        return std::hash<int32_t>()(t.date_);
    }
};

template <>
struct hash<openmldb::base::StringRef> {
    std::size_t operator()(const openmldb::base::StringRef& t) const {
        return hybridse::base::hash(t.data_, t.size_, hybridse::codec::SEED);
    }
};

}  // namespace std

#endif  // HYBRIDSE_INCLUDE_CODEC_TYPE_CODEC_H_
