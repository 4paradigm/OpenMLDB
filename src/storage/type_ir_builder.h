/*
 * type_ir_builder.h
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

#ifndef SRC_STORAGE_TYPE_IR_BUILDER_H_
#define SRC_STORAGE_TYPE_IR_BUILDER_H_

#include <stdint.h>
#include <cstddef>

namespace fesql {
namespace storage {

struct StringRef {
    uint32_t size;
    char* data;
};

struct String {
    uint32_t size;
    char* data;
};

struct Timestamp {
    uint64_t ts;
};

namespace v1 {

inline int32_t GetBoolField(const int8_t* row, uint32_t offset, bool* val) {
    int8_t value = *(row + offset);
    value == 1 ? * val = true : * val = false;
    return 0;
}

inline int32_t GetInt16Field(const int8_t* row, uint32_t offset, int16_t* val) {
    *val = *(reinterpret_cast<const int16_t*>(row + offset));
    return 0;
}

inline int32_t GetInt32Field(const int8_t* row, uint32_t offset, int32_t* val) {
    *val = *(reinterpret_cast<const int32_t*>(row + offset));
    return 0;
}

inline int32_t GetInt64Field(const int8_t* row, uint32_t offset, int64_t* val) {
    *val = *(reinterpret_cast<const int64_t*>(row + offset));
    return 0;
}

inline int32_t GetFloatField(const int8_t* row, uint32_t offset, float* val) {
    *val = *(reinterpret_cast<const float*>(row + offset));
    return 0;
}

inline int32_t GetDoubleField(const int8_t* row, uint32_t offset, double* val) {
    *val = *(reinterpret_cast<const double*>(row + offset));
    return 0;
}

inline int32_t GetStrAddr(const int8_t* row, uint32_t offset,
                          uint8_t addr_space, uint32_t* val) {
    switch (addr_space) {
        case 1:
            *val = *(reinterpret_cast<const uint8_t*>(row + offset));
            break;
        case 2:
            *val = *(reinterpret_cast<const uint16_t*>(row + offset));
            break;
        case 3: {
            const int8_t* ptr = row + offset;
            uint32_t str_offset = *(reinterpret_cast<const uint8_t*>(ptr));
            str_offset = (str_offset << 8) +
                         *(reinterpret_cast<const uint8_t*>(ptr + 1));
            str_offset = (str_offset << 8) +
                         *(reinterpret_cast<const uint8_t*>(ptr + 2));
            *val = str_offset;
            break;
        }
        case 4:
            *val = *(reinterpret_cast<const uint32_t*>(row + offset));
            break;
        default:
            return -1;
    }
    return 0;
}

// native get string field method
int32_t GetStrField(const int8_t* row, int32_t offset, int32_t next_str_offset,
                    int32_t addr_space, StringRef* sr);

}  // namespace v1
}  // namespace storage
}  // namespace fesql
#endif  // SRC_STORAGE_TYPE_IR_BUILDER_H_
