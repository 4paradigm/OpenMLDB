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
#include "vm/jit.h"

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

inline int8_t GetBoolField(const int8_t* row, uint32_t offset) {
    int8_t value = *(row + offset);
    return value;
}

inline int16_t GetInt16Field(const int8_t* row, uint32_t offset) {
    return  *(reinterpret_cast<const int16_t*>(row + offset));
}

inline int32_t GetInt32Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int32_t*>(row + offset));
}

inline int64_t GetInt64Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int64_t*>(row + offset));
}

inline float GetFloatField(const int8_t* row, uint32_t offset) {
    return  *(reinterpret_cast<const float*>(row + offset));
}

inline double GetDoubleField(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const double*>(row + offset));
}

// native get string field method
int32_t GetStrField(const int8_t* row, 
        uint32_t str_field_offset,
        uint32_t next_str_field_offset,
        uint32_t str_start_offset,
        uint32_t addr_space, 
        int8_t** data, uint32_t* size);


}  // namespace v1

void InitCodecSymbol(::llvm::orc::JITDylib& jd, 
        ::llvm::orc::MangleAndInterner& mi);

void InitCodecSymbol(vm::FeSQLJIT* jit_ptr);

}  // namespace storage
}  // namespace fesql
#endif  // SRC_STORAGE_TYPE_IR_BUILDER_H_
