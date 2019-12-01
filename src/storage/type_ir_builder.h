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

int32_t GetBoolField(const int8_t* row, uint32_t offset, bool* val);

int32_t GetInt16Field(const int8_t* row, uint32_t offset, int16_t* val);

int32_t GetInt32Field(const int8_t* row, uint32_t offset, int32_t* val);

int32_t GetInt64Field(const int8_t* row, uint32_t offset, int64_t* val);

int32_t GetFloatField(const int8_t* row, uint32_t offset, float* val);

int32_t GetDoubleField(const int8_t* row, uint32_t offset, double* val);

int32_t GetStrAddr(const int8_t* row, uint32_t offset, uint8_t addr_space,
                   uint32_t* val);

// native get string field method
int32_t GetStrField(const int8_t* row, int32_t offset, int32_t next_str_offset,
                    int32_t addr_space, StringRef* sr);

}  // namespace v1
}  // namespace storage
}  // namespace fesql
#endif  // SRC_STORAGE_TYPE_IR_BUILDER_H_
