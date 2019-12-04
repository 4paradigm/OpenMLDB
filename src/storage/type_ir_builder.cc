/*
 * type_ir_builder.cc
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

#include "storage/type_ir_builder.h"

namespace fesql {
namespace storage {
namespace v1 {

int32_t GetStrField(const int8_t* row, uint32_t offset,
                    uint32_t next_str_field_offset, uint32_t addr_space,
                    StringRef* sr) {
    if (row == NULL || sr == NULL) return -1;
    switch (addr_space) {
        case 1: {
            // no next str field
            if (next_str_field_offset <= 0) {
                uint8_t str_offset = (uint8_t)(*(row + offset));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                sr->size = (uint32_t)(total_length - str_offset);
                sr->data = (char*)(ptr);  // NOLINT
            } else {
                uint8_t str_offset = (uint8_t)(*(row + offset));
                uint8_t next_str_offset =
                    (uint8_t)(*(row + next_str_field_offset));
                const int8_t* ptr = row + str_offset;
                sr->size = (uint32_t)(next_str_offset - str_offset);
                sr->data = (char*)(ptr);  // NOLINT
            }
            break;
        }
        case 2: {
            // no next str field
            if (next_str_field_offset <= 0) {
                uint16_t str_offset = (uint16_t)(*(row + offset));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                sr->size = (uint32_t)(total_length - str_offset);
                sr->data = (char*)(ptr);  // NOLINT
            } else {
                uint16_t str_offset = (uint16_t)(*(row + offset));
                uint16_t next_str_offset =
                    (uint16_t)(*(row + next_str_field_offset));
                const int8_t* ptr = row + str_offset;
                sr->size = (uint32_t)(next_str_offset - str_offset);
                sr->data = (char*)(ptr);  // NOLINT
            }
            break;
        }
        case 3: {
            // no next str field
            if (next_str_field_offset <= 0) {
                uint32_t str_offset = (uint8_t)(*(row + offset));
                str_offset = (str_offset << 8) + (uint8_t)(*(row + offset + 1));
                str_offset = (str_offset << 8) + (uint8_t)(*(row + offset + 2));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                sr->size = (uint32_t)(total_length - str_offset);
                sr->data = (char*)(ptr);  // NOLINT
            } else {
                uint32_t str_offset = (uint8_t)(*(row + offset));
                str_offset = (str_offset << 8) + (uint8_t)(*(row + offset + 1));
                str_offset = (str_offset << 8) + (uint8_t)(*(row + offset + 2));
                uint32_t next_str_offset =
                    (uint8_t)(*(row + next_str_field_offset));
                next_str_offset = (next_str_offset << 8) +
                                  (uint8_t)(*(row + next_str_field_offset + 1));
                next_str_offset = (next_str_offset << 8) +
                                  (uint8_t)(*(row + next_str_field_offset + 2));
                const int8_t* ptr = row + str_offset;
                sr->size = (uint32_t)(next_str_offset - str_offset);
                sr->data = (char*)(ptr);  // NOLINT
            }
            break;
        }
        case 4: {
            // no next str field
            if (next_str_field_offset <= 0) {
                uint32_t str_offset = (uint32_t)(*(row + offset));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                sr->size = (uint32_t)(total_length - str_offset);
                sr->data = (char*)(ptr);  // NOLINT
            } else {
                uint32_t str_offset = (uint32_t)(*(row + offset));
                uint32_t next_str_offset =
                    (uint32_t)(*(row + next_str_field_offset));
                const int8_t* ptr = row + str_offset;
                sr->size = (uint32_t)(next_str_offset - str_offset);
                sr->data = (char*)(ptr);  // NOLINT
            }
            break;
        }
        default: {
            return -2;
        }
    }
    return 0;
}

}  // namespace v1
}  // namespace storage
}  // namespace fesql
