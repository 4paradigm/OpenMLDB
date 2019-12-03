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
#include "glog/logging.h"

namespace fesql {
namespace storage {
namespace v1 {

int32_t GetStrField(const int8_t* row, uint32_t field_offset,
                    uint32_t next_str_field_offset, 
                    uint32_t str_start_offset,
                    uint32_t addr_space,
                    int8_t** data,
                    uint32_t* size) {
    if (row == NULL
            || data == NULL
            || size == NULL) return -1;

    const int8_t* row_with_offset = row + str_start_offset;
    switch (addr_space) {
        case 1: {
            // no next str field
            if (next_str_field_offset <= 0) {
                uint8_t str_offset = (uint8_t)(*(row_with_offset + field_offset * addr_space));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(total_length - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            } else {
                uint8_t str_offset = (uint8_t)(*(row_with_offset + field_offset * addr_space));
                uint8_t next_str_offset =
                    (uint8_t)(*(row_with_offset + next_str_field_offset * addr_space));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(next_str_offset - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            }
            break;
        }
        case 2: {
            // no next str field
            if (next_str_field_offset <= 0) {
                uint16_t str_offset = (uint16_t)(*(row_with_offset + field_offset * addr_space));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(total_length - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            } else {
                uint16_t str_offset = (uint16_t)(*(row_with_offset + field_offset * addr_space));
                uint16_t next_str_offset =
                    (uint16_t)(*(row_with_offset + next_str_field_offset * addr_space));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(next_str_offset - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            }
            break;
        }
        case 3: {
            uint32_t offset = field_offset * addr_space;
            // no next str field
            if (next_str_field_offset <= 0) {
                uint32_t str_offset = (uint8_t)(*(row_with_offset + offset));
                str_offset = (str_offset << 8) + (uint8_t)(*(row_with_offset + offset + 1));
                str_offset = (str_offset << 8) + (uint8_t)(*(row_with_offset + offset + 2));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(total_length - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            } else {
                uint32_t next_offset = next_str_field_offset * addr_space;
                uint32_t str_offset = (uint8_t)(*(row_with_offset + offset));
                str_offset = (str_offset << 8) + (uint8_t)(*(row_with_offset + offset + 1));
                str_offset = (str_offset << 8) + (uint8_t)(*(row_with_offset + offset + 2));
                uint32_t next_str_offset =
                    (uint8_t)(*(row_with_offset + next_offset));
                next_str_offset = (next_str_offset << 8) +
                                  (uint8_t)(*(row + next_offset + 1));
                next_str_offset = (next_str_offset << 8) +
                                  (uint8_t)(*(row + next_offset + 2));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(next_str_offset - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            }
            break;
        }
        case 4: {
            // no next str field
            if (next_str_field_offset <= 0) {
                uint32_t str_offset = (uint32_t)(*(row_with_offset + field_offset * addr_space));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(total_length - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            } else {
                uint32_t str_offset = (uint32_t)(*(row_with_offset + field_offset * addr_space));
                uint32_t next_str_offset =
                    (uint32_t)(*(row_with_offset + next_str_field_offset * addr_space));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(next_str_offset - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
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

void AddSymbol(::llvm::orc::JITDylib& jd,
        ::llvm::orc::MangleAndInterner& mi,
        const std::string& fn_name,
        void* fn_ptr) {
    ::llvm::StringRef symbol(fn_name);
    ::llvm::JITEvaluatedSymbol jit_symbol(
        ::llvm::pointerToJITTargetAddress(
            fn_ptr),
        ::llvm::JITSymbolFlags());
    ::llvm::orc::SymbolMap symbol_map;
    symbol_map.insert(std::make_pair(mi(symbol), jit_symbol));
    auto err = jd.define(::llvm::orc::absoluteSymbols(symbol_map));
    if (err) {
        LOG(WARNING) << "fail to add symbol " << fn_name;
    }else {
        LOG(INFO) << "add fn symbol " << fn_name << " done";
    }
}

void InitCodecSymbol(::llvm::orc::JITDylib& jd, 
        ::llvm::orc::MangleAndInterner& mi) {
    AddSymbol(jd, mi, "fesql_storage_get_int16_field", reinterpret_cast<void*>(&v1::GetInt16Field));
    AddSymbol(jd, mi, "fesql_storage_get_int32_field", reinterpret_cast<void*>(&v1::GetInt32Field));
    AddSymbol(jd, mi, "fesql_storage_get_int64_field", reinterpret_cast<void*>(&v1::GetInt64Field));
    AddSymbol(jd, mi, "fesql_storage_get_float_field", reinterpret_cast<void*>(&v1::GetFloatField));
    AddSymbol(jd, mi, "fesql_storage_get_double_field", reinterpret_cast<void*>(&v1::GetDoubleField));
    AddSymbol(jd, mi, "fesql_storage_get_str_addr_space", reinterpret_cast<void*>(&v1::GetAddrSpace));
    AddSymbol(jd, mi, "fesql_storage_get_str_field", reinterpret_cast<void*>(&v1::GetStrField));

}


void InitCodecSymbol(vm::FeSQLJIT* jit_ptr) {
    jit_ptr->AddSymbol("fesql_storage_get_int16_field", reinterpret_cast<void*>(&v1::GetInt16Field));
    jit_ptr->AddSymbol("fesql_storage_get_int32_field", reinterpret_cast<void*>(&v1::GetInt32Field));
    jit_ptr->AddSymbol("fesql_storage_get_int64_field", reinterpret_cast<void*>(&v1::GetInt64Field));
    jit_ptr->AddSymbol("fesql_storage_get_float_field", reinterpret_cast<void*>(&v1::GetFloatField));
    jit_ptr->AddSymbol("fesql_storage_get_double_field", reinterpret_cast<void*>(&v1::GetDoubleField));
    jit_ptr->AddSymbol("fesql_storage_get_str_addr_space", reinterpret_cast<void*>(&v1::GetAddrSpace));
    jit_ptr->AddSymbol("fesql_storage_get_str_field", reinterpret_cast<void*>(&v1::GetStrField));
}

}  // namespace storage
}  // namespace fesql
