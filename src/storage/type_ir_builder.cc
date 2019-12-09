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
#include <string>
#include <utility>
#include "glog/logging.h"
#include "proto/type.pb.h"
#include "storage/window.h"

namespace fesql {
namespace storage {
namespace v1 {

using fesql::storage::ColumnIteratorImpl;
using fesql::storage::ColumnStringIteratorImpl;
using fesql::storage::WindowIteratorImpl;

int32_t GetStrField(const int8_t* row, uint32_t field_offset,
                    uint32_t next_str_field_offset, uint32_t str_start_offset,
                    uint32_t addr_space, int8_t** data, uint32_t* size) {
    if (row == NULL || data == NULL || size == NULL) return -1;
    DLOG(INFO) << "GetStrField : " << field_offset << ", " << next_str_field_offset << ", " << str_start_offset << ", " <<addr_space;
    const int8_t* row_with_offset = row + str_start_offset;
    switch (addr_space) {
        case 1: {
            // no next str field
            if (next_str_field_offset <= 0) {
                uint8_t str_offset =
                    (uint8_t)(*(row_with_offset + field_offset * addr_space));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(total_length - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            } else {
                uint8_t str_offset =
                    (uint8_t)(*(row_with_offset + field_offset * addr_space));
                uint8_t next_str_offset = (uint8_t)(
                    *(row_with_offset + next_str_field_offset * addr_space));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(next_str_offset - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            }
            break;
        }
        case 2: {
            // no next str field
            if (next_str_field_offset <= 0) {
                uint16_t str_offset =
                    (uint16_t)(*(row_with_offset + field_offset * addr_space));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(total_length - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            } else {
                uint16_t str_offset =
                    (uint16_t)(*(row_with_offset + field_offset * addr_space));
                uint16_t next_str_offset = (uint16_t)(
                    *(row_with_offset + next_str_field_offset * addr_space));
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
                str_offset = (str_offset << 8) +
                             (uint8_t)(*(row_with_offset + offset + 1));
                str_offset = (str_offset << 8) +
                             (uint8_t)(*(row_with_offset + offset + 2));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(total_length - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            } else {
                uint32_t next_offset = next_str_field_offset * addr_space;
                uint32_t str_offset = (uint8_t)(*(row_with_offset + offset));
                str_offset = (str_offset << 8) +
                             (uint8_t)(*(row_with_offset + offset + 1));
                str_offset = (str_offset << 8) +
                             (uint8_t)(*(row_with_offset + offset + 2));
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
                uint32_t str_offset =
                    (uint32_t)(*(row_with_offset + field_offset * addr_space));
                uint32_t total_length = (uint32_t)(*(row + 2));
                const int8_t* ptr = row + str_offset;
                *size = (uint32_t)(total_length - str_offset);
                *data = (int8_t*)(ptr);  // NOLINT
            } else {
                uint32_t str_offset =
                    (uint32_t)(*(row_with_offset + field_offset * addr_space));
                uint32_t next_str_offset = (uint32_t)(
                    *(row_with_offset + next_str_field_offset * addr_space));
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

int32_t AppendString(int8_t* buf_ptr, uint32_t buf_size, int8_t* val,
                     uint32_t size, uint32_t str_start_offset,
                     uint32_t str_field_offset, uint32_t str_addr_space,
                     uint32_t str_body_offset) {
    uint32_t str_offset = str_start_offset + str_field_offset * str_addr_space;
    if (str_offset + size > buf_size) {
        LOG(WARNING) << "invalid str size expect " << buf_size << " but "
                     << str_offset + size;
        return -1;
    }
    int8_t* ptr_offset = buf_ptr + str_offset;
    switch (str_addr_space) {
        case 1: {
            *(reinterpret_cast<uint8_t*>(ptr_offset)) =
                (uint8_t)str_body_offset;
            break;
        }

        case 2: {
            *(reinterpret_cast<uint16_t*>(ptr_offset)) =
                (uint16_t)str_body_offset;
            break;
        }

        case 3: {
            *(reinterpret_cast<uint8_t*>(ptr_offset)) =
                str_body_offset & 0x0F00;
            *(reinterpret_cast<uint8_t*>(ptr_offset + 1)) =
                str_body_offset & 0x00F0;
            *(reinterpret_cast<uint8_t*>(ptr_offset + 2)) =
                str_body_offset & 0x000F;
            break;
        }

        default: {
            *(reinterpret_cast<uint32_t*>(ptr_offset)) = str_body_offset;
        }
    }

    if (size != 0) {
        memcpy(reinterpret_cast<char*>(buf_ptr + str_body_offset), val, size);
    }

    return str_body_offset + size;
}

int32_t GetStrCol(int8_t* input, int32_t str_field_offset,
                  int32_t next_str_field_offset, int32_t str_start_offset,
                  int32_t type_id, int8_t** data) {
    if (nullptr == input) {
        return -2;
    }
    WindowIteratorImpl* w = reinterpret_cast<WindowIteratorImpl*>(input);
    fesql::type::Type type = static_cast<fesql::type::Type>(type_id);
    switch (type) {
        case fesql::type::kVarchar: {
            ColumnStringIteratorImpl* impl = new ColumnStringIteratorImpl(
                *w, str_field_offset, next_str_field_offset, str_start_offset);
            *data = reinterpret_cast<int8_t*>(impl);
            break;
        }
        default: {
            *data = nullptr;
            return -2;
        }
    }
    return 0;
}

int32_t GetCol(int8_t* input, int32_t offset, int32_t type_id, int8_t** data) {
    fesql::type::Type type = static_cast<fesql::type::Type>(type_id);
    if (nullptr == input) {
        return -2;
    }
    WindowIteratorImpl* w = reinterpret_cast<WindowIteratorImpl*>(input);
    switch (type) {
        case fesql::type::kInt32: {
            ColumnIteratorImpl<int>* impl =
                new ColumnIteratorImpl<int>(*w, offset);
            *data = reinterpret_cast<int8_t*>(impl);
            break;
        }
        case fesql::type::kInt16: {
            ColumnIteratorImpl<int16_t>* impl =
                new ColumnIteratorImpl<int16_t>(*w, offset);
            *data = reinterpret_cast<int8_t*>(impl);
            break;
        }
        case fesql::type::kInt64: {
            ColumnIteratorImpl<int64_t>* impl =
                new ColumnIteratorImpl<int64_t>(*w, offset);
            *data = reinterpret_cast<int8_t*>(impl);
            break;
        }
        case fesql::type::kFloat: {
            ColumnIteratorImpl<float>* impl =
                new ColumnIteratorImpl<float>(*w, offset);
            *data = reinterpret_cast<int8_t*>(impl);
            break;
        }
        case fesql::type::kDouble: {
            ColumnIteratorImpl<double>* impl =
                new ColumnIteratorImpl<double>(*w, offset);
            *data = reinterpret_cast<int8_t*>(impl);
            break;
        }
        default: {
            LOG(WARNING) << "cannot get col for type "
                         << ::fesql::type::Type_Name(type);
            *data = nullptr;
            return -2;
        }
    }
    return 0;
}

}  // namespace v1

void AddSymbol(::llvm::orc::JITDylib& jd,           // NOLINT
               ::llvm::orc::MangleAndInterner& mi,  // NOLINT
               const std::string& fn_name, void* fn_ptr) {
    ::llvm::StringRef symbol(fn_name);
    ::llvm::JITEvaluatedSymbol jit_symbol(
        ::llvm::pointerToJITTargetAddress(fn_ptr), ::llvm::JITSymbolFlags());
    ::llvm::orc::SymbolMap symbol_map;
    symbol_map.insert(std::make_pair(mi(symbol), jit_symbol));
    auto err = jd.define(::llvm::orc::absoluteSymbols(symbol_map));
    if (err) {
        LOG(WARNING) << "fail to add symbol " << fn_name;
    } else {
        LOG(INFO) << "add fn symbol " << fn_name << " done";
    }
}

void InitCodecSymbol(::llvm::orc::JITDylib& jd,             // NOLINT
                     ::llvm::orc::MangleAndInterner& mi) {  // NOLINT
    // decode
    AddSymbol(jd, mi, "fesql_storage_get_int16_field",
              reinterpret_cast<void*>(&v1::GetInt16Field));
    AddSymbol(jd, mi, "fesql_storage_get_int32_field",
              reinterpret_cast<void*>(&v1::GetInt32Field));
    AddSymbol(jd, mi, "fesql_storage_get_int64_field",
              reinterpret_cast<void*>(&v1::GetInt64Field));
    AddSymbol(jd, mi, "fesql_storage_get_float_field",
              reinterpret_cast<void*>(&v1::GetFloatField));
    AddSymbol(jd, mi, "fesql_storage_get_double_field",
              reinterpret_cast<void*>(&v1::GetDoubleField));
    AddSymbol(jd, mi, "fesql_storage_get_str_addr_space",
              reinterpret_cast<void*>(&v1::GetAddrSpace));
    AddSymbol(jd, mi, "fesql_storage_get_str_field",
              reinterpret_cast<void*>(&v1::GetStrField));
    AddSymbol(jd, mi, "fesql_storage_get_col",
              reinterpret_cast<void*>(&v1::GetCol));
    AddSymbol(jd, mi, "fesql_storage_get_str_col",
              reinterpret_cast<void*>(&v1::GetStrCol));

    // encode
    AddSymbol(jd, mi, "fesql_storage_encode_int16_field",
              reinterpret_cast<void*>(&v1::AppendInt16));

    AddSymbol(jd, mi, "fesql_storage_encode_int32_field",
              reinterpret_cast<void*>(&v1::AppendInt32));

    AddSymbol(jd, mi, "fesql_storage_encode_int64_field",
              reinterpret_cast<void*>(&v1::AppendInt64));

    AddSymbol(jd, mi, "fesql_storage_encode_float_field",
              reinterpret_cast<void*>(&v1::AppendFloat));

    AddSymbol(jd, mi, "fesql_storage_encode_double_field",
              reinterpret_cast<void*>(&v1::AppendDouble));

    AddSymbol(jd, mi, "fesql_storage_encode_string_field",
              reinterpret_cast<void*>(&v1::AppendString));
    AddSymbol(jd, mi, "fesql_storage_encode_calc_size",
              reinterpret_cast<void*>(&v1::CalcTotalLength));
}

void InitCodecSymbol(vm::FeSQLJIT* jit_ptr) {
    jit_ptr->AddSymbol("fesql_storage_get_int16_field",
                       reinterpret_cast<void*>(&v1::GetInt16Field));
    jit_ptr->AddSymbol("fesql_storage_get_int32_field",
                       reinterpret_cast<void*>(&v1::GetInt32Field));
    jit_ptr->AddSymbol("fesql_storage_get_int64_field",
                       reinterpret_cast<void*>(&v1::GetInt64Field));
    jit_ptr->AddSymbol("fesql_storage_get_float_field",
                       reinterpret_cast<void*>(&v1::GetFloatField));
    jit_ptr->AddSymbol("fesql_storage_get_double_field",
                       reinterpret_cast<void*>(&v1::GetDoubleField));
    jit_ptr->AddSymbol("fesql_storage_get_str_addr_space",
                       reinterpret_cast<void*>(&v1::GetAddrSpace));
    jit_ptr->AddSymbol("fesql_storage_get_str_field",
                       reinterpret_cast<void*>(&v1::GetStrField));
    jit_ptr->AddSymbol("fesql_storage_get_col",
                       reinterpret_cast<void*>(&v1::GetCol));
    jit_ptr->AddSymbol("fesql_storage_get_str_col",
                       reinterpret_cast<void*>(&v1::GetStrCol));

    jit_ptr->AddSymbol("fesql_storage_encode_int16_field",
                       reinterpret_cast<void*>(&v1::AppendInt16));

    jit_ptr->AddSymbol("fesql_storage_encode_int32_field",
                       reinterpret_cast<void*>(&v1::AppendInt32));

    jit_ptr->AddSymbol("fesql_storage_encode_int64_field",
                       reinterpret_cast<void*>(&v1::AppendInt64));

    jit_ptr->AddSymbol("fesql_storage_encode_float_field",
                       reinterpret_cast<void*>(&v1::AppendFloat));

    jit_ptr->AddSymbol("fesql_storage_encode_double_field",
                       reinterpret_cast<void*>(&v1::AppendDouble));

    jit_ptr->AddSymbol("fesql_storage_encode_string_field",
                       reinterpret_cast<void*>(&v1::AppendString));

    jit_ptr->AddSymbol("fesql_storage_encode_calc_size",
                       reinterpret_cast<void*>(&v1::CalcTotalLength));
}

}  // namespace storage
}  // namespace fesql
