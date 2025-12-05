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

#ifndef HYBRIDSE_INCLUDE_CODEC_FE_SCHEMA_CODEC_H_
#define HYBRIDSE_INCLUDE_CODEC_FE_SCHEMA_CODEC_H_

#include <cstring>
#include <string>
#include "vm/catalog.h"

namespace hybridse {
namespace codec {

const uint32_t MAX_ROW_BYTE_SIZE = 1024 * 1024;
const uint32_t FIELD_BYTE_SIZE = 3;
const uint16_t HEADER_SIZE = 2;

class SchemaCodec {
 public:
    static bool Encode(const vm::Schema& schema, std::string* buffer) {
        if (buffer == NULL) return false;
        if (schema.size() == 0) return true;
        uint32_t byte_size = GetSize(schema);
        if (byte_size > MAX_ROW_BYTE_SIZE) {
            return false;
        }
        buffer->resize(byte_size);
        char* cbuffer = reinterpret_cast<char*>(&(buffer->at(0)));
        uint16_t cnt = static_cast<uint16_t>(schema.size());
        memcpy(cbuffer, static_cast<const void*>(&cnt), 2);
        cbuffer += 2;
        vm::Schema::const_iterator it = schema.begin();
        for (; it != schema.end(); ++it) {
            uint8_t is_constant = it->is_constant() ? 1 : 0;
            memcpy(cbuffer, static_cast<const void*>(&is_constant), 1);
            cbuffer += 1;
            uint8_t type = static_cast<uint8_t>(it->type());
            memcpy(cbuffer, static_cast<const void*>(&type), 1);
            cbuffer += 1;
            if (it->name().size() >= 128) {
                return false;
            }
            uint8_t name_size = static_cast<uint8_t>(it->name().size());
            memcpy(cbuffer, static_cast<const void*>(&name_size), 1);
            cbuffer += 1;
            memcpy(cbuffer, static_cast<const void*>(it->name().c_str()),
                   name_size);
            cbuffer += name_size;
        }
        return true;
    }

    static bool Decode(const std::string& buf, codec::Schema* schema) {
        if (schema == NULL) return false;
        if (buf.size() <= 0) return true;
        const char* buffer = buf.c_str();
        uint32_t buf_size = buf.size();
        if (buf_size < HEADER_SIZE) {
            return false;
        }
        uint16_t cnt = 0;
        memcpy(static_cast<void*>(&cnt), buffer, 2);
        buffer += 2;
        schema->Reserve(cnt);
        uint32_t read_size = HEADER_SIZE;
        while (read_size < buf_size) {
            if (buf_size - read_size < FIELD_BYTE_SIZE) {
                break;
            }
            uint8_t is_constant = 0;
            memcpy(static_cast<void*>(&is_constant), buffer, 1);
            buffer += 1;
            uint8_t type = 0;
            memcpy(static_cast<void*>(&type), buffer, 1);
            if (!::hybridse::type::Type_IsValid(type)) {
                return false;
            }
            buffer += 1;
            uint8_t name_size = 0;
            memcpy(static_cast<void*>(&name_size), buffer, 1);
            buffer += 1;
            uint32_t total_size = FIELD_BYTE_SIZE + name_size;
            if (buf_size - read_size < total_size) {
                return false;
            }
            ::hybridse::type::ColumnDef* column = schema->Add();
            column->set_name(buffer, name_size);
            buffer += name_size;
            read_size += total_size;
            column->set_type(static_cast<::hybridse::type::Type>(type));
            column->set_is_constant(is_constant == 1);
        }
        return true;
    }

 private:
    static uint32_t GetSize(const vm::Schema& schema) {
        uint32_t byte_size = HEADER_SIZE;
        vm::Schema::const_iterator it = schema.begin();
        for (; it != schema.end(); ++it) {
            byte_size += (FIELD_BYTE_SIZE + it->name().size());
        }
        return byte_size;
    }
};

}  // namespace codec
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_CODEC_FE_SCHEMA_CODEC_H_
