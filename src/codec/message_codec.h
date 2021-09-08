/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <string>
#include "base/endianconv.h"
#include "base/slice.h"
#include "butil/iobuf.h"

namespace openmldb {
namespace codec {

/** message format
 * -----------------------------------
 * | version | term | offset | value |
 * -----------------------------------
 * version: codec format version
 * term: raft term
 * offset: message offset
 * value: real message value
**/

constexpr size_t MESSAGE_HEADER = sizeof(uint8_t) + sizeof(uint64_t) * 2;
constexpr uint8_t CURRENT_VERSION = 1;

struct Message {
    uint8_t version = CURRENT_VERSION;
    uint64_t term = 0;
    uint64_t offset = 0;
    ::openmldb::base::Slice value;
};

class MessageCodec {
 public:
    static bool Encode(const Message& message, ::openmldb::base::Slice* result) {
        if (message.value.empty()) {
            return false;
        }
        size_t size = MESSAGE_HEADER + message.value.size();
        char* data = new char[size];
        // slice will free memory
        *result = ::openmldb::base::Slice(data, size, true);
        *(reinterpret_cast<uint8_t*>(data)) = message.version;
        data += sizeof(uint8_t);
        *(reinterpret_cast<uint64_t*>(data)) = message.term;
        memrev64ifbe(data);
        data += sizeof(uint64_t);
        *(reinterpret_cast<uint64_t*>(data)) = message.offset;
        memrev64ifbe(data);
        data += sizeof(uint64_t);
        memcpy(data, message.value.data(), message.value.size());
        return true;
    }

    static bool Encode(uint64_t offset, const ::butil::IOBuf& buf, ::openmldb::base::Slice* result) {
        if (buf.empty()) {
            return false;
        }
        size_t size = MESSAGE_HEADER + buf.size();
        char* data = new char[size];
        // slice will free memory
        *result = ::openmldb::base::Slice(data, size, true);
        *(reinterpret_cast<uint8_t*>(data)) = CURRENT_VERSION;
        data += sizeof(uint8_t);
        *(reinterpret_cast<uint64_t*>(data)) = 0;  // term is unused now.
        memrev64ifbe(data);
        data += sizeof(uint64_t);
        *(reinterpret_cast<uint64_t*>(data)) = offset;
        memrev64ifbe(data);
        data += sizeof(uint64_t);
        buf.copy_to(data);
        return true;
    }

    static bool Decode(const ::openmldb::base::Slice& raw_data, Message* message) {
        if (message == nullptr) {
            return false;
        }
        if (raw_data.size() <= MESSAGE_HEADER) {
            return false;
        }
        const char* value = raw_data.data();
        message->version = *(reinterpret_cast<const uint8_t*>(value));
        value += sizeof(uint8_t);
        message->term = intrev64ifbe(*(reinterpret_cast<const uint64_t*>(value)));
        value += sizeof(uint64_t);
        message->offset = intrev64ifbe(*(reinterpret_cast<const uint64_t*>(value)));
        value += sizeof(uint64_t);
        message->value = ::openmldb::base::Slice(value, raw_data.size() - MESSAGE_HEADER);
        return true;
    }

    static uint8_t GetCurrentVersion() { return CURRENT_VERSION; }
};

}  // namespace codec
}  // namespace openmldb
