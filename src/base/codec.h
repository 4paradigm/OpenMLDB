//
// codec.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31 
// 


#ifndef RTIDB_BASE_CODEC_H
#define RTIDB_BASE_CODEC_H

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <map>
#include "storage/segment.h"
#include "logging.h"
#include "base/strings.h"
#include "base/endianconv.h"

using ::rtidb::storage::DataBlock;

using ::baidu::common::DEBUG;

namespace rtidb {
namespace base {

static inline void Encode(uint64_t time, const DataBlock* data, char* buffer, uint32_t offset) {
    buffer += offset;
    uint32_t total_size = 8 + data->size;
    PDLOG(DEBUG, "encode size %d", total_size);
    memcpy(buffer, static_cast<const void*>(&total_size), 4);
    memrev32ifbe(buffer);
    buffer += 4;
    memcpy(buffer, static_cast<const void*>(&time), 8);
    memrev64ifbe(buffer);
    buffer += 8;
    memcpy(buffer, static_cast<const void*>(data->data), data->size);
}

// encode pk, ts and value
static inline void EncodeFull(const std::string& pk, uint64_t time, const DataBlock* data, char* buffer, uint32_t offset) {
    buffer += offset;
    uint32_t pk_size = pk.length();
    uint32_t total_size = 8 + pk_size + data->size;
    PDLOG(DEBUG, "encode total size %u pk size %u", total_size, pk_size);
    memcpy(buffer, static_cast<const void*>(&total_size), 4);
    memrev32ifbe(buffer);
    buffer += 4;
    memcpy(buffer, static_cast<const void*>(&pk_size), 4);
    memrev32ifbe(buffer);
    buffer += 4;
    memcpy(buffer, static_cast<const void*>(&time), 8);
    memrev64ifbe(buffer);
    buffer += 8;
    memcpy(buffer, static_cast<const void*>(pk.c_str()), pk_size);
    buffer += pk_size;
    memcpy(buffer, static_cast<const void*>(data->data), data->size);
}

static inline void Decode(const std::string* str, std::vector<std::pair<uint64_t, std::string*> >& pairs) {
    const char* buffer = str->c_str();
    uint32_t total_size = str->length();
    PDLOG(DEBUG, "total size %d %s", total_size, DebugString(*str).c_str());
    while (total_size > 0) {
        uint32_t size = 0;
        memcpy(static_cast<void*>(&size), buffer, 4);
        memrev32ifbe(static_cast<void*>(&size));
        PDLOG(DEBUG, "decode size %d", size);
        buffer += 4;
        uint64_t time = 0;
        memcpy(static_cast<void*>(&time), buffer, 8);
        memrev64ifbe(static_cast<void*>(&time));
        buffer += 8;
        assert(size >= 8);
        std::string* data = new std::string(size - 8, '0');
        memcpy(reinterpret_cast<char*>(& ((*data)[0])), buffer, size - 8);
        buffer += (size - 8);
        pairs.push_back(std::make_pair(time, data));
        total_size -= (size + 4);
    }
}

static inline void DecodeFull(const std::string* str, std::map<std::string, std::vector<std::pair<uint64_t, std::string*>>>& value_map) {
    const char* buffer = str->c_str();
    uint32_t total_size = str->length();
    PDLOG(DEBUG, "total size %d %s", total_size, DebugString(*str).c_str());
    while (total_size > 0) {
        uint32_t size = 0;
        memcpy(static_cast<void*>(&size), buffer, 4);
        memrev32ifbe(static_cast<void*>(&size));
        PDLOG(DEBUG, "decode size %d", size);
        buffer += 4;
        uint32_t pk_size = 0;
        memcpy(static_cast<void*>(&pk_size), buffer, 4);
        buffer += 4;
        memrev32ifbe(static_cast<void*>(&pk_size));
        PDLOG(DEBUG, "decode size %d", pk_size);
        assert(size > pk_size + 8);
        uint64_t time = 0;
        memcpy(static_cast<void*>(&time), buffer, 8);
        memrev64ifbe(static_cast<void*>(&time));
        buffer += 8;
        std::string pk(buffer, pk_size);
        buffer += pk_size;
        uint32_t value_size = size - 8 - pk_size;
        std::string* data = new std::string(value_size, '0');
        memcpy(reinterpret_cast<char*>(& ((*data)[0])), buffer, value_size);
        buffer += value_size;
        if (value_map.find(pk) == value_map.end()) {
            value_map.insert(std::make_pair(pk, std::vector<std::pair<uint64_t, std::string*>>()));
        }
        value_map[pk].push_back(std::make_pair(time, data));
        total_size -= (size + 8);
    }
}

}
}

#endif /* !CODEC_H */
