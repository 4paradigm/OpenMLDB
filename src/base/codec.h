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

using ::rtidb::storage::DataBlock;

using ::baidu::common::DEBUG;

namespace rtidb {
namespace base {

static inline void Encode(uint64_t time, const DataBlock* data, char* buffer, uint32_t offset) {
    buffer += offset;
    uint32_t total_size = 8 + data->size;
    LOG(DEBUG, "encode size %d", total_size);
    memcpy(buffer, static_cast<const void*>(&total_size), 4);
    buffer += 4;
    memcpy(buffer, static_cast<const void*>(&time), 8);
    buffer += 8;
    memcpy(buffer, static_cast<const void*>(data->data), data->size);
}

static inline void Decode(const std::string* str, std::vector<std::pair<uint64_t, std::string*> >& pairs) {
    const char* buffer = str->c_str();
    uint32_t total_size = str->length();
    LOG(DEBUG, "total size %d %s", total_size, DebugString(*str).c_str());
    while (total_size > 0) {
        uint32_t size = 0;
        memcpy(static_cast<void*>(&size), buffer, 4);
        LOG(DEBUG, "decode size %d", size);
        buffer += 4;
        uint64_t time = 0;
        memcpy(static_cast<void*>(&time), buffer, 8);
        buffer += 8;
        assert(size >= 8);
        std::string* data = new std::string(size - 8, '0');
        memcpy(reinterpret_cast<char*>(& ((*data)[0])), buffer, size - 8);
        buffer += (size - 8);
        pairs.push_back(std::make_pair(time, data));
        total_size -= (size + 4);
    }
}

static inline void EncodeBigEndian(char* buf, uint64_t value) {
    buf[0] = (value >> 56) & 0xff;
    buf[1] = (value >> 48) & 0xff;
    buf[2] = (value >> 40) & 0xff;
    buf[3] = (value >> 32) & 0xff;
    buf[4] = (value >> 24) & 0xff;
    buf[5] = (value >> 16) & 0xff;
    buf[6] = (value >> 8) & 0xff;
    buf[7] = value & 0xff;
}

static inline uint64_t DecodeBigEndian64(const char* buf) {
    return ((static_cast<uint64_t>(static_cast<unsigned char>(buf[0]))) << 56
        | (static_cast<uint64_t>(static_cast<unsigned char>(buf[1])) << 48)
        | (static_cast<uint64_t>(static_cast<unsigned char>(buf[2])) << 40)
        | (static_cast<uint64_t>(static_cast<unsigned char>(buf[3])) << 32)
        | (static_cast<uint64_t>(static_cast<unsigned char>(buf[4])) << 24)
        | (static_cast<uint64_t>(static_cast<unsigned char>(buf[5])) << 16)
        | (static_cast<uint64_t>(static_cast<unsigned char>(buf[6])) << 8)
        | (static_cast<uint64_t>(static_cast<unsigned char>(buf[7]))));
}

static inline void EncodeBigEndian(char* buf, uint32_t value) {
    buf[0] = (value >> 24) & 0xff;
    buf[1] = (value >> 16) & 0xff;
    buf[2] = (value >> 8) & 0xff;
    buf[3] = value & 0xff;
}

static inline uint32_t DecodeBigEndian32(const char* buf) {
    return ((static_cast<uint64_t>(static_cast<unsigned char>(buf[0])) << 24)
        | (static_cast<uint64_t>(static_cast<unsigned char>(buf[1])) << 16)
        | (static_cast<uint64_t>(static_cast<unsigned char>(buf[2])) << 8)
        | (static_cast<uint64_t>(static_cast<unsigned char>(buf[3]))));
}



}

}

#endif /* !CODEC_H */
