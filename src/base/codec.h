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
#include "storage/segment.h"

using ::rtidb::storage::DataBlock;

namespace rtidb {
namespace base {

static inline void Encode(uint64_t time, const DataBlock* data, char* buffer) {
    uint32_t total_size = 8 + data->size;
    memcpy(buffer, static_cast<const void*>(&total_size), 4);
    buffer += 4;
    memcpy(buffer, static_cast<const void*>(&time), 8);
    buffer += 8;
    memcpy(buffer, static_cast<const void*>(data->data), data->size);
}

}
}

#endif /* !CODEC_H */
