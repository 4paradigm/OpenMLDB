//
// record.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize
// Date 2017-11-24
//

#ifndef RTIDB_RECORD_H
#define RTIDB_RECORD_H

#include "storage/segment.h"

namespace rtidb {
namespace storage {

const static uint32_t DATA_BLOCK_BYTE_SIZE = sizeof(DataBlock);
const static uint32_t KEY_ENTRY_BYTE_SIZE = sizeof(KeyEntry);
const static uint32_t ENTRY_NODE_SIZE = sizeof(::rtidb::base::Node<::rtidb::base::Slice, void*>);
const static uint32_t DATA_NODE_SIZE = sizeof(::rtidb::base::Node<uint64_t, void*>);
const static uint32_t MULTI_ENTRY_NODE_SIZE = sizeof(::rtidb::base::Node<::rtidb::base::Slice, void**>);
const static uint32_t KEY_ENTRY_PTR_SIZE = sizeof(KeyEntry*);

static inline uint32_t GetRecordSize(uint32_t value_size) {
    return value_size + DATA_BLOCK_BYTE_SIZE;
}

// the input height which is the height of skiplist node 
static inline uint32_t GetRecordPkIdxSize(uint8_t height, uint32_t key_size, uint8_t key_entry_max_height) {
    return height * 8 + ENTRY_NODE_SIZE + KEY_ENTRY_BYTE_SIZE + key_size + key_entry_max_height * 8 + DATA_NODE_SIZE;
}

static inline uint32_t GetRecordPkMultiIdxSize(uint8_t height, uint32_t key_size, 
            uint8_t key_entry_max_height, uint32_t ts_cnt) {
    return height * 8 + MULTI_ENTRY_NODE_SIZE + key_size + 
           (KEY_ENTRY_PTR_SIZE + KEY_ENTRY_BYTE_SIZE + key_entry_max_height * 8 + DATA_NODE_SIZE) * ts_cnt;
}

static inline uint32_t GetRecordTsIdxSize(uint8_t height) {
    return height * 8 + DATA_NODE_SIZE;
}

}
}
#endif

