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

#ifndef SRC_STORAGE_RECORD_H_
#define SRC_STORAGE_RECORD_H_

#include "storage/key_entry.h"

namespace openmldb {
namespace storage {

static const uint32_t DATA_BLOCK_BYTE_SIZE = sizeof(DataBlock);
static const uint32_t KEY_ENTRY_BYTE_SIZE = sizeof(KeyEntry);
static const uint32_t ENTRY_NODE_SIZE = sizeof(::openmldb::base::Node<::openmldb::base::Slice, void*>);
static const uint32_t DATA_NODE_SIZE = sizeof(::openmldb::base::Node<uint64_t, void*>);
static const uint32_t KEY_ENTRY_PTR_SIZE = sizeof(KeyEntry*);

static inline uint32_t GetRecordSize(uint32_t value_size) { return value_size + DATA_BLOCK_BYTE_SIZE; }

// the input height which is the height of skiplist node
static inline uint32_t GetRecordPkIdxSize(uint8_t height, uint32_t key_size, uint8_t key_entry_max_height) {
    return height * 8 + ENTRY_NODE_SIZE + KEY_ENTRY_BYTE_SIZE + key_size + key_entry_max_height * 8 + DATA_NODE_SIZE;
}

static inline uint32_t GetRecordPkMultiIdxSize(uint8_t height, uint32_t key_size, uint8_t key_entry_max_height,
                                               uint32_t ts_cnt) {
    return height * 8 + ENTRY_NODE_SIZE + key_size +
           (KEY_ENTRY_PTR_SIZE + KEY_ENTRY_BYTE_SIZE + key_entry_max_height * 8 + DATA_NODE_SIZE) * ts_cnt;
}

static inline uint32_t GetRecordTsIdxSize(uint8_t height) { return height * 8 + DATA_NODE_SIZE; }

struct StatisticsInfo {
    StatisticsInfo() {}
    StatisticsInfo(uint64_t idx_cnt_i, uint64_t record_cnt_i, uint64_t byte_size) :
        idx_cnt(idx_cnt_i), record_cnt(record_cnt_i), record_byte_size(byte_size) {}
    uint64_t idx_cnt = 0;
    uint64_t record_cnt = 0;
    uint64_t record_byte_size = 0;
};

}  // namespace storage
}  // namespace openmldb
#endif  // SRC_STORAGE_RECORD_H_
