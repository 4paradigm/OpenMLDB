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

#include <vector>
#include "storage/segment.h"

namespace openmldb {
namespace storage {

static const uint32_t DATA_BLOCK_BYTE_SIZE = sizeof(DataBlock);
static const uint32_t KEY_ENTRY_BYTE_SIZE = sizeof(KeyEntry);
static const uint32_t LIST_KEY_ENTRY_BYTE_SIZE = sizeof(ListKeyEntry);
static const uint32_t ENTRY_NODE_SIZE = sizeof(::openmldb::base::Node<::openmldb::base::Slice, void*>);
static const uint32_t DATA_NODE_SIZE = sizeof(::openmldb::base::Node<uint64_t, void*>);
static const uint32_t LIST_DATA_NODE_SIZE = sizeof(::openmldb::base::ListNode<uint64_t, void*>);
static const uint32_t KEY_ENTRY_PTR_SIZE = sizeof(KeyEntry*);

static inline uint32_t GetRecordSize(uint32_t value_size) { return value_size + DATA_BLOCK_BYTE_SIZE; }

// the input height which is the height of skiplist node
static inline uint32_t GetRecordPkIdxSize(uint8_t height, uint32_t key_size, uint8_t key_entry_max_height,
                                          bool is_skiplist) {
    if (is_skiplist)
        return height * 8 + ENTRY_NODE_SIZE + KEY_ENTRY_BYTE_SIZE + key_size + key_entry_max_height * 8 +
               DATA_NODE_SIZE;
    else
        return height * 8 + ENTRY_NODE_SIZE + LIST_KEY_ENTRY_BYTE_SIZE + key_size + LIST_DATA_NODE_SIZE;
}

static inline uint32_t GetRecordPkMultiIdxSize(uint8_t height, uint32_t key_size, uint8_t key_entry_max_height,
                                              uint32_t ts_cnt, const std::vector<bool>& is_skiplist_vec) {
    uint32_t sumSize = 0;
    for (int i = 0; i < ts_cnt; i++) {
        if (is_skiplist_vec[i])
            sumSize += height * 8 + ENTRY_NODE_SIZE + key_size +
                       (KEY_ENTRY_PTR_SIZE + KEY_ENTRY_BYTE_SIZE + key_entry_max_height * 8 + DATA_NODE_SIZE);
        else
            sumSize += height * 8 + ENTRY_NODE_SIZE + key_size +
                       (KEY_ENTRY_PTR_SIZE + LIST_KEY_ENTRY_BYTE_SIZE + LIST_DATA_NODE_SIZE);
    }
    return sumSize;
}

static inline uint32_t GetRecordTsIdxSize(uint8_t height, bool is_skiplist) {
    if (is_skiplist)
        return height * 8 + DATA_NODE_SIZE;
    else
        return LIST_DATA_NODE_SIZE;
}

}  // namespace storage
}  // namespace openmldb
#endif  // SRC_STORAGE_RECORD_H_
