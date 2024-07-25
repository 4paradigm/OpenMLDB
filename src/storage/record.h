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

#include "absl/strings/str_cat.h"
#include "base/skiplist.h"
#include "base/slice.h"
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
    explicit StatisticsInfo(uint32_t idx_num) : idx_cnt_vec(idx_num, 0) {}
    StatisticsInfo(const StatisticsInfo& other) {
        idx_cnt_vec = other.idx_cnt_vec;
        idx_byte_size = other.idx_byte_size;
        record_byte_size = other.record_byte_size;
    }
    void Reset() {
        for (auto& cnt : idx_cnt_vec) {
            cnt = 0;
        }
        idx_byte_size = 0;
        record_byte_size = 0;
    }

    void IncrIdxCnt(uint32_t idx) {
        if (idx < idx_cnt_vec.size()) {
            idx_cnt_vec[idx]++;
        }
    }

    uint64_t GetIdxCnt(uint32_t idx) const { return idx >= idx_cnt_vec.size() ? 0 : idx_cnt_vec[idx]; }

    uint64_t GetTotalCnt() const {
        uint64_t total_cnt = 0;
        for (auto& cnt : idx_cnt_vec) {
            total_cnt += cnt;
        }
        return total_cnt;
    }

    std::string DebugString() {
        std::string str;
        absl::StrAppend(&str, "idx_byte_size: ", idx_byte_size, " record_byte_size: ", record_byte_size, " idx_cnt: ");
        for (uint32_t i = 0; i < idx_cnt_vec.size(); i++) {
            absl::StrAppend(&str, i, ":", idx_cnt_vec[i], " ");
        }
        return str;
    }

    std::vector<uint64_t> idx_cnt_vec;
    uint64_t idx_byte_size = 0;
    uint64_t record_byte_size = 0;
};

}  // namespace storage
}  // namespace openmldb
#endif  // SRC_STORAGE_RECORD_H_
