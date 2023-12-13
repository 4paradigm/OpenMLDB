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

#ifndef SRC_STORAGE_KEY_ENTRY_H_
#define SRC_STORAGE_KEY_ENTRY_H_

#include <cstring>
#include <memory>
#include "base/skiplist.h"

namespace openmldb {
namespace storage {

struct DataBlock {
    // dimension count down
    uint8_t dim_cnt_down;
    uint32_t size;
    char* data;

    DataBlock(uint8_t dim_cnt, const char* input, uint32_t len) : dim_cnt_down(dim_cnt), size(len), data(nullptr) {
        data = new char[len];
        memcpy(data, input, len);
    }

    DataBlock(uint8_t dim_cnt, char* input, uint32_t len, bool skip_copy)
        : dim_cnt_down(dim_cnt), size(len), data(nullptr) {
        if (skip_copy) {
            data = input;
        } else {
            data = new char[len];
            memcpy(data, input, len);
        }
    }

    ~DataBlock() {
        delete[] data;
        data = nullptr;
    }

    bool EqualWithoutCnt(const DataBlock& other) const {
        if (size != other.size) {
            return false;
        }
        // ref RowBuilder::InitBuffer header version
        if (*(data + 1) != *(other.data + 1) ||
            *(reinterpret_cast<uint32_t*>(data + 2)) != *(reinterpret_cast<uint32_t*>(other.data + 2))) {
            return false;
        }
        return memcmp(data + 6, other.data + 6, size - 6) == 0;
    }
};

// the desc time comparator
struct TimeComparator {
    int operator()(uint64_t a, uint64_t b) const {
        if (a > b) {
            return -1;
        } else if (a == b) {
            return 0;
        }
        return 1;
    }
};

static const TimeComparator tcmp;
using TimeEntries = base::Skiplist<uint64_t, DataBlock*, TimeComparator>;
struct StatisticsInfo;

class KeyEntry {
 public:
    KeyEntry() : entries(12, 4, tcmp), refs_(0), count_(0) {}
    explicit KeyEntry(uint8_t height) : entries(height, 4, tcmp), refs_(0), count_(0) {}

    void Release(uint32_t idx, StatisticsInfo* statistics_info);

    void Ref() { refs_.fetch_add(1, std::memory_order_relaxed); }

    void UnRef() { refs_.fetch_sub(1, std::memory_order_relaxed); }

    uint64_t GetCount() { return count_.load(std::memory_order_relaxed); }

 public:
    TimeEntries entries;
    std::atomic<uint64_t> refs_;
    std::atomic<uint64_t> count_;
};

}  // namespace storage
}  // namespace openmldb

#endif  // SRC_STORAGE_KEY_ENTRY_H_
