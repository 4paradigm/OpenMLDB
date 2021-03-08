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

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "base/fe_slice.h"
#include "base/iterator.h"
#include "base/spin_lock.h"
#include "proto/fe_type.pb.h"
#include "storage/fe_skiplist.h"
#include "storage/list.h"

namespace fesql {
namespace storage {
using ::fesql::base::Iterator;
using ::fesql::base::Slice;

struct DataBlock {
    uint32_t ref_cnt;
    char data[];
};

constexpr uint8_t KEY_ENTRY_MAX_HEIGHT = 12;

struct TimeComparator {
    int operator()(const uint64_t& a, const uint64_t& b) const {
        if (a > b) {
            return -1;
        } else if (a == b) {
            return 0;
        }
        return 1;
    }
};

struct SliceComparator {
    int operator()(const Slice& a, const Slice& b) const {
        return a.compare(b);
    }
};

static constexpr SliceComparator scmp;
static constexpr TimeComparator tcmp;

using TimeEntry = List<uint64_t, DataBlock*, TimeComparator>;
using KeyEntry = SkipList<Slice, void*, SliceComparator>;

class Segment {
 public:
    Segment();
    ~Segment();

    void Put(const Slice& key, uint64_t time, DataBlock* row);
    inline KeyEntry* GetEntries() { return entries_; }

 private:
    KeyEntry* entries_;
    base::SpinMutex mu_;
};

}  // namespace storage
}  // namespace fesql
