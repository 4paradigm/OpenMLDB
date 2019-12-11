//
// segment.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include "base/iterator.h"
#include "base/slice.h"
#include "base/spin_lock.h"
#include "proto/type.pb.h"
#include "storage/list.h"
#include "storage/skiplist.h"

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

    bool Valid();

    void Put(const Slice& key, uint64_t time, DataBlock* row);

 private:
    KeyEntry* entries_;
    base::SpinMutex mu_;
    friend class TableIterator;
    friend class Table;
};

}  // namespace storage
}  // namespace fesql
