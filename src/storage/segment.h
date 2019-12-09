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
    uint32_t size;
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

class TableIterator {
 public:
    TableIterator() = default;
    TableIterator(Iterator<Slice, void*>* pk_it,
                  Iterator<uint64_t, DataBlock*>* ts_it);
    ~TableIterator();
    void Seek(uint64_t time);
    void Seek(const std::string& key, uint64_t ts);
    bool Valid();
    void Next();
    Slice GetValue() const;
    uint64_t GetKey() const;
    std::string GetPK() const;
    void SeekToFirst();

 private:
    Iterator<Slice, void*>* pk_it_ = NULL;
    Iterator<uint64_t, DataBlock*>* ts_it_ = NULL;
};

using TimeEntry = List<uint64_t, DataBlock*, TimeComparator>;
using KeyEntry = SkipList<Slice, void*, SliceComparator>;

class Segment {
 public:
    Segment();
    ~Segment();

    void Put(const Slice& key, uint64_t time, DataBlock* row);

    std::unique_ptr<TableIterator> NewIterator();
    std::unique_ptr<TableIterator> NewIterator(const Slice& key);

 private:
    KeyEntry* entries_;
    base::SpinMutex mu_;
};

}  // namespace storage
}  // namespace fesql
