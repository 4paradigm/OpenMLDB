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
#include <vector>
#include <string>
#include "base/slice.h"
#include "base/iterator.h"
#include "base/spin_lock.h"
#include "storage/list.h"
#include "storage/skiplist.h"

namespace fesql {
namespace storage {
using ::fesql::base::Iterator;
// the desc time comparator
using ::fesql::base::Slice;
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

class Segment;

struct DataBlock {
    uint32_t ref_cnt;
    uint32_t size;
    char data[];
};

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
const static TimeComparator tcmp;  // NOLINT
typedef List<uint64_t, DataBlock*, TimeComparator> TimeEntries;
typedef SkipList<Slice, void*, SliceComparator> KeyEntries;


class KeyEntry {
 public:
    KeyEntry() : entries(tcmp) {}
    ~KeyEntry() {}

 public:
    TimeEntries entries;
    friend Segment;
};

class Segment {
 public:
    Segment();
    explicit Segment(uint8_t height);
    ~Segment();

    // Put time data
    void Put(const Slice& key, uint64_t time, const char* data, uint32_t size);

    void Put(const Slice& key, uint64_t time, DataBlock* row);

    TableIterator* NewIterator();
    TableIterator* NewIterator(const Slice& key);

    KeyEntries* GetKeyEntries() { return entries_; }

 private:
    uint8_t key_entry_max_height_;
    KeyEntries* entries_;
    base::SpinMutex mu_;
};

}  // namespace storage
}  // namespace fesql
