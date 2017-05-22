//
// segment.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31 
// 


#ifndef RTIDB_STORAGE_SEGMENT_H
#define RTIDB_STORAGE_SEGMENT_H

#include "base/skiplist.h"
#include "mutex.h"
#include "boost/atomic.hpp"

namespace rtidb {
namespace storage {

class Segment;

using ::baidu::common::Mutex;
using ::baidu::common::MutexLock;

struct DataBlock {
    uint32_t size;
    char* data;

    DataBlock(const char* input, uint32_t len):size(len),data(NULL) {
        data = new char[len];
        memcpy(data, input, len);
    }

    uint32_t Release() {
        delete[] data;
        data = NULL;
        uint32_t ret = size;
        size = 0;
        return ret;
    }

    ~DataBlock() {
        delete[] data;
        data = NULL;
    }
};

// the desc time comparator
struct TimeComparator {
    int operator()(const uint64_t& a, const uint64_t& b) const {
        if (a > b) {
            return -1;
        }else if (a == b) {
            return 0;
        }
        return 1;
    }
};

const static TimeComparator tcmp;
typedef ::rtidb::base::Skiplist<uint64_t, DataBlock* , TimeComparator> TimeEntries;

class KeyEntry {
public:
    KeyEntry():entries(12, 4, tcmp),mu(){}
    ~KeyEntry() {}

    uint64_t Release() {
        uint64_t release_bytes = 0;
        TimeEntries::Iterator* it = entries.NewIterator();
        it->SeekToFirst();
        while(it->Valid()) {
            if (it->GetValue() != NULL) {
                // 4 bytes data size, 8 bytes key size 
                release_bytes += (4 + 8 + it->GetValue()->Release());
            }
            delete it->GetValue();
            it->Next();
        }
        entries.Clear();
        delete it;
        return release_bytes;
    }

private:
    std::string key;
    TimeEntries entries;
    Mutex mu;
    friend Segment;

};

struct StringComparator {
    int operator()(const std::string& a, const std::string& b) const {
        return a.compare(b);
    }
};

typedef ::rtidb::base::Skiplist<std::string, KeyEntry*, StringComparator> KeyEntries;

class Segment {

public:
    Segment();
    ~Segment();

    // Put time data 
    void Put(const std::string& key,
             const uint64_t& time,
             const char* data,
             uint32_t size);

    // Get time data
    bool Get(const std::string& key,
             const uint64_t& time,
             DataBlock** block);

    // Segment Iterator
    class Iterator {
    public:
        Iterator(TimeEntries::Iterator* it);
        ~Iterator();

        void Seek(const uint64_t& time);
        bool Valid() const;
        void Next();
        DataBlock* GetValue() const;
        uint64_t GetKey() const;
        void SeekToFirst();
    private:
        TimeEntries::Iterator* it_;
    };

    uint64_t Release();

    // gc with specify time, delete the data before time 
    uint64_t Gc4TTL(const uint64_t& time);

    Segment::Iterator* NewIterator(const std::string& key);

    uint64_t GetByteSize() {
        return data_byte_size_.load(boost::memory_order_relaxed);
    }
    uint64_t GetDataCnt() {
        return data_cnt_.load(boost::memory_order_relaxed);
    }
private:
    KeyEntries* entries_;
    // only Put need mutex
    Mutex mu_;
    boost::atomic<uint64_t> data_byte_size_;
    boost::atomic<uint64_t> data_cnt_;
};

}// namespace storage
}// namespace ritdb 
#endif /* !SEGMENT_H */
