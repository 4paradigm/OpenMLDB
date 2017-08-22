//
// segment.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31 
// 


#ifndef RTIDB_STORAGE_SEGMENT_H
#define RTIDB_STORAGE_SEGMENT_H

#include <map>
#include <vector>
#include "base/skiplist.h"
#include "mutex.h"
#include "boost/atomic.hpp"
#include "storage/ticket.h"
#include "boost/function.hpp"
#include "boost/bind.hpp"

namespace rtidb {
namespace storage {

class Segment;
class Ticket;
using ::baidu::common::Mutex;
using ::baidu::common::MutexLock;

typedef boost::function< bool (const std::vector<std::pair<std::string, uint64_t> >& keys) > SnapshotTTLFunc;
inline bool DefaultSnapshotTTLFunc(const std::vector<std::pair<std::string, uint64_t> >& keys) {
    return true;
}

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

    ~DataBlock() {}
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
    KeyEntry():entries(12, 4, tcmp),mu(), refs_(0){}
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

    void Ref() {
        refs_.fetch_add(1, boost::memory_order_relaxed);
    }

    void UnRef() {
        refs_.fetch_sub(1, boost::memory_order_relaxed);
    }


public:
    std::string key;
    TimeEntries entries;
    Mutex mu;
    // Reader refs
    boost::atomic<uint64_t> refs_;
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
    Segment(SnapshotTTLFunc ttl_fun);
    Segment();
    ~Segment();

    // Put time data 
    void Put(const std::string& key,
             uint64_t time,
             const char* data,
             uint32_t size);

    // Get time data
    bool Get(const std::string& key,
             uint64_t time,
             DataBlock** block);

    void BatchGet(const std::vector<std::string>& keys,
                  std::map<uint32_t, DataBlock*>& datas,
                  Ticket& ticket);

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
    uint64_t Gc4WithHead();
    Segment::Iterator* NewIterator(const std::string& key, Ticket& ticket);

    uint64_t GetDataCnt() {
        return data_cnt_.load(boost::memory_order_relaxed);
    }

private:
    uint64_t FreeList(const std::string& pk, ::rtidb::base::Node<uint64_t, DataBlock*>* node);
    void SplitList(KeyEntry* entry, uint64_t ts, ::rtidb::base::Node<uint64_t, DataBlock*>** node);
private:
    KeyEntries* entries_;
    // only Put need mutex
    Mutex mu_;
    boost::atomic<uint64_t> data_cnt_;
    SnapshotTTLFunc ttl_fun_;
};

}// namespace storage
}// namespace ritdb 
#endif /* !SEGMENT_H */
