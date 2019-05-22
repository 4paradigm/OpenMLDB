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
#include "base/slice.h"
#include <mutex>
#include <atomic>
#include "storage/ticket.h"
#include "proto/tablet.pb.h"

namespace rtidb {
namespace storage {

typedef google::protobuf::RepeatedPtrField<::rtidb::api::TSDimension> TSDimensions;

using ::rtidb::base::Slice;

class Segment;
class Ticket;

struct DataBlock {
    // dimension count down
    uint8_t dim_cnt_down;
    uint32_t size;
    char* data;

    DataBlock(uint8_t dim_cnt, const char* input, uint32_t len):dim_cnt_down(dim_cnt),size(len),data(NULL) {
        data = new char[len];
        memcpy(data, input, len);
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
    void SeekToLast();
    uint32_t GetSize();
private:
    TimeEntries::Iterator* it_;
};

class KeyEntry {
public:
    KeyEntry(): entries(12, 4, tcmp), refs_(0), count_(0){}
    KeyEntry(uint8_t height): entries(height, 4, tcmp), refs_(0), count_(0){}
    ~KeyEntry() {}

    // just return the count of datablock
    uint64_t Release() {
        uint64_t cnt = 0;
        TimeEntries::Iterator* it = entries.NewIterator();
        it->SeekToFirst();
        while(it->Valid()) {
            cnt += 1;
            DataBlock* block = it->GetValue();
            // Avoid double free
            if (block->dim_cnt_down > 1) {
                block->dim_cnt_down--;
            }else {
                delete block;
            }
            it->Next();
        }
        entries.Clear();
        delete it;
        return cnt;
    }

    void Ref() {
        refs_.fetch_add(1, std::memory_order_relaxed);
    }

    void UnRef() {
        refs_.fetch_sub(1, std::memory_order_relaxed);
    }

    uint64_t GetCount() {
        return count_.load(std::memory_order_relaxed);
    }

public:
    TimeEntries entries;
    std::atomic<uint64_t> refs_;
    std::atomic<uint64_t> count_;
    friend Segment;
};

struct SliceComparator {
    int operator()(const ::rtidb::base::Slice& a, const ::rtidb::base::Slice& b) const {
        return a.compare(b);
    }
};

typedef ::rtidb::base::Skiplist<::rtidb::base::Slice, void*, SliceComparator> KeyEntries;
typedef ::rtidb::base::Skiplist<uint64_t, ::rtidb::base::Node<Slice, void*>*, TimeComparator> KeyEntryNodeList;

class Segment {

public:
    Segment();
    Segment(uint8_t height);
    Segment(uint8_t height, const std::vector<uint32_t>& ts_idx_vec);
    ~Segment();

    // Put time data 
    void Put(const Slice& key, uint64_t time, const char* data, uint32_t size);

    void Put(const Slice& key, uint64_t time, DataBlock* row);

    void Put(const Slice& key, const TSDimensions& ts_dimension, DataBlock* row);

    // Get time data
    bool Get(const Slice& key, uint64_t time, DataBlock** block);

    bool Get(const Slice& key, uint32_t idx, uint64_t time, DataBlock** block);

    bool Delete(const Slice& key);

    uint64_t Release();
    // gc with specify time, delete the data before time 
    void Gc4TTL(const uint64_t time, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size);
    void Gc4TTL(const std::map<uint32_t, uint64_t>& time_map, uint64_t& gc_idx_cnt, 
            uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size);
    void Gc4Head(uint64_t keep_cnt, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size);
    void Gc4Head(const std::map<uint32_t, uint64_t>& keep_cnt_map, uint64_t& gc_idx_cnt, 
            uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size);
    Iterator* NewIterator(const Slice& key, Ticket& ticket);
    Iterator* NewIterator(const Slice& key, uint32_t idx, Ticket& ticket);

    inline uint64_t GetIdxCnt() {
        return idx_cnt_.load(std::memory_order_relaxed);
    }

    inline uint64_t GetTsCnt() {
        return ts_cnt_;
    }

    int GetTsIdx(uint32_t raw_idx, uint32_t& real_idx) {
        auto iter = ts_idx_map_.find(raw_idx);
        if (iter == ts_idx_map_.end()) {
            return -1;
        } else {
            real_idx = iter->second;
        }
        return 0;
    }

    inline uint64_t GetIdxByteSize() {
        return idx_byte_size_.load(std::memory_order_relaxed);
    }

    inline uint64_t GetPkCnt() {
        return pk_cnt_.load(std::memory_order_relaxed);
    }

    void GcFreeList(uint64_t& entry_gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size);

    KeyEntries* GetKeyEntries() {
        return entries_;
    }

    int GetCount(const Slice& key, uint64_t& count);
    int GetCount(const Slice& key, uint32_t idx, uint64_t& count);

    void IncrGcVersion() {
        gc_version_.fetch_add(1, std::memory_order_relaxed);
    }

private:
    void FreeList(::rtidb::base::Node<uint64_t, DataBlock*>* node,
                  uint64_t& gc_idx_cnt, 
                  uint64_t& gc_record_cnt,
                  uint64_t& gc_record_byte_size);
    void SplitList(KeyEntry* entry, uint64_t ts, 
                   ::rtidb::base::Node<uint64_t, DataBlock*>** node);
private:
    KeyEntries* entries_;
    // only Put need mutex
    std::mutex mu_;
    std::mutex gc_mu_;
    std::atomic<uint64_t> idx_cnt_;
    std::atomic<uint64_t> idx_byte_size_;
    std::atomic<uint64_t> pk_cnt_;
    uint8_t key_entry_max_height_;
    KeyEntryNodeList* entry_free_list_;
    uint32_t ts_cnt_;
    std::atomic<uint64_t> gc_version_;
    std::map<uint32_t, uint32_t> ts_idx_map_;
};

}// namespace storage
}// namespace ritdb 
#endif /* !SEGMENT_H */
