//
// segment.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/segment.h"

#include "storage/record.h"
#include "base/strings.h"
#include "logging.h"
#include "timer.h"
#include <gflags/gflags.h>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace storage {

const static SliceComparator scmp;

Segment::Segment():entries_(NULL),mu_(), idx_cnt_(0), idx_byte_size_(0), pk_cnt_(0){
    entries_ = new KeyEntries(12, 4, scmp);
}

Segment::~Segment() {
    delete entries_;
}

uint64_t Segment::Release() {
    uint64_t cnt = 0;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        if (it->GetValue() != NULL) {
            cnt += it->GetValue()->Release();
        }
        delete it->GetValue();
        it->Next();
    }
    entries_->Clear();
    delete it;
    return cnt;
}

void Segment::Put(const Slice& key,
        uint64_t time,
        const char* data,
        uint32_t size) {
    DataBlock* db = new DataBlock(1, data, size);
    Put(key, time, db);
}

void Segment::Put(const Slice& key, uint64_t time, DataBlock* row) {
    KeyEntry* entry = entries_->Get(key);
    std::lock_guard<std::mutex> lock(mu_);
    uint32_t byte_size = 0; 
    if (entry == NULL || scmp(key, entry->key) != 0) {
        entry = entries_->Get(key);
        // Need a double check
        if (entry == NULL || scmp(key, entry->key) != 0) {
            PDLOG(DEBUG, "new pk entry %s", key.data());
            char* pk = new char[key.size()];
            memcpy(pk, key.data(), key.size());
            entry = new KeyEntry(pk, (uint32_t)key.size());
            uint8_t height = entries_->Insert(entry->key, entry);
            byte_size += GetRecordPkIdxSize(height, key.size());
            pk_cnt_.fetch_add(1, std::memory_order_relaxed);
        }
    }
    idx_cnt_.fetch_add(1, std::memory_order_relaxed);
    uint8_t height = entry->entries.Insert(time, row);
    PDLOG(DEBUG, "add ts with height %u", height);
    byte_size += GetRecordTsIdxSize(height);
    idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
}

bool Segment::Get(const Slice& key,
                  const uint64_t time,
                  DataBlock** block) {
    if (block == NULL) {
        return false;
    }

    KeyEntry* entry = entries_->Get(key);
    if (entry == NULL || key.compare(entry->key) !=0) {
        return false;
    }

    *block = entry->entries.Get(time);
    return true;
}

void Segment::FreeList(const Slice& pk, 
                       ::rtidb::base::Node<uint64_t, DataBlock*>* node,
                       uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt,
                       uint64_t& gc_record_byte_size) {
    while (node != NULL) {
        gc_idx_cnt++;
        ::rtidb::base::Node<uint64_t, DataBlock*>* tmp = node;
        idx_byte_size_.fetch_sub(GetRecordTsIdxSize(tmp->Height()));
        node = node->GetNextNoBarrier(0);
        PDLOG(DEBUG, "delete key %lld with height %u", tmp->GetKey(), tmp->Height());
        if (tmp->GetValue()->dim_cnt_down > 1) {
            tmp->GetValue()->dim_cnt_down --;
        }else {
            PDLOG(DEBUG, "delele data block for key %lld", tmp->GetKey());
            gc_record_byte_size += GetRecordSize(tmp->GetValue()->size);
            delete tmp->GetValue();
            gc_record_cnt++;
        }
        delete tmp;
    }
}

void Segment::Gc4Head(uint64_t keep_cnt, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    if (keep_cnt == 0) {
        PDLOG(WARNING, "keep cnt is invalid");
        return;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = gc_idx_cnt;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = it->GetValue();
        TimeEntries::Iterator* tit = entry->entries.NewIterator();
        tit->SeekToFirst();
        uint32_t cnt = 0;
        uint64_t ts = 0;
        while (tit->Valid()) {
            if (cnt >= keep_cnt) {
                ts = tit->GetKey();
                break;
            }
            cnt++;
            tit->Next();
        }
        delete tit;
        ::rtidb::base::Node<uint64_t, DataBlock*>* node = NULL;
        if (cnt >= keep_cnt) {
            {
                std::lock_guard<std::mutex> lock(mu_);
                SplitList(entry, ts, &node);
            }
            FreeList(it->GetKey(), node, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        }
        it->Next();
    }
    PDLOG(DEBUG, "[Gc4Head] segment gc keep cnt %lu consumed %lld, count %lld", keep_cnt,
            (::baidu::common::timer::get_micros() - consumed)/1000, gc_idx_cnt - old);
    idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    delete it;
}

void Segment::SplitList(KeyEntry* entry, uint64_t ts, ::rtidb::base::Node<uint64_t, DataBlock*>** node) {
    // skip entry that ocupied by reader
    if (entry->refs_.load(std::memory_order_acquire) <= 0) {
        *node = entry->entries.Split(ts);
    }
}

// fast gc with no global pause
void Segment::Gc4TTL(const uint64_t time, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = gc_idx_cnt; 
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = it->GetValue();
        ::rtidb::base::Node<uint64_t, DataBlock*>* node = NULL;
        {
            std::lock_guard<std::mutex> lock(mu_);
            SplitList(entry, time, &node);
        }
        FreeList(it->GetKey(), node, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        it->Next();
    }
    PDLOG(DEBUG, "[Gc4TTL] segment gc with key %lld ,consumed %lld, count %lld", time,
            (::baidu::common::timer::get_micros() - consumed)/1000, gc_idx_cnt - old);
    idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    delete it;
}


// Iterator
Segment::Iterator* Segment::NewIterator(const Slice& key, Ticket& ticket) {
    KeyEntry* entry = entries_->Get(key);
    if (entry == NULL || key.compare(entry->key)!=0) {
        return NULL;
    }
    ticket.Push(entry);
    return new Iterator(entry->entries.NewIterator());
}

Segment::Iterator::Iterator(TimeEntries::Iterator* it): it_(it) {}

Segment::Iterator::~Iterator() {
    delete it_;
}


void Segment::Iterator::Seek(const uint64_t& time) {
    it_->Seek(time);
}

bool Segment::Iterator::Valid() const {
    return it_->Valid();
}

void Segment::Iterator::Next() {
    it_->Next();
}

DataBlock* Segment::Iterator::GetValue() const {
    return it_->GetValue();
}

uint64_t Segment::Iterator::GetKey() const {
    return it_->GetKey();
}

void Segment::Iterator::SeekToFirst() {
    it_->SeekToFirst();
}

uint32_t Segment::Iterator::GetSize() {
    if (it_ == NULL) {
        return 0; 
    }
    return it_->GetSize();
}

}
}



