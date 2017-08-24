//
// segment.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/segment.h"
#include "logging.h"
#include "timer.h"
#include <gflags/gflags.h>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_bool(enable_snapshot_ttl);

namespace rtidb {
namespace storage {

const static StringComparator scmp;
const static uint32_t data_block_size = sizeof(DataBlock);

Segment::Segment(SnapshotTTLFunc ttl_fun):entries_(NULL),mu_(), data_cnt_(0){
    entries_ = new KeyEntries(12, 4, scmp);
    ttl_fun_ = ttl_fun; 
}

Segment::Segment():entries_(NULL),mu_(), data_cnt_(0){
    entries_ = new KeyEntries(12, 4, scmp);
    ttl_fun_ = boost::bind(&DefaultSnapshotTTLFunc, _1);
}

Segment::~Segment() {
    delete entries_;
}

uint64_t Segment::Release() {
    uint64_t total_bytes = 0;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        if (it->GetValue() != NULL) {
            total_bytes += it->GetValue()->Release();
            total_bytes += it->GetKey().size();
        }
        delete it->GetValue();
        it->Next();
    }
    entries_->Clear();
    delete it;
    return total_bytes;
}

void Segment::Put(const std::string& key,
        uint64_t time,
        const char* data,
        uint32_t size) {
    KeyEntry* entry = entries_->Get(key);
    if (entry == NULL || key.compare(entry->key)!=0) {
        MutexLock lock(&mu_);
        entry = entries_->Get(key);
        // Need a double check
        if (entry == NULL || key.compare(entry->key) != 0) {
            entry = new KeyEntry();
            entry->key = key;
            entries_->Insert(key, entry);
        }
    }
    data_cnt_.fetch_add(1, boost::memory_order_relaxed);
    MutexLock lock(&entry->mu);
    DataBlock* db = new DataBlock(data, size);
    entry->entries.Insert(time, db);
}

bool Segment::Get(const std::string& key,
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

uint64_t Segment::FreeList(const std::string& pk, ::rtidb::base::Node<uint64_t, DataBlock*>* node) {
    uint64_t count = 0;
    std::vector<std::pair<std::string, uint64_t> > keys;
    while (node != NULL) {
        count ++;
        ::rtidb::base::Node<uint64_t, DataBlock*>* tmp = node;
        node = node->GetNextNoBarrier(0);
        LOG(DEBUG, "delete key %lld", tmp->GetKey());
        // clear the value that node hold
        // and clear node it's self
        if (tmp->GetValue() != NULL) {
            tmp->GetValue()->Release();
        }
        if (FLAGS_enable_snapshot_ttl) {
            keys.push_back(std::make_pair(pk, tmp->GetKey()));
        }
        delete tmp->GetValue();
        delete tmp;
    }
    if (FLAGS_enable_snapshot_ttl) {
        ttl_fun_(keys);
    }    
    return count;
}

uint64_t Segment::Gc4WithHead() {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t count = 0;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = it->GetValue();
        if (entry->entries.GetSize() <= 1) {
            continue;
        }
        TimeEntries::Iterator* tit = entry->entries.NewIterator();
        tit->SeekToFirst();
        uint32_t cnt = 0;
        uint64_t ts = 0;
        while (tit->Valid()) {
            if (cnt == 1) {
                ts = tit->GetKey();
                break;
            }
            cnt++;
            tit->Next();
        }
        delete tit;
        ::rtidb::base::Node<uint64_t, DataBlock*>* node = NULL;
        if (cnt == 1) {
            {
                MutexLock lock(&entry->mu);
                SplitList(entry, ts, &node);
            }
            count += FreeList(it->GetKey(), node);
        }
        it->Next();
    }
    LOG(INFO, "[GcWithHead] segment gc consumed %lld, count %lld",
            (::baidu::common::timer::get_micros() - consumed)/1000, count);
    data_cnt_.fetch_sub(count, boost::memory_order_relaxed);
    delete it;
    return count;
}

void Segment::SplitList(KeyEntry* entry, uint64_t ts, ::rtidb::base::Node<uint64_t, DataBlock*>** node) {
    // skip entry that ocupied by reader
    if (entry->refs_.load(boost::memory_order_acquire) <= 0) {
        *node = entry->entries.Split(ts);
    }
}

// fast gc with no global pause
uint64_t Segment::Gc4TTL(const uint64_t& time) {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t count = 0;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = it->GetValue();
        ::rtidb::base::Node<uint64_t, DataBlock*>* node = NULL;
        {
            MutexLock lock(&entry->mu);
            SplitList(entry, time, &node);
        }
        count += FreeList(it->GetKey(), node);
        it->Next();
    }
    LOG(INFO, "[Gc4TTL] segment gc with key %lld ,consumed %lld, count %lld", time,
            (::baidu::common::timer::get_micros() - consumed)/1000, count);
    data_cnt_.fetch_sub(count, boost::memory_order_relaxed);
    delete it;
    return count;
}

void Segment::BatchGet(const std::vector<std::string>& keys,
                       std::map<uint32_t, DataBlock*>& datas,
                       Ticket& ticket) {
    KeyEntries::Iterator* it = entries_->NewIterator();
    for (uint32_t i = 0; i < keys.size(); i++) {
        const std::string& key = keys[i];
        it->Seek(key);
        if (!it->Valid()) {
            continue;
        }
        KeyEntry* entry = it->GetValue();
        ticket.Push(entry);
        TimeEntries::Iterator* tit = entry->entries.NewIterator();
        tit->SeekToFirst();
        if (tit->Valid()) {
            datas.insert(std::make_pair(i, tit->GetValue()));
        }
        delete tit;
    }
    delete it;
}

// Iterator
Segment::Iterator* Segment::NewIterator(const std::string& key, Ticket& ticket) {
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

}
}



