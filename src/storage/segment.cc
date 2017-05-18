//
// segment.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/segment.h"
#include "logging.h"
#include "timer.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace storage {

const static StringComparator scmp;
const static uint32_t data_block_size = sizeof(DataBlock);

Segment::Segment():entries_(NULL),mu_(),
    data_byte_size_(0), data_cnt_(0){
    entries_ = new KeyEntries(12, 4, scmp);
}


Segment::~Segment() {
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        delete it->GetValue();
        it->Next();
    }
    delete it;
    delete entries_;
}

void Segment::Put(const std::string& key,
        const uint64_t& time,
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
    data_byte_size_.fetch_add(data_block_size + size, boost::memory_order_relaxed);
    data_cnt_.fetch_add(1, boost::memory_order_relaxed);
    MutexLock lock(&entry->mu);
    DataBlock* db = new DataBlock(data, size);
    entry->entries.Insert(time, db);
}

bool Segment::Get(const std::string& key,
        const uint64_t& time,
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

// fast gc with no global pause
uint64_t Segment::Gc4TTL(const uint64_t& time) {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t freed_data_byte_size = 0;
    uint64_t count = 0;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = it->GetValue();
        ::rtidb::base::Node<uint64_t, DataBlock*>* node = NULL;
        {
            MutexLock lock(&entry->mu);
            node = entry->entries.Split(time);
        }
        uint64_t freed_data_byte_size = 0;
        if (node != NULL) {
            count ++;
            LOG(DEBUG, "delete key %lld", node->GetKey());
            freed_data_byte_size += (data_block_size + node->GetValue()->size);
            delete node->GetValue();
            while (true) {
                node = node->GetNextNoBarrier(0);
                if (node == NULL) {
                    break;
                }
                count ++;
                LOG(DEBUG, "delete key %lld", node->GetKey());
                freed_data_byte_size += (data_block_size + node->GetValue()->size);
                delete node->GetValue();
            }

        }
        it->Next();
    }
    LOG(INFO, "[Gc] segment gc with key %lld ,consumed %lld, count %lld", time,
            (::baidu::common::timer::get_micros() - consumed)/1000, count);
    data_byte_size_.fetch_sub(freed_data_byte_size, boost::memory_order_relaxed);
    data_cnt_.fetch_sub(count, boost::memory_order_relaxed);
    delete it;
    return count;
}

// Iterator
Segment::Iterator* Segment::NewIterator(const std::string& key) {
    KeyEntry* entry = entries_->Get(key);
    if (entry == NULL || key.compare(entry->key)!=0) {
        return NULL;
    }
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



