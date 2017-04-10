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
Segment::Segment():entries_(NULL) {
    entries_ = new HashEntries(12, 4, scmp);
}

Segment::~Segment() {}

void Segment::Put(const std::string& key,
        const uint64_t& time,
        const char* data,
        uint32_t size) {
    HashEntry* entry = entries_->Get(key);
    if (entry == NULL || key.compare(entry->key)!=0) {
        MutexLock lock(&mu_);
        entry = entries_->Get(key);
        // Need a double check
        if (entry == NULL || key.compare(entry->key) != 0) {
            entry = new HashEntry();
            entry->key = key;
            entries_->Insert(key, entry);
        }
    }
    MutexLock lock(&entry->mu);
    char* block = new char[size];
    memcpy(block, data, size);
    DataBlock* db = new DataBlock();
    db->size = size;
    db->data = block;
    entry->entries.Insert(time, db);
}

bool Segment::Get(const std::string& key,
        const uint64_t& time,
        DataBlock** block) {
    if (block == NULL) {
        return false;
    }

    HashEntry* entry = entries_->Get(key);
    if (entry == NULL || key.compare(entry->key) !=0) {
        return false;
    }
    *block = entry->entries.Get(time);
    return true;
}

// fast gc with no global pause
uint64_t Segment::Gc4TTL(const uint64_t& time) {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    HashEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    uint64_t count = 0;
    while (it->Valid()) {
        HashEntry* entry = it->GetValue();
        ::rtidb::base::Node<uint64_t, DataBlock*>* node = NULL;
        {
            MutexLock lock(&entry->mu);
            node = entry->entries.Split(time);
        }
        if (node != NULL) {
            count ++;
            LOG(DEBUG, "delete key %lld", node->GetKey());
            delete node->GetValue();
            while (true) {
                node = node->GetNextNoBarrier(0);
                if (node == NULL) {
                    break;
                }
                count ++;
                LOG(DEBUG, "delete key %lld", node->GetKey());
                delete node->GetValue();
            }
        }
        it->Next();
    }
    LOG(INFO, "[Gc] segment gc with key %lld ,consumed %lld, count %lld", time,
            (::baidu::common::timer::get_micros() - consumed)/1000, count);
    return count;
}

// Iterator
Segment::Iterator* Segment::NewIterator(const std::string& key) {
    HashEntry* entry = entries_->Get(key);
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

}
}



