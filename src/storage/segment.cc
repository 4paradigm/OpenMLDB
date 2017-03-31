//
// segment.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/segment.h"

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
    if (entry == NULL) {
        MutexLock lock(&mu_);
        entry = new HashEntry();
    }
    MutexLock lock(&entry->mu);
    char* block = new char[size];
    memcpy(block, data, size);
    DataBlock* db = new DataBlock();
    db->size = size;
    db->data = block;
    entry->entries.Insert(time, db);
}


}
}



