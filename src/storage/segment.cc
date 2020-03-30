//
// segment.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//

#include "storage/segment.h"
#include <mutex>  //NOLINT

namespace fesql {
namespace storage {

Segment::Segment() : entries_(NULL), mu_() {
    entries_ = new KeyEntry(KEY_ENTRY_MAX_HEIGHT, 4, scmp);
}

Segment::~Segment() { delete entries_; }

void Segment::Put(const Slice& key, uint64_t time, DataBlock* row) {
    void* entry = NULL;
    std::lock_guard<base::SpinMutex> lock(mu_);
    int ret = entries_->Get(key, entry);
    if (ret < 0 || entry == NULL) {
        entry = reinterpret_cast<void*>(new TimeEntry(tcmp));
        char* pk = new char[key.size()];
        memcpy(pk, key.data(), key.size());
        Slice skey(pk, key.size());
        entries_->Insert(skey, entry);
    }
    reinterpret_cast<TimeEntry*>(entry)->Insert(time, row);
}

}  // namespace storage
}  // namespace fesql
