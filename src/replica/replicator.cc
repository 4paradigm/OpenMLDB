//
// replicator.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-12
//

#include "replica/replicator.h"

namespace rtidb {
namespace replica {

MemoryReplicator::MemoryReplicator(boost::atomic<uint64_t>* toffset,
        uint32_t max_pending_size):logs_(NULL),
    toffset_(toffset), dests_(), max_pending_size_(max_pending_size),
    mu_(), cond_(&mu_), running_(false){}

MemoryReplicator::~MemoryReplicator() {}

bool MemoryReplicator::Append(const std::string& pk, uint64_t time,
        const char* data, uint32_t size) {
    // add pending size check
    MutexLock lock(&mu_);
    LogEntry* entry = new LogEntry();
    entry->pk = pk;
    entry->time = time;
    entry->size = size;
    entry->data = new char[size];
    memcpy(entry->data, data, size);
    // always use previous data
    uint64_t pre = toffset_->fetch_add(1, boost::memory_order_relaxed);
    entry->offset = pre;
    logs_->AddToFirst(entry->offset, entry);
    cond_->Signal();
}


} // end of replica
} // end of rtidb

