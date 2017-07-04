//
// replicator.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-09
//

#ifndef RTIDB_REPLICATOR_H
#define RTIBD_REPLICATOR_H

#include "base/skiplist.h"
#include "boost/atomic.hpp"
#include "mutex.h"
#include <map>

using ::baidu::common::Mutex;
using ::baidu::common::MutexLock;
using ::baidu::common::CondVar;

namespace rtidb {
namespace replica {

struct LogEntry {
    std::string pk;
    uint64_t time;
    char* data;
    uint32_t size;
    uint64_t offset;
};

struct LogOffsetDescComparator {
    int operator()(const uint64_t a, const uint64_t b) const {
        if (a > b) {
            return -1;
        }else if (a == b) {
            return 0;
        }
        return 1;
    }
};

struct LogDest {
    std::string endpoint;
    uint64_t dest_offset;
};


typedef ::rtidb::base::SkipList<uint64_t, LogEntry*, LogOffsetDescComparator> LogEntries;

class Replicator {

public:
    virtual ~Replicator();

    // need external synchronization
    virtual bool Append(const std::string& pk, 
                        uint64_t time, 
                        const char* data,
                        uint32_t size) = 0; 
};


class MemoryReplicator : public Replicator {

public:
    MemoryReplicator(boost::atomic<uint64_t>* toffset,
            uint32_t max_pending_size);
    ~MemoryReplicator();

    bool Append(const std::string& pk, uint64_t time,
                const char* data, uint32_t size);
    void AddDest(const LogDest& dest); 

private:
    void Replicate();

private:
    LogEntries* logs_;
    boost::atomic<uint64_t>* toffset_;
    std::map<std::string, LogDest> dests_;
    uint32_t max_pending_size_;
    Mutex mu_;
    CondVar cond_;
    bool running_;

};

} // end of rtidb 
} // end of rtidb

#endif /* !RTIDB_REPLICATOR_H */
