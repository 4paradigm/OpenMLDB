//
// snapshot.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-24
//


#ifndef RTIDB_SNAPSHOT_H
#define RTIDB_SNAPSHOT_H

#include <vector>
#include "leveldb/db.h"
#include "proto/tablet.pb.h"
#include "boost/atomic.hpp"
#include "storage/table.h"

using ::rtidb::api::LogEntry;
namespace rtidb {
namespace storage {

// table snapshot
class Snapshot {

public:
    Snapshot(uint32_t tid, uint32_t pid);

    ~Snapshot();

    bool Init();

    bool Recover(Table* table);

    inline uint64_t GetOffset() {
        return offset_.load(boost::memory_order_relaxed);
    }

private:
    uint32_t tid_;
    uint32_t pid_;
    boost::atomic<uint64_t> offset_;
};


}
}

#endif /* !RTIDB_SNAPSHOT_H */
