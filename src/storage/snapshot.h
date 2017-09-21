//
// snapshot.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-24
//


#ifndef RTIDB_SNAPSHOT_H
#define RTIDB_SNAPSHOT_H

#include <vector>
#include <atomic>
#include "leveldb/db.h"
#include "proto/tablet.pb.h"
#include "boost/atomic.hpp"
#include "base/count_down_latch.h"
#include "storage/table.h"

using ::rtidb::api::LogEntry;
namespace rtidb {
namespace storage {

typedef ::rtidb::base::Skiplist<uint32_t, uint64_t, ::rtidb::base::DefaultComparator> LogParts;


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
    // load single snapshot to table
    void RecoverSingleSnapshot(const std::string& path,
                               Table* table,
                               std::atomic<uint64_t>* g_succ_cnt,
                               std::atomic<uint64_t>* g_failed_cnt,
                               ::rtidb::base::CountDownLatch* latch);
private:
    uint32_t tid_;
    uint32_t pid_;
    boost::atomic<uint64_t> offset_;
    LogParts* log_part_;
    // the snapshot path
    std::string path_;
};

}
}

#endif /* !RTIDB_SNAPSHOT_H */
