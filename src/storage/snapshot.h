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

struct DeleteEntry {
    std::string pk;
    uint64_t ts;
};

// table snapshot
class Snapshot {

public:
    Snapshot(uint32_t tid, uint32_t pid, uint64_t offset);

    bool Init();
    // the log entry must be filled with log offset
    bool Put(const std::string& entry, uint64_t offset,
             const std::string& pk, uint64_t ts);

    bool BatchDelete(const std::vector<DeleteEntry>& entries);

    bool Recover(Table* table);

    inline uint64_t GetOffset() {
        return offset_.load(boost::memory_order_relaxed);
    }

    void Ref();
    void UnRef();

private:
    ~Snapshot();
private:
    uint32_t tid_;
    uint32_t pid_;
    leveldb::DB* db_;
    boost::atomic<uint64_t> offset_;
    boost::atomic<uint64_t> refs_;
};


}
}

#endif /* !RTIDB_SNAPSHOT_H */
