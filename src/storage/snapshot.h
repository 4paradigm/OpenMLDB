//
// snapshot.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-24
//


#ifndef RTIDB_SNAPSHOT_H
#define RTIDB_SNAPSHOT_H

#include "leveldb/db.h"
#include "proto/tablet.pb.h"

namespace rtidb {
namespace replica {

// table snapshot
class Snapshot {

public:
    Snapshot(uint32_t tid, uint32_t pid);
    ~Snapshot();

    bool Init();
    // the log entry must be filled with log offset
    bool Put(const LogEntry* entry);

private:
    uint32_t tid_;
    uint32_t pid_;
    leveldb::DB* db_;
};

}
}

#endif /* !RTIDB_SNAPSHOT_H */
