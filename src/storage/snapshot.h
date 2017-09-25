//
// snapshot.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-24
//


#ifndef RTIDB_SNAPSHOT_H
#define RTIDB_SNAPSHOT_H

#include <vector>
#include "log/log_reader.h"
#include "proto/tablet.pb.h"
#include "boost/atomic.hpp"
#include "storage/table.h"
#include "log/log_writer.h"
#include "log/sequential_file.h"

using ::rtidb::api::LogEntry;
namespace rtidb {
namespace storage {

using ::rtidb::log::WriteHandle;

typedef ::rtidb::base::Skiplist<uint32_t, uint64_t, ::rtidb::base::DefaultComparator> LogParts;

// table snapshot
class Snapshot {

public:
    Snapshot(uint32_t tid, uint32_t pid, LogParts* log_part);

    ~Snapshot();

    bool Init();

    bool Recover(Table* table);

    inline uint64_t GetOffset() {
        return offset_;
    }

    int MakeSnapshot();

    int GetSnapshotRecord(::rtidb::api::Manifest& manifest);

    int RecordOffset(const std::string& snapshot_name, uint64_t key_count, uint64_t offset);

private:
    uint32_t tid_;
    uint32_t pid_;
    uint64_t offset_;
    boost::atomic<bool> making_snapshot_;
    LogParts* log_part_;
    std::string snapshot_path_;
    std::string log_path_;
};


}
}

#endif /* !RTIDB_SNAPSHOT_H */
