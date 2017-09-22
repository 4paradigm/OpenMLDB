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
        return offset_;
    }

    int MakeSnapshot();

    int RecordOffset(const std::string& log_entry, const std::string& snapshot_name, uint64_t key_count);

private:
    uint32_t tid_;
    uint32_t pid_;
    uint64_t offset_;
    boost::atomic<bool> making_snapshot_;
    LogParts* log_part_;
    ::rtidb::log::LogReader* log_reader_;
    ::rtidb::log::WriteHandle* wh_;
    std::string snapshot_path_;
};


}
}

#endif /* !RTIDB_SNAPSHOT_H */
