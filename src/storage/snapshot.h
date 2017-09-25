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
#include "log/log_reader.h"
#include "proto/tablet.pb.h"
#include "boost/atomic.hpp"
#include "base/count_down_latch.h"
#include "storage/table.h"
#include "log/log_writer.h"
#include "log/sequential_file.h"

using ::rtidb::api::LogEntry;
namespace rtidb {
namespace storage {

using ::rtidb::log::WriteHandle;

typedef ::rtidb::base::Skiplist<uint32_t, uint64_t, ::rtidb::base::DefaultComparator> LogParts;

struct RecoverStat {
    uint64_t succ_cnt;
    uint64_t failed_cnt;
};


// table snapshot
class Snapshot {

public:
    Snapshot(uint32_t tid, uint32_t pid, LogParts* log_part);

    ~Snapshot();

    bool Init();

    bool Recover(Table* table, RecoverStat& stat);

    bool RecoverFromSnapshot(Table* table, RecoverStat& stat);
    bool RecoverFromBinlog(Table* table, RecoverStat& stat);

    inline uint64_t GetOffset() {
        return offset_;
    }

    int MakeSnapshot();

    int RecordOffset(const std::string& snapshot_name, uint64_t key_count, uint64_t offset);

private:

    // load single snapshot to table
    void RecoverSingleSnapshot(const std::string& path,
                               Table* table,
                               std::atomic<uint64_t>* g_succ_cnt,
                               std::atomic<uint64_t>* g_failed_cnt,
                               ::rtidb::base::CountDownLatch* latch);

    // Read manifest from local storage return 0 if ok , 1 if manifest does not exist,
    // or -1 if some error ocurrs 
    int ReadManifest(::rtidb::api::Manifest* manifest);

    void GetSnapshots(const std::vector<std::string>& files, 
                      std::vector<std::string>& snapshots);

private:
    uint32_t tid_;
    uint32_t pid_;
    uint64_t offset_;
    boost::atomic<bool> making_snapshot_;
    LogParts* log_part_;
    // the snapshot path
    std::string snapshot_path_;
    std::string log_path_;
};

}
}

#endif /* !RTIDB_SNAPSHOT_H */
