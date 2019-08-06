//
// snapshot.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-24
//
#pragma once

#include <vector>
#include <atomic>
#include <memory>
#include <map>
#include "log/log_reader.h"
#include "proto/tablet.pb.h"
#include "base/count_down_latch.h"
#include "storage/snapshot.h"
#include "log/log_writer.h"
#include "log/sequential_file.h"

using ::rtidb::api::LogEntry;
namespace rtidb {
namespace base {
class Status;
}
namespace storage {

using ::rtidb::log::WriteHandle;


typedef ::rtidb::base::Skiplist<uint32_t, uint64_t, ::rtidb::base::DefaultComparator> LogParts;

// table snapshot
class MemTableSnapshot : public Snapshot {

public:
    MemTableSnapshot(uint32_t tid, uint32_t pid, LogParts* log_part);

    virtual ~MemTableSnapshot() = default;

    bool Init();

    virtual bool Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) override;

    void RecoverFromSnapshot(const std::string& snapshot_name, uint64_t expect_cnt, std::shared_ptr<Table> table);
    bool RecoverFromBinlog(std::shared_ptr<Table> table, uint64_t offset, uint64_t& latest_offset);

    virtual int MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset) override;

    int TTLSnapshot(std::shared_ptr<Table> table, const ::rtidb::api::Manifest& manifest, WriteHandle* wh, 
                uint64_t& count, uint64_t& expired_key_num, uint64_t& deleted_key_num);

    // Read manifest from local storage return 0 if ok , 1 if manifest does not exist,
    // or -1 if some error ocurrs 
    int GetSnapshotRecord(::rtidb::api::Manifest& manifest);

    int RecordOffset(const std::string& snapshot_name, uint64_t key_count, uint64_t offset, uint64_t term);

private:

    // load single snapshot to table
    void RecoverSingleSnapshot(const std::string& path,
                               std::shared_ptr<Table> table,
                               std::atomic<uint64_t>* g_succ_cnt,
                               std::atomic<uint64_t>* g_failed_cnt);

   uint64_t CollectDeletedKey();                           

private:
    uint32_t tid_;
    uint32_t pid_;
    LogParts* log_part_;
    std::string log_path_;
    std::map<std::string, uint64_t> deleted_keys_;
};

}
}
