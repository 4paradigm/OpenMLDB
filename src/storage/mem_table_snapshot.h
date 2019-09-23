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
    MemTableSnapshot(uint32_t tid, uint32_t pid, LogParts* log_part,
            const std::string& db_root_path);

    virtual ~MemTableSnapshot() = default;

    virtual bool Init() override;

    virtual bool Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) override;

    void RecoverFromSnapshot(const std::string& snapshot_name, uint64_t expect_cnt, std::shared_ptr<Table> table);

    virtual int MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset) override;

    int TTLSnapshot(std::shared_ptr<Table> table, const ::rtidb::api::Manifest& manifest, WriteHandle* wh, 
                uint64_t& count, uint64_t& expired_key_num, uint64_t& deleted_key_num);
    void Put(std::string& path, std::shared_ptr<Table>& table, std::vector<std::string*> recordPtr, std::atomic<uint64_t>* succ_cnt, std::atomic<uint64_t>* failed_cnt);

private:

    // load single snapshot to table
    void RecoverSingleSnapshot(const std::string& path,
                               std::shared_ptr<Table> table,
                               std::atomic<uint64_t>* g_succ_cnt,
                               std::atomic<uint64_t>* g_failed_cnt);

   uint64_t CollectDeletedKey();                           

private:
    LogParts* log_part_;
    std::string log_path_;
    std::map<std::string, uint64_t> deleted_keys_;
    std::string db_root_path_;
};

}
}
