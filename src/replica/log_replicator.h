//
// log_appender.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-06-07 
// 

#ifndef RTIDB_LOG_REPLICATOR_H
#define RTIDB_LOG_REPLICATOR_H

#include <vector>
#include <map>
#include "base/skiplist.h"
#include <atomic>
#include <mutex>
#include <condition_variable>
#include "thread_pool.h"
#include "log/log_writer.h"
#include "log/log_reader.h"
#include "log/sequential_file.h"
#include "proto/tablet.pb.h"
#include "replica/replicate_node.h"
#include "storage/table.h"

namespace rtidb {
namespace replica {

using ::baidu::common::ThreadPool;
using ::rtidb::log::SequentialFile;
using ::rtidb::log::Reader;
using ::rtidb::log::WriteHandle;
using ::rtidb::storage::Table;
using ::rtidb::api::LogEntry;

enum ReplicatorRole {
    kLeaderNode = 1,
    kFollowerNode
};


class LogReplicator {

public:

    LogReplicator(const std::string& path,
                  const std::vector<std::string>& endpoints,
                  const ReplicatorRole& role,
                  std::shared_ptr<Table> table);

    ~LogReplicator();

    bool Init();

    // the slave node receives master log entries
    bool AppendEntries(const ::rtidb::api::AppendEntriesRequest* request,
                       ::rtidb::api::AppendEntriesResponse* response);

    // the master node append entry
    bool AppendEntry(::rtidb::api::LogEntry& entry);

    //  data to slave nodes
    void Notify();
    // recover logs meta
    bool Recover();

    void Stop();

    bool RollWLogFile();

    void DeleteBinlog();

    // add replication
    bool AddReplicateNode(const std::vector<std::string>& endpoint_vec);

    bool DelReplicateNode(const std::string& endpoint);

    void MatchLogOffset();

    void ReplicateToNode(const std::string& endpoint);

    // Sync Write Buffer to Disk
    void SyncToDisk();
    void SetOffset(uint64_t offset);
    uint64_t GetOffset();

    LogParts* GetLogPart();

    inline uint64_t GetLogOffset() {
        return  log_offset_.load(std::memory_order_relaxed);
    }
    void SetRole(const ReplicatorRole& role);

    uint64_t GetLeaderTerm();
    void SetLeaderTerm(uint64_t term);

    void SetSnapshotLogPartIndex(uint64_t offset);

    bool ParseBinlogIndex(const std::string& path, uint32_t& index);

private:
    bool OpenSeqFile(const std::string& path, SequentialFile** sf);

    void ApplyEntryToTable(const LogEntry& entry);

private:
    // the replicator root data path
    std::string path_;
    std::string log_path_;
    // the term for leader judgement
    std::atomic<uint64_t> log_offset_;
    std::atomic<uint32_t> binlog_index_;
    LogParts* logs_;
    WriteHandle* wh_;
    ReplicatorRole role_;
    std::vector<std::string> endpoints_;
    std::vector<std::shared_ptr<ReplicateNode> > nodes_;

    uint64_t term_;
    // sync mutex
    std::mutex mu_;
    std::condition_variable cv_;
    std::condition_variable coffee_cv_;

    // for background task
    std::atomic<bool> running_;

    // background task pool
    ThreadPool tp_;

    // reference cnt
    std::atomic<uint64_t> refs_;

    std::atomic<int> snapshot_log_part_index_;

    std::mutex wmu_;

    std::shared_ptr<Table> table_;
};

} // end of replica
} // end of rtidb

#endif /* !RTIDB_LOG_REPLICATOR_H */
