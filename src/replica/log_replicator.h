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
#include "boost/atomic.hpp"
#include "boost/function.hpp"
#include "mutex.h"
#include "thread_pool.h"
#include "log/log_writer.h"
#include "log/log_reader.h"
#include "log/sequential_file.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"
#include "replica/replicate_node.h"
#include "storage/table.h"

namespace rtidb {
namespace replica {

using ::baidu::common::MutexLock;
using ::baidu::common::Mutex;
using ::baidu::common::CondVar;
using ::baidu::common::ThreadPool;
using ::rtidb::log::SequentialFile;
using ::rtidb::log::Reader;
using ::rtidb::log::WriteHandle;
using ::rtidb::storage::Table;

typedef boost::function< bool (const ::rtidb::api::LogEntry& entry)> ApplyLogFunc;

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
    bool AddReplicateNode(const std::string& endpoint);

    bool DelReplicateNode(const std::string& endpoint);

    void MatchLogOffset();

    void ReplicateToNode(const std::string& endpoint);

    // Sync Write Buffer to Disk
    void SyncToDisk();
    void SetOffset(uint64_t offset);
    uint64_t GetOffset();

    LogParts* GetLogPart();

    inline uint64_t GetLogOffset() {
        return  log_offset_.load(boost::memory_order_relaxed);
    }
    void SetRole(const ReplicatorRole& role);

    void SetSnapshotLogPartIndex(uint64_t offset);

    bool ParseBinlogIndex(const std::string& path, uint32_t& index);

private:
    bool OpenSeqFile(const std::string& path, SequentialFile** sf);

private:
    // the replicator root data path
    std::string path_;
    std::string log_path_;
    // the term for leader judgement
    boost::atomic<uint64_t> log_offset_;
    boost::atomic<uint32_t> binlog_index_;
    LogParts* logs_;
    WriteHandle* wh_;
    ReplicatorRole role_;
    std::vector<std::string> endpoints_;
    std::vector<std::shared_ptr<ReplicateNode> > nodes_;
    // sync mutex
    Mutex mu_;
    CondVar cv_;
    CondVar coffee_cv_;

    ::rtidb::RpcClient* rpc_client_;

    ApplyLogFunc func_;

    // for background task
    boost::atomic<bool> running_;

    // background task pool
    ThreadPool tp_;

    // reference cnt
    boost::atomic<uint64_t> refs_;

    boost::atomic<int> snapshot_log_part_index_;

    Mutex wmu_;

    std::shared_ptr<Table> table_;
};

} // end of replica
} // end of rtidb

#endif /* !RTIDB_LOG_REPLICATOR_H */
