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
#include "leveldb/db.h"
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
using ::rtidb::log::WritableFile;
using ::rtidb::log::SequentialFile;
using ::rtidb::log::Writer;
using ::rtidb::log::Reader;
using ::rtidb::storage::Table;

typedef boost::function< bool (const ::rtidb::api::LogEntry& entry)> ApplyLogFunc;
typedef boost::function< bool (const std::string& entry, const std::string& pk, uint64_t offset, uint64_t ts)> SnapshotFunc;

enum ReplicatorRole {
    kLeaderNode = 1,
    kFollowerNode
};

inline bool DefaultSnapshotFunc(const std::string& entry, const std::string& pk, uint64_t offset, uint64_t ts) {
    return true;
} 

class LogReplicator;

struct WriteHandle {
    FILE* fd_;
    WritableFile* wf_;
    Writer* lw_;
    WriteHandle(const std::string& fname, FILE* fd):fd_(fd),
    wf_(NULL), lw_(NULL) {
        wf_ = ::rtidb::log::NewWritableFile(fname, fd);
        lw_ = new Writer(wf_);
    }

    ::rtidb::base::Status Write(const ::rtidb::base::Slice& slice) {
        return lw_->AddRecord(slice);
    }

    ::rtidb::base::Status Sync() {
        return wf_->Sync(); 
    }

    ::rtidb::base::Status EndLog() {
        return lw_->EndLog();
    }

    ~WriteHandle() {
        delete lw_;
        delete wf_;
    }
};

class LogReplicator {

public:

    LogReplicator(const std::string& path,
                  const std::vector<std::string>& endpoints,
                  const ReplicatorRole& role,
                  Table* table,
                  SnapshotFunc ssf = boost::bind(&DefaultSnapshotFunc, _1, _2, _3, _4));

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

    int PauseReplicate(std::shared_ptr<ReplicateNode> node);

    // Incr ref
    void Ref();
    // Descr ref
    void UnRef();
    // Sync Write Buffer to Disk
    void SyncToDisk();
    void SetOffset(uint64_t offset);
    uint64_t GetOffset();

    inline uint64_t GetLogOffset() {
        return  log_offset_.load(boost::memory_order_relaxed);
    }
    void SetRole(const ReplicatorRole& role);

private:
    bool OpenSeqFile(const std::string& path, SequentialFile** sf);

private:
    // the replicator root data path
    std::string path_;
    std::string log_path_;
    // the term for leader judgement
    boost::atomic<uint64_t> log_offset_;
    uint32_t binlog_index_;
    LogParts* logs_;
    WriteHandle* wh_;
    uint32_t wsize_;
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

    Mutex wmu_;

    SnapshotFunc ssf_;

    Table* table_;
};

} // end of replica
} // end of rtidb

#endif /* !RTIDB_LOG_REPLICATOR_H */
