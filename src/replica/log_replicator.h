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
#include "replica/file_appender.h"
#include "base/skiplist.h"
#include "boost/atomic.hpp"
#include "leveldb/db.h"
#include "mutex.h"
#include "thread_pool.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"

namespace rtidb {
namespace replica {

using ::baidu::common::MutexLock;
using ::baidu::common::Mutex;
using ::baidu::common::CondVar;
using ::baidu::common::ThreadPool;
using ::rtidb::log::WritableFile;
using ::rtidb::log::Writer;

enum ReplicatorRole {
    kLeaderNode = 1,
    kSlaveNode
};

class LogReplicator;

struct LogPart {
    // the first log id in the log file
    uint64_t slog_id_;
    std::string log_name_;
    LogPart(uint64_t slog_id, const std::string& log_name):slog_id_(slog_id),
    log_name_(log_name) {}
    LogPart() {}
    ~LogPart() {}
};

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

    ~WriteHandle() {
        delete lw_;
        delete wf_;
    }
};

struct ReplicaNode {
    std::string endpoint;
    uint64_t last_sync_term;
    uint64_t last_sync_offset;
    ::rtidb::RpcClient* client;
    ::rtidb::api::TabletServer_Stub* stub;
};

struct StringComparator {
    int operator()(const std::string& a, const std::string& b) const {
        return a.compare(b);
    }
};


typedef ::rtidb::base::Skiplist<std::string, LogPart*, StringComparator> LogParts;

class LogReplicator {

public:

    LogReplicator(const std::string& path);

    ~LogReplicator();

    bool Init();

    // the slave node receives master log entries
    bool AppendEntries(const ::rtidb::api::AppendEntriesRequest* request);

    // the master node append entry
    bool AppendEntry(::rtidb::api::LogEntry& entry);

private:

    // recover logs meta
    bool Recover();
    bool RollWLogFile();

private:
    // the replicator root data path
    std::string path_;
    std::string meta_path_;
    std::string log_path_;
    // the meta db based on leveldb
    leveldb::DB* meta_;
    // the term for leader judgement
    uint64_t term_;
    uint64_t log_offset_;
    LogParts* logs_;
    WriteHandle* wh_;
    uint32_t wsize_;
    ReplicatorRole role_;
    uint64_t last_log_term_;
    uint64_t last_log_offset_;
};

} // end of replica
} // end of rtidb

#endif /* !RTIDB_LOG_REPLICATOR_H */
