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

typedef boost::function< bool (const ::rtidb::api::LogEntry& entry)> ApplyLogFunc;

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
    SequentialFile* sf;
    Reader* lr; 
    std::vector<::rtidb::api::AppendEntriesRequest> cache;
    ReplicaNode():endpoint(),last_sync_term(0),
    last_sync_offset(0), client(NULL), stub(NULL), sf(NULL), lr(NULL),cache(){}
    ~ReplicaNode() {
        delete client;
        delete stub;
        delete sf;
        delete lr;
    }
};

struct StringComparator {
    int operator()(const std::string& a, const std::string& b) const {
        return a.compare(b);
    }
};


typedef ::rtidb::base::Skiplist<std::string, LogPart*, StringComparator> LogParts;

class LogReplicator {

public:

    LogReplicator(const std::string& path,
                  const std::vector<std::string>& endpoints,
                  const ReplicatorRole& role);

    LogReplicator(const std::string& path,
                  ApplyLogFunc& func,
                  const ReplicatorRole& role);

    ~LogReplicator();

    bool Init();

    // the slave node receives master log entries
    bool AppendEntries(const ::rtidb::api::AppendEntriesRequest* request);

    // the master node append entry
    bool AppendEntry(::rtidb::api::LogEntry& entry);

    // sync data to slave nodes
    void Sync();
    // recover logs meta
    bool Recover();
    bool RollWLogFile();
    bool RollRLogFile(SequentialFile** sf, 
                      Reader** lr, 
                      uint64_t offset);
    
    // read next record from log file
    // when one of log file reaches the end , it will auto 
    // roll it
    bool ReadNextRecord(Reader** lr, 
                        SequentialFile** sf, 
                        ::rtidb::base::Slice* record,
                        std::string* buffer,
                        uint64_t offset);

    void ReplicateLog();
    void ReplicateToNode(ReplicaNode& node);
    void ApplyLog();
private:
    // the replicator root data path
    std::string path_;
    std::string meta_path_;
    std::string log_path_;
    // the meta db based on leveldb
    leveldb::DB* meta_;
    // the term for leader judgement
    uint64_t term_;
    boost::atomic<uint64_t> log_offset_;
    LogParts* logs_;
    WriteHandle* wh_;
    uint32_t wsize_;
    ReplicatorRole role_;
    uint64_t last_log_term_;
    uint64_t last_log_offset_;
    std::vector<std::string> endpoints_;
    std::vector<ReplicaNode> nodes_;

    // sync mutex
    Mutex mu_;
    CondVar cv_;

    // for slave node to apply log to itself
    SequentialFile* sf_;
    Reader* lr_;
    uint64_t apply_log_offset_;
    ApplyLogFunc func_;

    // for background task
    boost::atomic<bool> running_;

    // background task pool
    ThreadPool tp_;
};

} // end of replica
} // end of rtidb

#endif /* !RTIDB_LOG_REPLICATOR_H */
