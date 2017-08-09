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
typedef boost::function< bool (const std::string& entry, const std::string& pk, uint64_t offset, uint64_t ts)> SnapshotFunc;

enum ReplicatorRole {
    kLeaderNode = 1,
    kFollowerNode
};

inline bool DefaultSnapshotFunc(const std::string& entry, const std::string& pk, uint64_t offset, uint64_t ts) {
    return true;
} 

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

struct ReplicaNode {
    std::string endpoint;
    uint64_t last_sync_offset;
    SequentialFile* sf;
    Reader* reader;
    int32_t log_part_index;
    std::vector<::rtidb::api::AppendEntriesRequest> cache;
    std::string buffer;
    bool log_matched;
    ReplicaNode():endpoint(),
    last_sync_offset(0), sf(NULL), reader(NULL), 
    log_part_index(-1), cache(), buffer(), log_matched(false){}
    ~ReplicaNode() {
        delete sf;
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
                  const ReplicatorRole& role,
                  uint32_t tid,
                  uint32_t pid,
                  SnapshotFunc ssf = boost::bind(&DefaultSnapshotFunc, _1, _2, _3, _4));

    LogReplicator(const std::string& path,
                  ApplyLogFunc func,
                  const ReplicatorRole& role,
                  uint32_t tid,
                  uint32_t pid,
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

    // roll read log file fd
    // offset: the log entry offset, not byte offset
    // last_log_part_index: log index in logs
    // return log part index
    int32_t RollRLogFile(SequentialFile** sf, 
                         uint64_t offset,
                         int32_t last_log_part_index);

    // read next record from log file
    // when one of log file reaches the end , it will auto 
    // roll it
    ::rtidb::base::Status ReadNextRecord(ReplicaNode* node,
                        ::rtidb::base::Slice* record,
                        std::string* buffer);

    // add replication
    bool AddReplicateNode(const std::string& endpoint);

    void MatchLogOffset();

    bool MatchLogOffsetFromNode(ReplicaNode* node);

    void ReplicateToNode(const std::string& endpoint);

    void ApplyLog();
    // Incr ref
    void Ref();
    // Descr ref
    void UnRef();
    // Sync Write Buffer to Disk
    void SyncToDisk();

    inline uint64_t GetLogOffset() {
        return  log_offset_.load(boost::memory_order_relaxed);
    }

private:
    bool OpenSeqFile(const std::string& path, SequentialFile** sf);

private:
    // the replicator root data path
    std::string path_;
    std::string meta_path_;
    std::string log_path_;
    // the meta db based on leveldb
    leveldb::DB* meta_;
    // the term for leader judgement
    boost::atomic<uint64_t> log_offset_;
    LogParts* logs_;
    WriteHandle* wh_;
    uint32_t wsize_;
    ReplicatorRole role_;
    uint64_t last_log_offset_;
    std::vector<std::string> endpoints_;
    std::vector<ReplicaNode*> nodes_;
    // sync mutex
    Mutex mu_;
    CondVar cv_;

    ::rtidb::RpcClient* rpc_client_;

    // for slave node to apply log to itself
    ReplicaNode* self_;
    ApplyLogFunc func_;

    // for background task
    boost::atomic<bool> running_;

    // background task pool
    ThreadPool tp_;

    // reference cnt
    boost::atomic<uint64_t> refs_;

    uint32_t tid_;
    uint32_t pid_;
    Mutex wmu_;

    SnapshotFunc ssf_;
};

} // end of replica
} // end of rtidb

#endif /* !RTIDB_LOG_REPLICATOR_H */
