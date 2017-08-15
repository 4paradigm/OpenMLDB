/*
 * replicate_node.h
 * Copyright (C) 2017 4paradigm.com
 * Author denglong
 * Date 2017-08-11
 *
*/
#ifndef RTIDB_REPLICATE_NODE_H
#define RTIDB_REPLICATE_NODE_H

#include <vector>
#include "base/skiplist.h"
#include "boost/function.hpp"
#include "log/log_writer.h"
#include "log/log_reader.h"
#include "log/sequential_file.h"
#include "rpc/rpc_client.h"
#include "proto/tablet.pb.h"

namespace rtidb {
namespace replica {

using ::rtidb::log::SequentialFile;
using ::rtidb::log::Reader;

const static uint32_t FOLLOWER_REPLICATE_MODE = 0;
const static uint32_t SNAPSHOT_REPLICATE_MODE = 1;
struct StringComparator {
    int operator()(const std::string& a, const std::string& b) const {
        return a.compare(b);
    }
};

struct LogPart {
    // the first log id in the log file
    uint64_t slog_id_;
    std::string log_name_;
    LogPart(uint64_t slog_id, const std::string& log_name):slog_id_(slog_id),
        log_name_(log_name) {}
    LogPart() {}
    ~LogPart() {}
};

typedef ::rtidb::base::Skiplist<std::string, LogPart*, StringComparator> LogParts;
typedef boost::function< bool (const std::string& entry, const std::string& pk, uint64_t offset, uint64_t ts)> SnapshotFunc;

class ReplicateNode {
public:
    ReplicateNode(const std::string& point, LogParts* logs, const std::string& log_path, uint32_t tid, uint32_t pid);
    virtual ~ReplicateNode();
    ::rtidb::base::Status ReadNextRecord(::rtidb::base::Slice* record, std::string* buffer);
    int RollRLogFile();
    int OpenSeqFile(const std::string& path);
    uint32_t GetMode() {
        return replicate_node_mode_;
    }
    virtual int SyncData(uint64_t log_offset) = 0;
    virtual int MatchLogOffsetFromNode() = 0;
    bool IsLogMatched();
    void SetLogMatch(bool log_match);
    std::string GetEndPoint();
    uint64_t GetLastSyncOffset();
    ReplicateNode(const ReplicateNode&) = delete;
    ReplicateNode& operator= (const ReplicateNode&) = delete;
protected:
    std::string endpoint;
    uint64_t last_sync_offset;
    std::string log_path_;
    int log_part_index;
    SequentialFile* sf;
    Reader* reader;
    LogParts* logs_;
    uint32_t replicate_node_mode_;
    bool log_matched;
    uint32_t tid_;
    uint32_t pid_;
};

class FollowerReplicateNode: public ReplicateNode {
public:
    FollowerReplicateNode(const std::string& point, LogParts* logs, const std::string& log_path,
            uint32_t tid, uint32_t pid, ::rtidb::RpcClient* rpc_client);
    int MatchLogOffsetFromNode();        
    int SyncData(uint64_t log_offset);
    FollowerReplicateNode(const FollowerReplicateNode&) = delete;
    FollowerReplicateNode& operator= (const FollowerReplicateNode&) = delete;

private:
    std::vector<::rtidb::api::AppendEntriesRequest> cache;
    ::rtidb::RpcClient* rpc_client_;
};

class SnapshotReplicateNode: public ReplicateNode {
public:
    SnapshotReplicateNode(const std::string& point, LogParts* logs, const std::string& log_path, 
        uint32_t tid, uint32_t pid, SnapshotFunc snapshot_fun);
    ~SnapshotReplicateNode(){}
    int MatchLogOffsetFromNode();        
    int SyncData(uint64_t log_offset);
    SnapshotReplicateNode(const SnapshotReplicateNode&) = delete;
    SnapshotReplicateNode& operator= (const SnapshotReplicateNode&) = delete;

private:
    SnapshotFunc snapshot_fun_;

};

}
}

#endif
