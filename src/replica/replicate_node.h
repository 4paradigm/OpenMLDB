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
#include "log/log_writer.h"
#include "log/log_reader.h"
#include "log/sequential_file.h"
#include "rpc/rpc_client.h"
#include "proto/tablet.pb.h"
#include <atomic>

namespace rtidb {
namespace replica {

using ::rtidb::log::LogReader;
typedef ::rtidb::base::Skiplist<uint32_t, uint64_t, ::rtidb::base::DefaultComparator> LogParts;

class ReplicateNode {
public:
    ReplicateNode(const std::string& point, LogParts* logs, const std::string& log_path,
            uint32_t tid, uint32_t pid);
    int Init();
    int MatchLogOffsetFromNode(uint64_t offset, uint64_t term);
    int SyncData(uint64_t log_offset);
    void SetLastSyncOffset(uint64_t offset);
    bool IsLogMatched();
    void SetLogMatch(bool log_match);
    std::string GetEndPoint();
    uint64_t GetLastSyncOffset();
    int GetLogIndex();

    ReplicateNode(const ReplicateNode&) = delete;
    ReplicateNode& operator= (const ReplicateNode&) = delete;

private:
    LogReader log_reader_;
    std::vector<::rtidb::api::AppendEntriesRequest> cache_;
    std::string endpoint_;
    std::atomic<bool> making_snapshot_;
    uint64_t last_sync_offset_;
    bool log_matched_;
    uint32_t tid_;
    uint32_t pid_;
    uint64_t term_;
    ::rtidb::RpcClient<::rtidb::api::TabletServer_Stub> rpc_client_;
};

}
}

#endif
