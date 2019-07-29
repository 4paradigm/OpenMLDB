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
#include "bthread/bthread.h"
#include "bthread/condition_variable.h"
#include <atomic>

namespace rtidb {
namespace replica {

using ::rtidb::log::LogReader;
typedef ::rtidb::base::Skiplist<uint32_t, uint64_t, ::rtidb::base::DefaultComparator> LogParts;

class ReplicateNode {
public:
    ReplicateNode(const std::string& point, 
                  LogParts* logs, 
                  const std::string& log_path,
                  uint32_t tid, uint32_t pid,
                  std::atomic<uint64_t>* term, std::atomic<uint64_t>* leader_log_offset,
                  bthread::Mutex* mu, bthread::ConditionVariable* cv);
    int Init();

    int Start();

    // start match log offset, will block until the log matched
    void MatchLogOffset();
    // sync data to follower node 
    void SyncData();

    int SyncData(uint64_t log_offset);

    void SetLastSyncOffset(uint64_t offset);

    bool IsLogMatched();

    std::string GetEndPoint();

    uint64_t GetLastSyncOffset();

    int GetLogIndex();

    void Stop();

    ReplicateNode(const ReplicateNode&) = delete;

    ReplicateNode& operator= (const ReplicateNode&) = delete;

private:
    int MatchLogOffsetFromNode();
private:
    LogReader log_reader_;
    std::vector<::rtidb::api::AppendEntriesRequest> cache_;
    std::string endpoint_;
    uint64_t last_sync_offset_;
    bool log_matched_;
    uint32_t tid_;
    uint32_t pid_;
    std::atomic<uint64_t>* term_;
    ::rtidb::RpcClient<::rtidb::api::TabletServer_Stub> rpc_client_;
    bthread_t worker_;
    std::atomic<uint64_t>* leader_log_offset_;
    std::atomic<bool> is_running_;
    bthread::Mutex* mu_;
    bthread::ConditionVariable* cv_;
    uint32_t go_back_cnt_;
};

}
}

#endif
