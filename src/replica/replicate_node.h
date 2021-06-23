/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_REPLICA_REPLICATE_NODE_H_
#define SRC_REPLICA_REPLICATE_NODE_H_

#include <atomic>
#include <vector>
#include <string>
#include "base/skiplist.h"
#include "bthread/bthread.h"
#include "bthread/condition_variable.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "log/sequential_file.h"
#include "proto/tablet.pb.h"
#include "rpc/rpc_client.h"

namespace openmldb {
namespace replica {

using ::openmldb::log::LogReader;
typedef ::openmldb::base::Skiplist<uint32_t, uint64_t,
                                ::openmldb::base::DefaultComparator>
    LogParts;

class ReplicateNode {
 public:
    ReplicateNode(const std::string& point, LogParts* logs,
                  const std::string& log_path, uint32_t tid, uint32_t pid,
                  std::atomic<uint64_t>* term,
                  std::atomic<uint64_t>* leader_log_offset, bthread::Mutex* mu,
                  bthread::ConditionVariable* cv, bool rep_follower,
                  std::atomic<uint64_t>* follower_offset,
                  const std::string& real_point);
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

    ReplicateNode& operator=(const ReplicateNode&) = delete;

 private:
    int MatchLogOffsetFromNode();

 private:
    LogReader log_reader_;
    std::vector<::openmldb::api::AppendEntriesRequest> cache_;
    std::string endpoint_;
    uint64_t last_sync_offset_;
    bool log_matched_;
    uint32_t tid_;
    uint32_t pid_;
    std::atomic<uint64_t>* term_;
    ::openmldb::RpcClient<::openmldb::api::TabletServer_Stub> rpc_client_;
    bthread_t worker_;
    std::atomic<uint64_t>* leader_log_offset_;
    std::atomic<bool> is_running_;
    bthread::Mutex* mu_;
    bthread::ConditionVariable* cv_;
    uint32_t go_back_cnt_;
    std::atomic<bool> rep_node_;
    std::atomic<uint64_t>*
        follower_offset_;  // max local cluster follower offset
};

}  // namespace replica
}  // namespace openmldb

#endif  // SRC_REPLICA_REPLICATE_NODE_H_
