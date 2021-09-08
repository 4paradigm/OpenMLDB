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

#ifndef SRC_REPLICA_MEMTABLE_REPLICATE_NODE_H_
#define SRC_REPLICA_MEMTABLE_REPLICATE_NODE_H_

#include <atomic>
#include <string>
#include <vector>

#include "bthread/bthread.h"
#include "bthread/condition_variable.h"
#include "proto/tablet.pb.h"
#include "replica/replicate_node.h"
#include "rpc/rpc_client.h"

namespace openmldb {
namespace replica {

class MemTableReplicateNode : public ReplicateNode {
 public:
    MemTableReplicateNode(const std::string& endpoint, LogParts* logs, const std::string& log_path,
                  uint32_t tid, uint32_t pid,
                  std::atomic<uint64_t>* term, std::atomic<uint64_t>* leader_log_offset, bthread::Mutex* mu,
                  bthread::ConditionVariable* cv, bool rep_follower, std::atomic<uint64_t>* follower_offset,
                  const std::string& real_point);
    int Init() override;

    // sync data to follower node
    void MatchLogOffset() override;
    void SyncData() override;

    int SyncData(uint64_t log_offset);

    MemTableReplicateNode(const MemTableReplicateNode&) = delete;
    MemTableReplicateNode& operator=(const MemTableReplicateNode&) = delete;

    int MatchLogOffsetFromNode();

 private:
    std::atomic<uint64_t>* term_;
    bthread::Mutex* mu_;
    bthread::ConditionVariable* cv_;
    uint32_t go_back_cnt_;
    std::atomic<bool> rep_node_;
    std::atomic<uint64_t>* leader_log_offset_;
    std::atomic<uint64_t>* follower_offset_;  // max local cluster follower offset
    std::vector<::openmldb::api::AppendEntriesRequest> cache_;
    ::openmldb::RpcClient<::openmldb::api::TabletServer_Stub> rpc_client_;
};

}  // namespace replica
}  // namespace openmldb

#endif  // SRC_REPLICA_MEMTABLE_REPLICATE_NODE_H_
