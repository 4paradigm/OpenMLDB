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


#ifndef SRC_REPLICA_LOG_REPLICATOR_H_
#define SRC_REPLICA_LOG_REPLICATOR_H_

#include <atomic>
#include <condition_variable> // NOLINT
#include <map>
#include <mutex> // NOLINT
#include <vector>
#include <string>
#include <memory>
#include "base/skiplist.h"
#include "bthread/bthread.h"
#include "bthread/condition_variable.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "log/sequential_file.h"
#include "proto/tablet.pb.h"
#include "replica/replicate_node.h"
#include "storage/table.h"
#include "thread_pool.h" // NOLINT

namespace rtidb {
namespace replica {

using ::baidu::common::ThreadPool;
using ::rtidb::api::LogEntry;
using ::rtidb::log::Reader;
using ::rtidb::log::SequentialFile;
using ::rtidb::log::WriteHandle;
using ::rtidb::storage::Table;

enum ReplicatorRole { kLeaderNode = 1, kFollowerNode };

class LogReplicator {
 public:
    LogReplicator(const std::string& path,
            const std::map<std::string, std::string>& real_ep_map,
            const ReplicatorRole& role, std::shared_ptr<Table> table,
            std::atomic<bool>* follower);

    ~LogReplicator();

    bool Init();

    bool StartSyncing();

    // the slave node receives master log entries
    bool AppendEntries(const ::rtidb::api::AppendEntriesRequest* request,
                       ::rtidb::api::AppendEntriesResponse* response);

    // the master node append entry
    bool AppendEntry(::rtidb::api::LogEntry& entry); // NOLINT

    //  data to slave nodes
    void Notify();
    // recover logs meta
    bool Recover();

    bool RollWLogFile();

    void DeleteBinlog();

    // add replication
    int AddReplicateNode(
            const std::map<std::string, std::string>& real_ep_map);
    // add replication with tid
    int AddReplicateNode(
            const std::map<std::string, std::string>& real_ep_map,
            uint32_t tid);

    int DelReplicateNode(const std::string& endpoint);

    void GetReplicateInfo(std::map<std::string, uint64_t>& info_map); // NOLINT

    void MatchLogOffset();

    void ReplicateToNode(const std::string& endpoint);

    // Sync Write Buffer to Disk
    void SyncToDisk();
    void SetOffset(uint64_t offset);

    uint64_t GetOffset();

    LogParts* GetLogPart();

    inline uint64_t GetLogOffset() {
        return log_offset_.load(std::memory_order_relaxed);
    }
    void SetRole(const ReplicatorRole& role);

    uint64_t GetLeaderTerm();
    void SetLeaderTerm(uint64_t term);

    void SetSnapshotLogPartIndex(uint64_t offset);

    bool ParseBinlogIndex(const std::string& path, uint32_t& index); // NOLINT

    bool DelAllReplicateNode();

 private:
    bool OpenSeqFile(const std::string& path, SequentialFile** sf);

    bool ApplyEntryToTable(const LogEntry& entry);

 private:
    // the replicator root data path
    std::string path_;
    std::string log_path_;
    // the term for leader judgement
    std::atomic<uint64_t> log_offset_;
    std::atomic<uint64_t> follower_offset_;
    std::atomic<uint32_t> binlog_index_;
    LogParts* logs_;
    WriteHandle* wh_;
    ReplicatorRole role_;
    std::map<std::string, std::string> real_ep_map_;
    std::vector<std::shared_ptr<ReplicateNode> > nodes_;
    std::vector<std::string> local_endpoints_;

    std::atomic<uint64_t> term_;
    // sync mutex
    bthread::Mutex mu_;
    bthread::ConditionVariable cv_;

    std::atomic<int> snapshot_log_part_index_;
    std::atomic<uint64_t> snapshot_last_offset_;

    std::shared_ptr<Table> table_;

    std::mutex wmu_;
    std::atomic<bool>* follower_;
};

}  // namespace replica
}  // namespace rtidb

#endif  // SRC_REPLICA_LOG_REPLICATOR_H_
