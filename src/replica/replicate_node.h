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
#include <string>

#include "base/skiplist.h"
#include "bthread/bthread.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "log/sequential_file.h"

namespace openmldb {
namespace replica {

using ::openmldb::log::LogReader;
typedef ::openmldb::base::Skiplist<uint32_t, uint64_t, ::openmldb::base::DefaultComparator> LogParts;

class ReplicateNode {
 public:
    ReplicateNode(uint32_t tid, uint32_t pid, const std::string& endpoint, LogParts* logs, const std::string& log_path);
    virtual ~ReplicateNode() = default;
    virtual int Init() = 0;

    int Start();

    // start match log offset, will block until the log matched
    virtual void MatchLogOffset();
    // sync data to follower node
    virtual void SyncData() = 0;

    void SetLastSyncOffset(uint64_t offset);

    bool IsLogMatched() const;

    const std::string& GetEndPoint() const;

    uint64_t GetLastSyncOffset();

    int GetLogIndex();

    void Stop();

    ReplicateNode(const ReplicateNode&) = delete;

    ReplicateNode& operator=(const ReplicateNode&) = delete;

 protected:
    uint32_t tid_;
    uint32_t pid_;
    LogReader log_reader_;
    std::string endpoint_;
    uint64_t last_sync_offset_;
    bool log_matched_;
    bthread_t worker_;
    std::atomic<bool> is_running_;
};

}  // namespace replica
}  // namespace openmldb

#endif  // SRC_REPLICA_REPLICATE_NODE_H_
