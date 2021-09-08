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

#include "replica/replicate_node.h"

#include <algorithm>
#include "base/glog_wapper.h"

namespace openmldb {
namespace replica {

static void* RunSyncTask(void* args) {
    if (args == NULL) {
        PDLOG(WARNING, "input args is null");
        return NULL;
    }
    ::openmldb::replica::ReplicateNode* rn = static_cast<::openmldb::replica::ReplicateNode*>(args);
    rn->MatchLogOffset();
    rn->SyncData();
    return NULL;
}

ReplicateNode::ReplicateNode(uint32_t tid, uint32_t pid, const std::string& endpoint, LogParts* logs,
        const std::string& log_path)
    : tid_(tid),
      pid_(pid),
      log_reader_(logs, log_path, false),
      endpoint_(endpoint),
      last_sync_offset_(0),
      log_matched_(false),
      worker_(),
      is_running_(false) {}

int ReplicateNode::Start() {
    if (is_running_.load(std::memory_order_relaxed)) {
        PDLOG(WARNING, "sync thread has been started for table #tid %u, #pid %u", tid_, pid_);
        return 0;
    }
    is_running_.store(true, std::memory_order_relaxed);
    int ok = bthread_start_background(&worker_, NULL, RunSyncTask, this);
    if (ok != 0) {
        PDLOG(WARNING, "fail to start bthread with errno %d", ok);
    } else {
        PDLOG(INFO, "start sync thread for table #tid %u, #pid %u done", tid_, pid_);
    }
    return ok;
}

void ReplicateNode::MatchLogOffset() {}

int ReplicateNode::GetLogIndex() { return log_reader_.GetLogIndex(); }

bool ReplicateNode::IsLogMatched() const { return log_matched_; }

const std::string& ReplicateNode::GetEndPoint() const { return endpoint_; }

uint64_t ReplicateNode::GetLastSyncOffset() { return last_sync_offset_; }

void ReplicateNode::SetLastSyncOffset(uint64_t offset) { last_sync_offset_ = offset; }


void ReplicateNode::Stop() {
    is_running_.store(false, std::memory_order_relaxed);
    if (worker_ == 0) {
        return;
    }

    if (bthread_stopped(worker_) == 1) {
        PDLOG(INFO, "sync thread for table #tid %u #pid %u has been stoped", tid_, pid_);
        return;
    }

    bthread_stop(worker_);
    bthread_join(worker_, NULL);
    worker_ = 0;
}

}  // namespace replica
}  // namespace openmldb
