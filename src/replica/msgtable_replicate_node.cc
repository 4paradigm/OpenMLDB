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

#include "replica/msgtable_replicate_node.h"

#include <gflags/gflags.h>
#include <algorithm>

#include "base/glog_wapper.h"
#include "base/strings.h"
#include "codec/message_codec.h"

DECLARE_int32(binlog_sync_batch_size);
DECLARE_int32(binlog_sync_wait_time);
DECLARE_int32(binlog_coffee_time);
DECLARE_int32(binlog_match_logoffset_interval);
DECLARE_int32(request_max_retry);
DECLARE_int32(request_timeout_ms);
DECLARE_string(zk_cluster);
DECLARE_uint32(go_back_max_try_cnt);

namespace openmldb {
namespace replica {

static void EmptyDeleter(void* args) {}

MsgTableReplicateNode::MsgTableReplicateNode(uint32_t tid, uint32_t pid, const std::string& endpoint,
                             LogParts* logs, const std::string& log_path,
                             std::atomic<uint64_t>* leader_log_offset,
                             bthread::Mutex* mu, bthread::ConditionVariable* cv)
    : ReplicateNode(tid, pid, endpoint, logs, log_path),
      mu_(mu),
      cv_(cv),
      go_back_cnt_(0),
      leader_log_offset_(leader_log_offset),
      rpc_client_(endpoint) {}

int MsgTableReplicateNode::Init() {
    int ok = rpc_client_.Init();
    if (rpc_client_.Init() != 0) {
        PDLOG(WARNING, "fail to open rpc client with errno %d", ok);
    }
    PDLOG(INFO, "open rpc client for endpoint %s done", endpoint_.c_str());
    return ok;
}

void MsgTableReplicateNode::MatchLogOffset() {
    log_matched_ = true;
    log_reader_.SetOffset(last_sync_offset_);
}

void MsgTableReplicateNode::SyncData() {
    uint32_t coffee_time = 0;
    while (is_running_.load(std::memory_order_relaxed)) {
        if (coffee_time > 0) {
            bthread_usleep(coffee_time * 1000);
            coffee_time = 0;
        }
        if (last_sync_offset_ >= leader_log_offset_->load(std::memory_order_relaxed)) {
            std::unique_lock<bthread::Mutex> lock(*mu_);
            // no new data append and wait
            while (last_sync_offset_ >= leader_log_offset_->load(std::memory_order_relaxed)) {
                cv_->wait_for(lock, FLAGS_binlog_sync_wait_time * 1000);
                if (!is_running_.load(std::memory_order_relaxed)) {
                    PDLOG(INFO, "replicate log to endpoint %s for table #tid %u #pid %u exist",
                          endpoint_.c_str(), tid_, pid_);
                    return;
                }
            }
        }
        int ret = SyncData(leader_log_offset_->load(std::memory_order_relaxed));
        if (ret == 1) {
            coffee_time = FLAGS_binlog_coffee_time;
        }
    }
    PDLOG(INFO, "replicate log to endpoint %s for table #tid %u #pid %u exist", endpoint_.c_str(), tid_, pid_);
}

int MsgTableReplicateNode::SyncData(uint64_t log_offset) {
    DEBUGLOG("node[%s] offset[%lu] log offset[%lu]", endpoint_.c_str(), last_sync_offset_, log_offset);
    if (log_offset <= last_sync_offset_) {
        PDLOG(WARNING, "log offset [%lu] le last sync offset [%lu], do nothing", log_offset, last_sync_offset_);
        return 1;
    }
    uint64_t sync_log_offset = last_sync_offset_;

    std::string buffer;
    ::openmldb::base::Slice record;
    ::openmldb::base::Status status = log_reader_.ReadNextRecord(&record, &buffer);
    if (status.ok()) {
        ::openmldb::codec::Message message;
        if (!::openmldb::codec::MessageCodec::Decode(record, &message)) {
             PDLOG(WARNING, "fail to decode message. tid %u pid %u", tid_, pid_);
             return -1;
        }
        uint64_t cur_offset = message.offset;
        DEBUGLOG("message val %s log index %lu", message.value.ToString().c_str(), cur_offset);
        if (cur_offset <= sync_log_offset) {
            DEBUGLOG("skip duplicate log offset %lld", cur_offset);
            return 0;
        }
        // the log index should incr by 1
        if ((sync_log_offset + 1) != cur_offset) {
            PDLOG(WARNING, "log missing expect offset %lu but %ld. tid %u pid %u", sync_log_offset + 1,
                  cur_offset, tid_, pid_);
            if (go_back_cnt_ > FLAGS_go_back_max_try_cnt) {
                log_reader_.GoBackToStart();
                go_back_cnt_ = 0;
                PDLOG(WARNING, "go back to start. tid %u pid %u endpoint %s", tid_, pid_, endpoint_.c_str());
            } else {
                log_reader_.GoBackToLastBlock();
                go_back_cnt_++;
            }
            return 1;
        }
        ::openmldb::nltablet::ConsumeMessageRequest request;
        ::openmldb::nltablet::ConsumeMessageResponse response;
        request.set_tid(tid_);
        sync_log_offset = message.offset;
        ::butil::IOBuf buf;
        // iobuf do not free data by using empty deleter
        buf.append_user_data(const_cast<char*>(record.data()), record.size(), EmptyDeleter);
        bool ret = rpc_client_.SendRequestWithAttachment(&::openmldb::nltablet::NLTabletServer_Stub::ConsumeMessage,
                &request, FLAGS_request_timeout_ms, FLAGS_request_max_retry, buf, &response);
        if (ret && response.code() == 0) {
            last_sync_offset_ = sync_log_offset;
        }
        return 0;
    } else if (status.IsWaitRecord()) {
        DEBUGLOG("got a coffee time for[%s]", endpoint_.c_str());
    } else if (status.IsInvalidRecord()) {
        DEBUGLOG("fail to get record. %s. tid %u pid %u", status.ToString().c_str(), tid_, pid_);
        if (go_back_cnt_ > FLAGS_go_back_max_try_cnt) {
            log_reader_.GoBackToStart();
            go_back_cnt_ = 0;
            PDLOG(WARNING, "go back to start. tid %u pid %u endpoint %s", tid_, pid_, endpoint_.c_str());
        } else {
            log_reader_.GoBackToLastBlock();
            go_back_cnt_++;
        }
    } else {
        PDLOG(WARNING, "fail to get record: %s. tid %u pid %u", status.ToString().c_str(), tid_, pid_);
    }
    return 1;
}

}  // namespace replica
}  // namespace openmldb
