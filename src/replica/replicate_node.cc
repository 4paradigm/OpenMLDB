/*
 * replicate_node.cc
 * Copyright (C) 2017 4paradigm.com
 * Author denglong
 * Date 2017-08-11
 *
*/
#include "replicate_node.h"
#include "logging.h"
#include "base/strings.h"
#include <gflags/gflags.h>

DECLARE_int32(binlog_sync_batch_size);
DECLARE_int32(binlog_sync_wait_time);
DECLARE_int32(binlog_coffee_time);
DECLARE_int32(binlog_match_logoffset_interval);
DECLARE_int32(request_max_retry);
DECLARE_int32(request_timeout_ms);
DECLARE_string(zk_cluster);

namespace rtidb {
namespace replica {

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

static void* RunSyncTask(void* args) {
    if (args == NULL) {
        PDLOG(WARNING, "input args is null");
        return NULL;
    }
    ::rtidb::replica::ReplicateNode* rn = static_cast<::rtidb::replica::ReplicateNode*>(args);
    rn->MatchLogOffset();
    rn->SyncData();
    return NULL;
}

ReplicateNode::ReplicateNode(const std::string& point, 
                             LogParts* logs, 
                             const std::string& log_path, 
                             uint32_t tid, uint32_t pid,
                             uint64_t term, std::atomic<uint64_t>* leader_log_offset,
                             bthread::Mutex* mu, bthread::ConditionVariable* cv): log_reader_(logs, log_path), cache_(),
    endpoint_(point), last_sync_offset_(0), log_matched_(false),
    tid_(tid), pid_(pid), term_(term), 
    rpc_client_(point), worker_(), leader_log_offset_(leader_log_offset),
    is_running_(false), mu_(mu), cv_(cv){
}

int ReplicateNode::Init() {
    int ok = rpc_client_.Init();
    if (ok != 0) {
        PDLOG(WARNING, "fail to open rpc client with errno %d", ok);
        return ok;
    }
    is_running_.store(true, std::memory_order_relaxed); 
    ok = bthread_start_background(&worker_, NULL, RunSyncTask, this);
    if (ok != 0) {
        PDLOG(WARNING, "fail to start bthread with errno %d", ok);
    }
    return ok;
}

void ReplicateNode::MatchLogOffset() {
    while (is_running_.load(std::memory_order_relaxed)) {
        int ok = MatchLogOffsetFromNode();
        if (ok != 0) {
            bthread_usleep(FLAGS_binlog_match_logoffset_interval * 1000); 
        }else {
            log_matched_ = true; 
            return;
        }
    }
}

void ReplicateNode::SyncData() {
    uint32_t coffee_time = 0;
    while (is_running_.load(std::memory_order_relaxed)) {
        if (coffee_time > 0) {
            bthread_usleep(coffee_time * 1000);
            coffee_time = 0;
        }
        {
            std::unique_lock<bthread::Mutex> lock(*mu_);
            // no new data append and wait
            while (last_sync_offset_ >= leader_log_offset_->load(std::memory_order_relaxed)) {
                cv_->wait_for(lock, FLAGS_binlog_sync_wait_time * 1000);
                if (!is_running_.load(std::memory_order_relaxed)) {
                    PDLOG(INFO, "replicate log to endpoint %s for table #tid %u #pid %u exist", endpoint_.c_str(), tid_, pid_);
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

int ReplicateNode::GetLogIndex() {
    return log_reader_.GetLogIndex();
}

bool ReplicateNode::IsLogMatched() {
    return log_matched_;
}

std::string ReplicateNode::GetEndPoint() {
    return endpoint_;
}

uint64_t ReplicateNode::GetLastSyncOffset() {
    return last_sync_offset_;
}

void ReplicateNode::SetLastSyncOffset(uint64_t offset) {
    last_sync_offset_ = offset;
}

int ReplicateNode::MatchLogOffsetFromNode() {
    ::rtidb::api::AppendEntriesRequest request;
    request.set_tid(tid_);
    request.set_pid(pid_);
    request.set_term(term_);
    request.set_pre_log_index(0);
    ::rtidb::api::AppendEntriesResponse response;
    bool ret = rpc_client_.SendRequest(&::rtidb::api::TabletServer_Stub::AppendEntries,
                        &request, &response, FLAGS_request_timeout_ms, FLAGS_request_max_retry);
    if (ret && response.code() == 0) {
        last_sync_offset_ = response.log_offset();
        log_matched_ = true;
        log_reader_.SetOffset(last_sync_offset_);
        PDLOG(INFO, "match node %s log offset %lu for table tid %u pid %u",
                  endpoint_.c_str(), last_sync_offset_, tid_, pid_);
        return 0;
    }
    PDLOG(WARNING, "match node %s log offset failed. tid %u pid %u",
                    endpoint_.c_str(), tid_, pid_);
    return -1;
}

int ReplicateNode::SyncData(uint64_t log_offset) {
    PDLOG(DEBUG, "node[%s] offset[%lu] log offset[%lu]", 
                endpoint_.c_str(), last_sync_offset_, log_offset);
    ::rtidb::api::AppendEntriesRequest request;
    ::rtidb::api::AppendEntriesResponse response;
    uint64_t sync_log_offset =  last_sync_offset_;
    bool request_from_cache = false;
    bool need_wait = false;
    if (cache_.size() > 0) {
        request_from_cache = true;
        request = cache_[0];
        if (request.entries_size() <= 0) {
            cache_.clear(); 
            PDLOG(WARNING, "empty append entry request from node %s cache", endpoint_.c_str());
            return -1;
        }
        const ::rtidb::api::LogEntry& entry = request.entries(request.entries_size() - 1);
        if (entry.log_index() <= last_sync_offset_) {
            PDLOG(DEBUG, "duplicate log index from node %s cache", endpoint_.c_str());
            cache_.clear();
            return -1;
        }
        PDLOG(INFO, "use cached request to send last index  %lu", entry.log_index());
        sync_log_offset = entry.log_index();
    } else {
        request.set_tid(tid_);
        request.set_pid(pid_);
        request.set_pre_log_index(last_sync_offset_);
        if (!FLAGS_zk_cluster.empty()) {
            request.set_term(term_);
        }
        uint32_t batchSize = log_offset - last_sync_offset_;
        batchSize = std::min(batchSize, (uint32_t)FLAGS_binlog_sync_batch_size);
        for (uint64_t i = 0; i < batchSize; ) {
            std::string buffer;
            ::rtidb::base::Slice record;
            ::rtidb::base::Status status = log_reader_.ReadNextRecord(&record, &buffer);
            if (status.ok()) {
                ::rtidb::api::LogEntry* entry = request.add_entries();
                if (!entry->ParseFromString(record.ToString())) {
                    PDLOG(WARNING, "bad protobuf format %s size %ld", ::rtidb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                    request.mutable_entries()->RemoveLast();
                    break;
                }
                PDLOG(DEBUG, "entry val %s log index %lld", entry->value().c_str(), entry->log_index());
                if (entry->log_index() <= sync_log_offset) {
                    PDLOG(DEBUG, "skip duplicate log offset %lld", entry->log_index());
                    request.mutable_entries()->RemoveLast();
                    continue;
                }
                // the log index should incr by 1
                if ((sync_log_offset + 1) != entry->log_index()) {
                    PDLOG(WARNING, "log missing expect offset %lu but %ld", sync_log_offset + 1, entry->log_index());
                    request.mutable_entries()->RemoveLast();
                    need_wait = true;
                    log_reader_.GoBackToLastBlock();
                    break;
                }
                sync_log_offset = entry->log_index();
            } else if (status.IsWaitRecord()) {
                PDLOG(DEBUG, "got a coffee time for[%s]", endpoint_.c_str());
                need_wait = true;
                break;
            } else {
                PDLOG(WARNING, "fail to get record %s", status.ToString().c_str());
                need_wait = true;
                break;
            }
            i++;
        }
    }    
    if (request.entries_size() > 0) {
        bool ret = rpc_client_.SendRequest(&::rtidb::api::TabletServer_Stub::AppendEntries,
                                 &request, &response, 12, 1);
        if (ret && response.code() == 0) {
            PDLOG(DEBUG, "sync log to node[%s] to offset %lld", endpoint_.c_str(), sync_log_offset);
            last_sync_offset_ = sync_log_offset;
            if (request_from_cache) {
                cache_.clear(); 
            }
        } else {
            if (!request_from_cache) {
                cache_.push_back(request);
            }
            need_wait = true;
            PDLOG(WARNING, "fail to sync log to node %s", endpoint_.c_str());
        }
    }
    if (need_wait) {
        return 1;
    }
    return 0;
}

void ReplicateNode::Stop() {
    is_running_.store(false, std::memory_order_relaxed);
    if (bthread_stopped(worker_) == 1) {
        PDLOG(INFO, "sync thread for table #tid %u #pid %u has been stoped", tid_, pid_);
        return;
    }
    int ok = bthread_stop(worker_);
    if (ok != 0) {
        PDLOG(WARNING, "fail to stop sync thread for table #tid %u #pid %u", tid_, pid_);
    }
}

}
}
