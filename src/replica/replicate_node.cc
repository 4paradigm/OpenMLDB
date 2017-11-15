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

namespace rtidb {
namespace replica {

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

ReplicateNode::ReplicateNode(const std::string& point, LogParts* logs, 
        const std::string& log_path, uint32_t tid, uint32_t pid) : 
        log_reader_(logs, log_path), endpoint_(point), tid_(tid), pid_(pid),
        rpc_client_(point) {
    log_matched_ = false;
}

int ReplicateNode::Init() {
    int ret = rpc_client_.Init();
    printf("INIT%d\n", ret);
    return ret;
}


int ReplicateNode::GetLogIndex() {
    return log_reader_.GetLogIndex();
}

bool ReplicateNode::IsLogMatched() {
    return log_matched_;
}

void ReplicateNode::SetLogMatch(bool log_match) {
    log_matched_ = log_match;
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
    request.set_pre_log_index(0);
    ::rtidb::api::AppendEntriesResponse response;
    bool ret = rpc_client_.SendRequest(&::rtidb::api::TabletServer_Stub::AppendEntries,
                        &request, &response, 12, 1);
    if (ret && response.code() == 0) {
        last_sync_offset_ = response.log_offset();
        log_matched_ = true;
        log_reader_.SetOffset(last_sync_offset_);
        PDLOG(INFO, "match node %s log offset %lld for table tid %d pid %d",
                  endpoint_.c_str(), last_sync_offset_, tid_, pid_);
        return 0;
    }
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

}
}
