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
DECLARE_bool(binlog_enable_crc);

namespace rtidb {
namespace replica {

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

ReplicateNode::ReplicateNode(const std::string& point, LogParts* logs, const std::string& log_path, 
        uint32_t tid, uint32_t pid):endpoint(point), log_path_(log_path), tid_(tid), pid_(pid) {
    sf_ = NULL;
    reader_ = NULL;
    last_sync_offset_ = 0;
    logs_ = logs;
    log_part_index_ = -1;
    log_matched_ = false;
} 

ReplicateNode::~ReplicateNode() {
    delete sf_;
    delete reader_;
}

void ReplicateNode::GoBackToLastBlock() {
    if (sf_ == NULL || reader_ == NULL) {
        return;
    }
    reader_->GoBackToLastBlock();
}

::rtidb::base::Status ReplicateNode::ReadNextRecord(
        ::rtidb::base::Slice* record, std::string* buffer) {
    // first read record 
    if (sf_ == NULL) {
        int32_t new_log_part_index = RollRLogFile();
        if (new_log_part_index == -2) {
            LOG(WARNING, "no log avaliable tid[%u] pid[%u]", tid_, pid_);
            return ::rtidb::base::Status::IOError("no log avaliable");
        }

        if (new_log_part_index < 0) {
            LOG(WARNING, "fail to roll read log tid[%u] pid[%u]", tid_, pid_);
            return ::rtidb::base::Status::IOError("no log avaliable");
        }
        reader_ = new Reader(sf_, NULL, FLAGS_binlog_enable_crc, 0);
        // when change log part index , reset
        // last_log_byte_offset to 0
        log_part_index_ = new_log_part_index;
    }
    ::rtidb::base::Status status = reader_->ReadRecord(record, buffer);
    if (status.IsEof()) {
        // reache the end of file 
        int32_t new_log_part_index = RollRLogFile();
        LOG(WARNING, "reach the end of file tid[%u] pid[%u]  new index %d  old index %d", tid_, pid_, new_log_part_index,
                log_part_index_);
        // reache the latest log part
        if (new_log_part_index == log_part_index_) {
            LOG(DEBUG, "no new log entry tid[%u] pid[%u]", tid_, pid_);
            return status;
        }
        if (new_log_part_index < 0) {
            return status;
        }
        delete reader_;
        // roll a new log part file, reset status
        log_part_index_ = new_log_part_index;
        reader_ = new Reader(sf_, NULL, FLAGS_binlog_enable_crc, 0);
    }
    return status;
}

bool ReplicateNode::IsLogMatched() {
    return log_matched_;
}

void ReplicateNode::SetLogMatch(bool log_match) {
    log_matched_ = log_match;
}

std::string ReplicateNode::GetEndPoint() {
    return endpoint;
}

uint64_t ReplicateNode::GetLastSyncOffset() {
    return last_sync_offset_;
}

void ReplicateNode::SetLastSyncOffset(uint64_t offset) {
    last_sync_offset_ = offset;
}

int ReplicateNode::RollRLogFile() {
    int32_t index = -1;
    LogParts::Iterator* it = logs_->NewIterator();
    if (logs_->GetSize() <= 0) {
        return -2;
    }
    it->SeekToFirst();
    // use log entry offset to find the log part file
    LogPart* last_part = NULL;
    LogPart* part = NULL;
    if (log_part_index_ < 0) {
        while (it->Valid()) {
            index ++;
            part = it->GetValue();
            LOG(DEBUG, "log with name %s and start offset %lld", part->log_name_.c_str(),
                    part->slog_id_);
            if (part->slog_id_ < last_sync_offset_) {
                it->Next();
                last_part = part;
            } else if (part->slog_id_ > last_sync_offset_) {
                if (last_part && last_part->slog_id_ < last_sync_offset_) {
                    part = last_part;
                    index--;
                    break;
                } else {
                    delete it;
                    LOG(WARNING, "fail to find log include offset [%lu] path[%s]", 
                                 last_sync_offset_, log_path_.c_str());
                    return -1;
                }
            } else {
                break;
            }
        }
        delete it;
        if (part) {
            std::string full_path = log_path_ + "/" + part->log_name_;
            if (OpenSeqFile(full_path) < 0) {
                LOG(WARNING, "OpenSeqFile failed! full path[%s]", full_path.c_str()); 
                return -1;
            }
            return index;
        } else {
            LOG(WARNING, "no log part"); 
            return -1;
        }
    } else {
        uint32_t current_index = (uint32_t)log_part_index_;
        // the latest log part was already opened
        if (current_index == (logs_->GetSize() - 1)) {
            delete it;
            return current_index;
        }
        while (it->Valid()) {
            index ++;
            LogPart* part = it->GetValue();
            // find the next of current index log file part
            if ((uint32_t)index > current_index) {
                delete it;
                // open a new log part file
                std::string full_path = log_path_ + "/" + part->log_name_;
                if (OpenSeqFile(full_path) < 0) {
                    return -1;
                }
                return index;
            }
            it->Next();
        }
        delete it;
        return -1;
    }

}

int ReplicateNode::OpenSeqFile(const std::string& path) {
    FILE* fd = fopen(path.c_str(), "rb");
    if (fd == NULL) {
        LOG(WARNING, "fail to open file %s", path.c_str());
        return -1;
    }
    // close old Sequentialfile 
    if (sf_ != NULL) {
        delete sf_;
        sf_ = NULL;
    }
    LOG(INFO, "open log file %s for table tid[%u] pid[%u]", path.c_str(), tid_, pid_);
    sf_ = ::rtidb::log::NewSeqFile(path, fd);
    return 0;
}

FollowerReplicateNode::FollowerReplicateNode(const std::string& point, LogParts* logs, const std::string& log_path,
        uint32_t tid, uint32_t pid, ::rtidb::RpcClient* rpc_client):ReplicateNode(point, logs, log_path, tid, pid) {
    rpc_client_ = rpc_client;
    replicate_node_mode_ = FOLLOWER_REPLICATE_MODE;
    task_status_ = REPLICATE_UNDEFINED;
}

int FollowerReplicateNode::MatchLogOffsetFromNode() {
    ::rtidb::api::TabletServer_Stub* stub;
    if (!rpc_client_->GetStub(endpoint, &stub)) {
        LOG(WARNING, "fail to get rpc stub with endpoint %s", endpoint.c_str());
        return -1;
    }
    ::rtidb::api::AppendEntriesRequest request;
    request.set_tid(tid_);
    request.set_pid(pid_);
    request.set_pre_log_index(0);
    ::rtidb::api::AppendEntriesResponse response;
    bool ret = rpc_client_->SendRequest(stub, 
                                 &::rtidb::api::TabletServer_Stub::AppendEntries,
                                 &request, &response, 12, 1);
    delete stub;                             
    if (ret && response.code() == 0) {
        last_sync_offset_ = response.log_offset();
        log_matched_ = true;
        LOG(INFO, "match node %s log offset %lld for table tid %d pid %d",
                  endpoint.c_str(), last_sync_offset_, tid_, pid_);
        task_status_ = REPLICATE_RUNNING;
        return 0;
    }
    return -1;
}


int FollowerReplicateNode::SyncData(uint64_t log_offset) {
    if (!rpc_client_) {
        LOG(WARNING, "rpc_client is NULL! tid[%u] pid[%u] endpint[%s]", 
                    tid_, pid_, endpoint.c_str()); 
        return -1;
    }
    LOG(DEBUG, "node[%s] offset[%lu] log offset[%lu]", 
                endpoint.c_str(), last_sync_offset_, log_offset);
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
            LOG(WARNING, "empty append entry request from node %s cache", endpoint.c_str());
            return -1;
        }
        const ::rtidb::api::LogEntry& entry = request.entries(request.entries_size() - 1);
        if (entry.log_index() <= last_sync_offset_) {
            LOG(DEBUG, "duplicate log index from node %s cache", endpoint.c_str());
            cache_.clear();
            return -1;
        }
        LOG(INFO, "use cached request to send last index  %lu", entry.log_index());
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
            ::rtidb::base::Status status = ReadNextRecord(&record, &buffer);
            if (status.ok()) {
                ::rtidb::api::LogEntry* entry = request.add_entries();
                if (!entry->ParseFromString(record.ToString())) {
                    LOG(WARNING, "bad protobuf format %s size %ld", ::rtidb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                    request.mutable_entries()->RemoveLast();
                    break;
                }
                LOG(DEBUG, "entry val %s log index %lld", entry->value().c_str(), entry->log_index());
                if (entry->log_index() <= last_sync_offset_) {
                    LOG(DEBUG, "skip duplicate log offset %lld", entry->log_index());
                    request.mutable_entries()->RemoveLast();
                    continue;
                }
                // the log index should incr by 1
                if ((sync_log_offset + 1) != entry->log_index()) {
                    LOG(WARNING, "log missing expect offset %lu but %ld", sync_log_offset + 1, entry->log_index());
                    request.mutable_entries()->RemoveLast();
                    need_wait = true;
                    GoBackToLastBlock();
                    break;
                }
                sync_log_offset = entry->log_index();
            } else if (status.IsWaitRecord()) {
                LOG(DEBUG, "got a coffee time for[%s]", endpoint.c_str());
                need_wait = true;
                break;
            } else {
                LOG(WARNING, "fail to get record %s current log part index %d", status.ToString().c_str(), log_part_index_);
                need_wait = true;
                break;
            }
            i++;
        }
    }    
    if (request.entries_size() > 0) {
        ::rtidb::api::TabletServer_Stub* stub;
        if (!rpc_client_->GetStub(endpoint, &stub)) {
            LOG(WARNING, "fail to get rpc stub with endpoint %s", endpoint.c_str());
            return 0;
        }
        bool ret = rpc_client_->SendRequest(stub, 
                                     &::rtidb::api::TabletServer_Stub::AppendEntries,
                                     &request, &response, 12, 1);
        delete stub;
        if (ret && response.code() == 0) {
            LOG(DEBUG, "sync log to node[%s] to offset %lld", endpoint.c_str(), sync_log_offset);
            last_sync_offset_ = sync_log_offset;
            if (request_from_cache) {
                cache_.clear(); 
            }
        } else {
            if (!request_from_cache) {
                cache_.push_back(request);
            }
            need_wait = true;
            LOG(WARNING, "fail to sync log to node %s", endpoint.c_str());
        }
    }
    if (need_wait) {
        return 1;
    }
    return 0;
}

SnapshotReplicateNode::SnapshotReplicateNode(const std::string& point, LogParts* logs, const std::string& log_path, 
        uint32_t tid, uint32_t pid, SnapshotFunc snapshot_fun):ReplicateNode(point, logs, log_path, tid, pid) {
    replicate_node_mode_ = SNAPSHOT_REPLICATE_MODE;
    snapshot_fun_ = snapshot_fun;
}

int SnapshotReplicateNode::MatchLogOffsetFromNode() {
    log_matched_ = true;
    return 0;
}

int SnapshotReplicateNode::SyncData(uint64_t log_offset) {
    uint32_t batchSize = log_offset - last_sync_offset_;
    batchSize = std::min(batchSize, (uint32_t)FLAGS_binlog_sync_batch_size);
    for (uint64_t i = 0; i < batchSize; ) {
        std::string buffer;
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::rtidb::api::LogEntry entry;
            if (!entry.ParseFromString(record.ToString())) {
                LOG(WARNING, "bad protobuf format %s size %ld", 
                            ::rtidb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                break;
            }
            LOG(DEBUG, "entry val %s log index %lld", entry.value().c_str(), entry.log_index());
            if (entry.log_index() <= last_sync_offset_) {
                LOG(DEBUG, "skip duplicate log offset %lld", entry.log_index());
                continue;
            }

            snapshot_fun_(record.ToString(), entry.pk().c_str(), entry.log_index(), entry.ts());
            last_sync_offset_ = entry.log_index();
        } else if (status.IsWaitRecord()) {
            LOG(DEBUG, "got a coffee time for[%s]", endpoint.c_str());
            return 1;
        } else {
            LOG(WARNING, "fail to get record %s", status.ToString().c_str());
            return 1;
        }
        i++;
    }
    return 0;
}

}
}
