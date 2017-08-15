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
using ::baidu::common::FATAL;

ReplicateNode::ReplicateNode(const std::string& point, LogParts* logs, const std::string& log_path, 
        uint32_t tid, uint32_t pid):endpoint(point), log_path_(log_path), tid_(tid), pid_(pid) {
    sf = NULL;
    reader = NULL;
    last_sync_offset = 0;
    logs_ = logs;
    log_part_index = -1;
    log_matched = false;
} 

ReplicateNode::~ReplicateNode() {
    delete sf;
    delete reader;
}

::rtidb::base::Status ReplicateNode::ReadNextRecord(
        ::rtidb::base::Slice* record, std::string* buffer) {
    // first read record 
    if (sf == NULL) {
        int32_t new_log_part_index = RollRLogFile();
        if (new_log_part_index == -2) {
            LOG(WARNING, "no log avaliable tid[%u] pid[%u]", tid_, pid_);
            return ::rtidb::base::Status::IOError("no log avaliable");
        }

        if (new_log_part_index < 0) {
            LOG(WARNING, "fail to roll read log tid[%u] pid[%u]", tid_, pid_);
            return ::rtidb::base::Status::IOError("no log avaliable");
        }
        reader = new Reader(sf, NULL, false, 0);
        // when change log part index , reset
        // last_log_byte_offset to 0
        log_part_index = new_log_part_index;
    }
    ::rtidb::base::Status status = reader->ReadRecord(record, buffer);
    if (!status.ok()) {
        LOG(DEBUG, "reach the end of file tid[%u] pid[%u]", tid_, pid_);
        // reache the end of file 
        int32_t new_log_part_index = RollRLogFile();
        // reache the latest log part
        if (new_log_part_index == log_part_index) {
            LOG(DEBUG, "no new log entry tid[%u] pid[%u]", tid_, pid_);
            return status;
        }
        if (new_log_part_index < 0) {
            return status;
        }
        delete reader;
        // roll a new log part file, reset status
        log_part_index = new_log_part_index;
        reader = new Reader(sf, NULL, false, 0);
        status = reader->ReadRecord(record, buffer);
    }
    return status;
}

bool ReplicateNode::IsLogMatched() {
    return log_matched;
}

void ReplicateNode::SetLogMatch(bool log_match) {
    log_matched = log_match;
}

std::string ReplicateNode::GetEndPoint() {
    return endpoint;
}

uint64_t ReplicateNode::GetLastSyncOffset() {
    return last_sync_offset;
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
    if (log_part_index < 0) {
        while (it->Valid()) {
            index ++;
            LogPart* part = it->GetValue();
            LOG(DEBUG, "log with name %s and start offset %lld", part->log_name_.c_str(),
                    part->slog_id_);
            if (part->slog_id_ < last_sync_offset) {
                it->Next();
                last_part = part;
                continue;
            } else if (part->slog_id_ > last_sync_offset) {
                if (last_part && last_part->slog_id_ < last_sync_offset) {
                    part = last_part;
                    index--;
                } else {
                    return -1;
                }
            }
            delete it;
            // open a new log part file
            std::string full_path = log_path_ + "/" + part->log_name_;
            if (OpenSeqFile(full_path) < 0) {
                return -1;
            }
            return index;
        }
        delete it;
        LOG(WARNING, "fail to find log include offset %lld", last_sync_offset);
        return -1;
    } else {
        uint32_t current_index = (uint32_t) log_part_index;
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
    if (sf != NULL) {
        delete sf;
        sf = NULL;
    }
    LOG(INFO, "open log file %s for table tid[%u] pid[%u]", path.c_str(), tid_, pid_);
    sf = ::rtidb::log::NewSeqFile(path, fd);
    return 0;
}

FollowerReplicateNode::FollowerReplicateNode(const std::string& point, LogParts* logs, const std::string& log_path,
        uint32_t tid, uint32_t pid, ::rtidb::RpcClient* rpc_client):ReplicateNode(point, logs, log_path, tid, pid) {
    rpc_client_ = rpc_client;
    replicate_node_mode_ = FOLLOWER_REPLICATE_MODE;
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
    ::rtidb::api::AppendEntriesResponse response;
    bool ret = rpc_client_->SendRequest(stub, 
                                 &::rtidb::api::TabletServer_Stub::AppendEntries,
                                 &request, &response, 12, 1);
    if (ret && response.code() == 0) {
        last_sync_offset = response.log_offset();
        log_matched = true;
        LOG(INFO, "match node %s log offset %lld for table tid %d pid %d",
                  endpoint.c_str(), last_sync_offset, tid_, pid_);
        return 0;
    }
    return -1;
}


int FollowerReplicateNode::SyncData(uint64_t log_offset) {
    if (!rpc_client_) {
        LOG(FATAL, "rpc_client is NULL! tid[%u] pid[%u] endpint[%s]", 
                    tid_, pid_, endpoint.c_str()); 
        return -1;
    }
    ::rtidb::api::TabletServer_Stub* stub;
    if (!rpc_client_->GetStub(endpoint, &stub)) {
        LOG(WARNING, "fail to get rpc stub with endpoint %s", endpoint.c_str());
        return 0;
    }
    LOG(DEBUG, "node[%s] offset[%lu] log offset[%lu]", 
                endpoint.c_str(), last_sync_offset, log_offset);
    ::rtidb::api::AppendEntriesRequest request;
    request.set_tid(tid_);
    request.set_pid(pid_);
    ::rtidb::api::AppendEntriesResponse response;
    request.set_pre_log_index(last_sync_offset);
    uint32_t batchSize = log_offset - last_sync_offset;
    batchSize = std::min(batchSize, (uint32_t)FLAGS_binlog_sync_batch_size);
    uint64_t sync_log_offset = 0;
    bool need_wait = false;
    for (uint64_t i = 0; i < batchSize; i++) {
        std::string buffer;
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = ReadNextRecord(&record, &buffer);
        if (status.ok()) {
            ::rtidb::api::LogEntry* entry = request.add_entries();
            if (!entry->ParseFromString(record.ToString())) {
                LOG(WARNING, "bad protobuf format %s size %ld", ::rtidb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                break;
            }
            LOG(DEBUG, "entry val %s log index %lld", entry->value().c_str(), entry->log_index());
            if (entry->log_index() <= last_sync_offset) {
                LOG(WARNING, "skip duplicate log offset %lld", entry->log_index());
                request.mutable_entries()->RemoveLast();
                continue;
            }
            sync_log_offset = entry->log_index();
        } else if (status.IsWaitRecord()) {
            LOG(WARNING, "got a coffee time for[%s]", endpoint.c_str());
            need_wait = true;
            break;
        } else {
            LOG(WARNING, "fail to get record %s", status.ToString().c_str());
            break;
        }
    }
    if (request.entries_size() > 0) {
        bool ret = rpc_client_->SendRequest(stub, 
                                     &::rtidb::api::TabletServer_Stub::AppendEntries,
                                     &request, &response, 12, 1);
        if (ret && response.code() == 0) {
            LOG(DEBUG, "sync log to node[%s] to offset %lld", endpoint.c_str(), sync_log_offset);
            last_sync_offset = sync_log_offset;
        } else {
            cache.push_back(request);
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
    log_matched = true;
    return 0;
}

int SnapshotReplicateNode::SyncData(uint64_t log_offset) {
    uint32_t batchSize = log_offset - last_sync_offset;
    batchSize = std::min(batchSize, (uint32_t)FLAGS_binlog_sync_batch_size);
    for (uint64_t i = 0; i < batchSize; i++) {
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
            if (entry.log_index() <= last_sync_offset) {
                LOG(WARNING, "skip duplicate log offset %lld", entry.log_index());
                continue;
            }
            snapshot_fun_(record.ToString(), entry.pk().c_str(), entry.log_index(), entry.ts());
            last_sync_offset = entry.log_index();
        } else if (status.IsWaitRecord()) {
            LOG(WARNING, "got a coffee time for[%s]", endpoint.c_str());
            return 1;
        } else {
            LOG(WARNING, "fail to get record %s", status.ToString().c_str());
            break;
        }
    }
    return 0;
}

}
}
