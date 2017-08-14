//
// log_appender.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-06-07
//

#include "replica/log_replicator.h"

#include "base/file_util.h"
#include "base/strings.h"
#include "leveldb/options.h"
#include "logging.h"
#include <boost/ref.hpp>
#include <cstring>
#include <gflags/gflags.h>
#include <stdio.h>
#include <stdlib.h>

DECLARE_int32(binlog_single_file_max_size);
DECLARE_int32(binlog_apply_batch_size);
DECLARE_int32(binlog_coffee_time);
DECLARE_int32(binlog_sync_wait_time);
DECLARE_int32(binlog_sync_to_disk_interval);
DECLARE_int32(binlog_match_logoffset_interval);

namespace rtidb {
namespace replica {

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

const static StringComparator scmp;

LogReplicator::LogReplicator(const std::string& path,
                             const std::vector<std::string>& endpoints,
                             const ReplicatorRole& role,
                             Table* table,
                             SnapshotFunc ssf):path_(path), log_path_(),
    log_offset_(0), logs_(NULL), wh_(NULL), wsize_(0), role_(role), 
    endpoints_(endpoints), nodes_(), mu_(), cv_(&mu_),rpc_client_(NULL),
    running_(true), tp_(4), refs_(0), wmu_(),
    ssf_(ssf) {
    table_ = table;
    table_->Ref();
}

LogReplicator::~LogReplicator() {
    if (logs_ != NULL) {
        LogParts::Iterator* it = logs_->NewIterator();
        it->SeekToFirst();
        while (it->Valid()) {
            LogPart* lp = it->GetValue();
            delete lp;
            it->Next();
        }
        delete it;
        logs_->Clear();
    }
    delete logs_;
    logs_ = NULL;
    delete wh_;
    wh_ = NULL;
    std::vector<ReplicateNode*>::iterator nit = nodes_.begin();
    for (; nit != nodes_.end(); ++nit) {
        delete (*nit);
    }
    delete rpc_client_;
    table_->UnRef();
}

void LogReplicator::SyncToDisk() {
    MutexLock lock(&wmu_);
    if (wh_ != NULL) {
        uint64_t consumed = ::baidu::common::timer::get_micros();
        ::rtidb::base::Status status = wh_->Sync();
        if (!status.ok()) {
            LOG(WARNING, "fail to sync data for path %s", path_.c_str());
        }
        consumed = ::baidu::common::timer::get_micros() - consumed;
        LOG(INFO, "sync to disk for path %s consumed %lld ms", path_.c_str(), consumed / 1000);
    }
    tp_.DelayTask(FLAGS_binlog_sync_to_disk_interval, boost::bind(&LogReplicator::SyncToDisk, this));
}

bool LogReplicator::Init() {
    rpc_client_ = new ::rtidb::RpcClient();
    logs_ = new LogParts(12, 4, scmp);
    log_path_ = path_ + "/logs/";
    if (!::rtidb::base::MkdirRecur(log_path_)) {
       LOG(WARNING, "fail to log dir %s", log_path_.c_str());
       return false;
    }
    nodes_.push_back(new SnapshotReplicateNode("snapshot_replicate_node", logs_, log_path_, table_->GetId(), table_->GetPid(), ssf_));
    if (role_ == kLeaderNode) {
        std::vector<std::string>::iterator it = endpoints_.begin();
        for (; it != endpoints_.end(); ++it) {
            ReplicateNode* node = new FollowerReplicateNode(*it, logs_, log_path_, table_->GetId(), table_->GetPid(), rpc_client_);
            nodes_.push_back(node);
            LOG(INFO, "add replica node with endpoint %s", it->c_str());
        }
        LOG(INFO, "init leader node for path %s ok", path_.c_str());
    }
    tp_.DelayTask(FLAGS_binlog_sync_to_disk_interval, boost::bind(&LogReplicator::SyncToDisk, this));
    return true;
}

void LogReplicator::SetOffset(uint64_t offset) {
    log_offset_.store(offset, boost::memory_order_relaxed);
}

uint64_t LogReplicator::GetOffset() {
    return log_offset_.load(boost::memory_order_relaxed);
}

void LogReplicator::Ref() {
    refs_.fetch_add(1, boost::memory_order_relaxed);
}

void LogReplicator::UnRef() {
    refs_.fetch_sub(1, boost::memory_order_acquire);
    if (refs_.load(boost::memory_order_relaxed) <= 0) {
        delete this;
    }
}

bool LogReplicator::AppendEntries(const ::rtidb::api::AppendEntriesRequest* request,
        ::rtidb::api::AppendEntriesResponse* response) {
    MutexLock lock(&wmu_);
    if (wh_ == NULL || (wsize_ / (1024* 1024)) > (uint32_t)FLAGS_binlog_single_file_max_size) {
        bool ok = RollWLogFile();
        if (!ok) {
            LOG(WARNING, "fail to roll write log for path %s", path_.c_str());
            return false;
        }
    }
    uint64_t last_log_offset = GetOffset();
    if (request->pre_log_index() !=  last_log_offset) {
        LOG(WARNING, "log mismatch for path %s, pre_log_index %lld, come log index %lld", path_.c_str(),
                last_log_offset, request->pre_log_index());
        response->set_log_offset(last_log_offset);
        return false;
    }
    for (int32_t i = 0; i < request->entries_size(); i++) {
        std::string buffer;
        request->entries(i).SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer.c_str(), buffer.size());
        ::rtidb::base::Status status = wh_->Write(slice);
        if (!status.ok()) {
            LOG(WARNING, "fail to write replication log in dir %s for %s", path_.c_str(), status.ToString().c_str());
            return false;
        }
        wsize_ += buffer.size();
        //ssf_(buffer, request->entries(i).pk(), request->entries(i).log_index(), request->entries(i).ts());
        table_->Put(request->entries(i).pk(), request->entries(i).ts(), 
                request->entries(i).value().c_str(), request->entries(i).value().length());
        log_offset_.store(request->entries(i).log_index(), boost::memory_order_relaxed);
        response->set_log_offset(GetOffset());
    }
    LOG(DEBUG, "sync log entry to offset %lld for %s", GetOffset(), path_.c_str());
    return true;
}

bool LogReplicator::AddReplicateNode(const std::string& endpoint) {
    {
        MutexLock lock(&mu_);
        if (role_ != kLeaderNode) {
            return false;
        }
        std::vector<ReplicateNode*>::iterator it = nodes_.begin();
        for (; it != nodes_.end(); ++it) {
            std::string ep = (*it)->GetEndPoint();
            if (ep.compare(endpoint) == 0) {
                LOG(WARNING, "replica endpoint %s does exist", ep.c_str());
                return false;
            }
        }
        ReplicateNode* node = new FollowerReplicateNode(endpoint, logs_, log_path_, table_->GetId(), table_->GetPid(), rpc_client_);
        nodes_.push_back(node);
        LOG(INFO, "add ReplicateNode with endpoint %s ok", endpoint.c_str());
    }
    tp_.DelayTask(FLAGS_binlog_match_logoffset_interval, boost::bind(&LogReplicator::MatchLogOffset, this));
    return true;
}

bool LogReplicator::AppendEntry(::rtidb::api::LogEntry& entry) {
    MutexLock lock(&wmu_);
    if (wh_ == NULL || wsize_ / (1024 * 1024) > (uint32_t)FLAGS_binlog_single_file_max_size) {
        bool ok = RollWLogFile();
        if (!ok) {
            return false;
        }
    }
    entry.set_log_index(1 + log_offset_.fetch_add(1, boost::memory_order_relaxed));
    std::string buffer;
    entry.SerializeToString(&buffer);
    ::rtidb::base::Slice slice(buffer);
    ::rtidb::base::Status status = wh_->Write(slice);
    if (!status.ok()) {
        LOG(WARNING, "fail to write replication log in dir %s for %s", path_.c_str(), status.ToString().c_str());
        return false;
    }
    wsize_ += buffer.size();
    //TODO handle fails
    //ssf_(buffer, entry.pk(), entry.log_index(), entry.ts());
    LOG(DEBUG, "entry index %lld, log offset %lld", entry.log_index(), log_offset_.load(boost::memory_order_relaxed));
    return true;
}

bool LogReplicator::RollWLogFile() {
    if (wh_ != NULL) {
        wh_->EndLog();
        delete wh_;
        wh_ = NULL;
    }
    std::string name = ::rtidb::base::FormatToString(logs_->GetSize(), 8) + ".log";
    std::string full_path = log_path_ + "/" + name;
    FILE* fd = fopen(full_path.c_str(), "ab+");
    if (fd == NULL) {
        LOG(WARNING, "fail to create file %s", full_path.c_str());
        return false;
    }
    uint64_t offset = log_offset_.load(boost::memory_order_relaxed);
    LogPart* part = new LogPart(offset, name);
    logs_->Insert(name, part);
    LOG(INFO, "roll write log for name %s and start offset %lld", name.c_str(), part->slog_id_);
    wh_ = new WriteHandle(name, fd);
    wsize_ = 0;
    return true;
}

void LogReplicator::Notify() {
    MutexLock lock(&mu_);
    cv_.Broadcast();
}


void LogReplicator::MatchLogOffset() {
    MutexLock lock(&mu_);
    bool all_matched = true;
    std::vector<ReplicateNode*>::iterator it = nodes_.begin();
    for (; it != nodes_.end(); ++it) {
        ReplicateNode* node = *it;
        if (node->IsLogMatched()) {
            continue;
        }
        if (node->GetMode() == SNAPSHOTREPLICATEMODE && (table_->GetTableStat() == ::rtidb::storage::TSPAUSING
                || table_->GetTableStat() == ::rtidb::storage::TSPAUSED)) {
            all_matched = false;
            continue;
        }
        if (node->MatchLogOffsetFromNode() < 0) {
            all_matched = false;
        } else {
            tp_.AddTask(boost::bind(&LogReplicator::ReplicateToNode, this, node));
        }
    }
    if (!all_matched) {
        // retry after 1 second
        tp_.DelayTask(1000, boost::bind(&LogReplicator::MatchLogOffset, this));
    }
}

void LogReplicator::ReplicateToNode(ReplicateNode* node) {
    uint32_t coffee_time = 0;
    while (running_.load(boost::memory_order_relaxed)) {
        MutexLock lock(&mu_);
        if (coffee_time > 0) {
            cv_.TimeWait(coffee_time);
            coffee_time = 0;
        }
        if (node->GetMode() == SNAPSHOTREPLICATEMODE && table_->GetTableStat() == ::rtidb::storage::TSPAUSING) {
            table_->SetTableStat(::rtidb::storage::TSPAUSED);
            node->SetLogMatch(false);
            break;
        }
        while (node->GetLastSyncOffset() >= (log_offset_.load(boost::memory_order_relaxed))) {
            cv_.TimeWait(FLAGS_binlog_sync_wait_time);
            if (!running_.load(boost::memory_order_relaxed)) {
                LOG(INFO, "replicate log exist for path %s", path_.c_str());
                return;
            }
        }
        int ret = node->SyncData(log_offset_.load(boost::memory_order_relaxed));
        if (ret == 1) {
            coffee_time = FLAGS_binlog_coffee_time;
        }
    }
}

void LogReplicator::Stop() {
    running_.store(false, boost::memory_order_relaxed);
    // wait all task to shutdown
    tp_.Stop(true);
    LOG(INFO, "stop replicator for path %s ok", path_.c_str());
}

} // end of replica
} // end of ritdb
