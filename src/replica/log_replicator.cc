//
// log_appender.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-06-07
//

#include "replica/log_replicator.h"

#include "base/file_util.h"
#include "base/strings.h"
#include "log/log_format.h"
#include "logging.h"
#include <boost/ref.hpp>
#include <boost/lexical_cast.hpp>
#include <cstring>
#include <gflags/gflags.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <algorithm> 

DECLARE_int32(binlog_single_file_max_size);
DECLARE_int32(binlog_apply_batch_size);
DECLARE_int32(binlog_coffee_time);
DECLARE_int32(binlog_sync_wait_time);
DECLARE_int32(binlog_sync_to_disk_interval);
DECLARE_int32(binlog_match_logoffset_interval);
DECLARE_int32(binlog_delete_interval);
DECLARE_int32(binlog_name_length);

namespace rtidb {
namespace replica {

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

const static ::rtidb::base::DefaultComparator scmp;

LogReplicator::LogReplicator(const std::string& path,
                             const std::vector<std::string>& endpoints,
                             const ReplicatorRole& role,
                             std::shared_ptr<Table> table):path_(path), log_path_(),
    log_offset_(0), logs_(NULL), wh_(NULL), role_(role), 
    endpoints_(endpoints), nodes_(), mu_(), cv_(&mu_),coffee_cv_(&mu_),
    rpc_client_(NULL),
    running_(true), tp_(4), refs_(0), wmu_() {
    table_ = table;
    binlog_index_ = 0;
    snapshot_log_part_index_.store(-1, boost::memory_order_relaxed);;
}

LogReplicator::~LogReplicator() {
    if (logs_ != NULL) {
        logs_->Clear();
    }
    delete logs_;
    logs_ = NULL;
    delete wh_;
    wh_ = NULL;
    nodes_.clear();
    delete rpc_client_;
}

void LogReplicator::SetRole(const ReplicatorRole& role) {
    role_ = role;
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
    log_path_ = path_ + "/binlog/";
    if (!::rtidb::base::MkdirRecur(log_path_)) {
       LOG(WARNING, "fail to log dir %s", log_path_.c_str());
       return false;
    }
    if (role_ == kLeaderNode) {
        std::vector<std::string>::iterator it = endpoints_.begin();
        for (; it != endpoints_.end(); ++it) {
            nodes_.push_back(std::shared_ptr<ReplicateNode>(
                    new ReplicateNode(*it, logs_, log_path_, table_->GetId(), table_->GetPid(), rpc_client_)));
            LOG(INFO, "add replica node with endpoint %s", it->c_str());
        }
        LOG(INFO, "init leader node for path %s ok", path_.c_str());
    }
    if (!Recover()) {
        return false;
    }
    tp_.DelayTask(FLAGS_binlog_sync_to_disk_interval, boost::bind(&LogReplicator::SyncToDisk, this));
    tp_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&LogReplicator::DeleteBinlog, this));
    return true;
}

bool LogReplicator::ParseBinlogIndex(const std::string& path, uint32_t& index) {
    if (path.size() <= 4 
        || path.substr(path.length() - 4, 4) != ".log" ) {
        LOG(WARNING, "invalid log name %s", path.c_str());
        return false;
    }
    size_t rindex = path.rfind('/');
    if (rindex == std::string::npos) {
        rindex = 0;
    }else {
        rindex += 1;
    }
    std::string name = path.substr(rindex, path.length() - rindex - 4);
    size_t no_zero_index = 0;
    for (size_t i = 0; i < name.length(); i++) {
        no_zero_index = i;
        if (name[no_zero_index] != '0') {
            break;
        }
    }
    std::string num = name.substr(no_zero_index, name.length());
    if (num.length() == 0) {
        index = 0;
        return true;
    }
    bool ok = ::rtidb::base::IsNumber(num);
    if (!ok) {
        LOG(WARNING, "fail to parse binlog index from name %s, num %s", name.c_str(), num.c_str());
        return false;
    }
    index = boost::lexical_cast<uint32_t>(num);
    return true;
}

bool LogReplicator::Recover() {
    std::vector<std::string> logs;
    int ret = ::rtidb::base::GetFileName(log_path_, logs);
    if (ret != 0) {
        LOG(WARNING, "fail to get binlog log list for tid %u pid %u", table_->GetId(), table_->GetPid());
        return false;
    }
    if (logs.size() <= 0) {
        return true;
    }
    std::sort(logs.begin(), logs.end());
    std::string buffer;
    ::rtidb::api::LogEntry entry;
    for (uint32_t i = 0; i < logs.size(); i++) {
        std::string& full_path = logs[i];
        uint32_t binlog_index = 0;
        bool ok = ParseBinlogIndex(full_path, binlog_index);
        if (!ok) {
            break;
        }
        FILE* fd = fopen(full_path.c_str(), "rb+");
        if (fd == NULL) {
            LOG(WARNING, "fail to open path %s for error %s", full_path.c_str(), strerror(errno));
            break;
        }
        ::rtidb::log::SequentialFile* seq_file = ::rtidb::log::NewSeqFile(full_path, fd);
        ::rtidb::log::Reader reader(seq_file, NULL, false, 0);
        ::rtidb::base::Slice record;
        ::rtidb::base::Status status = reader.ReadRecord(&record, &buffer);
        delete seq_file;
        if (!status.ok()) {
            LOG(WARNING, "fail to get offset from file %s", full_path.c_str());
            return false;
        }
        ok = entry.ParseFromString(record.ToString());
        if (!ok) {
            LOG(WARNING, "fail to parse log entry %s ", 
                    ::rtidb::base::DebugString(record.ToString()).c_str());
            return false;
        }
        if (entry.log_index() <= 0) {
            LOG(WARNING, "invalid entry offset %lu ", entry.log_index());
            return false;
        }
        uint64_t offset = entry.log_index();
        if (offset > 0) {
            offset -= 1;
        }
        logs_->Insert(binlog_index, offset);
        LOG(INFO, "recover binlog index %u and offset %lu from path %s",
                binlog_index, entry.log_index(), full_path.c_str());
        binlog_index_.store(binlog_index + 1, boost::memory_order_relaxed);
    }
    return true;
}

LogParts* LogReplicator::GetLogPart() {
    return logs_;
}

void LogReplicator::SetOffset(uint64_t offset) {
    log_offset_.store(offset, boost::memory_order_relaxed);
}

uint64_t LogReplicator::GetOffset() {
    return log_offset_.load(boost::memory_order_relaxed);
}

void LogReplicator::SetSnapshotLogPartIndex(uint64_t offset) {
    ::rtidb::log::LogReader log_reader(logs_, log_path_);
    log_reader.SetOffset(offset);
    int log_part_index = log_reader.RollRLogFile();
    snapshot_log_part_index_.store(log_part_index, boost::memory_order_relaxed);
}

void LogReplicator::DeleteBinlog() {
    if (!running_.load(boost::memory_order_relaxed)) {
        return;
    }
    if (logs_->GetSize() <= 1) {
        LOG(DEBUG, "log part size is one or less, need not delete"); 
        tp_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&LogReplicator::DeleteBinlog, this));
        return;
    }
    int min_log_index = snapshot_log_part_index_.load(boost::memory_order_relaxed);
    {
        MutexLock lock(&mu_);
        for (auto iter = nodes_.begin(); iter != nodes_.end(); ++iter) {
            if ((*iter)->GetLogIndex() < min_log_index) {
                min_log_index = (*iter)->GetLogIndex();
            }
        }
    }
    min_log_index--;
    if (min_log_index < 0) {
        LOG(DEBUG, "min_log_index is[%d], need not delete!", min_log_index);
        tp_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&LogReplicator::DeleteBinlog, this));
        return;
    }

    LOG(DEBUG, "min_log_index[%d] cur binlog_index[%u]", 
                min_log_index, binlog_index_.load(boost::memory_order_relaxed));
    ::rtidb::base::Node<uint32_t, uint64_t>* node = NULL;
    {
        MutexLock lock(&wmu_);
        node = logs_->Split(min_log_index);
    }

    while (node) {
        ::rtidb::base::Node<uint32_t, uint64_t>* tmp_node = node;
        node = node->GetNextNoBarrier(0);
        std::string full_path = log_path_ + "/" + 
                ::rtidb::base::FormatToString(tmp_node->GetKey(), FLAGS_binlog_name_length) + ".log";
        if (unlink(full_path.c_str()) < 0) {
            LOG(WARNING, "delete binlog[%s] failed! errno[%d] errinfo[%s]", 
                         full_path.c_str(), errno, strerror(errno));
        } else {
            LOG(INFO, "delete binlog[%s] success", full_path.c_str()); 
        }
        delete tmp_node;
    }
    tp_.DelayTask(FLAGS_binlog_delete_interval, boost::bind(&LogReplicator::DeleteBinlog, this));
}

bool LogReplicator::AppendEntries(const ::rtidb::api::AppendEntriesRequest* request,
        ::rtidb::api::AppendEntriesResponse* response) {
    MutexLock lock(&wmu_);
    uint64_t last_log_offset = GetOffset();
    if (request->pre_log_index() == 0 && request->entries_size() == 0) {
        LOG(INFO, "first sync log_index! log_offset[%lu] tid[%u] pid[%u]", 
                    last_log_offset, table_->GetId(), table_->GetPid());
        response->set_log_offset(last_log_offset);
        return true;
    }
    if (wh_ == NULL || (wh_->GetSize() / (1024* 1024)) > (uint32_t)FLAGS_binlog_single_file_max_size) {
        bool ok = RollWLogFile();
        if (!ok) {
            LOG(WARNING, "fail to roll write log for path %s", path_.c_str());
            return false;
        }
    }
    if (request->pre_log_index() > last_log_offset) {
        LOG(WARNING, "log mismatch for path %s, pre_log_index %lld, come log index %lld", path_.c_str(),
                last_log_offset, request->pre_log_index());
        return false;
    }
    for (int32_t i = 0; i < request->entries_size(); i++) {
        if (request->entries(i).log_index() <= last_log_offset) {
            LOG(WARNING, "entry log_index %lu cur log_offset %lu", 
                          request->entries(i).log_index(), last_log_offset);
            continue;
        }
        std::string buffer;
        request->entries(i).SerializeToString(&buffer);
        ::rtidb::base::Slice slice(buffer.c_str(), buffer.size());
        ::rtidb::base::Status status = wh_->Write(slice);
        if (!status.ok()) {
            LOG(WARNING, "fail to write replication log in dir %s for %s", path_.c_str(), status.ToString().c_str());
            return false;
        }
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
            LOG(WARNING, "cur table is not leader, cannot add replicate");
            return false;
        }
        std::vector<std::shared_ptr<ReplicateNode> >::iterator it = nodes_.begin();
        for (; it != nodes_.end(); ++it) {
            std::string ep = (*it)->GetEndPoint();
            if (ep.compare(endpoint) == 0) {
                LOG(WARNING, "replica endpoint %s does exist", ep.c_str());
                return false;
            }
        }
        nodes_.push_back(std::shared_ptr<ReplicateNode>(
                    new ReplicateNode(endpoint, logs_, log_path_, table_->GetId(), table_->GetPid(), rpc_client_)));
        endpoints_.push_back(endpoint);
        LOG(INFO, "add ReplicateNode with endpoint %s ok. tid[%u] pid[%u]",
                    endpoint.c_str(), table_->GetId(), table_->GetPid());
    }
    tp_.DelayTask(FLAGS_binlog_match_logoffset_interval, boost::bind(&LogReplicator::MatchLogOffset, this));
    return true;
}

bool LogReplicator::DelReplicateNode(const std::string& endpoint) {
    {
        MutexLock lock(&mu_);
        if (role_ != kLeaderNode) {
            LOG(WARNING, "cur table is not leader, cannot delete replicate");
            return false;
        }
        std::vector<std::shared_ptr<ReplicateNode> >::iterator it = nodes_.begin();
        for (; it != nodes_.end(); ++it) {
            if ((*it)->GetEndPoint().compare(endpoint) == 0) {
                break;
            }
        }
        if (it == nodes_.end()) {
            LOG(WARNING, "replica endpoint[%s] does not exist", endpoint.c_str());
            return false;
        }
        nodes_.erase(it);
        endpoints_.erase(std::remove(endpoints_.begin(), endpoints_.end(), endpoint), endpoints_.end());
        LOG(INFO, "delete replica. endpoint[%s] tid[%u] pid[%u]", 
                    endpoint.c_str(), table_->GetId(), table_->GetPid());
    }
    return true;
}

bool LogReplicator::AppendEntry(::rtidb::api::LogEntry& entry) {
    MutexLock lock(&wmu_);
    if (wh_ == NULL || wh_->GetSize() / (1024 * 1024) > (uint32_t)FLAGS_binlog_single_file_max_size) {
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
    // add record header size
    LOG(DEBUG, "entry index %lld, log offset %lld", entry.log_index(), log_offset_.load(boost::memory_order_relaxed));
    return true;
}

bool LogReplicator::RollWLogFile() {
    wmu_.AssertHeld();
    if (wh_ != NULL) {
        wh_->EndLog();
        delete wh_;
        wh_ = NULL;
    }
    std::string name = ::rtidb::base::FormatToString(
                binlog_index_.load(boost::memory_order_relaxed), FLAGS_binlog_name_length) + ".log";
    std::string full_path = log_path_ + "/" + name;
    FILE* fd = fopen(full_path.c_str(), "ab+");
    if (fd == NULL) {
        LOG(WARNING, "fail to create file %s", full_path.c_str());
        return false;
    }
    uint64_t offset = log_offset_.load(boost::memory_order_relaxed);
    logs_->Insert(binlog_index_.load(boost::memory_order_relaxed), offset);
    binlog_index_.fetch_add(1, boost::memory_order_relaxed);
    LOG(INFO, "roll write log for name %s and start offset %lld", name.c_str(), offset);
    wh_ = new WriteHandle(name, fd);
    return true;
}

void LogReplicator::Notify() {
    MutexLock lock(&mu_);
    cv_.Broadcast();
}


void LogReplicator::MatchLogOffset() {
    MutexLock lock(&mu_);
    bool all_matched = true;
    std::vector<std::shared_ptr<ReplicateNode> >::iterator it = nodes_.begin();
    for (; it != nodes_.end(); ++it) {
        std::shared_ptr<ReplicateNode> node = *it;
        if (node->IsLogMatched()) {
            continue;
        }
        if (node->MatchLogOffsetFromNode() < 0) {
            all_matched = false;
        } else {
            tp_.AddTask(boost::bind(&LogReplicator::ReplicateToNode, this, node->GetEndPoint()));
        }
    }
    if (!all_matched) {
        // retry after 1 second
        tp_.DelayTask(FLAGS_binlog_match_logoffset_interval, boost::bind(&LogReplicator::MatchLogOffset, this));
    }
}

void LogReplicator::ReplicateToNode(const std::string& endpoint) {
    uint32_t coffee_time = 0;
    while (running_.load(boost::memory_order_relaxed)) {
        std::shared_ptr<ReplicateNode> node;
        {
            MutexLock lock(&mu_);
            std::vector<std::shared_ptr<ReplicateNode> >::iterator it = nodes_.begin();
            for ( ; it != nodes_.end(); ++it) {
                if ((*it)->GetEndPoint().compare(endpoint) == 0) {
                    node = *it;
                    break;
                }
            }
            if (it == nodes_.end()) {
                LOG(INFO, "replicate node[%s] has deleted. task exit!", endpoint.c_str());
                return;
            }
            if (coffee_time > 0) {
                coffee_cv_.TimeWait(coffee_time);
                coffee_time = 0;
            }
        }
        int ret = node->SyncData(log_offset_.load(boost::memory_order_relaxed));
        if (ret == 1) {
            coffee_time = FLAGS_binlog_coffee_time;
        }
        MutexLock lock(&mu_);
        while (node->GetLastSyncOffset() >= (log_offset_.load(boost::memory_order_relaxed))) {
            cv_.TimeWait(FLAGS_binlog_sync_wait_time);
            if (!running_.load(boost::memory_order_relaxed)) {
                LOG(INFO, "replicate log exist for path %s", path_.c_str());
                return;
            }
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
