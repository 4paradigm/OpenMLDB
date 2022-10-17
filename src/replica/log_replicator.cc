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

#include "replica/log_replicator.h"

#include <errno.h>
#include <gflags/gflags.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <algorithm>
#include <chrono>  // NOLINT
#include <cstring>
#include <utility>

#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "base/strings.h"
#include "log/log_format.h"
#include "storage/segment.h"

DECLARE_int32(binlog_single_file_max_size);
DECLARE_int32(binlog_name_length);
DECLARE_string(zk_cluster);

namespace openmldb {
namespace replica {

static const ::openmldb::base::DefaultComparator scmp;

LogReplicator::LogReplicator(uint32_t tid, uint32_t pid, const std::string& path,
                             const std::map<std::string, std::string>& real_ep_map,
                             const ReplicatorRole& role)
    : tid_(tid),
      pid_(pid),
      path_(path),
      log_path_(),
      log_offset_(0),
      logs_(NULL),
      wh_(NULL),
      role_(role),
      real_ep_map_(real_ep_map),
      nodes_(),
      local_endpoints_(),
      term_(0),
      mu_(),
      cv_(),
      wmu_() {
    binlog_index_ = 0;
    snapshot_log_part_index_.store(-1, std::memory_order_relaxed);
    snapshot_last_offset_.store(0, std::memory_order_relaxed);
    follower_offset_.store(0);
}

LogReplicator::~LogReplicator() {
    DelAllReplicateNode();
    if (logs_ != NULL) {
        logs_->Clear();
    }
    delete logs_;
    logs_ = NULL;
    delete wh_;
    wh_ = NULL;
    nodes_.clear();
}

void LogReplicator::SetRole(const ReplicatorRole& role) {
    std::lock_guard<bthread::Mutex> lock(mu_);
    role_ = role;
}

void LogReplicator::SyncToDisk() {
    std::lock_guard<std::mutex> lock(wmu_);
    if (wh_ != NULL) {
        uint64_t consumed = ::baidu::common::timer::get_micros();
        ::openmldb::log::Status status = wh_->Sync();
        if (!status.ok()) {
            PDLOG(WARNING, "fail to sync data for path %s", path_.c_str());
        }
        consumed = ::baidu::common::timer::get_micros() - consumed;
        if (consumed > 20000) {
            PDLOG(INFO, "sync to disk for path %s consumed %lld ms", path_.c_str(), consumed / 1000);
        }
    }
}

bool LogReplicator::Init() {
    logs_ = new LogParts(12, 4, scmp);
    log_path_ = path_ + "/binlog/";
    if (!::openmldb::base::MkdirRecur(log_path_)) {
        PDLOG(WARNING, "fail to log dir %s", log_path_.c_str());
        return false;
    }
    if (role_ == kLeaderNode) {
        for (const auto& kv : real_ep_map_) {
            std::shared_ptr<ReplicateNode> replicate_node =
                std::make_shared<ReplicateNode>(kv.first, logs_, log_path_, tid_, pid_, &term_,
                                                &log_offset_, &mu_, &cv_, false, &follower_offset_, kv.second);
            if (replicate_node->Init() < 0) {
                PDLOG(WARNING, "init replicate node %s error", kv.first.c_str());
                return false;
            }
            nodes_.push_back(replicate_node);
            local_endpoints_.push_back(kv.first);
            PDLOG(INFO, "add replica node with endpoint %s", kv.first.c_str());
        }
        PDLOG(INFO, "init leader node for path %s ok", path_.c_str());
    }
    if (!Recover()) {
        return false;
    }
    return true;
}

bool LogReplicator::StartSyncing() {
    std::lock_guard<bthread::Mutex> lock(mu_);
    std::vector<std::shared_ptr<ReplicateNode>>::iterator it = nodes_.begin();
    for (; it != nodes_.end(); ++it) {
        std::shared_ptr<ReplicateNode> node = *it;
        int ok = node->Start();
        if (ok != 0) {
            return false;
        }
    }
    return true;
}

bool LogReplicator::ParseBinlogIndex(const std::string& path, uint32_t& index) {
    if (path.size() <= 4 || path.substr(path.length() - 4, 4) != ".log") {
        PDLOG(WARNING, "invalid log name %s", path.c_str());
        return false;
    }
    size_t rindex = path.rfind('/');
    if (rindex == std::string::npos) {
        rindex = 0;
    } else {
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
    bool ok = ::openmldb::base::IsNumber(num);
    if (!ok) {
        PDLOG(WARNING, "fail to parse binlog index from name %s, num %s", name.c_str(), num.c_str());
        return false;
    }
    index = std::stoul(num);
    return true;
}

bool LogReplicator::Recover() {
    std::vector<std::string> logs;
    int ret = ::openmldb::base::GetFileName(log_path_, logs);
    if (ret != 0) {
        PDLOG(WARNING, "fail to get binlog log list for tid %u pid %u", tid_, pid_);
        return false;
    }
    if (logs.size() <= 0) {
        return true;
    }
    std::sort(logs.begin(), logs.end());
    std::string buffer;
    LogEntry entry;
    for (uint32_t i = 0; i < logs.size(); i++) {
        std::string& full_path = logs[i];
        uint32_t binlog_index = 0;
        bool ok = ParseBinlogIndex(full_path, binlog_index);
        if (!ok) {
            break;
        }
        FILE* fd = fopen(full_path.c_str(), "rb+");
        if (fd == NULL) {
            PDLOG(WARNING, "fail to open path %s for error %s", full_path.c_str(), strerror(errno));
            break;
        }
        ::openmldb::log::SequentialFile* seq_file = ::openmldb::log::NewSeqFile(full_path, fd);
        ::openmldb::log::Reader reader(seq_file, NULL, false, 0, false);
        ::openmldb::base::Slice record;
        ::openmldb::log::Status status = reader.ReadRecord(&record, &buffer);
        delete seq_file;
        if (!status.ok()) {
            PDLOG(WARNING, "fail to get offset from file %s", full_path.c_str());
            continue;
        }
        ok = entry.ParseFromString(record.ToString());
        if (!ok) {
            PDLOG(WARNING, "fail to parse log entry %s ", ::openmldb::base::DebugString(record.ToString()).c_str());
            return false;
        }
        if (entry.log_index() <= 0) {
            PDLOG(WARNING, "invalid entry offset %lu ", entry.log_index());
            return false;
        }
        uint64_t offset = entry.log_index();
        if (offset > 0) {
            offset -= 1;
        }
        logs_->Insert(binlog_index, offset);
        PDLOG(INFO, "recover binlog index %u and offset %lu from path %s", binlog_index, entry.log_index(),
              full_path.c_str());
        binlog_index_.store(binlog_index + 1, std::memory_order_relaxed);
    }
    return true;
}

LogParts* LogReplicator::GetLogPart() { return logs_; }

void LogReplicator::SetOffset(uint64_t offset) { log_offset_.store(offset, std::memory_order_relaxed); }

uint64_t LogReplicator::GetOffset() { return log_offset_.load(std::memory_order_relaxed); }

void LogReplicator::SetSnapshotLogPartIndex(uint64_t offset) {
    snapshot_last_offset_.store(offset, std::memory_order_relaxed);
    ::openmldb::log::LogReader log_reader(logs_, log_path_, false);
    log_reader.SetOffset(offset);
    log_reader.RollRLogFile();
    int log_part_index = log_reader.GetLogIndex();
    snapshot_log_part_index_.store(log_part_index, std::memory_order_relaxed);
}

void LogReplicator::DeleteBinlog(bool* deleted) {
    if (logs_->GetSize() <= 1) {
        DEBUGLOG("log part size is one or less, need not delete");
        return;
    }
    int min_log_index = snapshot_log_part_index_.load(std::memory_order_relaxed);
    {
        std::lock_guard<bthread::Mutex> lock(mu_);
        for (auto iter = nodes_.begin(); iter != nodes_.end(); ++iter) {
            if ((*iter)->GetLogIndex() < min_log_index) {
                min_log_index = (*iter)->GetLogIndex();
            }
        }
    }
    min_log_index -= 1;
    if (min_log_index < 0) {
        DEBUGLOG("min_log_index is[%d], need not delete!", min_log_index);
        return;
    }
    DEBUGLOG("min_log_index[%d] cur binlog_index[%u]", min_log_index, binlog_index_.load(std::memory_order_relaxed));
    ::openmldb::base::Node<uint32_t, uint64_t>* node = NULL;
    {
        std::lock_guard<std::mutex> lock(wmu_);
        node = logs_->Split(min_log_index);
    }
    while (node) {
        ::openmldb::base::Node<uint32_t, uint64_t>* tmp_node = node;
        node = node->GetNextNoBarrier(0);
        std::string full_path =
            log_path_ + "/" + ::openmldb::base::FormatToString(tmp_node->GetKey(), FLAGS_binlog_name_length) + ".log";
        if (unlink(full_path.c_str()) < 0) {
            PDLOG(WARNING, "delete binlog[%s] failed! errno[%d] errinfo[%s]", full_path.c_str(), errno,
                  strerror(errno));
        } else {
            if (deleted) {
                *deleted = true;
            }
            PDLOG(INFO, "delete binlog[%s] success", full_path.c_str());
        }
        delete tmp_node;
    }
}

uint64_t LogReplicator::GetLeaderTerm() { return term_.load(std::memory_order_relaxed); }

void LogReplicator::SetLeaderTerm(uint64_t term) { term_.store(term, std::memory_order_relaxed); }

bool LogReplicator::ApplyEntry(const LogEntry& entry) {
    std::lock_guard<std::mutex> lock(wmu_);
    uint64_t last_log_offset = GetOffset();
    if (wh_ == NULL || (wh_->GetSize() / (1024 * 1024)) > (uint32_t)FLAGS_binlog_single_file_max_size) {
        if (!RollWLogFile()) {
            PDLOG(WARNING, "fail to roll write log for path %s", path_.c_str());
            return false;
        }
    }
    if (entry.log_index() <= last_log_offset) {
        PDLOG(WARNING, "entry log_index %lu cur log_offset %lu tid %u pid %u",
                entry.log_index(), last_log_offset, tid_, pid_);
        return true;
    }
    std::string buffer;
    entry.SerializeToString(&buffer);
    ::openmldb::base::Slice slice(buffer.c_str(), buffer.size());
    ::openmldb::log::Status status = wh_->Write(slice);
    if (!status.ok()) {
        PDLOG(WARNING, "fail to write replication log in dir %s for %s", path_.c_str(), status.ToString().c_str());
        return false;
    }
    log_offset_.store(entry.log_index(), std::memory_order_relaxed);
    DEBUGLOG("sync log entry to offset %lu for %s", GetOffset(), path_.c_str());
    return true;
}

int LogReplicator::AddReplicateNode(const std::map<std::string, std::string>& real_ep_map) {
    return AddReplicateNode(real_ep_map, UINT32_MAX);
}

int LogReplicator::AddReplicateNode(const std::map<std::string, std::string>& real_ep_map, uint32_t tid) {
    if (real_ep_map.empty()) {
        return 1;
    }
    std::lock_guard<bthread::Mutex> lock(mu_);
    if (role_ != kLeaderNode) {
        PDLOG(WARNING, "cur table is not leader, cannot add replicate");
        return -1;
    }
    for (const auto& kv : real_ep_map) {
        const std::string& endpoint = kv.first;
        std::vector<std::shared_ptr<ReplicateNode>>::iterator it = nodes_.begin();
        for (; it != nodes_.end(); ++it) {
            std::string ep = (*it)->GetEndPoint();
            if (ep.compare(endpoint) == 0) {
                PDLOG(WARNING, "replica endpoint %s does exist", ep.c_str());
                return 1;
            }
        }
        std::shared_ptr<ReplicateNode> replicate_node;
        if (tid == UINT32_MAX) {
            replicate_node =
                std::make_shared<ReplicateNode>(endpoint, logs_, log_path_, tid_, pid_, &term_,
                                                &log_offset_, &mu_, &cv_, false, &follower_offset_, kv.second);
        } else {
            replicate_node =
                std::make_shared<ReplicateNode>(endpoint, logs_, log_path_, tid, pid_, &term_, &log_offset_,
                                                &mu_, &cv_, true, &follower_offset_, kv.second);
        }
        if (replicate_node->Init() < 0) {
            PDLOG(WARNING, "init replicate node %s error", endpoint.c_str());
            return -1;
        }
        if (replicate_node->Start() != 0) {
            PDLOG(WARNING, "fail to start sync thread for table #tid %u, #pid %u", tid_, pid_);
            return -1;
        }
        nodes_.push_back(replicate_node);
        real_ep_map_.insert(std::make_pair(endpoint, kv.second));
        if (tid == UINT32_MAX) {
            local_endpoints_.push_back(endpoint);
        }
        PDLOG(INFO, "add ReplicateNode with endpoint %s ok. tid[%u] pid[%u]", endpoint.c_str(), tid_, pid_);
    }
    return 0;
}

int LogReplicator::DelReplicateNode(const std::string& endpoint) {
    std::shared_ptr<ReplicateNode> node;
    {
        std::lock_guard<bthread::Mutex> lock(mu_);
        if (role_ != kLeaderNode) {
            PDLOG(WARNING, "cur table is not leader, cannot delete replicate");
            return -1;
        }
        std::vector<std::shared_ptr<ReplicateNode>>::iterator it = nodes_.begin();
        for (; it != nodes_.end(); ++it) {
            if ((*it)->GetEndPoint().compare(endpoint) == 0) {
                break;
            }
        }
        if (it == nodes_.end()) {
            PDLOG(WARNING, "replica endpoint[%s] does not exist", endpoint.c_str());
            return 1;
        }
        node = *it;
        nodes_.erase(it);
        real_ep_map_.erase(endpoint);
        local_endpoints_.erase(std::remove(local_endpoints_.begin(), local_endpoints_.end(), endpoint),
                               local_endpoints_.end());
        PDLOG(INFO, "delete replica. endpoint[%s] tid[%u] pid[%u]", endpoint.c_str(), tid_, pid_);
    }
    if (node) {
        node->Stop();
    }
    return 0;
}

void LogReplicator::GetReplicateInfo(std::map<std::string, uint64_t>& info_map) {
    std::lock_guard<bthread::Mutex> lock(mu_);
    if (role_ != kLeaderNode) {
        DEBUGLOG("cur table is not leader");
        return;
    }
    if (nodes_.empty()) {
        return;
    }
    for (const auto& node : nodes_) {
        info_map.insert(std::make_pair(node->GetEndPoint(), node->GetLastSyncOffset()));
    }
}

bool LogReplicator::DelAllReplicateNode() {
    std::vector<std::shared_ptr<ReplicateNode>> copied_nodes = nodes_;
    {
        std::lock_guard<bthread::Mutex> lock(mu_);
        if (nodes_.size() <= 0) {
            return true;
        }
        PDLOG(INFO, "delete all replica. replica num [%u] tid[%u] pid[%u]", nodes_.size(), tid_, pid_);
        nodes_.clear();
        real_ep_map_.clear();
        local_endpoints_.clear();
    }
    std::vector<std::shared_ptr<ReplicateNode>>::iterator it = copied_nodes.begin();
    for (; it != copied_nodes.end(); ++it) {
        DEBUGLOG("stop replicator node");
        std::shared_ptr<ReplicateNode> node = *it;
        node->Stop();
    }
    return true;
}

bool LogReplicator::AppendEntry(LogEntry& entry, ::google::protobuf::Closure* done) {
    std::lock_guard<std::mutex> lock(wmu_);
    if (wh_ == NULL || wh_->GetSize() / (1024 * 1024) > (uint32_t)FLAGS_binlog_single_file_max_size) {
        bool ok = RollWLogFile();
        if (!ok) {
            return false;
        }
    }
    uint64_t cur_offset = log_offset_.load(std::memory_order_relaxed);
    entry.set_log_index(1 + cur_offset);
    std::string buffer;
    entry.SerializeToString(&buffer);
    ::openmldb::base::Slice slice(buffer);
    ::openmldb::log::Status status = wh_->Write(slice);
    if (!status.ok()) {
        PDLOG(WARNING, "fail to write replication log in dir %s for %s", path_.c_str(), status.ToString().c_str());
        return false;
    }
    log_offset_.fetch_add(1, std::memory_order_relaxed);
    if (local_endpoints_.empty()) {  // if local replica are dead, leader direct
                                     // sync to remote replica
        follower_offset_.store(cur_offset + 1, std::memory_order_relaxed);
    }
    if (done) {
        done->Run();
    }
    return true;
}

bool LogReplicator::RollWLogFile() {
    if (wh_ != NULL) {
        wh_->EndLog();
        delete wh_;
        wh_ = NULL;
    }
    std::string name =
        ::openmldb::base::FormatToString(binlog_index_.load(std::memory_order_relaxed), FLAGS_binlog_name_length) +
        ".log";
    std::string full_path = log_path_ + "/" + name;
    FILE* fd = fopen(full_path.c_str(), "wb");
    if (fd == NULL) {
        PDLOG(WARNING, "fail to create file %s", full_path.c_str());
        return false;
    }
    uint64_t offset = log_offset_.load(std::memory_order_relaxed);
    logs_->Insert(binlog_index_.load(std::memory_order_relaxed), offset);
    binlog_index_.fetch_add(1, std::memory_order_relaxed);
    PDLOG(INFO, "roll write log for name %s and start offset %lld. tid %u pid %u", name.c_str(), offset, tid_, pid_);
    wh_ = new WriteHandle("off", name, fd);
    return true;
}

void LogReplicator::Notify() { cv_.notify_all(); }

}  // namespace replica
}  // namespace openmldb
