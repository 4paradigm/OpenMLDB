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
DECLARE_int32(binlog_sync_batch_size);
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
const static std::string LOG_META_PREFIX="/logs/";

LogReplicator::LogReplicator(const std::string& path,
                             const std::vector<std::string>& endpoints,
                             const ReplicatorRole& role,
                             uint32_t tid, uint32_t pid,
                             SnapshotFunc ssf):path_(path), meta_path_(), log_path_(),
    meta_(NULL), log_offset_(0), logs_(NULL), wh_(NULL), wsize_(0),
    role_(role), last_log_offset_(0),
    endpoints_(endpoints), nodes_(), mu_(), cv_(&mu_),rpc_client_(NULL),
    self_(NULL), running_(true), tp_(4), refs_(0), tid_(tid), pid_(pid), wmu_(),
    ssf_(ssf){}

LogReplicator::LogReplicator(const std::string& path,
                             ApplyLogFunc func,
                             const ReplicatorRole& role,
                             uint32_t tid, uint32_t pid,
                             SnapshotFunc ssf):path_(path), meta_path_(), log_path_(),
    meta_(NULL), log_offset_(0), logs_(NULL), wh_(NULL), wsize_(0),
    role_(role), last_log_offset_(0),
    endpoints_(), nodes_(), mu_(), cv_(&mu_),rpc_client_(NULL),
    self_(NULL),
    func_(func), 
    running_(true), tp_(4), refs_(0), tid_(tid), pid_(pid), wmu_(),
    ssf_(ssf){}

LogReplicator::~LogReplicator() {
    delete meta_;
    meta_ = NULL;
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
    std::vector<ReplicaNode*>::iterator nit = nodes_.begin();
    for (; nit != nodes_.end(); ++nit) {
        delete (*nit);
    }
    delete rpc_client_;
    delete self_;
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
    leveldb::Options options;
    options.create_if_missing = true;
    meta_path_ = path_ + "/meta/";
    bool ok = ::rtidb::base::MkdirRecur(meta_path_);
    if (!ok) {
        LOG(WARNING, "fail to create db meta path %s", meta_path_.c_str());
        return false;
    }
    log_path_ = path_ + "/logs/";
    ok = ::rtidb::base::MkdirRecur(log_path_);
    if (!ok) {
        LOG(WARNING, "fail to log dir %s", log_path_.c_str());
        return false;
    }
    leveldb::Status status = leveldb::DB::Open(options,
                                               meta_path_, 
                                               &meta_);
    if (!status.ok()) {
        LOG(WARNING, "fail to create meta db for %s", status.ToString().c_str());
        return false;
    }

    ok = Recover();
    if (!ok) {
        return false;
    }

    if (role_ == kLeaderNode) {
        std::vector<std::string>::iterator it = endpoints_.begin();
        uint32_t index = 0;
        for (; it != endpoints_.end(); ++it) {
            ReplicaNode* node = new ReplicaNode();
            node->endpoint = *it;
            index ++;
            nodes_.push_back(node);
            LOG(INFO, "add replica node with endpoint %s", node->endpoint.c_str());
        }
        tp_.DelayTask(FLAGS_binlog_match_logoffset_interval, boost::bind(&LogReplicator::MatchLogOffset, this));
        LOG(INFO, "init leader node for path %s ok", path_.c_str());
    }else {
        self_ = new ReplicaNode();
        tp_.AddTask(boost::bind(&LogReplicator::ApplyLog, this));
        LOG(INFO, "init follower node for path %s ok", path_.c_str());
    }
    tp_.DelayTask(FLAGS_binlog_sync_to_disk_interval, boost::bind(&LogReplicator::SyncToDisk, this));
    rpc_client_ = new ::rtidb::RpcClient();
    return ok;
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
    if (request->pre_log_index() !=  last_log_offset_) {
        LOG(WARNING, "log mismatch for path %s, pre_log_index %lld, come log index %lld", path_.c_str(),
                last_log_offset_, request->pre_log_index());
        response->set_log_offset(last_log_offset_);
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
        ssf_(buffer, request->entries(i).pk(), request->entries(i).log_index(), request->entries(i).ts());
        wsize_ += buffer.size();
        last_log_offset_ = request->entries(i).log_index();
        log_offset_.store(request->entries(i).log_index(), boost::memory_order_relaxed);
        response->set_log_offset(last_log_offset_);
    }
    LOG(DEBUG, "sync log entry to offset %lld for %s", last_log_offset_, path_.c_str());
    return true;
}

bool LogReplicator::AddReplicateNode(const std::string& endpoint) {
    {
        MutexLock lock(&mu_);
        if (role_ != kLeaderNode) {
            return false;
        }
        std::vector<std::string>::iterator it = endpoints_.begin();
        for (; it != endpoints_.end(); ++it) {
            std::string ep = *it;
            if (ep.compare(endpoint) == 0) {
                LOG(WARNING, "replica endpoint %s does exist", ep.c_str());
                return false;
            }
        }
        ReplicaNode* node = new ReplicaNode();
        node->endpoint = endpoint;
        nodes_.push_back(node);
        LOG(INFO, "add ReplicaNode with endpoint %s ok", endpoint.c_str());
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
    ssf_(buffer, entry.pk(), entry.log_index(), entry.ts());
    LOG(DEBUG, "entry index %lld, log offset %lld", entry.log_index(), log_offset_.load(boost::memory_order_relaxed));
    return true;
}

bool LogReplicator::OpenSeqFile(const std::string& path, SequentialFile** sf) {
    if (sf == NULL) {
        LOG(WARNING, "input sf in null");
        return false;
    }
    FILE* fd = fopen(path.c_str(), "rb");
    if (fd == NULL) {
        LOG(WARNING, "fail to open file %s", path.c_str());
        return false;
    }
    // close old Sequentialfile 
    if ((*sf) != NULL) {
        delete (*sf);
        *sf = NULL;
    }
    LOG(INFO, "open log file to %s for path %s ok", path.c_str(),
            path_.c_str());
    *sf = ::rtidb::log::NewSeqFile(path, fd);
    return true;
}

int32_t LogReplicator::RollRLogFile(SequentialFile** sf,
                                    uint64_t offset,
                                    int32_t last_log_part_index) {
    if (sf == NULL) {
        LOG(WARNING, "invalid input sf which is null");
        return -1;
    }
    mu_.AssertHeld();
    int32_t index = -1;
    LogParts::Iterator* it = logs_->NewIterator();
    if (logs_->GetSize() <= 0) {
        return -2;
    }
    it->SeekToFirst();
    // use log entry offset to find the log part file
    if (last_log_part_index < 0) {
        while (it->Valid()) {
            index ++;
            LogPart* part = it->GetValue();
            LOG(DEBUG, "log with name %s and start offset %lld", part->log_name_.c_str(),
                    part->slog_id_);
            if (part->slog_id_ < offset) {
                it->Next();
                continue;
            }
            delete it;
            // open a new log part file
            std::string full_path = log_path_ + "/" + part->log_name_;
            bool ok = OpenSeqFile(full_path, sf);
            if (!ok) {
                return -1;
            }
            return index;
        }
        delete it;
        LOG(WARNING, "fail to find log include offset %lld", offset);
        return -1;
    }else {
        uint32_t current_index = (uint32_t) last_log_part_index;
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
                bool ok = OpenSeqFile(full_path, sf);
                if (!ok) {
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
    std::string buffer;
    buffer.resize(8 + 4 + name.size() + 1);
    char* cbuffer = reinterpret_cast<char*>(&(buffer[0]));
    memcpy(cbuffer, static_cast<const void*>(&part->slog_id_), 8);
    cbuffer += 8;
    uint32_t name_len = name.size() + 1;
    memcpy(cbuffer, static_cast<const void*>(&name_len), 4);
    cbuffer += 4;
    memcpy(cbuffer, static_cast<const void*>(name.c_str()), name_len);
    std::string key = LOG_META_PREFIX + name;
    leveldb::WriteOptions options;
    options.sync = true;
    const leveldb::Slice value(buffer);
    leveldb::Status status = meta_->Put(options, key, value);
    if (!status.ok()) {
        LOG(WARNING, "fail to save meta for %s", name.c_str());
        delete part;
        return false;
    }
    logs_->Insert(name, part);
    LOG(INFO, "roll write log for name %s and start offset %lld", name.c_str(), part->slog_id_);
    wh_ = new WriteHandle(name, fd);
    wsize_ = 0;
    return true;
}

bool LogReplicator::Recover() {
    logs_ = new LogParts(12, 4, scmp);
    ::leveldb::ReadOptions options;
    leveldb::Iterator* it = meta_->NewIterator(options);
    it->Seek(LOG_META_PREFIX);
    std::string log_meta_end = LOG_META_PREFIX + "~";
    while(it->Valid()) {
        if (it->key().compare(log_meta_end) >= 0) {
            break;
        }
        leveldb::Slice data = it->value();
        if (data.size() < 12) {
            LOG(WARNING, "bad data formate");
            assert(0);
        }
        LogPart* log_part = new LogPart();
        const char* buffer = data.data();
        memcpy(static_cast<void*>(&log_part->slog_id_), buffer, 8);
        buffer += 8;
        uint32_t name_size = 0;
        memcpy(static_cast<void*>(&name_size), buffer, 4);
        buffer += 4;
        char name_con[name_size];
        memcpy(reinterpret_cast<char*>(&name_con), buffer, name_size);
        std::string name_str(name_con);
        log_part->log_name_ = name_str;
        LOG(INFO, "recover log part with slog_id %lld name %s",
                log_part->slog_id_, log_part->log_name_.c_str());
        logs_->Insert(name_str, log_part);
        it->Next();
    }
    delete it;
    return true;
}

void LogReplicator::Notify() {
    MutexLock lock(&mu_);
    cv_.Broadcast();
}


void LogReplicator::MatchLogOffset() {
    MutexLock lock(&mu_);
    bool all_matched = true;
    std::vector<ReplicaNode*>::iterator it = nodes_.begin();
    for (; it != nodes_.end(); ++it) {
        ReplicaNode* node = *it;
        if (node->log_matched) {
            continue;
        }
        bool ok = MatchLogOffsetFromNode(node);
        if (ok) {
            tp_.AddTask(boost::bind(&LogReplicator::ReplicateToNode, this, boost::ref(node->endpoint)));
        }else {
            all_matched = false;
        }
    }
    if (!all_matched) {
        // retry after 1 second
        tp_.DelayTask(1000, boost::bind(&LogReplicator::MatchLogOffset, this));
    }
}

::rtidb::base::Status LogReplicator::ReadNextRecord(ReplicaNode* node,
                                   ::rtidb::base::Slice* record,
                                   std::string* buffer) {
    mu_.AssertHeld();
    // first read record 
    if (node->sf == NULL) {
        int32_t new_log_part_index = RollRLogFile(&node->sf, 
                                                  node->last_sync_offset,
                                                  node->log_part_index);
        if (new_log_part_index == -2) {
            LOG(WARNING, "no log avaliable tid %d pid %d", tid_, pid_);
            return Status::IOError("no log avaliable");
        }

        if (new_log_part_index < 0) {
            LOG(WARNING, "fail to roll read log for path %s", path_.c_str());
            return Status::IOError("no log avaliable");
        }
        node->reader = new Reader(node->sf, NULL, false, 0);
        // when change log part index , reset
        // last_log_byte_offset to 0
        node->log_part_index = new_log_part_index;
    }
    ::rtidb::base::Status status = node->reader->ReadRecord(record, buffer);
    if (!status.ok()) {
        LOG(DEBUG, "reach the end of file for path %s", path_.c_str());
        // reache the end of file 
        int32_t new_log_part_index = RollRLogFile(&node->sf,
                                                  node->last_sync_offset,
                                                  node->log_part_index);
        // reache the latest log part
        if (new_log_part_index == node->log_part_index) {
            LOG(DEBUG, "no new log entry for path %s", path_.c_str());
            return status;
        }
        if (new_log_part_index < 0) {
            return status;
        }
        delete node->reader;
        // roll a new log part file, reset status
        node->log_part_index = new_log_part_index;
        node->reader = new Reader(node->sf, NULL, false, 0);
        status = node->reader->ReadRecord(record, buffer);
    }
    return status;
}

void LogReplicator::ApplyLog() {
    uint32_t coffee_time = 0;
    while(running_.load(boost::memory_order_relaxed)) {
        MutexLock lock(&mu_);
        if (coffee_time > 0) {
            cv_.TimeWait(coffee_time);
            coffee_time = 0;
        }
        while (self_->last_sync_offset >= log_offset_.load(boost::memory_order_relaxed)) {
            cv_.TimeWait(FLAGS_binlog_sync_wait_time);
            if (!running_.load(boost::memory_order_relaxed)) {
                LOG(INFO, "apply log exist for path %s", path_.c_str());
                return;
            }
        }
        uint32_t batchSize = log_offset_.load(boost::memory_order_relaxed) - self_->last_sync_offset;
        if (batchSize > (uint32_t)FLAGS_binlog_apply_batch_size) {
            batchSize = FLAGS_binlog_apply_batch_size;
        }
        for (uint32_t i = 0; i < batchSize; i++) {
            std::string buffer;
            ::rtidb::base::Slice record;
            ::rtidb::base::Status status = ReadNextRecord(self_, &record, &buffer);
            if (status.ok()) {
                ::rtidb::api::LogEntry entry;
                entry.ParseFromString(record.ToString());
                bool ok = func_(entry);
                if (ok) {
                    LOG(DEBUG, "apply log with path %s ok to offset %lld", path_.c_str(),
                            entry.log_index());
                    self_->last_sync_offset = entry.log_index();
                }else {
                    //TODO cache log entry and reapply 
                    LOG(WARNING, "fail to apply log with path %s to offset %lld", path_.c_str(),
                            entry.log_index());
                }
            }else if (status.IsWaitRecord()) {
                LOG(WARNING, "got a coffee time for path %s", path_.c_str());
                coffee_time = FLAGS_binlog_coffee_time;
                break;
            }else {
                LOG(WARNING, "fail to read record for %s", status.ToString().c_str());
                break;
            }
        }
    }
}

bool LogReplicator::MatchLogOffsetFromNode(ReplicaNode* node) {
    mu_.AssertHeld();
    ::rtidb::api::TabletServer_Stub* stub;
    bool ok = rpc_client_->GetStub(node->endpoint, &stub);
    if (!ok) {
        LOG(WARNING, "fail to get rpc stub with endpoint %s", node->endpoint.c_str());
        return false;
    }
    ::rtidb::api::AppendEntriesRequest request;
    request.set_tid(tid_);
    request.set_pid(pid_);
    ::rtidb::api::AppendEntriesResponse response;
    ok = rpc_client_->SendRequest(stub, 
                                 &::rtidb::api::TabletServer_Stub::AppendEntries,
                                 &request, &response, 12, 1);
    if (ok && response.code() == 0) {
        node->last_sync_offset = response.log_offset();
        node->log_matched = true;
        LOG(INFO, "match node %s log offset %lld for table tid %d pid %d",
                node->endpoint.c_str(), node->last_sync_offset, tid_, pid_);
        return true;
    }
    return false;
}

void LogReplicator::ReplicateToNode(const std::string& endpoint) {
    uint32_t coffee_time = 0;
    while (running_.load(boost::memory_order_relaxed)) {
        MutexLock lock(&mu_);
        std::vector<ReplicaNode*>::iterator it = nodes_.begin();
        ReplicaNode* node = NULL;
        for (; it != nodes_.end(); ++it) {
            ReplicaNode* inode = *it;
            if (inode->endpoint == endpoint) {
                node = inode;
                break;
            }
        }

        if (node == NULL) {
            LOG(WARNING, "fail to find node with endpoint %s", endpoint.c_str());
            break;
        }

        if (coffee_time > 0) {
            cv_.TimeWait(coffee_time);
            coffee_time = 0;
        }

        LOG(DEBUG, "node %s offset %lld, log offset %lld ", node->endpoint.c_str(), node->last_sync_offset, log_offset_.load(boost::memory_order_relaxed));
        while (node->last_sync_offset >= (log_offset_.load(boost::memory_order_relaxed))) {
            cv_.TimeWait(FLAGS_binlog_sync_wait_time);
            if (!running_.load(boost::memory_order_relaxed)) {
                LOG(INFO, "replicate log exist for path %s", path_.c_str());
                return;
            }
        }

        ::rtidb::api::TabletServer_Stub* stub;
        bool ok = rpc_client_->GetStub(node->endpoint, &stub);
        if (!ok) {
            LOG(WARNING, "fail to get rpc stub with endpoint %s", node->endpoint.c_str());
            continue;
        }
        bool request_from_cache = false;
        ::rtidb::api::AppendEntriesRequest request;
        ::rtidb::api::AppendEntriesResponse response;
        uint64_t sync_log_offset = 0;
        if (node->cache.size() > 0) {
            request_from_cache = true;
            request = node->cache[0];
            if (request.entries_size() <= 0) {
                node->cache.clear(); 
                LOG(WARNING, "empty append entry request from node %s cache", node->endpoint.c_str());
                continue;
            }
            const ::rtidb::api::LogEntry& entry = request.entries(request.entries_size() - 1);
            if (entry.log_index() <= node->last_sync_offset) {
                LOG(WARNING, "duplicate log index from node %s cache", node->endpoint.c_str());
                node->cache.clear();
                continue;
            }
            sync_log_offset = entry.log_index();
        }else {
            request.set_tid(tid_);
            request.set_pid(pid_);
            request.set_pre_log_index(node->last_sync_offset);
            uint32_t batchSize = log_offset_.load(boost::memory_order_relaxed) - node->last_sync_offset;
            if (batchSize > (uint32_t)FLAGS_binlog_sync_batch_size) {
                batchSize = FLAGS_binlog_sync_batch_size;
            }
            for (uint64_t i = 0; i < batchSize; i++) {
                std::string buffer;
                ::rtidb::base::Slice record;
                ::rtidb::base::Status status = ReadNextRecord(node, &record, &buffer);
                if (status.ok()) {
                    ::rtidb::api::LogEntry* entry = request.add_entries();
                    ok = entry->ParseFromString(record.ToString());
                    if (!ok) {
                        LOG(WARNING, "bad protobuf format %s size %ld", ::rtidb::base::DebugString(record.ToString()).c_str(), record.ToString().size());
                        request.mutable_entries()->RemoveLast();
                        break;
                    }
                    LOG(DEBUG, "entry val %s log index %lld", entry->value().c_str(), entry->log_index());
                    if (entry->log_index() <= node->last_sync_offset) {
                        LOG(WARNING, "skip duplicate log offset %lld", entry->log_index());
                        request.mutable_entries()->RemoveLast();
                        continue;
                    }
                    sync_log_offset = entry->log_index();
                }else if (status.IsWaitRecord()) {
                    LOG(WARNING, "got a coffee time for path %s", path_.c_str());
                    coffee_time = FLAGS_binlog_coffee_time;
                    break;
                }else {
                    LOG(WARNING, "fail to get record %s", status.ToString().c_str());
                    break;
                }
            }
        }
        if (request.entries_size() <= 0) {
            continue;
        }
        ok = rpc_client_->SendRequest(stub, 
                                     &::rtidb::api::TabletServer_Stub::AppendEntries,
                                     &request, &response, 12, 1);

        if (ok && response.code() == 0) {
            LOG(DEBUG, "sync log to node %s to offset %lld",
                    node->endpoint.c_str(), sync_log_offset);
            node->last_sync_offset = sync_log_offset;
            if (request_from_cache) {
                node->cache.clear(); 
            }
        }else if(!request_from_cache){
            node->cache.push_back(request);
            LOG(WARNING, "fail to sync log to node %s", node->endpoint.c_str());
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
