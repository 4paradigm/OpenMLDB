//
// log_appender.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-06-07
//

#include "replica/log_replicator.h"

#include <stdlib.h>
#include <stdio.h>
#include <cstring>
#include "leveldb/options.h"
#include "logging.h"
#include "base/strings.h"
#include "base/file_util.h"
#include <gflags/gflags.h>

DECLARE_int32(binlog_single_file_max_size);

namespace rtidb {
namespace replica {

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

const static StringComparator scmp;
const static std::string LOG_META_PREFIX="/logs/";

LogReplicator::LogReplicator(const std::string& path):path_(path), meta_path_(), log_path_(),
    meta_(NULL), term_(0), log_offset_(0), logs_(NULL), wh_(NULL), wsize_(0),
    role_(kSlaveNode), last_log_term_(0), last_log_offset_(0){}

LogReplicator::~LogReplicator() {}

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
                                               log_path_, 
                                               &meta_);
    if (!status.ok()) {
        LOG(WARNING, "fail to create meta db for %s", status.ToString().c_str());
        return false;
    }

    ok = Recover();
    if (!ok) {
        return false;
    }

    return ok;
}

bool LogReplicator::AppendEntries(const ::rtidb::api::AppendEntriesRequest* request) {
    if (wh_ == NULL || wsize_ > (uint32_t)FLAGS_binlog_single_file_max_size) {
        bool ok = RollWLogFile();
        if (!ok) {
            return false;
        }
    }
    if (request->pre_log_index() !=  last_log_offset_
        || request->pre_log_term() != last_log_term_) {
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
        last_log_term_ = request->term();
        last_log_offset_ = request->entries(i).log_index();
    }
    LOG(DEBUG, "sync log entry to offset %lld for %s", last_log_offset_, path_.c_str());
    return true;
}

bool LogReplicator::AppendEntry(::rtidb::api::LogEntry& entry) {
    if (wh_ == NULL || wsize_ > (uint32_t)FLAGS_binlog_single_file_max_size) {
        bool ok = RollWLogFile();
        if (!ok) {
            return false;
        }
    }
    entry.set_log_index(++log_offset_);
    std::string buffer;
    entry.SerializeToString(&buffer);
    ::rtidb::base::Slice slice(buffer.c_str(), buffer.size());
    ::rtidb::base::Status status = wh_->Write(slice);
    if (!status.ok()) {
        LOG(WARNING, "fail to write replication log in dir %s for %s", path_.c_str(), status.ToString().c_str());
        return false;
    }
    return true;
}

bool LogReplicator::RollWLogFile() {
    if (wh_ != NULL) {
        delete wh_;
    }

    std::string name = ::rtidb::base::FormatToString(logs_->GetSize(), 8) + ".log";
    std::string full_path = log_path_ + "/" + name;
    FILE* fd = fopen(full_path.c_str(), "ab+");
    if (fd == NULL) {
        LOG(WARNING, "fail to create file %s", full_path.c_str());
        return false;
    }
    LogPart* part = new LogPart(log_offset_ + 1, name);
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
    //TODO(wangtaize) recover term and log offset from log part
    return true;
}

} // end of replica
} // end of ritdb
