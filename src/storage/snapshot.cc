//
// snapshot.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-07-24
//
//
#include "storage/snapshot.h"

#include "base/file_util.h"
#include "base/strings.h"
#include "boost/lexical_cast.hpp"
#include "gflags/gflags.h"
#include "leveldb/options.h"
#include "leveldb/write_batch.h"
#include "logging.h"
#include "timer.h"

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_string(snapshot_root_path);

namespace rtidb {
namespace storage {

const std::string LOG_PREFIX="/logs/";
const std::string OFFSET_PREFIX="/offset/";

Snapshot::Snapshot(uint32_t tid, uint32_t pid, uint64_t offset):tid_(tid), pid_(pid),
    db_(NULL), offset_(offset), refs_(0) {}

Snapshot::~Snapshot() {
    delete db_;
}

bool Snapshot::Init() {
    std::string snapshot_path = FLAGS_snapshot_root_path + "/" + boost::lexical_cast<std::string>(tid_) + "_" + boost::lexical_cast<std::string>(pid_);
    bool ok = ::rtidb::base::MkdirRecur(snapshot_path);
    if (!ok) {
        LOG(WARNING, "fail to create db meta path %s", snapshot_path.c_str());
        return false;
    }
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options,
                                               snapshot_path, 
                                               &db_);
    if (!status.ok()) {
        LOG(WARNING, "fail open snapshot with path %s for tid %d, pid %d with error %s", snapshot_path.c_str(),
                tid_, pid_, status.ToString().c_str());
        return false;
    }
    ::leveldb::ReadOptions roptions;
    std::string offset_str;
    leveldb::Slice key(OFFSET_PREFIX);
    status = db_->Get(roptions, key, &offset_str);
    if (status.IsNotFound()) {
        LOG(WARNING, "use default offset 0 tid %d, pid %d", tid_, pid_);
    }else if(status.ok()) {
        offset_.store(boost::lexical_cast<uint64_t>(offset_str), boost::memory_order_relaxed);
    }else {
        LOG(WARNING, "fail to get table offset for tid %d pid %d", tid_, pid_);
        return false;
    }
    LOG(INFO, "init snapshot %s for tid %d, pid %d with offset %lld", snapshot_path.c_str(), tid_, pid_, offset_.load(boost::memory_order_relaxed));
    return true;
}

bool Snapshot::Put(const std::string& entry, uint64_t offset,
                   const std::string& pk, uint64_t ts) {
    offset_.store(offset, boost::memory_order_relaxed);
    std::string combined_key = LOG_PREFIX + pk + "/" + boost::lexical_cast<std::string>(ts);
    leveldb::Slice key(combined_key);
    leveldb::Slice offset_key(OFFSET_PREFIX);
    leveldb::WriteBatch batch;
    batch.Put(offset_key, boost::lexical_cast<std::string>(offset));
    batch.Put(key, entry);
    leveldb::Status status = db_->Write(leveldb::WriteOptions(), &batch);
    if (status.ok()) {
        return true;
    }
    LOG(WARNING, "fail to put entry tid %d pid %d for %s", tid_, pid_, status.ToString().c_str());
    return false;
}

bool Snapshot::BatchDelete(const std::vector<DeleteEntry>& entries) {
    leveldb::WriteBatch batch;
    for (size_t i = 0; i < entries.size(); i++) {
        const DeleteEntry& entry = entries[i];
        std::string key = LOG_PREFIX + entry.pk + "/" + boost::lexical_cast<std::string>(entry.ts);
        batch.Delete(key);
    }
    leveldb::Status status = db_->Write(leveldb::WriteOptions(), &batch);
    if (status.ok()) {
        return true;
    }
    LOG(WARNING, "fail to delete entry tid %d pid %d for %s", tid_, pid_, status.ToString().c_str());
    return false;
}

bool Snapshot::Recover(Table* table) {
    //TODO multi thread recover
    if (table == NULL) {
        LOG(WARNING, "table is NULL");
        return false;
    }
    leveldb::Iterator* it = db_->NewIterator(leveldb::ReadOptions());
    it->SeekToFirst();
    LOG(INFO, "start to recover table tid %d, pid %d", tid_, pid_);
    uint64_t count = 0;
    uint64_t consumed = ::baidu::common::timer::get_micros();
    while (it->Valid()) {
        LOG(DEBUG, "key %s value %s", it->key().ToString().c_str(), ::rtidb::base::DebugString(it->value().ToString()).c_str());
        LogEntry entry;
        bool ok = entry.ParseFromString(it->value().ToString());
        if (!ok) {
            LOG(WARNING, "bad pb format for key %s value %s", it->key().ToString().c_str(), ::rtidb::base::DebugString(it->value().ToString()).c_str());
        } else {
            table->Put(entry.pk(), entry.ts(), entry.value().c_str(), entry.value().length());
        }
        it->Next();
        count++;
        if (count % 10000 == 0) {
            LOG(INFO, "load cur_index[%lu] offset[%lu] tid[%u] pid[%u]", 
                        count, offset_.load(boost::memory_order_relaxed), tid_, pid_);
        }
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;
    LOG(INFO, "table tid %d, pid %d recovered with time %lld ms, count %lld", tid_, pid_, consumed/1000, count);
    return true;
}

void Snapshot::Ref() {
    refs_.fetch_add(1, boost::memory_order_relaxed);
}

void Snapshot::UnRef() {
    refs_.fetch_sub(1, boost::memory_order_acquire);
    if (refs_.load(boost::memory_order_relaxed) <= 0) {
        LOG(INFO, "drop snapshot for tid %d pid %d", tid_, pid_);
        delete this;
    }
}

}
}



