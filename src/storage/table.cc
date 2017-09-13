//
// table.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//
//
#include "storage/table.h"

#include "base/hash.h"
#include "logging.h"
#include "timer.h"
#include <boost/lexical_cast.hpp>
#include <gflags/gflags.h>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_string(db_root_path);

namespace rtidb {
namespace storage {

const static uint32_t SEED = 9527;
const std::string SNAPSHOT_PREFIX="/snapshot/";
const std::string DATA_PREFIX="/data/";

Table::Table(const std::string& name,
        uint32_t id,
        uint32_t pid,
        uint32_t seg_cnt,
        uint32_t ttl,
        bool is_leader,
        const std::vector<std::string>& replicas,
        bool wal):name_(name), id_(id),
    pid_(pid), seg_cnt_(seg_cnt),
    segments_(NULL), 
    ref_(0), enable_gc_(false), ttl_(ttl),
    ttl_offset_(60 * 1000), is_leader_(is_leader),
    replicas_(replicas), wal_(wal), term_(0), table_status_(kUndefined)
{}

Table::Table(const std::string& name,
        uint32_t id,
        uint32_t pid,
        uint32_t seg_cnt,
        uint32_t ttl,
        bool wal):name_(name), id_(id),
    pid_(pid), seg_cnt_(seg_cnt),
    segments_(NULL), 
    ref_(0), enable_gc_(false), ttl_(ttl),
    ttl_offset_(60 * 1000), is_leader_(false),
    replicas_(), wal_(wal), term_(0), table_status_(kUndefined)
{}

void Table::Init(SnapshotTTLFunc ttl_fun) {
    segments_ = new Segment*[seg_cnt_];
    for (uint32_t i = 0; i < seg_cnt_; i++) {
        segments_[i] = new Segment(ttl_fun);
    }
    if (ttl_ > 0) {
        enable_gc_ = true;
    }
    LOG(INFO, "init table name %s, id %d, pid %d, seg_cnt %d , ttl %d", name_.c_str(),
            id_, pid_, seg_cnt_, ttl_);
}

void Table::Put(const std::string& pk, uint64_t time,
        const char* data, uint32_t size) {
    uint32_t index = 0;
    if (seg_cnt_ > 1) {
        index = ::rtidb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Segment* segment = segments_[index];
    segment->Put(pk, time, data, size);
    data_cnt_.fetch_add(1, boost::memory_order_relaxed);
}

void Table::Ref() {
    ref_.fetch_add(1, boost::memory_order_relaxed);
}

void Table::UnRef() {
    ref_.fetch_sub(1, boost::memory_order_acquire);
    if (ref_.load(boost::memory_order_relaxed) <= 0) {
        Release();
        for (uint32_t i = 0; i < seg_cnt_; i++) {
            delete segments_[i];
        }
        delete[] segments_;
        delete this;
    }
}

void Table::BatchGet(const std::vector<std::string>& keys,
                     std::map<uint32_t, DataBlock*>& pairs,
                     Ticket& ticket) {
    // batch get just support one segment
    Segment* segment = segments_[0];
    segment->BatchGet(keys, pairs, ticket);
}

uint64_t Table::Release() {
    uint64_t total_bytes = 0;
    for (uint32_t i = 0; i < seg_cnt_; i++) {
        if (segments_[i] != NULL) {
            total_bytes += segments_[i]->Release();
        }
    }
    return total_bytes;
}

void Table::SetGcSafeOffset(uint64_t offset) {
    ttl_offset_ = offset;
}

uint64_t Table::SchedGc() {
    if (!enable_gc_) {
        return 0;
    }
    LOG(INFO, "table %s start to make a gc", name_.c_str()); 
    uint64_t time = ::baidu::common::timer::get_micros() / 1000 - ttl_offset_ - ttl_ * 60 * 1000; 
    uint64_t count = 0;
    for (uint32_t i = 0; i < seg_cnt_; i++) {
        Segment* segment = segments_[i];
        count += segment->Gc4TTL(time);
    }
    data_cnt_.fetch_sub(count, boost::memory_order_relaxed);
    return count;
}

Table::Iterator::Iterator(Segment::Iterator* it):it_(it){}

Table::Iterator::~Iterator() {
    delete it_;
}

bool Table::Iterator::Valid() const {
    if (it_ == NULL) {
        return false;
    }
    return it_->Valid();
}

void Table::Iterator::Next() {
    it_->Next();
}

void Table::Iterator::Seek(const uint64_t& time) {
    if (it_ == NULL) {
        return;
    }
    it_->Seek(time);
}

void Table::Iterator::SeekToFirst() {
    if (it_ == NULL) {
        return;
    }
    it_->SeekToFirst();
}

DataBlock* Table::Iterator::GetValue() const {
    return it_->GetValue();
}

uint64_t Table::Iterator::GetKey() const {
    return it_->GetKey();
}

Table::Iterator* Table::NewIterator(const std::string& pk, Ticket& ticket) {
    uint32_t index = 0;
    if (seg_cnt_ > 1) {
        index = ::rtidb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Segment* segment = segments_[index];
    return new Table::Iterator(segment->NewIterator(pk, ticket));
}

}
}


