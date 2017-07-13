//
// table.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "logging.h"
#include "storage/table.h"
#include "base/hash.h"
#include "timer.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace storage {

const static uint32_t SEED = 9527;
Table::Table(const std::string& name,
        uint32_t id,
        uint32_t pid,
        uint32_t seg_cnt,
        uint32_t ttl,
        bool is_leader,
        const std::vector<std::string>& replicas):name_(name), id_(id),
    pid_(pid), seg_cnt_(seg_cnt),
    segments_(NULL), 
    ref_(0), enable_gc_(false), ttl_(ttl),
    ttl_offset_(60 * 1000), is_leader_(is_leader),
    replicas_(replicas)
{}

Table::Table(const std::string& name,
        uint32_t id,
        uint32_t pid,
        uint32_t seg_cnt,
        uint32_t ttl):name_(name), id_(id),
    pid_(pid), seg_cnt_(seg_cnt),
    segments_(NULL), 
    ref_(0), enable_gc_(false), ttl_(ttl),
    ttl_offset_(60 * 1000), is_leader_(false),
    replicas_()
{}

void Table::Init() {
    segments_ = new Segment*[seg_cnt_];
    for (uint32_t i = 0; i < seg_cnt_; i++) {
        segments_[i] = new Segment();
    }
    if (ttl_ > 0) {
        enable_gc_ = true;
    }
    LOG(INFO, "init table name %s, id %d, pid %d, seg_cnt %d , ttl %d", name_.c_str(),
            id_, pid_, seg_cnt_, ttl_);
}

void Table::Put(const std::string& pk, const uint64_t& time,
        const char* data, uint32_t size) {
    uint32_t index = ::rtidb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
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
        for (uint32_t i = 0; i < seg_cnt_; i++) {
            delete segments_[i];
        }
        delete[] segments_;
        delete this;
    }
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

Table::Iterator* Table::NewIterator(const std::string& pk) {
    uint32_t index = ::rtidb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    Segment* segment = segments_[index];
    return new Table::Iterator(segment->NewIterator(pk));
}

}
}


