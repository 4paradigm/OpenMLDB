//
// table.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "logging.h"
#include "storage/table.h"
#include "base/hash.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace storage {

const static uint32_t SEED = 9527;
Table::Table(const std::string& name,
        uint32_t id,
        uint32_t pid,
        uint32_t seg_cnt):name_(name), id_(id),
    pid_(pid), seg_cnt_(seg_cnt), segments_(NULL), ref_(0), enable_gc_(false){}

void Table::Init() {
    segments_ = new Segment*[seg_cnt_];
    for (uint32_t i = 0; i < seg_cnt_; i++) {
        segments_[i] = new Segment();
    }
    LOG(INFO, "init table name %s, id %d, pid %d, seg_cnt %d", name_.c_str(),
            id_, pid_, seg_cnt_);
}

void Table::Put(const std::string& pk, const uint64_t& time,
        const char* data, uint32_t size) {
    uint32_t index = ::rtidb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    Segment* segment = segments_[index];
    segment->Put(pk, time, data, size);
}

void Table::Ref() {
    ref_.fetch_add(1, boost::memory_order_relaxed);
}

void Table::UnRef() {
    ref_.fetch_sub(1, boost::memory_order_acquire);
    if (ref_.load(boost::memory_order_relaxed) <= 0) {
        delete this;
    }
}

Table::Iterator::Iterator(Segment::Iterator* it):it_(it){

}

Table::Iterator::~Iterator() {

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


