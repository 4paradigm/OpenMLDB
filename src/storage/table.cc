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

Table::Table(const std::string& name,
        uint32_t id,
        uint32_t pid,
        uint32_t seg_cnt,
        const std::map<std::string, uint32_t>& mapping,
        uint64_t ttl,
        bool is_leader,
        const std::vector<std::string>& replicas):name_(name), id_(id),
    pid_(pid), seg_cnt_(seg_cnt),idx_cnt_(mapping.size()),
    segments_(NULL), 
    ref_(0), enable_gc_(false), ttl_(ttl * 60 * 1000),
    ttl_offset_(60 * 1000), record_cnt_(0), is_leader_(is_leader), time_offset_(0),
    replicas_(replicas), table_status_(kUndefined), schema_(),
    mapping_(mapping), segment_released_(false)
{}

Table::Table(const std::string& name,
        uint32_t id,
        uint32_t pid,
        uint32_t seg_cnt,
        const std::map<std::string, uint32_t>& mapping,
        uint64_t ttl):name_(name), id_(id),
    pid_(pid), seg_cnt_(seg_cnt),idx_cnt_(mapping.size()),
    segments_(NULL), 
    ref_(0), enable_gc_(false), ttl_(ttl * 60 * 1000),
    ttl_offset_(60 * 1000), record_cnt_(0), is_leader_(false), time_offset_(0),
    replicas_(), table_status_(kUndefined), schema_(),
    mapping_(mapping), segment_released_(false)
{}

Table::~Table() {
    Release();
    for (uint32_t i = 0; i < idx_cnt_; i++) {
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            delete segments_[i][j];
        }
        delete[] segments_[i];
    }
    delete[] segments_;
}

void Table::Init() {
    segments_ = new Segment**[idx_cnt_];
    for (uint32_t i = 0; i < idx_cnt_; i++) {
        segments_[i] = new Segment*[seg_cnt_];
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            segments_[i][j] = new Segment();
            LOG(DEBUG, "init %u, %u segment", i, j);
        }
    }
    if (ttl_ > 0) {
        enable_gc_ = true;
    }
    LOG(INFO, "init table name %s, id %d, pid %d, idx_cnt %u seg_cnt %u , ttl %d", name_.c_str(),
            id_, pid_, idx_cnt_, seg_cnt_, ttl_ / (60 * 1000));
}

bool Table::Put(const std::string& pk, 
                uint64_t time,
                const char* data, 
                uint32_t size) {
    uint32_t index = 0;
    if (seg_cnt_ > 1) {
        index = ::rtidb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Segment* segment = segments_[0][index];
    segment->Put(pk, time, data, size);
    return true;
}

bool Table::Put(const std::string& pk,
                uint64_t time, 
                DataBlock* row,
                uint32_t idx) {
    if (idx >= idx_cnt_) {
        return false;
    }
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::rtidb::base::hash(pk.c_str(), pk.size(), SEED) % seg_cnt_;
    }
    Segment* segment = segments_[idx][seg_idx];
    segment->Put(pk, time, row);
    LOG(DEBUG, "add row to index %u with value %s for tid %u pid %u ok", idx,
               pk.c_str(), id_, pid_);
    return true;
}

bool Table::Put(uint64_t time, 
                DataBlock* row,
                const std::vector<std::pair<uint32_t, std::string> >& indexes) {
    std::vector<std::pair<uint32_t, std::string> >::const_iterator it = indexes.begin();
    for (; it != indexes.end(); ++it) {
        if (!Put(it->second, time, row, it->first)) {
            return false;
        }
    }
    return true;
}

uint64_t Table::Release() {
    if (segment_released_) {
        return 0;
    }
    uint64_t total_cnt = 0;
    for (uint32_t i = 0; i < idx_cnt_; i++) {
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            if (segments_[i] != NULL) {
                total_cnt += segments_[i][j]->Release();
            } 
        }
    }
    segment_released_ = true;
    return total_cnt;
}

void Table::SetGcSafeOffset(uint64_t offset) {
    ttl_offset_ = offset;
}

uint64_t Table::SchedGc() {
    if (!enable_gc_.load(boost::memory_order_relaxed)) {
        return 0;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    LOG(INFO, "start making gc for table %s, tid %u, pid %u", name_.c_str(),
            id_, pid_); 
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    uint64_t time = cur_time + time_offset_.load(boost::memory_order_relaxed) - ttl_offset_ - ttl_;
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    for (uint32_t i = 0; i < idx_cnt_; i++) {
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            Segment* segment = segments_[i][j];
            segment->Gc4TTL(time, gc_idx_cnt, gc_record_cnt);
        }
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;
    record_cnt_.fetch_sub(gc_record_cnt, boost::memory_order_relaxed);
    LOG(INFO, "gc finished, gc_idx_cnt %lu, gc_record_cnt %lu consumed %lu ms for table %s tid %u pid %u",
            gc_idx_cnt, gc_record_cnt, consumed / 1000, name_.c_str(), id_, pid_);
    return gc_record_cnt;
}

bool Table::IsExpired(const ::rtidb::api::LogEntry& entry, uint64_t cur_time) {
    if (!enable_gc_.load(boost::memory_order_relaxed) || ttl_ == 0) {
        return false;
    }
    uint64_t time = cur_time + time_offset_.load(boost::memory_order_relaxed) - ttl_offset_ - ttl_;
    if (entry.ts() < time) {
        return true;
    }
    return false;
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
    return NewIterator(0, pk, ticket); 
}

Table::Iterator* Table::NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) {
    if (index >= idx_cnt_) {
        LOG(WARNING, "invalid idx %u, the max idx cnt %u", index, idx_cnt_);
        return NULL;
    }
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::rtidb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Segment* segment = segments_[index][seg_idx];
    return new Table::Iterator(segment->NewIterator(pk, ticket));
}

}
}


