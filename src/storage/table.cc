//
// table.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//
//
#include "storage/table.h"

#include "base/hash.h"
#include "base/slice.h"
#include "storage/record.h"
#include "logging.h"
#include "timer.h"
#include <gflags/gflags.h>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_string(db_root_path);
DECLARE_uint32(skiplist_max_height);


namespace rtidb {
namespace storage {

const static uint32_t SEED = 0xe17a1465;

Table::Table(const std::string& name,
        uint32_t id,
        uint32_t pid,
        uint32_t seg_cnt,
        const std::map<std::string, uint32_t>& mapping,
        uint64_t ttl,
        bool is_leader,
        const std::vector<std::string>& replicas,
        uint32_t key_entry_max_height):name_(name), id_(id),
    pid_(pid), seg_cnt_(seg_cnt),idx_cnt_(mapping.size()),
    segments_(NULL), 
    enable_gc_(false), ttl_(ttl * 60 * 1000),
    ttl_offset_(60 * 1000), record_cnt_(0), is_leader_(is_leader), time_offset_(0),
    replicas_(replicas), table_status_(kUndefined), schema_(),
    mapping_(mapping), segment_released_(false), record_byte_size_(0), ttl_type_(::rtidb::api::TTLType::kAbsoluteTime),
    compress_type_(::rtidb::api::CompressType::kNoCompress), key_entry_max_height_(key_entry_max_height)
{}

Table::Table(const std::string& name,
        uint32_t id,
        uint32_t pid,
        uint32_t seg_cnt,
        const std::map<std::string, uint32_t>& mapping,
        uint64_t ttl):name_(name), id_(id),
    pid_(pid), seg_cnt_(seg_cnt),idx_cnt_(mapping.size()),
    segments_(NULL), 
    enable_gc_(false), ttl_(ttl * 60 * 1000),
    ttl_offset_(60 * 1000), record_cnt_(0), is_leader_(false), time_offset_(0),
    replicas_(), table_status_(kUndefined), schema_(),
    mapping_(mapping), segment_released_(false),ttl_type_(::rtidb::api::TTLType::kAbsoluteTime),
    compress_type_(::rtidb::api::CompressType::kNoCompress), key_entry_max_height_(FLAGS_skiplist_max_height)
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
            segments_[i][j] = new Segment(key_entry_max_height_);
            PDLOG(INFO, "init %u, %u segment. height %u", i, j, key_entry_max_height_);
        }
    }
    if (ttl_ > 0) {
        enable_gc_ = true;
    }
    PDLOG(INFO, "init table name %s, id %d, pid %d, seg_cnt %d , ttl %d", name_.c_str(),
            id_, pid_, seg_cnt_, ttl_ / (60 * 1000));
}

void Table::SetCompressType(::rtidb::api::CompressType compress_type) {
    compress_type_ = compress_type;
}

::rtidb::api::CompressType Table::GetCompressType() {
    return compress_type_;
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
    Slice spk(pk);
    segment->Put(spk, time, data, size);
    record_cnt_.fetch_add(1, std::memory_order_relaxed);
    record_byte_size_.fetch_add(GetRecordSize(size));
    PDLOG(DEBUG, "record key %s, value %s tid %u pid %u index %u", pk.c_str(), data, id_, pid_, index);
    return true;
}

// Put a multi dimension record
bool Table::Put(uint64_t time, 
                const std::string& value,
                const Dimensions& dimensions) {
    DataBlock* block = new DataBlock(dimensions.size(), 
                                     value.c_str(), 
                                     value.length());
    Dimensions::const_iterator it = dimensions.begin();
    for (;it != dimensions.end(); ++it) {
        Slice spk(it->key());
        bool ok = Put(spk, time, block, it->idx());
        // decr the data block dimension count
        if (!ok) {
            block->dim_cnt_down --;
            PDLOG(WARNING, "failed putting key %s to dimension %u in table tid %u pid %u",
                    it->key().c_str(), it->idx(), id_, pid_);
        }
    }
    if (block->dim_cnt_down<= 0) {
        delete block;
        return false;
    }
    record_cnt_.fetch_add(1, std::memory_order_relaxed);
    record_byte_size_.fetch_add(GetRecordSize(value.length()));
    return true;
}


bool Table::Put(const Slice& pk,
                uint64_t time, 
                DataBlock* row,
                uint32_t idx) {
    if (idx >= idx_cnt_) {
        return false;
    }
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::rtidb::base::hash(pk.data(), pk.size(), SEED) % seg_cnt_;
    }
    Segment* segment = segments_[idx][seg_idx];
    segment->Put(pk, time, row);
    PDLOG(DEBUG, "add row to index %u with pk %s value size %u for tid %u pid %u ok", idx,
               pk.data(), row->size, id_, pid_);
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
    if (!enable_gc_.load(std::memory_order_relaxed)) {
        return 0;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    PDLOG(INFO, "start making gc for table %s, tid %u, pid %u with type %s ttl %lu", name_.c_str(),
            id_, pid_, ::rtidb::api::TTLType_Name(ttl_type_).c_str(), ttl_.load(std::memory_order_relaxed)); 
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    uint64_t time = cur_time + time_offset_.load(std::memory_order_relaxed) - ttl_offset_ - ttl_.load(std::memory_order_relaxed);
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    for (uint32_t i = 0; i < idx_cnt_; i++) {
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            uint64_t seg_gc_time = ::baidu::common::timer::get_micros() / 1000;
            Segment* segment = segments_[i][j];
            switch (ttl_type_) {
            case ::rtidb::api::TTLType::kAbsoluteTime:
                segment->Gc4TTL(time, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
                break;
            case ::rtidb::api::TTLType::kLatestTime:
                segment->Gc4Head(ttl_.load(std::memory_order_relaxed) / 60 / 1000, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
                break;
            default:
                PDLOG(WARNING, "not supported ttl type %s", ::rtidb::api::TTLType_Name(ttl_type_).c_str());
            }
            seg_gc_time = ::baidu::common::timer::get_micros() / 1000 - seg_gc_time;
            PDLOG(INFO, "gc segment[%u][%u] done consumed %lu for table %s tid %u pid %u", i, j, seg_gc_time, name_.c_str(), id_, pid_);
        }
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;
    record_cnt_.fetch_sub(gc_record_cnt, std::memory_order_relaxed);
    record_byte_size_.fetch_sub(gc_record_byte_size, std::memory_order_relaxed);
    PDLOG(INFO, "gc finished, gc_idx_cnt %lu, gc_record_cnt %lu consumed %lu ms for table %s tid %u pid %u",
            gc_idx_cnt, gc_record_cnt, consumed / 1000, name_.c_str(), id_, pid_);
    return gc_record_cnt;
}

uint64_t Table::GetExpireTime() {
    if (!enable_gc_.load(std::memory_order_relaxed) || ttl_.load(std::memory_order_relaxed) == 0 
            || ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        return 0;
    }
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    return cur_time + time_offset_.load(std::memory_order_relaxed) - ttl_.load(std::memory_order_relaxed);
}

bool Table::IsExpire(const LogEntry& entry) {
    if (!enable_gc_.load(std::memory_order_relaxed) || ttl_.load(std::memory_order_relaxed) == 0) { 
        return false;
    }
    uint64_t expired_time = 0;
    if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        if (entry.dimensions_size() > 0) {
            for (auto iter = entry.dimensions().begin(); iter != entry.dimensions().end(); ++iter) {
                ::rtidb::storage::Ticket ticket;
                ::rtidb::storage::Iterator* it = NewIterator(iter->idx(), iter->key(), ticket);
                it->SeekToLast();
                if (it->Valid()) {
                    if (expired_time == 0) {
                        expired_time = it->GetKey();
                    } else {
                        expired_time = std::min(expired_time, it->GetKey());
                    }
                }
                delete it;
            }
        } else {
            ::rtidb::storage::Ticket ticket;
            ::rtidb::storage::Iterator* it = NewIterator(entry.pk(), ticket);
            it->SeekToLast();
            if (it->Valid()) {
                expired_time = it->GetKey();
            }
            delete it;
        }
    } else {
        expired_time = GetExpireTime();
    }
    return entry.ts() < expired_time;
}


Iterator* Table::NewIterator(const std::string& pk, Ticket& ticket) {
    return NewIterator(0, pk, ticket); 
}

Iterator* Table::NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) {
    if (index >= idx_cnt_) {
        PDLOG(WARNING, "invalid idx %u, the max idx cnt %u", index, idx_cnt_);
        return NULL;
    }
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::rtidb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Slice spk(pk);
    Segment* segment = segments_[index][seg_idx];
    return segment->NewIterator(spk, ticket);
}

uint64_t Table::GetRecordIdxByteSize() {
    uint64_t record_idx_byte_size = 0;
    for (uint32_t i = 0; i < idx_cnt_; i++) {
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            record_idx_byte_size += segments_[i][j]->GetIdxByteSize(); 
        }
    }
    return record_idx_byte_size;
}

uint64_t Table::GetRecordIdxCnt() {
    uint64_t record_idx_cnt = 0;
    for (uint32_t i = 0; i < idx_cnt_; i++) {
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            record_idx_cnt += segments_[i][j]->GetIdxCnt(); 
        }
    }
    return record_idx_cnt;
}

uint64_t Table::GetRecordPkCnt() {
    uint64_t record_pk_cnt = 0;
    for (uint32_t i = 0; i < idx_cnt_; i++) {
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            record_pk_cnt += segments_[i][j]->GetPkCnt(); 
        }
    }
    return record_pk_cnt;
}

bool Table::GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size) {
    if (stat == NULL) {
        return false;
    }
    if (idx >= idx_cnt_) {
        return false;
    }
    uint64_t* data_array = new uint64_t[seg_cnt_];
    for (uint32_t i = 0; i < seg_cnt_; i++) {
        data_array[i] = segments_[idx][i]->GetIdxCnt();
    }
    *stat = data_array;
    *size = seg_cnt_;
    return true;
}

TableIterator* Table::NewTableIterator(uint32_t index) {
    uint64_t expire_value = 0;
    if (!enable_gc_.load(std::memory_order_relaxed) || ttl_ == 0) {
        expire_value = 0;
    } else if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        expire_value = ttl_ / 60 / 1000;
    } else {
        expire_value = GetExpireTime();
    }
    return new TableIterator(segments_[index], seg_cnt_, ttl_type_, expire_value);
}

TableIterator::TableIterator(Segment** segments, uint32_t seg_cnt, 
            ::rtidb::api::TTLType ttl_type, uint64_t expire_value) : segments_(segments),
        seg_cnt_(seg_cnt), seg_idx_(0), pk_it_(NULL), it_(NULL), 
        ttl_type_(ttl_type), record_idx_(0), expire_value_(expire_value), ticket_() {}

TableIterator::~TableIterator() {
    if (pk_it_ != NULL) delete pk_it_;
    if (it_ != NULL) delete it_;
}

bool TableIterator::Valid() const {
    return pk_it_ != NULL && pk_it_->Valid() && it_ != NULL && it_->Valid();
}

void TableIterator::Next() {
    it_->Next();
    record_idx_++;
    if (!it_->Valid() || IsExpired()) {
        NextPK();
        return;
    }
}

bool TableIterator::IsExpired() {
    if (expire_value_ == 0) {
        return false;
    }
    if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
        return record_idx_ > expire_value_;       
    }
    return it_->GetKey() < expire_value_;
}

void TableIterator::NextPK() {
    delete it_;
    it_ = NULL;
    do { 
        ticket_.Pop();
        if (pk_it_->Valid()) {
            pk_it_->Next();
        }
        if (!pk_it_->Valid()) {
            delete pk_it_;
            pk_it_ = NULL;
            seg_idx_++;
            if (seg_idx_ < seg_cnt_) {
                pk_it_ = segments_[seg_idx_]->GetKeyEntries()->NewIterator();
                pk_it_->SeekToFirst();
                if (!pk_it_->Valid()) {
                    continue;
                }
            } else {
                break;
            }
        }
        if (it_ != NULL) {
            delete it_;
        }
        it_ = pk_it_->GetValue()->entries.NewIterator();
        ticket_.Push(pk_it_->GetValue());
        it_->SeekToFirst();
        record_idx_ = 1;
    } while(it_ == NULL || !it_->Valid() || IsExpired());
}

void TableIterator::Seek(const std::string& key, uint64_t ts) {
    if (pk_it_ != NULL) {
        delete pk_it_;
        pk_it_ = NULL;
    }
    if (it_ != NULL) {
        delete it_;
        it_ = NULL;
    }
    ticket_.Pop();
    if (seg_cnt_ > 1) {
        seg_idx_ = ::rtidb::base::hash(key.c_str(), key.length(), SEED) % seg_cnt_;
    }
    Slice spk(key);
    pk_it_ = segments_[seg_idx_]->GetKeyEntries()->NewIterator();
    pk_it_->Seek(spk);
    if (pk_it_->Valid() && spk.compare(pk_it_->GetKey()) == 0) {
        ticket_.Push(pk_it_->GetValue());
        it_ = pk_it_->GetValue()->entries.NewIterator();
        if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
            it_->SeekToFirst();
            record_idx_ = 1;
            while(it_->Valid() && record_idx_ <= expire_value_) {
                if (it_->GetKey() < ts) {
                    PDLOG(DEBUG, "cur pk %s ts %lu idx %u record_idx_ %u", key.c_str(), it_->GetKey(), seg_idx_, record_idx_);
                    return;
                }
                it_->Next();
                record_idx_++;
            }
            NextPK();
        } else {
            it_->Seek(ts);
            if (it_->Valid() && it_->GetKey() == ts) {
                PDLOG(DEBUG, "pk %s ts %lu has found, skip to next", key.c_str(), it_->GetKey());
                it_->Next();
            }
            if (!it_->Valid() || IsExpired()) {
                PDLOG(DEBUG, "skip to next pk, cur pk %s idx %u", key.c_str(), seg_idx_);
                NextPK();
            }
        }
    } else {
        PDLOG(DEBUG, "has not found pk %s. seg_idx[%u]", key.c_str(), seg_idx_);
        delete pk_it_;
        pk_it_ = NULL;
    }
}

DataBlock* TableIterator::GetValue() const {
    return it_->GetValue();
}

uint64_t TableIterator::GetKey() const {
    return it_->GetKey();
}

std::string TableIterator::GetPK() const {
    return pk_it_->GetKey().ToString();
}

void TableIterator::SeekToFirst() {
    ticket_.Pop();
    if (pk_it_ != NULL) {
        delete pk_it_;
        pk_it_ = NULL;
    }
    if (it_ != NULL) {
        delete it_;
        it_ = NULL;
    }
    for (seg_idx_ = 0; seg_idx_ < seg_cnt_; seg_idx_++) {
        pk_it_ = segments_[seg_idx_]->GetKeyEntries()->NewIterator();
        pk_it_->SeekToFirst();
        while (pk_it_->Valid()) {
            ticket_.Push(pk_it_->GetValue());
            it_ = pk_it_->GetValue()->entries.NewIterator();
            it_->SeekToFirst();
            if (it_->Valid() && !IsExpired()) {
                record_idx_ = 1;
                return;
            }
            delete it_;
            it_ = NULL;
            pk_it_->Next();
            ticket_.Pop();
        }
        delete pk_it_;
        pk_it_ = NULL;
    }
}

}
}


