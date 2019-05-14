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
#include <algorithm>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_string(db_root_path);
DECLARE_uint32(skiplist_max_height);
DECLARE_int32(gc_safe_offset);
DECLARE_uint32(skiplist_max_height);
DECLARE_uint32(key_entry_max_height);
DECLARE_uint32(absolute_default_skiplist_height);
DECLARE_uint32(latest_default_skiplist_height);

namespace rtidb {
namespace storage {

const static uint32_t SEED = 0xe17a1465;

Table::Table(const std::string& name,
        uint32_t id,
        uint32_t pid,
        uint32_t seg_cnt,
        const std::map<std::string, uint32_t>& mapping,
        uint64_t ttl):name_(name), id_(id),
    pid_(pid), seg_cnt_(seg_cnt),
    segments_(NULL), 
    enable_gc_(false), ttl_(ttl * 60 * 1000),
    ttl_offset_(60 * 1000), record_cnt_(0), is_leader_(true), time_offset_(0),
    table_status_(kUndefined), schema_(),
    mapping_(mapping), segment_released_(false), record_byte_size_(0), 
    ttl_type_(::rtidb::api::TTLType::kAbsoluteTime),
    compress_type_(::rtidb::api::CompressType::kNoCompress)
{
    new_ttl_.store(ttl_.load());
}

Table::Table(const ::rtidb::api::TableMeta& table_meta) : name_(table_meta.name()), id_(table_meta.tid()), 
    pid_(table_meta.pid()), seg_cnt_(8), 
    segments_(NULL), 
    enable_gc_(false),
    ttl_offset_(60 * 1000), record_cnt_(0), is_leader_(true), time_offset_(0),
    segment_released_(false), record_byte_size_(0), 
    ttl_type_(::rtidb::api::TTLType::kAbsoluteTime),
    compress_type_(::rtidb::api::CompressType::kNoCompress) {
    table_meta_.CopyFrom(table_meta);
}

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

int Table::InitColumnDesc() {
	if (table_meta_.column_desc_size() > 0) {
        uint32_t key_idx = 0;
        uint32_t ts_idx = 0;
        for (const auto& column_desc : table_meta_.column_desc()) {
            if (column_desc.add_ts_idx()) {
                mapping_.insert(std::make_pair(column_desc.name(), key_idx));
                key_idx++;
            } else if (column_desc.is_ts_col()) {
                ts_mapping_.insert(std::make_pair(column_desc.name(), ts_idx));
                if (column_desc.has_ttl()) {
                    ttl_map_.insert(std::make_pair(ts_idx, column_desc.ttl() * 60 * 1000));
                } else {
                    ttl_map_.insert(std::make_pair(ts_idx, table_meta_.ttl() * 60 * 1000));
                }
                ts_idx++;
            }
        }
        if (ts_mapping_.size() > 1 && !mapping_.empty()) {
            mapping_.clear();
            key_idx = 0;
        }
        if (table_meta_.column_key_size() > 0) {
            for (const auto& column_key : table_meta_.column_key()) {
                uint32_t cur_key_idx = key_idx;
                if (column_key.key_name_size() == 0) {
                    continue;
                } else if (column_key.key_name_size() == 1) {
                    std::string name = column_key.key_name(0);
                    auto it = mapping_.find(name);
                    if (it == mapping_.end()) {
                        mapping_.insert(std::make_pair(name, key_idx));
                        key_idx++;
                    } else {
                        cur_key_idx = it->second;
                    }
                } else {
                    std::string combined_key;
                    for (const auto& name : column_key.key_name()) {
                        if (combined_key.empty()) {
                            combined_key = name;
                        } else {
                            combined_key += "|" + name;
                        }
                    }
                    auto it = mapping_.find(combined_key);
                    if (it == mapping_.end()) {
                        mapping_.insert(std::make_pair(combined_key, key_idx));
                        key_idx++;
                    } else {
                        cur_key_idx = it->second;
                    }
                }
				if (ts_mapping_.empty()) {
					continue;
				}
                auto ts_iter = ts_mapping_.find(column_key.ts_name());
                if (ts_iter == ts_mapping_.end()) {
                    PDLOG(WARNING, "not found ts_name[%s]. tid %u pid %u",
                                    column_key.ts_name().c_str(), id_, pid_);
                    return -1;
                }
                if (column_key_map_.find(cur_key_idx) == column_key_map_.end()) {
                    column_key_map_.insert(std::make_pair(cur_key_idx, std::vector<uint32_t>()));
                }
                if (std::find(column_key_map_[cur_key_idx].begin(), column_key_map_[cur_key_idx].end(), 
                            ts_iter->second) == column_key_map_[cur_key_idx].end()) {
                    column_key_map_[cur_key_idx].push_back(ts_iter->second);
                }
            }
        } else {
            if (ts_mapping_.size() > 1) {
                PDLOG(WARNING, "column_key should be set when has two or more ts columns. tid %u pid %u",
                                id_, pid_);
                return -1;
            } else if (!ts_mapping_.empty()) {
				for (const auto& kv : mapping_) {
					uint32_t cur_key_idx = kv.second;
					if (column_key_map_.find(cur_key_idx) == column_key_map_.end()) {
						column_key_map_.insert(std::make_pair(cur_key_idx, std::vector<uint32_t>()));
					}
					uint32_t cur_ts_idx = ts_mapping_.begin()->second;
					if (std::find(column_key_map_[cur_key_idx].begin(), column_key_map_[cur_key_idx].end(), 
								cur_ts_idx) == column_key_map_[cur_key_idx].end()) {
						column_key_map_[cur_key_idx].push_back(cur_ts_idx);
					}
				}
			}
        }
	} else {
		for (int32_t i = 0; i < table_meta_.dimensions_size(); i++) {
			mapping_.insert(std::make_pair(table_meta_.dimensions(i), (uint32_t)i));
			PDLOG(INFO, "add index name %s, idx %d to table %s, tid %u, pid %u", 
                        table_meta_.dimensions(i).c_str(), i, table_meta_.name().c_str(), id_, pid_);
		}
	}
    // add default dimension
    if (mapping_.empty()) {
        mapping_.insert(std::make_pair("idx0", 0));
        PDLOG(INFO, "no index specified with default");
    }
    return 0;
}

int Table::Init() {
    key_entry_max_height_ = FLAGS_key_entry_max_height;
    ttl_offset_ = FLAGS_gc_safe_offset * 60 * 1000;
    if (table_meta_.seg_cnt() > 0) {
        seg_cnt_ = table_meta_.seg_cnt();
    }
    if (table_meta_.has_mode() && table_meta_.mode() != ::rtidb::api::TableMode::kTableLeader) {
        is_leader_ = false;
    }
    if (InitColumnDesc() < 0) {
        return -1;
    }
    if (table_meta_.has_ttl()) {
        ttl_ = table_meta_.ttl() * 60 * 1000;
        new_ttl_.store(ttl_.load());
    }    
    if (table_meta_.has_schema()) schema_ = table_meta_.schema();
    if (table_meta_.has_ttl_type()) ttl_type_ = table_meta_.ttl_type();
    if (table_meta_.has_compress_type()) compress_type_ = table_meta_.compress_type();
    if (table_meta_.has_key_entry_max_height() && table_meta_.key_entry_max_height() <= FLAGS_skiplist_max_height
            && table_meta_.key_entry_max_height() > 0) {
        key_entry_max_height_ = table_meta_.key_entry_max_height();
    } else {
        if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
            key_entry_max_height_ = FLAGS_latest_default_skiplist_height;
        } else if (ttl_type_ == ::rtidb::api::TTLType::kAbsoluteTime) {
            key_entry_max_height_ = FLAGS_absolute_default_skiplist_height;
        }
    }
    idx_cnt_ = mapping_.size();

    segments_ = new Segment**[idx_cnt_];
    for (uint32_t i = 0; i < idx_cnt_; i++) {
        segments_[i] = new Segment*[seg_cnt_];
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            if (column_key_map_.find(i) != column_key_map_.end()) {
                segments_[i][j] = new Segment(key_entry_max_height_, column_key_map_[i]);
                PDLOG(INFO, "init %u, %u segment. height %u, ts col num %u", 
                            i, j, key_entry_max_height_, column_key_map_[i].size());
            } else {
                segments_[i][j] = new Segment(key_entry_max_height_);
                PDLOG(INFO, "init %u, %u segment. height %u", 
                            i, j, key_entry_max_height_);
            }
        }
    }
    if (ttl_ > 0) {
        enable_gc_ = true;
    }
    PDLOG(INFO, "init table name %s, id %d, pid %d, seg_cnt %d , ttl %d", name_.c_str(),
                id_, pid_, seg_cnt_, ttl_ / (60 * 1000));
    return 0;
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

bool Table::Put(const Dimensions& dimensions,
                const TSDimensions& ts_dimemsions,
                const std::string& value) {
    if (dimensions.size() == 0 || ts_dimemsions.size() == 0) {
        PDLOG(WARNING, "empty dimesion. tid %u pid %u", id_, pid_);
        return false;
    }
    uint32_t real_ref_cnt = 0;
    for (auto iter = dimensions.begin(); iter != dimensions.end(); iter++) {
        auto pos = column_key_map_.find(iter->idx());
        if (pos == column_key_map_.end()) {
            PDLOG(WARNING, "can not found dimesion idx %u. tid %u pid %u", 
                            iter->idx(), id_, pid_);
            return false;
        }
        bool has_ts = false;
        for (auto ts_idx : pos->second) {
            for (auto it = ts_dimemsions.begin(); it != ts_dimemsions.end(); it++) {
                if (it->idx() == ts_idx) {
                    real_ref_cnt++;
                    has_ts = true;
                    break;
                }
            }
        }
        if (!has_ts) {
            PDLOG(WARNING, "can not found ts in dimesion idx %u. tid %u pid %u", 
                            iter->idx(), id_, pid_);
            return false;
        }
    }
    DataBlock* block = new DataBlock(real_ref_cnt, value.c_str(), value.length());
    for (auto it = dimensions.begin(); it != dimensions.end(); it++) {
        Slice spk(it->key());
        uint32_t seg_idx = 0;
        if (seg_cnt_ > 1) {
            seg_idx = ::rtidb::base::hash(spk.data(), spk.size(), SEED) % seg_cnt_;
        }
        Segment* segment = segments_[it->idx()][seg_idx];
        segment->Put(spk, ts_dimemsions, block);
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
    return true;
}

bool Table::Delete(const std::string& pk, uint32_t idx) {
    if (idx >= idx_cnt_) {
        return false;
    }
    Slice spk(pk);
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::rtidb::base::hash(spk.data(), spk.size(), SEED) % seg_cnt_;
    }
    Segment* segment = segments_[idx][seg_idx];
    return segment->Delete(spk);
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

uint64_t Table::SchedGc() {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    PDLOG(INFO, "start making gc for table %s, tid %u, pid %u with type %s ttl %lu", name_.c_str(),
            id_, pid_, ::rtidb::api::TTLType_Name(ttl_type_).c_str(), ttl_.load(std::memory_order_relaxed)); 
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    uint64_t time = cur_time + time_offset_.load(std::memory_order_relaxed) - ttl_offset_ - ttl_.load(std::memory_order_relaxed);
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    for (uint32_t i = 0; i < idx_cnt_; i++) {
        std::map<uint32_t, uint64_t> cur_ttl_map;
        if (!column_key_map_.empty()) {
            auto pos = column_key_map_.find(i);
            if (pos == column_key_map_.end()) {
                continue;
            }
            for (auto ts_idx : pos->second) {
                auto in_pos = ttl_map_.find(ts_idx);
                if (in_pos == ttl_map_.end()) {
                    continue;
                }
                if (ttl_type_ == ::rtidb::api::TTLType::kAbsoluteTime) {
                    cur_ttl_map.insert(std::make_pair(ts_idx, 
                                cur_time + time_offset_.load(std::memory_order_relaxed) - ttl_offset_ - in_pos->second));
                } else {
                    cur_ttl_map.insert(std::make_pair(ts_idx, in_pos->second / 60 / 1000));
                }
            }
        }
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            uint64_t seg_gc_time = ::baidu::common::timer::get_micros() / 1000;
            Segment* segment = segments_[i][j];
            segment->IncrGcVersion();
            segment->GcFreeList(gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            if (!enable_gc_.load(std::memory_order_relaxed)) {
                continue;
            }
            switch (ttl_type_) {
                case ::rtidb::api::TTLType::kAbsoluteTime:
                    if (cur_ttl_map.empty()) {
                        segment->Gc4TTL(time, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
                    } else {
                        segment->Gc4TTL(cur_ttl_map, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
                    }
                    break;
                case ::rtidb::api::TTLType::kLatestTime:
                    if (cur_ttl_map.empty()) {
                        segment->Gc4Head(ttl_.load(std::memory_order_relaxed) / 60 / 1000, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
                    } else {
                        segment->Gc4Head(cur_ttl_map, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
                    }
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
    if (ttl_.load(std::memory_order_relaxed) != new_ttl_.load(std::memory_order_relaxed)) {
        PDLOG(INFO, "update ttl form %lu to %lu, table %s tid %u pid %u", 
                    ttl_.load(std::memory_order_relaxed), 
                    new_ttl_.load(std::memory_order_relaxed), 
                    name_.c_str(), id_, pid_);
        ttl_.store(new_ttl_.load(std::memory_order_relaxed), std::memory_order_relaxed);
    }
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

int Table::GetCount(uint32_t index, const std::string& pk, uint64_t& count) {
    if (index >= idx_cnt_) {
        return -1;
    }
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::rtidb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Slice spk(pk);
    Segment* segment = segments_[index][seg_idx];
    return segment->GetCount(spk, count);
}

int Table::GetCount(uint32_t index, uint32_t ts_idx, const std::string& pk, uint64_t& count) {
    if (index >= idx_cnt_) {
        return -1;
    }
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::rtidb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Slice spk(pk);
    Segment* segment = segments_[index][seg_idx];
    return segment->GetCount(spk, ts_idx, count);
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

Iterator* Table::NewIterator(uint32_t index, uint32_t ts_idx, const std::string& pk, Ticket& ticket) {
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
    return segment->NewIterator(spk, ts_idx, ticket);
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
    if (pk_it_->Valid()) {
        ticket_.Push(pk_it_->GetValue());
        it_ = pk_it_->GetValue()->entries.NewIterator();
        if (spk.compare(pk_it_->GetKey()) != 0) {
            it_->SeekToFirst();
            record_idx_ = 1;
            if (!it_->Valid() || IsExpired()) {
                NextPK();
            }
        } else {
            if (ttl_type_ == ::rtidb::api::TTLType::kLatestTime) {
                it_->SeekToFirst();
                record_idx_ = 1;
                while(it_->Valid() && record_idx_ <= expire_value_) {
                    if (it_->GetKey() < ts) {
                        return;
                    }
                    it_->Next();
                    record_idx_++;
                }
                NextPK();
            } else {
                it_->Seek(ts);
                if (it_->Valid() && it_->GetKey() == ts) {
                    it_->Next();
                }
                if (!it_->Valid() || IsExpired()) {
                    NextPK();
                }
            }
        }
    } else {
        NextPK();
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


