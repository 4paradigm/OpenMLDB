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

#include "storage/mem_table.h"

#include <snappy.h>
#include <algorithm>
#include <utility>

#include "base/glog_wrapper.h"
#include "base/hash.h"
#include "base/slice.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "storage/record.h"
#include "storage/window_iterator.h"

DECLARE_uint32(skiplist_max_height);
DECLARE_uint32(skiplist_max_height);
DECLARE_uint32(key_entry_max_height);
DECLARE_uint32(absolute_default_skiplist_height);
DECLARE_uint32(latest_default_skiplist_height);
DECLARE_uint32(max_traverse_cnt);

namespace openmldb {
namespace storage {

static const uint32_t SEED = 0xe17a1465;

MemTable::MemTable(const std::string& name, uint32_t id, uint32_t pid, uint32_t seg_cnt,
                   const std::map<std::string, uint32_t>& mapping, uint64_t ttl, ::openmldb::type::TTLType ttl_type)
    : Table(::openmldb::common::StorageMode::kMemory, name, id, pid, ttl * 60 * 1000, true, 60 * 1000, mapping,
            ttl_type, ::openmldb::type::CompressType::kNoCompress),
      seg_cnt_(seg_cnt),
      segments_(MAX_INDEX_NUM, NULL),
      enable_gc_(true),
      record_cnt_(0),
      segment_released_(false),
      record_byte_size_(0) {}

MemTable::MemTable(const ::openmldb::api::TableMeta& table_meta)
    : Table(table_meta.storage_mode(), table_meta.name(), table_meta.tid(), table_meta.pid(), 0, true, 60 * 1000,
            std::map<std::string, uint32_t>(), ::openmldb::type::TTLType::kAbsoluteTime,
            ::openmldb::type::CompressType::kNoCompress),
      segments_(MAX_INDEX_NUM, NULL) {
    seg_cnt_ = 8;
    enable_gc_ = true;
    record_cnt_ = 0;
    segment_released_ = false;
    record_byte_size_ = 0;
    diskused_ = 0;
    table_meta_ = std::make_shared<::openmldb::api::TableMeta>(table_meta);
}

MemTable::~MemTable() {
    if (segments_.empty()) {
        return;
    }
    Release();
    for (uint32_t i = 0; i < segments_.size(); i++) {
        if (segments_[i] != NULL) {
            for (uint32_t j = 0; j < seg_cnt_; j++) {
                delete segments_[i][j];
            }
            delete[] segments_[i];
        }
    }
    segments_.clear();
    PDLOG(INFO, "drop memtable. tid %u pid %u", id_, pid_);
}

bool MemTable::Init() {
    key_entry_max_height_ = FLAGS_key_entry_max_height;
    if (!InitFromMeta()) {
        return false;
    }
    if (table_meta_->seg_cnt() > 0) {
        seg_cnt_ = table_meta_->seg_cnt();
    }
    uint32_t global_key_entry_max_height = 0;
    if (table_meta_->has_key_entry_max_height() && table_meta_->key_entry_max_height() <= FLAGS_skiplist_max_height &&
        table_meta_->key_entry_max_height() > 0) {
        global_key_entry_max_height = table_meta_->key_entry_max_height();
    }
    auto inner_indexs = table_index_.GetAllInnerIndex();
    for (uint32_t i = 0; i < inner_indexs->size(); i++) {
        const std::vector<uint32_t>& ts_vec = inner_indexs->at(i)->GetTsIdx();
        uint32_t cur_key_entry_max_height = 0;
        if (global_key_entry_max_height > 0) {
            cur_key_entry_max_height = global_key_entry_max_height;
        } else {
            cur_key_entry_max_height = inner_indexs->at(i)->GetKeyEntryMaxHeight(FLAGS_absolute_default_skiplist_height,
                                                                                 FLAGS_latest_default_skiplist_height);
        }
        Segment** seg_arr = new Segment*[seg_cnt_];
        if (!ts_vec.empty()) {
            for (uint32_t j = 0; j < seg_cnt_; j++) {
                seg_arr[j] = new Segment(cur_key_entry_max_height, ts_vec);
                PDLOG(INFO, "init %u, %u segment. height %u, ts col num %u. tid %u pid %u", i, j,
                      cur_key_entry_max_height, ts_vec.size(), id_, pid_);
            }
        } else {
            for (uint32_t j = 0; j < seg_cnt_; j++) {
                seg_arr[j] = new Segment(cur_key_entry_max_height);
                PDLOG(INFO, "init %u, %u segment. height %u tid %u pid %u", i, j, cur_key_entry_max_height, id_, pid_);
            }
        }
        segments_[i] = seg_arr;
        key_entry_max_height_ = cur_key_entry_max_height;
    }
    PDLOG(INFO, "init table name %s, id %d, pid %d, seg_cnt %d", name_.c_str(), id_, pid_, seg_cnt_);
    return true;
}

void MemTable::SetCompressType(::openmldb::type::CompressType compress_type) { compress_type_ = compress_type; }

::openmldb::type::CompressType MemTable::GetCompressType() { return compress_type_; }

bool MemTable::Put(const std::string& pk, uint64_t time, const char* data, uint32_t size) {
    if (segments_.empty()) return false;
    uint32_t index = 0;
    if (seg_cnt_ > 1) {
        index = ::openmldb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Segment* segment = segments_[0][index];
    Slice spk(pk);
    segment->Put(spk, time, data, size);
    record_cnt_.fetch_add(1, std::memory_order_relaxed);
    record_byte_size_.fetch_add(GetRecordSize(size));
    return true;
}

bool MemTable::Put(uint64_t time, const std::string& value, const Dimensions& dimensions) {
    if (dimensions.empty()) {
        PDLOG(WARNING, "empty dimension. tid %u pid %u", id_, pid_);
        return false;
    }
    if (value.length() < codec::HEADER_LENGTH) {
        PDLOG(WARNING, "invalid value. tid %u pid %u", id_, pid_);
        return false;
    }
    std::map<int32_t, Slice> inner_index_key_map;
    for (auto iter = dimensions.begin(); iter != dimensions.end(); iter++) {
        int32_t inner_pos = table_index_.GetInnerIndexPos(iter->idx());
        if (inner_pos < 0) {
            PDLOG(WARNING, "invalid dimension. dimension idx %u, tid %u pid %u", iter->idx(), id_, pid_);
            return false;
        }
        inner_index_key_map.emplace(inner_pos, iter->key());
    }
    uint32_t real_ref_cnt = 0;
    const int8_t* data = reinterpret_cast<const int8_t*>(value.data());
    std::string uncompress_data;
    if (GetCompressType() == openmldb::type::kSnappy) {
        snappy::Uncompress(value.data(), value.size(), &uncompress_data);
        data = reinterpret_cast<const int8_t*>(uncompress_data.data());
    }
    uint8_t version = codec::RowView::GetSchemaVersion(data);
    auto decoder = GetVersionDecoder(version);
    if (decoder == nullptr) {
        PDLOG(WARNING, "invalid schema version %u, tid %u pid %u", version, id_, pid_);
        return false;
    }
    std::map<int32_t, uint64_t> ts_map;
    for (const auto& kv : inner_index_key_map) {
        auto inner_index = table_index_.GetInnerIndex(kv.first);
        if (!inner_index) {
            PDLOG(WARNING, "invalid inner index pos %d. tid %u pid %u", kv.first, id_, pid_);
            return false;
        }
        for (const auto& index_def : inner_index->GetIndex()) {
            auto ts_col = index_def->GetTsColumn();
            if (ts_col) {
                int64_t ts = 0;
                if (ts_col->IsAutoGenTs()) {
                    ts = time;
                } else if (decoder->GetInteger(data, ts_col->GetId(), ts_col->GetType(), &ts) != 0) {
                    PDLOG(WARNING, "get ts failed. tid %u pid %u", id_, pid_);
                    return false;
                }
                if (ts < 0) {
                    PDLOG(WARNING, "ts %ld is negative. tid %u pid %u", ts, id_, pid_);
                    return false;
                }
                ts_map.emplace(ts_col->GetId(), ts);
            }
            if (index_def->IsReady()) {
                real_ref_cnt++;
            }
        }
    }
    if (ts_map.empty()) {
        return false;
    }
    auto* block = new DataBlock(real_ref_cnt, value.c_str(), value.length());
    for (const auto& kv : inner_index_key_map) {
        auto inner_index = table_index_.GetInnerIndex(kv.first);
        bool need_put = false;
        for (const auto& index_def : inner_index->GetIndex()) {
            if (index_def->IsReady()) {
                // TODO(hw): if we don't find this ts(has_found_ts==false), but it's ready, will put too?
                need_put = true;
                break;
            }
        }
        if (need_put) {
            uint32_t seg_idx = 0;
            if (seg_cnt_ > 1) {
                seg_idx = ::openmldb::base::hash(kv.second.data(), kv.second.size(), SEED) % seg_cnt_;
            }
            Segment* segment = segments_[kv.first][seg_idx];
            segment->Put(::openmldb::base::Slice(kv.second), ts_map, block);
        }
    }
    record_cnt_.fetch_add(1, std::memory_order_relaxed);
    record_byte_size_.fetch_add(GetRecordSize(value.length()));
    return true;
}

bool MemTable::Delete(const std::string& pk, uint32_t idx) {
    std::shared_ptr<IndexDef> index_def = GetIndex(idx);
    if (!index_def || !index_def->IsReady()) {
        return false;
    }
    Slice spk(pk);
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::openmldb::base::hash(spk.data(), spk.size(), SEED) % seg_cnt_;
    }
    uint32_t real_idx = index_def->GetInnerPos();
    Segment* segment = segments_[real_idx][seg_idx];
    return segment->Delete(spk);
}

uint64_t MemTable::Release() {
    if (segment_released_) {
        return 0;
    }
    if (segments_.empty()) {
        return 0;
    }
    uint64_t total_cnt = 0;
    for (uint32_t i = 0; i < segments_.size(); i++) {
        if (segments_[i] != NULL) {
            for (uint32_t j = 0; j < seg_cnt_; j++) {
                total_cnt += segments_[i][j]->Release();
            }
        }
    }
    segment_released_ = true;
    segments_.clear();
    return total_cnt;
}

void MemTable::SchedGc() {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    PDLOG(INFO, "start making gc for table %s, tid %u, pid %u", name_.c_str(), id_, pid_);
    uint64_t gc_idx_cnt = 0;
    uint64_t gc_record_cnt = 0;
    uint64_t gc_record_byte_size = 0;
    auto inner_indexs = table_index_.GetAllInnerIndex();
    for (uint32_t i = 0; i < inner_indexs->size(); i++) {
        const std::vector<std::shared_ptr<IndexDef>>& real_index = inner_indexs->at(i)->GetIndex();
        std::map<uint32_t, TTLSt> ttl_st_map;
        bool need_gc = true;
        size_t deleted_num = 0;
        for (size_t pos = 0; pos < real_index.size(); pos++) {
            auto cur_index = real_index[pos];
            auto ts_col = cur_index->GetTsColumn();
            if (ts_col) {
                ttl_st_map.emplace(ts_col->GetId(), *(cur_index->GetTTL()));
            } else {
                ttl_st_map.emplace(0, *(cur_index->GetTTL()));
            }
            if (cur_index->GetStatus() == IndexStatus::kWaiting) {
                cur_index->SetStatus(IndexStatus::kDeleting);
                need_gc = false;
            } else if (cur_index->GetStatus() == IndexStatus::kDeleting) {
                if (real_index.size() == 1) {
                    if (segments_[i] != NULL) {
                        for (uint32_t k = 0; k < seg_cnt_; k++) {
                            if (segments_[i][k] != NULL) {
                                segments_[i][k]->ReleaseAndCount(gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
                            }
                        }
                    }
                    deleted_num++;
                }
                cur_index->SetStatus(IndexStatus::kDeleted);
            } else if (cur_index->GetStatus() == IndexStatus::kDeleted) {
                deleted_num++;
            }
        }
        if (!enable_gc_.load(std::memory_order_relaxed) || !need_gc) {
            continue;
        }
        if (deleted_num == real_index.size() || ttl_st_map.empty()) {
            continue;
        }
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            uint64_t seg_gc_time = ::baidu::common::timer::get_micros() / 1000;
            Segment* segment = segments_[i][j];
            segment->IncrGcVersion();
            segment->GcFreeList(gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            if (ttl_st_map.size() == 1) {
                segment->ExecuteGc(ttl_st_map.begin()->second, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            } else {
                segment->ExecuteGc(ttl_st_map, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            }
            seg_gc_time = ::baidu::common::timer::get_micros() / 1000 - seg_gc_time;
            PDLOG(INFO, "gc segment[%u][%u] done consumed %lu for table %s tid %u pid %u", i, j, seg_gc_time,
                  name_.c_str(), id_, pid_);
        }
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;
    record_cnt_.fetch_sub(gc_record_cnt, std::memory_order_relaxed);
    record_byte_size_.fetch_sub(gc_record_byte_size, std::memory_order_relaxed);
    PDLOG(INFO,
          "gc finished, gc_idx_cnt %lu, gc_record_cnt %lu consumed %lu ms for "
          "table %s tid %u pid %u",
          gc_idx_cnt, gc_record_cnt, consumed / 1000, name_.c_str(), id_, pid_);
    UpdateTTL();
}

// tll as ms
uint64_t MemTable::GetExpireTime(const TTLSt& ttl_st) {
    if (!enable_gc_.load(std::memory_order_relaxed) || ttl_st.abs_ttl == 0 ||
        ttl_st.ttl_type == ::openmldb::storage::TTLType::kLatestTime) {
        return 0;
    }
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    return cur_time - ttl_st.abs_ttl;
}

bool MemTable::CheckLatest(uint32_t index_id, const std::string& key, uint64_t ts) {
    ::openmldb::storage::Ticket ticket;
    ::openmldb::storage::TableIterator* it = NewIterator(index_id, key, ticket);
    it->SeekToLast();
    if (it->Valid()) {
        if (ts >= it->GetKey()) {
            delete it;
            return false;
        }
    }
    delete it;
    return true;
}

inline bool MemTable::CheckAbsolute(const TTLSt& ttl_st, uint64_t ts) { return ts < GetExpireTime(ttl_st); }

bool MemTable::IsExpire(const LogEntry& entry) {
    if (!enable_gc_.load(std::memory_order_relaxed)) {
        return false;
    }
    std::map<int32_t, std::string> inner_index_key_map;
    if (entry.dimensions_size() > 0) {
        for (auto iter = entry.dimensions().begin(); iter != entry.dimensions().end(); iter++) {
            int32_t inner_pos = table_index_.GetInnerIndexPos(iter->idx());
            if (inner_pos >= 0) {
                inner_index_key_map.emplace(inner_pos, iter->key());
            }
        }
    } else {
        int32_t inner_pos = table_index_.GetInnerIndexPos(0);
        if (inner_pos >= 0) {
            inner_index_key_map.emplace(inner_pos, entry.pk());
        }
    }
    const int8_t* data = reinterpret_cast<const int8_t*>(entry.value().data());
    uint8_t version = codec::RowView::GetSchemaVersion(data);
    auto decoder = GetVersionDecoder(version);
    if (decoder == nullptr) {
        PDLOG(WARNING, "invalid schema version %u, tid %u pid %u", static_cast<uint32_t>(version), id_, pid_);
        return false;
    }
    for (const auto& kv : inner_index_key_map) {
        auto inner_index = table_index_.GetInnerIndex(kv.first);
        if (!inner_index) {
            continue;
        }
        const std::vector<std::shared_ptr<IndexDef>>& indexs = inner_index->GetIndex();
        for (const auto& index_def : indexs) {
            if (!index_def || !index_def->IsReady()) {
                continue;
            }
            auto ttl = index_def->GetTTL();
            if (!ttl->NeedGc()) {
                return false;
            }
            TTLType ttl_type = index_def->GetTTLType();
            int64_t ts = entry.ts();
            auto ts_col = index_def->GetTsColumn();
            if (ts_col && !ts_col->IsAutoGenTs()) {
                if (decoder->GetInteger(data, ts_col->GetId(), ts_col->GetType(), &ts) != 0) {
                    continue;
                }
            }
            bool is_expire = false;
            uint32_t index_id = index_def->GetId();
            switch (ttl_type) {
                case ::openmldb::storage::TTLType::kLatestTime:
                    is_expire = CheckLatest(index_id, kv.second, ts);
                    break;
                case ::openmldb::storage::TTLType::kAbsoluteTime:
                    is_expire = CheckAbsolute(*ttl, ts);
                    break;
                case ::openmldb::storage::TTLType::kAbsOrLat:
                    is_expire = CheckAbsolute(*ttl, ts) || CheckLatest(index_id, kv.second, ts);
                    break;
                case ::openmldb::storage::TTLType::kAbsAndLat:
                    is_expire = CheckAbsolute(*ttl, ts) && CheckLatest(index_id, kv.second, ts);
                    break;
                default:
                    return true;
            }
            if (!is_expire) {
                return false;
            }
        }
    }
    return true;
}

int MemTable::GetCount(uint32_t index, const std::string& pk, uint64_t& count) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(index);
    if (index_def && !index_def->IsReady()) {
        return -1;
    }
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::openmldb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Slice spk(pk);
    uint32_t real_idx = index_def->GetInnerPos();
    Segment* segment = segments_[real_idx][seg_idx];
    auto ts_col = index_def->GetTsColumn();
    if (ts_col) {
        return segment->GetCount(spk, ts_col->GetId(), count);
    }
    return segment->GetCount(spk, count);
}

TableIterator* MemTable::NewIterator(const std::string& pk, Ticket& ticket) { return NewIterator(0, pk, ticket); }

TableIterator* MemTable::NewIterator(uint32_t index, const std::string& pk, Ticket& ticket) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(index);
    if (!index_def || !index_def->IsReady()) {
        PDLOG(WARNING, "index %d not found in table, tid %u pid %u", index, id_, pid_);
        return NULL;
    }
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::openmldb::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Slice spk(pk);
    uint32_t real_idx = index_def->GetInnerPos();
    Segment* segment = segments_[real_idx][seg_idx];
    auto ts_col = index_def->GetTsColumn();
    if (ts_col) {
        return segment->NewIterator(spk, ts_col->GetId(), ticket);
    }
    return segment->NewIterator(spk, ticket);
}

uint64_t MemTable::GetRecordIdxByteSize() {
    uint64_t record_idx_byte_size = 0;
    auto inner_indexs = table_index_.GetAllInnerIndex();
    for (size_t i = 0; i < inner_indexs->size(); i++) {
        bool is_valid = false;
        for (const auto& index_def : inner_indexs->at(i)->GetIndex()) {
            if (index_def && index_def->IsReady()) {
                is_valid = true;
                break;
            }
        }
        if (is_valid) {
            for (uint32_t j = 0; j < seg_cnt_; j++) {
                record_idx_byte_size += segments_[i][j]->GetIdxByteSize();
            }
        }
    }
    return record_idx_byte_size;
}

uint64_t MemTable::GetRecordIdxCnt() {
    uint64_t record_idx_cnt = 0;
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(0);
    if (!index_def || !index_def->IsReady()) {
        return record_idx_cnt;
    }
    uint32_t inner_idx = index_def->GetInnerPos();
    auto inner_index = table_index_.GetInnerIndex(inner_idx);
    int32_t ts_col_id = -1;
    auto ts_col = index_def->GetTsColumn();
    if (ts_col) {
        ts_col_id = ts_col->GetId();
    }
    for (uint32_t i = 0; i < seg_cnt_; i++) {
        if (inner_index->GetIndex().size() > 1 && ts_col_id >= 0) {
            uint64_t record_cnt = 0;
            segments_[inner_idx][i]->GetIdxCnt(ts_col_id, record_cnt);
            record_idx_cnt += record_cnt;
        } else {
            record_idx_cnt += segments_[inner_idx][i]->GetIdxCnt();
        }
    }
    return record_idx_cnt;
}

uint64_t MemTable::GetRecordPkCnt() {
    uint64_t record_pk_cnt = 0;
    auto inner_indexs = table_index_.GetAllInnerIndex();
    for (size_t i = 0; i < inner_indexs->size(); i++) {
        bool is_valid = false;
        for (const auto& index_def : inner_indexs->at(i)->GetIndex()) {
            if (index_def && index_def->IsReady()) {
                is_valid = true;
                break;
            }
        }
        if (is_valid) {
            for (uint32_t j = 0; j < seg_cnt_; j++) {
                record_pk_cnt += segments_[i][j]->GetPkCnt();
            }
        }
    }
    return record_pk_cnt;
}

bool MemTable::GetRecordIdxCnt(uint32_t idx, uint64_t** stat, uint32_t* size) {
    if (stat == NULL) {
        return false;
    }
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(idx);
    if (!index_def || !index_def->IsReady()) {
        return false;
    }
    auto* data_array = new uint64_t[seg_cnt_]();
    uint32_t inner_idx = index_def->GetInnerPos();
    auto inner_index = table_index_.GetInnerIndex(inner_idx);
    int32_t ts_col_id = -1;
    auto ts_col = index_def->GetTsColumn();
    if (ts_col) {
        ts_col_id = ts_col->GetId();
    }
    for (uint32_t i = 0; i < seg_cnt_; i++) {
        if (inner_index->GetIndex().size() > 1 && ts_col_id >= 0) {
            segments_[inner_idx][i]->GetIdxCnt(ts_col_id, data_array[i]);
        } else {
            data_array[i] += segments_[inner_idx][i]->GetIdxCnt();
        }
    }
    *stat = data_array;
    *size = seg_cnt_;
    return true;
}

bool MemTable::AddIndex(const ::openmldb::common::ColumnKey& column_key) {
    // TODO(denglong): support ttl type and merge index
    auto table_meta = GetTableMeta();
    auto new_table_meta = std::make_shared<::openmldb::api::TableMeta>(*table_meta);
    std::shared_ptr<IndexDef> index_def = GetIndex(column_key.index_name());
    if (index_def) {
        if (index_def->GetStatus() != IndexStatus::kDeleted) {
            PDLOG(WARNING, "index %s is exist. tid %u pid %u", column_key.index_name().c_str(), id_, pid_);
            return false;
        }
        new_table_meta->mutable_column_key(index_def->GetId())->CopyFrom(column_key);
        if (column_key.has_ttl()) {
            index_def->SetTTL(::openmldb::storage::TTLSt(column_key.ttl()));
        }
    } else {
        ::openmldb::common::ColumnKey* added_column_key = new_table_meta->add_column_key();
        added_column_key->CopyFrom(column_key);
    }
    if (!index_def) {
        auto cols = GetSchema();
        if (!cols) {
            return false;
        }
        std::map<std::string, ColumnDef> schema;
        for (int idx = 0; idx < cols->size(); idx++) {
            const auto& col = cols->Get(idx);
            schema.emplace(col.name(), ColumnDef(col.name(), idx, col.data_type(), col.not_null()));
        }
        std::vector<ColumnDef> col_vec;
        for (const auto& col_name : column_key.col_name()) {
            auto it = schema.find(col_name);
            if (it == schema.end()) {
                PDLOG(WARNING, "not found col_name[%s]. tid %u pid %u", col_name.c_str(), id_, pid_);
                return false;
            }
            col_vec.push_back(it->second);
        }
        std::vector<uint32_t> ts_vec;
        if (!column_key.ts_name().empty()) {
            auto ts_iter = schema.find(column_key.ts_name());
            if (ts_iter == schema.end()) {
                PDLOG(WARNING, "not found ts_name[%s]. tid %u pid %u", column_key.ts_name().c_str(), id_, pid_);
                return false;
            }
            ts_vec.push_back(ts_iter->second.GetId());
        } else {
            ts_vec.push_back(DEFUALT_TS_COL_ID);
        }
        uint32_t inner_id = table_index_.GetAllInnerIndex()->size();
        Segment** seg_arr = new Segment*[seg_cnt_];
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            seg_arr[j] = new Segment(FLAGS_absolute_default_skiplist_height, ts_vec);
            PDLOG(INFO, "init %u, %u segment. height %u, ts col num %u. tid %u pid %u", inner_id, j,
                  FLAGS_absolute_default_skiplist_height, ts_vec.size(), id_, pid_);
        }
        index_def = std::make_shared<IndexDef>(column_key.index_name(), table_index_.GetMaxIndexId() + 1,
                IndexStatus::kReady, ::openmldb::type::IndexType::kTimeSerise, col_vec);
        if (table_index_.AddIndex(index_def) < 0) {
            PDLOG(WARNING, "add index failed. tid %u pid %u", id_, pid_);
            return false;
        }
        segments_[inner_id] = seg_arr;
        if (!column_key.ts_name().empty()) {
            auto ts_iter = schema.find(column_key.ts_name());
            index_def->SetTsColumn(std::make_shared<ColumnDef>(ts_iter->second));
        } else {
            index_def->SetTsColumn(std::make_shared<ColumnDef>(DEFUALT_TS_COL_NAME, DEFUALT_TS_COL_ID,
                        ::openmldb::type::kTimestamp, true));
        }
        if (column_key.has_ttl()) {
            index_def->SetTTL(::openmldb::storage::TTLSt(column_key.ttl()));
        } else {
            index_def->SetTTL(*(table_index_.GetIndex(0)->GetTTL()));
        }
        index_def->SetInnerPos(inner_id);
        std::vector<std::shared_ptr<IndexDef>> index_vec = {index_def};
        auto inner_index_st = std::make_shared<InnerIndexSt>(inner_id, index_vec);
        table_index_.AddInnerIndex(inner_index_st);
        table_index_.SetInnerIndexPos(new_table_meta->column_key_size() - 1, inner_id);
    }
    index_def->SetStatus(IndexStatus::kReady);
    std::atomic_store_explicit(&table_meta_, new_table_meta, std::memory_order_release);
    return true;
}

bool MemTable::DeleteIndex(const std::string& idx_name) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(idx_name);
    if (!index_def) {
        PDLOG(WARNING, "index %s does not exist. tid %u pid %u", idx_name.c_str(), id_, pid_);
        return false;
    }
    if (index_def->GetId() == 0) {
        PDLOG(WARNING, "index %s is primary key, cannot delete. tid %u pid %u", idx_name.c_str(), id_, pid_);
        return false;
    }
    if (!index_def->IsReady()) {
        PDLOG(WARNING, "index %s can't delete. tid %u pid %u", idx_name.c_str(), id_, pid_);
        return false;
    }
    auto table_meta = GetTableMeta();
    auto new_table_meta = std::make_shared<::openmldb::api::TableMeta>(*table_meta);
    if (index_def->GetId() < (uint32_t)table_meta->column_key_size()) {
        new_table_meta->mutable_column_key(index_def->GetId())->set_flag(1);
    }
    std::atomic_store_explicit(&table_meta_, new_table_meta, std::memory_order_release);
    index_def->SetStatus(IndexStatus::kWaiting);
    return true;
}

::hybridse::vm::WindowIterator* MemTable::NewWindowIterator(uint32_t index) {
    std::shared_ptr<IndexDef> index_def = table_index_.GetIndex(index);
    if (!index_def || !index_def->IsReady()) {
        LOG(WARNING) << "index id " << index << "  not found. tid " << id_ << " pid " << pid_;
        return NULL;
    }
    uint64_t expire_time = 0;
    uint64_t expire_cnt = 0;
    auto ttl = index_def->GetTTL();
    if (enable_gc_.load(std::memory_order_relaxed)) {
        expire_time = GetExpireTime(*ttl);
        expire_cnt = ttl->lat_ttl;
    }
    uint32_t real_idx = index_def->GetInnerPos();
    auto ts_col = index_def->GetTsColumn();
    uint32_t ts_idx = 0;
    if (ts_col) {
        ts_idx = ts_col->GetId();
    }
    return new MemTableKeyIterator(segments_[real_idx], seg_cnt_, ttl->ttl_type, expire_time, expire_cnt, ts_idx);
}

TraverseIterator* MemTable::NewTraverseIterator(uint32_t index) {
    std::shared_ptr<IndexDef> index_def = GetIndex(index);
    if (!index_def || !index_def->IsReady()) {
        PDLOG(WARNING, "index %u not found. tid %u pid %u", index, id_, pid_);
        return NULL;
    }
    uint64_t expire_time = 0;
    uint64_t expire_cnt = 0;
    auto ttl = index_def->GetTTL();
    if (enable_gc_.load(std::memory_order_relaxed)) {
        expire_time = GetExpireTime(*ttl);
        expire_cnt = ttl->lat_ttl;
    }
    uint32_t real_idx = index_def->GetInnerPos();
    auto ts_col = index_def->GetTsColumn();
    if (ts_col) {
        return new MemTableTraverseIterator(segments_[real_idx], seg_cnt_, ttl->ttl_type, expire_time, expire_cnt,
                                            ts_col->GetId());
    }
    return new MemTableTraverseIterator(segments_[real_idx], seg_cnt_, ttl->ttl_type, expire_time, expire_cnt, 0);
}

bool MemTable::GetBulkLoadInfo(::openmldb::api::BulkLoadInfoResponse* response) {
    response->set_seg_cnt(seg_cnt_);

    // TODO(hw): out of range will get -1, only a temporary solution.
    uint32_t idx = 0;
    int32_t pos;
    while ((pos = table_index_.GetInnerIndexPos(idx)) != -1) {
        response->add_inner_index_pos(pos);
        idx++;
    }
    // repeated InnerIndexSt, all index, even not ready
    auto inner_indexes = table_index_.GetAllInnerIndex();
    for (auto& i : *inner_indexes) {
        i->GetId();
        auto pb = response->add_inner_index();
        for (const auto& index_def : i->GetIndex()) {
            auto new_def = pb->add_index_def();
            new_def->set_is_ready(index_def->GetStatus() == IndexStatus::kReady);
            new_def->set_ts_idx(-1);
            auto ts_col = index_def->GetTsColumn();
            if (ts_col) {
                new_def->set_ts_idx(ts_col->GetId());
            }
        }
    }
    // repeated InnerSegments
    for (decltype(inner_indexes->size()) inner_id = 0; inner_id < inner_indexes->size(); ++inner_id) {
        auto segments = segments_[inner_id];
        auto pb_segments = response->add_inner_segments();
        for (decltype(seg_cnt_) i = 0; i < seg_cnt_; ++i) {
            auto seg = segments[i];
            auto pb_seg = pb_segments->add_segment();
            pb_seg->set_ts_cnt(seg->GetTsCnt());
            const auto& ts_idx_map = seg->GetTsIdxMap();
            for (auto entry : ts_idx_map) {
                auto pb_entry = pb_seg->add_ts_idx_map();
                pb_entry->set_key(entry.first);
                pb_entry->set_value(entry.second);
            }
        }
    }
    return true;
}

bool MemTable::BulkLoad(const std::vector<DataBlock*>& data_blocks,
                        const ::google::protobuf::RepeatedPtrField<::openmldb::api::BulkLoadIndex>& indexes) {
    // data_block[i] is the block which id == i
    for (int i = 0; i < indexes.size(); ++i) {
        const auto& inner_index = indexes.Get(i);
        auto real_idx = inner_index.inner_index_id();
        for (int j = 0; j < inner_index.segment_size(); ++j) {
            const auto& segment_index = inner_index.segment(j);
            auto seg_idx = segment_index.id();
            auto segment = segments_[real_idx][seg_idx];
            for (int key_idx = 0; key_idx < segment_index.key_entries_size(); ++key_idx) {
                const auto& key_entries = segment_index.key_entries(key_idx);
                auto pk = Slice(key_entries.key());
                for (int key_entry_idx = 0; key_entry_idx < key_entries.key_entry_size(); ++key_entry_idx) {
                    const auto& key_entry = key_entries.key_entry(key_entry_idx);
                    auto key_entry_id = key_entry.key_entry_id();
                    for (int time_idx = 0; time_idx < key_entry.time_entry_size(); ++time_idx) {
                        const auto& time_entry = key_entry.time_entry(time_idx);
                        auto* block =
                            time_entry.block_id() < data_blocks.size() ? data_blocks[time_entry.block_id()] : nullptr;
                        if (block == nullptr) {
                            // TODO(hw): error handle
                            LOG(INFO) << "block info mismatch";
                            return false;
                        }

                        VLOG(1) << "do segment(" << real_idx << "-" << seg_idx << ") put, key" << pk.ToString()
                                << ", time " << time_entry.time() << ", key_entry_id " << key_entry_id << ", block id "
                                << time_entry.block_id();
                        block->dim_cnt_down++;
                        segment->BulkLoadPut(key_entry_id, pk, time_entry.time(), block);
                    }
                }
            }
        }
    }

    return true;
}

MemTableTraverseIterator::MemTableTraverseIterator(Segment** segments, uint32_t seg_cnt,
                                                   ::openmldb::storage::TTLType ttl_type, uint64_t expire_time,
                                                   uint64_t expire_cnt, uint32_t ts_index)
    : segments_(segments),
      seg_cnt_(seg_cnt),
      seg_idx_(0),
      pk_it_(NULL),
      it_(NULL),
      record_idx_(0),
      ts_idx_(0),
      expire_value_(expire_time, expire_cnt, ttl_type),
      ticket_(),
      traverse_cnt_(0) {
    uint32_t idx = 0;
    if (segments_[0]->GetTsIdx(ts_index, idx) == 0) {
        ts_idx_ = idx;
    }
}

MemTableTraverseIterator::~MemTableTraverseIterator() {
    if (pk_it_ != NULL) delete pk_it_;
    if (it_ != NULL) delete it_;
}

bool MemTableTraverseIterator::Valid() {
    return pk_it_ != NULL && pk_it_->Valid() && it_ != NULL && it_->Valid() &&
           !expire_value_.IsExpired(it_->GetKey(), record_idx_);
}

void MemTableTraverseIterator::Next() {
    it_->Next();
    record_idx_++;
    traverse_cnt_++;
    if (!it_->Valid() || expire_value_.IsExpired(it_->GetKey(), record_idx_)) {
        NextPK();
        return;
    }
}
uint64_t MemTableTraverseIterator::GetCount() const { return traverse_cnt_; }

void MemTableTraverseIterator::NextPK() {
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
            it_ = NULL;
        }
        if (segments_[seg_idx_]->GetTsCnt() > 1) {
            KeyEntry* entry = ((KeyEntry**)pk_it_->GetValue())[0];  // NOLINT
            it_ = entry->entries.NewIterator();
            ticket_.Push(entry);
        } else {
            it_ = ((KeyEntry*)pk_it_->GetValue())  // NOLINT
                      ->entries.NewIterator();
            ticket_.Push((KeyEntry*)pk_it_->GetValue());  // NOLINT
        }
        it_->SeekToFirst();
        record_idx_ = 1;
        traverse_cnt_++;
        if (traverse_cnt_ >= FLAGS_max_traverse_cnt) {
            break;
        }
    } while (it_ == NULL || !it_->Valid() || expire_value_.IsExpired(it_->GetKey(), record_idx_));
}

void MemTableTraverseIterator::Seek(const std::string& key, uint64_t ts) {
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
        seg_idx_ = ::openmldb::base::hash(key.c_str(), key.length(), SEED) % seg_cnt_;
    }
    Slice spk(key);
    pk_it_ = segments_[seg_idx_]->GetKeyEntries()->NewIterator();
    pk_it_->Seek(spk);
    if (pk_it_->Valid()) {
        if (segments_[seg_idx_]->GetTsCnt() > 1) {
            KeyEntry* entry = ((KeyEntry**)pk_it_->GetValue())[ts_idx_];  // NOLINT
            ticket_.Push(entry);
            it_ = entry->entries.NewIterator();
        } else {
            ticket_.Push((KeyEntry*)pk_it_->GetValue());  // NOLINT
            it_ = ((KeyEntry*)pk_it_->GetValue())         // NOLINT
                      ->entries.NewIterator();
        }
        if (spk.compare(pk_it_->GetKey()) != 0) {
            it_->SeekToFirst();
            traverse_cnt_++;
            record_idx_ = 1;
            if (!it_->Valid() || expire_value_.IsExpired(it_->GetKey(), record_idx_)) {
                NextPK();
            }
        } else {
            if (expire_value_.ttl_type == ::openmldb::storage::TTLType::kLatestTime) {
                it_->SeekToFirst();
                record_idx_ = 1;
                while (it_->Valid() && record_idx_ <= expire_value_.lat_ttl) {
                    traverse_cnt_++;
                    if (it_->GetKey() <= ts) {
                        return;
                    }
                    it_->Next();
                    record_idx_++;
                }
                NextPK();
            } else {
                it_->Seek(ts);
                traverse_cnt_++;
                if (!it_->Valid() || expire_value_.IsExpired(it_->GetKey(), record_idx_)) {
                    NextPK();
                }
            }
        }
    } else {
        NextPK();
    }
}

openmldb::base::Slice MemTableTraverseIterator::GetValue() const {
    return openmldb::base::Slice(it_->GetValue()->data, it_->GetValue()->size);
}

uint64_t MemTableTraverseIterator::GetKey() const {
    if (it_ != NULL && it_->Valid()) {
        return it_->GetKey();
    }
    return UINT64_MAX;
}

std::string MemTableTraverseIterator::GetPK() const {
    if (pk_it_ == NULL) {
        return std::string();
    }
    return pk_it_->GetKey().ToString();
}

void MemTableTraverseIterator::SeekToFirst() {
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
            if (segments_[seg_idx_]->GetTsCnt() > 1) {
                KeyEntry* entry = ((KeyEntry**)pk_it_->GetValue())[ts_idx_];  // NOLINT
                ticket_.Push(entry);
                it_ = entry->entries.NewIterator();
            } else {
                ticket_.Push((KeyEntry*)pk_it_->GetValue());  // NOLINT
                it_ = ((KeyEntry*)pk_it_->GetValue())         // NOLINT
                          ->entries.NewIterator();
            }
            it_->SeekToFirst();
            traverse_cnt_++;
            if (it_->Valid() && !expire_value_.IsExpired(it_->GetKey(), record_idx_)) {
                record_idx_ = 1;
                return;
            }
            delete it_;
            it_ = NULL;
            pk_it_->Next();
            ticket_.Pop();
            if (traverse_cnt_ >= FLAGS_max_traverse_cnt) {
                return;
            }
        }
        delete pk_it_;
        pk_it_ = NULL;
    }
}

}  // namespace storage
}  // namespace openmldb
