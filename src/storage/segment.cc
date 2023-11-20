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

#include "storage/segment.h"
#include <snappy.h>
#include <memory>

#include "base/glog_wrapper.h"
#include "base/strings.h"
#include "common/timer.h"
#include "gflags/gflags.h"
#include "storage/record.h"

DECLARE_int32(gc_safe_offset);
DECLARE_uint32(skiplist_max_height);
DECLARE_uint32(gc_deleted_pk_version_delta);

namespace openmldb {
namespace storage {

static const SliceComparator scmp;

Segment::Segment(uint8_t height)
    : entries_(nullptr),
      mu_(),
      idx_byte_size_(0),
      pk_cnt_(0),
      key_entry_max_height_(height),
      ts_cnt_(1),
      gc_version_(0),
      ttl_offset_(FLAGS_gc_safe_offset * 60 * 1000),
      node_cache_(1, height) {
    entries_ = new KeyEntries((uint8_t)FLAGS_skiplist_max_height, 4, scmp);
    idx_cnt_vec_.push_back(std::make_shared<std::atomic<uint64_t>>(0));
}

Segment::Segment(uint8_t height, const std::vector<uint32_t>& ts_idx_vec)
    : entries_(nullptr),
      mu_(),
      idx_byte_size_(0),
      pk_cnt_(0),
      key_entry_max_height_(height),
      ts_cnt_(ts_idx_vec.size()),
      gc_version_(0),
      ttl_offset_(FLAGS_gc_safe_offset * 60 * 1000),
      node_cache_(ts_idx_vec.size(), height) {
    entries_ = new KeyEntries((uint8_t)FLAGS_skiplist_max_height, 4, scmp);
    for (uint32_t i = 0; i < ts_idx_vec.size(); i++) {
        ts_idx_map_[ts_idx_vec[i]] = i;
        idx_cnt_vec_.push_back(std::make_shared<std::atomic<uint64_t>>(0));
    }
}

Segment::~Segment() {
    delete entries_;
}

void Segment::Release(StatisticsInfo* statistics_info) {
    std::unique_ptr<KeyEntries::Iterator> it(entries_->NewIterator());
    it->SeekToFirst();
    while (it->Valid()) {
        delete[] it->GetKey().data();
        if (it->GetValue() != nullptr) {
            if (ts_cnt_ > 1) {
                KeyEntry** entry_arr = reinterpret_cast<KeyEntry**>(it->GetValue());
                for (uint32_t i = 0; i < ts_cnt_; i++) {
                    entry_arr[i]->Release(i, statistics_info);
                    delete entry_arr[i];
                }
                delete[] entry_arr;
            } else {
                KeyEntry* entry = reinterpret_cast<KeyEntry*>(it->GetValue());
                entry->Release(0, statistics_info);
                delete entry;
            }
        }
        it->Next();
    }
    entries_->Clear();
    node_cache_.Clear();
    idx_byte_size_.store(0);
    pk_cnt_.store(0);
    for (auto& idx_cnt : idx_cnt_vec_) {
        idx_cnt->store(0);
    }
}

void Segment::ReleaseAndCount(StatisticsInfo* statistics_info) {
    Release(statistics_info);
}

void Segment::ReleaseAndCount(const std::vector<size_t>& id_vec, StatisticsInfo* statistics_info) {
    if (ts_cnt_ <= 1) {
        return;
    }
    std::vector<size_t> real_idx_vec;
    for (auto id : id_vec) {
        if (auto it = ts_idx_map_.find(id); it != ts_idx_map_.end()) {
            real_idx_vec.push_back(it->second);
        }
    }
    if (real_idx_vec.empty()) {
        return;
    }
    std::unique_ptr<KeyEntries::Iterator> it(entries_->NewIterator());
    it->SeekToFirst();
    while (it->Valid()) {
        if (it->GetValue() != nullptr) {
            KeyEntry** entry_arr = reinterpret_cast<KeyEntry**>(it->GetValue());
            for (auto pos : real_idx_vec) {
                KeyEntry* entry = entry_arr[pos];
                std::unique_ptr<TimeEntries::Iterator> ts_it(entry->entries.NewIterator());
                ts_it->SeekToFirst();
                if (ts_it->Valid()) {
                    uint64_t ts = ts_it->GetKey();
                    auto data_node = entry->entries.Split(ts);
                    FreeList(pos, data_node, statistics_info);
                }
            }
        }
        it->Next();
    }
}

void Segment::Put(const Slice& key, uint64_t time, const char* data, uint32_t size) {
    if (ts_cnt_ > 1) {
        return;
    }
    auto* db = new DataBlock(1, data, size);
    Put(key, time, db);
}

void Segment::Put(const Slice& key, uint64_t time, DataBlock* row) {
    if (ts_cnt_ > 1) {
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);
    PutUnlock(key, time, row);
}

void Segment::PutUnlock(const Slice& key, uint64_t time, DataBlock* row) {
    void* entry = nullptr;
    uint32_t byte_size = 0;
    int ret = entries_->Get(key, entry);
    if (ret < 0 || entry == nullptr) {
        char* pk = new char[key.size()];
        memcpy(pk, key.data(), key.size());
        // need to delete memory when free node
        Slice skey(pk, key.size());
        entry = reinterpret_cast<void*>(new KeyEntry(key_entry_max_height_));
        uint8_t height = entries_->Insert(skey, entry);
        byte_size += GetRecordPkIdxSize(height, key.size(), key_entry_max_height_);
        pk_cnt_.fetch_add(1, std::memory_order_relaxed);
    }
    idx_cnt_vec_[0]->fetch_add(1, std::memory_order_relaxed);
    uint8_t height = reinterpret_cast<KeyEntry*>(entry)->entries.Insert(time, row);
    reinterpret_cast<KeyEntry*>(entry)->count_.fetch_add(1, std::memory_order_relaxed);
    byte_size += GetRecordTsIdxSize(height);
    idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
}

void Segment::BulkLoadPut(unsigned int key_entry_id, const Slice& key, uint64_t time, DataBlock* row) {
    void* key_entry_or_list = nullptr;
    uint32_t byte_size = 0;
    std::lock_guard<std::mutex> lock(mu_);  // TODO(hw): need lock?
    int ret = entries_->Get(key, key_entry_or_list);
    if (ts_cnt_ == 1) {
        PutUnlock(key, time, row);
    } else {
        if (ret < 0 || key_entry_or_list == nullptr) {
            char* pk = new char[key.size()];
            memcpy(pk, key.data(), key.size());
            Slice skey(pk, key.size());
            auto** entry_arr_tmp = new KeyEntry*[ts_cnt_];
            for (uint32_t i = 0; i < ts_cnt_; i++) {
                entry_arr_tmp[i] = new KeyEntry(key_entry_max_height_);
            }
            auto entry_arr = reinterpret_cast<void*>(entry_arr_tmp);
            uint8_t height = entries_->Insert(skey, entry_arr);
            byte_size += GetRecordPkMultiIdxSize(height, key.size(), key_entry_max_height_, ts_cnt_);
            pk_cnt_.fetch_add(1, std::memory_order_relaxed);
        }
        uint8_t height = reinterpret_cast<KeyEntry**>(key_entry_or_list)[key_entry_id]->entries.Insert(time, row);
        reinterpret_cast<KeyEntry**>(key_entry_or_list)[key_entry_id]->count_.fetch_add(1, std::memory_order_relaxed);
        byte_size += GetRecordTsIdxSize(height);
        idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
        idx_cnt_vec_[key_entry_id]->fetch_add(1, std::memory_order_relaxed);
    }
}

void Segment::Put(const Slice& key, const std::map<int32_t, uint64_t>& ts_map, DataBlock* row) {
    uint32_t ts_size = ts_map.size();
    if (ts_size == 0) {
        return;
    }
    if (ts_cnt_ == 1) {
        if (auto pos = ts_map.find(ts_idx_map_.begin()->first); pos != ts_map.end()) {
            Put(key, pos->second, row);
        }
        return;
    }
    void* entry_arr = nullptr;
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& kv : ts_map) {
        uint32_t byte_size = 0;
        auto pos = ts_idx_map_.find(kv.first);
        if (pos == ts_idx_map_.end()) {
            continue;
        }
        if (entry_arr == nullptr) {
            int ret = entries_->Get(key, entry_arr);
            if (ret < 0 || entry_arr == nullptr) {
                char* pk = new char[key.size()];
                memcpy(pk, key.data(), key.size());
                Slice skey(pk, key.size());
                KeyEntry** entry_arr_tmp = new KeyEntry*[ts_cnt_];
                for (uint32_t i = 0; i < ts_cnt_; i++) {
                    entry_arr_tmp[i] = new KeyEntry(key_entry_max_height_);
                }
                entry_arr = reinterpret_cast<void*>(entry_arr_tmp);
                uint8_t height = entries_->Insert(skey, entry_arr);
                byte_size += GetRecordPkMultiIdxSize(height, key.size(), key_entry_max_height_, ts_cnt_);
                pk_cnt_.fetch_add(1, std::memory_order_relaxed);
            }
        }
        auto entry = reinterpret_cast<KeyEntry**>(entry_arr)[pos->second];
        uint8_t height = entry->entries.Insert(kv.second, row);
        entry->count_.fetch_add(1, std::memory_order_relaxed);
        byte_size += GetRecordTsIdxSize(height);
        idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
        idx_cnt_vec_[pos->second]->fetch_add(1, std::memory_order_relaxed);
    }
}

bool Segment::Delete(const std::optional<uint32_t>& idx, const Slice& key) {
    if (ts_cnt_ == 1) {
        if (idx.has_value() && ts_idx_map_.find(idx.value()) == ts_idx_map_.end()) {
            return false;
        }
        ::openmldb::base::Node<Slice, void*>* entry_node = nullptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            entry_node = entries_->Remove(key);
        }
        if (entry_node != nullptr) {
            node_cache_.AddKeyEntryNode(gc_version_.load(std::memory_order_relaxed), entry_node);
            return true;
        }
    } else {
        if (!idx.has_value()) {
            return false;
        }
        auto iter = ts_idx_map_.find(idx.value());
        if (iter == ts_idx_map_.end()) {
            return false;
        }
        base::Node<uint64_t, DataBlock*>* data_node = nullptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            void* entry_arr = nullptr;
            if (entries_->Get(key, entry_arr) < 0 || entry_arr == nullptr) {
                return true;
            }
            KeyEntry* key_entry = reinterpret_cast<KeyEntry**>(entry_arr)[iter->second];
            std::unique_ptr<TimeEntries::Iterator> it(key_entry->entries.NewIterator());
            it->SeekToFirst();
            if (it->Valid()) {
                uint64_t ts = it->GetKey();
                data_node = key_entry->entries.Split(ts);
            }
        }
        if (data_node != nullptr) {
            node_cache_.AddValueNodeList(iter->second, gc_version_.load(std::memory_order_relaxed), data_node);
        }
    }
    return true;
}

bool Segment::Delete(const std::optional<uint32_t>& idx, const Slice& key,
            uint64_t ts, const std::optional<uint64_t>& end_ts) {
    void* entry = nullptr;
    if (entries_->Get(key, entry) < 0 || entry == nullptr) {
        return true;
    }
    KeyEntry* key_entry = nullptr;
    uint32_t ts_idx = 0;
    if (ts_cnt_ == 1) {
        if (idx.has_value() && ts_idx_map_.find(idx.value()) == ts_idx_map_.end()) {
            return false;
        }
        key_entry = reinterpret_cast<KeyEntry*>(entry);
    } else {
        if (!idx.has_value()) {
            return false;
        }
        auto iter = ts_idx_map_.find(idx.value());
        if (iter == ts_idx_map_.end()) {
            return false;
        }
        key_entry = reinterpret_cast<KeyEntry**>(entry)[iter->second];
        ts_idx = iter->second;
    }
    if (end_ts.has_value()) {
        if (auto node = key_entry->entries.GetLast(); node == nullptr) {
            return true;
        } else if (node->GetKey() <= end_ts.value()) {
            std::unique_ptr<TimeEntries::Iterator> it(key_entry->entries.NewIterator());
            it->Seek(ts);
            while (it->Valid()) {
                uint64_t cur_ts = it->GetKey();
                it->Next();
                base::Node<uint64_t, DataBlock*>* data_node = nullptr;
                if (cur_ts <= ts && cur_ts > end_ts.value()) {
                    std::lock_guard<std::mutex> lock(mu_);
                    data_node = key_entry->entries.Remove(cur_ts);
                } else {
                    return true;
                }
                node_cache_.AddSingleValueNode(ts_idx, gc_version_.load(std::memory_order_relaxed), data_node);
            }
            return true;
        }
    }
    base::Node<uint64_t, DataBlock*>* data_node = nullptr;
    {
        std::lock_guard<std::mutex> lock(mu_);
        data_node = key_entry->entries.Split(ts);
        DLOG(INFO) << "entry " << key.ToString() << " split by " << ts;
    }
    if (data_node != nullptr) {
        node_cache_.AddValueNodeList(ts_idx, gc_version_.load(std::memory_order_relaxed), data_node);
    }
    return true;
}

void Segment::FreeList(uint32_t ts_idx, ::openmldb::base::Node<uint64_t, DataBlock*>* node,
    StatisticsInfo* statistics_info) {
    while (node != nullptr) {
        statistics_info->IncrIdxCnt(ts_idx);
        ::openmldb::base::Node<uint64_t, DataBlock*>* tmp = node;
        idx_byte_size_.fetch_sub(GetRecordTsIdxSize(tmp->Height()));
        node = node->GetNextNoBarrier(0);
        DEBUGLOG("delete key %lu with height %u", tmp->GetKey(), tmp->Height());
        if (tmp->GetValue()->dim_cnt_down > 1) {
            tmp->GetValue()->dim_cnt_down--;
        } else {
            DEBUGLOG("delele data block for key %lu", tmp->GetKey());
            statistics_info->record_byte_size += GetRecordSize(tmp->GetValue()->size);
            delete tmp->GetValue();
        }
        delete tmp;
    }
}


void Segment::GcFreeList(StatisticsInfo* statistics_info) {
    uint64_t cur_version = gc_version_.load(std::memory_order_relaxed);
    if (cur_version < FLAGS_gc_deleted_pk_version_delta) {
        return;
    }
    StatisticsInfo old = *statistics_info;
    uint64_t free_list_version = cur_version - FLAGS_gc_deleted_pk_version_delta;
    node_cache_.Free(free_list_version, statistics_info);
    for (size_t idx = 0; idx < idx_cnt_vec_.size(); idx++) {
        idx_cnt_vec_[idx]->fetch_sub(statistics_info->GetIdxCnt(idx) - old.GetIdxCnt(idx), std::memory_order_relaxed);
    }
    idx_byte_size_.fetch_sub(statistics_info->idx_byte_size - old.idx_byte_size);
}

void Segment::ExecuteGc(const TTLSt& ttl_st, StatisticsInfo* statistics_info) {
    uint64_t cur_time = ::baidu::common::timer::get_micros() / 1000;
    switch (ttl_st.ttl_type) {
        case ::openmldb::storage::TTLType::kAbsoluteTime: {
            if (ttl_st.abs_ttl == 0) {
                return;
            }
            uint64_t expire_time = cur_time - ttl_offset_ - ttl_st.abs_ttl;
            Gc4TTL(expire_time, statistics_info);
            break;
        }
        case ::openmldb::storage::TTLType::kLatestTime: {
            if (ttl_st.lat_ttl == 0) {
                return;
            }
            Gc4Head(ttl_st.lat_ttl, statistics_info);
            break;
        }
        case ::openmldb::storage::TTLType::kAbsAndLat: {
            if (ttl_st.abs_ttl == 0 || ttl_st.lat_ttl == 0) {
                return;
            }
            uint64_t expire_time = cur_time - ttl_offset_ - ttl_st.abs_ttl;
            Gc4TTLAndHead(expire_time, ttl_st.lat_ttl, statistics_info);
            break;
        }
        case ::openmldb::storage::TTLType::kAbsOrLat: {
            if (ttl_st.abs_ttl == 0 && ttl_st.lat_ttl == 0) {
                return;
            }
            uint64_t expire_time = ttl_st.abs_ttl == 0 ? 0 : cur_time - ttl_offset_ - ttl_st.abs_ttl;
            Gc4TTLOrHead(expire_time, ttl_st.lat_ttl, statistics_info);
            break;
        }
        default:
            PDLOG(WARNING, "ttl type %d is unsupported", ttl_st.ttl_type);
    }
}

void Segment::ExecuteGc(const std::map<uint32_t, TTLSt>& ttl_st_map, StatisticsInfo* statistics_info) {
    if (ttl_st_map.empty()) {
        return;
    }
    if (ts_cnt_ <= 1) {
        ExecuteGc(ttl_st_map.begin()->second, statistics_info);
        return;
    }
    bool need_gc = false;
    for (const auto& kv : ttl_st_map) {
        if (ts_idx_map_.find(kv.first) == ts_idx_map_.end()) {
            return;
        }
        if (kv.second.NeedGc()) {
            need_gc = true;
        }
    }
    if (!need_gc) {
        return;
    }
    GcAllType(ttl_st_map, statistics_info);
}

void Segment::Gc4Head(uint64_t keep_cnt, StatisticsInfo* statistics_info) {
    if (keep_cnt == 0) {
        PDLOG(WARNING, "[Gc4Head] segment gc4head is disabled");
        return;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = statistics_info->GetIdxCnt(0);
    std::unique_ptr<KeyEntries::Iterator> it(entries_->NewIterator());
    it->SeekToFirst();
    while (it->Valid()) {
        auto entry = reinterpret_cast<KeyEntry*>(it->GetValue());
        ::openmldb::base::Node<uint64_t, DataBlock*>* node = nullptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                node = entry->entries.SplitByPos(keep_cnt);
            }
        }
        uint64_t cur_idx_cnt = statistics_info->GetIdxCnt(0);
        FreeList(0, node, statistics_info);
        entry->count_.fetch_sub(statistics_info->GetIdxCnt(0) - cur_idx_cnt, std::memory_order_relaxed);
        it->Next();
    }
    DEBUGLOG("[Gc4Head] segment gc keep cnt %lu consumed %lu, count %lu", keep_cnt,
             (::baidu::common::timer::get_micros() - consumed) / 1000, statistics_info->GetIdxCnt(0) - old);
    idx_cnt_vec_[0]->fetch_sub(statistics_info->GetIdxCnt(0) - old, std::memory_order_relaxed);
}

void Segment::GcAllType(const std::map<uint32_t, TTLSt>& ttl_st_map, StatisticsInfo* statistics_info) {
    uint64_t old = statistics_info->GetTotalCnt();
    uint64_t consumed = ::baidu::common::timer::get_micros();
    std::unique_ptr<KeyEntries::Iterator> it(entries_->NewIterator());
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry** entry_arr = reinterpret_cast<KeyEntry**>(it->GetValue());
        Slice key = it->GetKey();
        it->Next();
        uint32_t empty_cnt = 0;
        for (const auto& kv : ttl_st_map) {
            if (!kv.second.NeedGc()) {
                continue;
            }
            auto pos = ts_idx_map_.find(kv.first);
            if (pos == ts_idx_map_.end() || pos->second >= ts_cnt_) {
                continue;
            }
            KeyEntry* entry = entry_arr[pos->second];
            ::openmldb::base::Node<uint64_t, DataBlock*>* node = nullptr;
            bool continue_flag = false;
            switch (kv.second.ttl_type) {
                case ::openmldb::storage::TTLType::kAbsoluteTime: {
                    node = entry->entries.GetLast();
                    if (node == nullptr || node->GetKey() > kv.second.abs_ttl) {
                        continue_flag = true;
                    } else {
                        node = nullptr;
                        std::lock_guard<std::mutex> lock(mu_);
                        SplitList(entry, kv.second.abs_ttl, &node);
                        if (entry->entries.IsEmpty()) {
                            DLOG(INFO) << "gc key " << key.ToString() << " is empty";
                            empty_cnt++;
                        }
                    }
                    break;
                }
                case ::openmldb::storage::TTLType::kLatestTime: {
                    std::lock_guard<std::mutex> lock(mu_);
                    if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                        node = entry->entries.SplitByPos(kv.second.lat_ttl);
                    }
                    break;
                }
                case ::openmldb::storage::TTLType::kAbsAndLat: {
                    node = entry->entries.GetLast();
                    if (node == nullptr || node->GetKey() > kv.second.abs_ttl) {
                        continue_flag = true;
                    } else {
                        node = nullptr;
                        std::lock_guard<std::mutex> lock(mu_);
                        if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                            node = entry->entries.SplitByKeyAndPos(kv.second.abs_ttl, kv.second.lat_ttl);
                        }
                    }
                    break;
                }
                case ::openmldb::storage::TTLType::kAbsOrLat: {
                    node = entry->entries.GetLast();
                    if (node == nullptr) {
                        continue_flag = true;
                    } else {
                        node = nullptr;
                        std::lock_guard<std::mutex> lock(mu_);
                        if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                            if (kv.second.abs_ttl == 0) {
                                node = entry->entries.SplitByPos(kv.second.lat_ttl);
                            } else if (kv.second.lat_ttl == 0) {
                                node = entry->entries.Split(kv.second.abs_ttl);
                            } else {
                                node = entry->entries.SplitByKeyOrPos(kv.second.abs_ttl, kv.second.lat_ttl);
                            }
                        }
                        if (entry->entries.IsEmpty()) {
                            empty_cnt++;
                        }
                    }
                    break;
                }
                default:
                    return;
            }
            if (continue_flag) {
                continue;
            }
            uint64_t cur_idx_cnt = statistics_info->GetIdxCnt(pos->second);
            FreeList(pos->second, node, statistics_info);
            uint64_t free_idx_cnt = statistics_info->GetIdxCnt(pos->second) - cur_idx_cnt;
            entry->count_.fetch_sub(free_idx_cnt, std::memory_order_relaxed);
            idx_cnt_vec_[pos->second]->fetch_sub(free_idx_cnt, std::memory_order_relaxed);
        }
        if (empty_cnt == ts_cnt_) {
            bool is_empty = true;
            ::openmldb::base::Node<Slice, void*>* entry_node = nullptr;
            {
                std::lock_guard<std::mutex> lock(mu_);
                for (uint32_t i = 0; i < ts_cnt_; i++) {
                    if (!entry_arr[i]->entries.IsEmpty()) {
                        is_empty = false;
                        break;
                    }
                }
                if (is_empty) {
                    entry_node = entries_->Remove(key);
                }
            }
            if (entry_node != nullptr) {
                node_cache_.AddKeyEntryNode(gc_version_.load(std::memory_order_relaxed), entry_node);
            }
        }
    }
    DEBUGLOG("[GcAll] segment gc consumed %lu, count %lu", (::baidu::common::timer::get_micros() - consumed) / 1000,
             statistics_info->GetTotalCnt() - old);
}

void Segment::SplitList(KeyEntry* entry, uint64_t ts, ::openmldb::base::Node<uint64_t, DataBlock*>** node) {
    // skip entry that ocupied by reader
    if (entry->refs_.load(std::memory_order_acquire) <= 0) {
        *node = entry->entries.Split(ts);
    }
}

// fast gc with no global pause
void Segment::Gc4TTL(const uint64_t time, StatisticsInfo* statistics_info) {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = statistics_info->GetIdxCnt(0);
    std::unique_ptr<KeyEntries::Iterator> it(entries_->NewIterator());
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = reinterpret_cast<KeyEntry*>(it->GetValue());
        Slice key = it->GetKey();
        it->Next();
        ::openmldb::base::Node<uint64_t, DataBlock*>* node = entry->entries.GetLast();
        if (node == nullptr) {
            continue;
        } else if (node->GetKey() > time) {
            DEBUGLOG("[Gc4TTL] segment gc with key %lu need not ttl, last node key %lu",
                time, node->GetKey());
            continue;
        }
        node = nullptr;
        ::openmldb::base::Node<Slice, void*>* entry_node = nullptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            SplitList(entry, time, &node);
            if (entry->entries.IsEmpty()) {
                entry_node = entries_->Remove(key);
            }
        }
        if (entry_node != nullptr) {
            DLOG(INFO) << "add key " << key.ToString() << " to node cache. version " << gc_version_;
            node_cache_.AddKeyEntryNode(gc_version_.load(std::memory_order_relaxed), entry_node);
        }
        uint64_t cur_idx_cnt = statistics_info->GetIdxCnt(0);
        FreeList(0, node, statistics_info);
        entry->count_.fetch_sub(statistics_info->GetIdxCnt(0) - cur_idx_cnt, std::memory_order_relaxed);
    }
    DEBUGLOG("[Gc4TTL] segment gc with key %lu ,consumed %lu, count %lu", time,
             (::baidu::common::timer::get_micros() - consumed) / 1000, statistics_info->GetIdxCnt(0) - old);
    idx_cnt_vec_[0]->fetch_sub(statistics_info->GetIdxCnt(0) - old, std::memory_order_relaxed);
}

void Segment::Gc4TTLAndHead(const uint64_t time, const uint64_t keep_cnt, StatisticsInfo* statistics_info) {
    if (time == 0 || keep_cnt == 0) {
        PDLOG(INFO, "[Gc4TTLAndHead] segment gc4ttlandhead is disabled");
        return;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = statistics_info->GetIdxCnt(0);
    std::unique_ptr<KeyEntries::Iterator> it(entries_->NewIterator());
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = reinterpret_cast<KeyEntry*>(it->GetValue());
        ::openmldb::base::Node<uint64_t, DataBlock*>* node = entry->entries.GetLast();
        it->Next();
        if (node == nullptr) {
            continue;
        } else if (node->GetKey() > time) {
            DEBUGLOG("[Gc4TTLAndHead] segment gc with key %lu need not ttl, last node key %lu",
                time, node->GetKey());
            continue;
        }
        node = nullptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                node = entry->entries.SplitByKeyAndPos(time, keep_cnt);
            }
        }
        uint64_t cur_idx_cnt = statistics_info->GetIdxCnt(0);
        FreeList(0, node, statistics_info);
        entry->count_.fetch_sub(statistics_info->GetIdxCnt(0) - cur_idx_cnt, std::memory_order_relaxed);
    }
    DEBUGLOG("[Gc4TTLAndHead] segment gc time %lu and keep cnt %lu consumed %lu, count %lu",
        time, keep_cnt, (::baidu::common::timer::get_micros() - consumed) / 1000, statistics_info->GetIdxCnt(0) - old);
    idx_cnt_vec_[0]->fetch_sub(statistics_info->GetIdxCnt(0) - old, std::memory_order_relaxed);
}

void Segment::Gc4TTLOrHead(const uint64_t time, const uint64_t keep_cnt, StatisticsInfo* statistics_info) {
    if (time == 0 && keep_cnt == 0) {
        PDLOG(INFO, "[Gc4TTLOrHead] segment gc4ttlorhead is disabled");
        return;
    } else if (time == 0) {
        Gc4Head(keep_cnt, statistics_info);
        return;
    } else if (keep_cnt == 0) {
        Gc4TTL(time, statistics_info);
        return;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = statistics_info->GetIdxCnt(0);
    std::unique_ptr<KeyEntries::Iterator> it(entries_->NewIterator());
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = reinterpret_cast<KeyEntry*>(it->GetValue());
        Slice key = it->GetKey();
        it->Next();
        ::openmldb::base::Node<uint64_t, DataBlock*>* node = entry->entries.GetLast();
        if (node == nullptr) {
            continue;
        }
        node = nullptr;
        ::openmldb::base::Node<Slice, void*>* entry_node = nullptr;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                node = entry->entries.SplitByKeyOrPos(time, keep_cnt);
            }
            if (entry->entries.IsEmpty()) {
                entry_node = entries_->Remove(key);
            }
        }
        if (entry_node != nullptr) {
            node_cache_.AddKeyEntryNode(gc_version_.load(std::memory_order_relaxed), entry_node);
        }
        uint64_t cur_idx_cnt = statistics_info->GetIdxCnt(0);
        FreeList(0, node, statistics_info);
        entry->count_.fetch_sub(statistics_info->GetIdxCnt(0) - cur_idx_cnt, std::memory_order_relaxed);
    }
    DEBUGLOG("[Gc4TTLAndHead] segment gc time %lu and keep cnt %lu consumed %lu, count %lu",
        time, keep_cnt, (::baidu::common::timer::get_micros() - consumed) / 1000, statistics_info->GetIdxCnt(0) - old);
    idx_cnt_vec_[0]->fetch_sub(statistics_info->GetIdxCnt(0) - old, std::memory_order_relaxed);
}

int Segment::GetCount(const Slice& key, uint64_t& count) {
    if (ts_cnt_ > 1) {
        return -1;
    }
    void* entry = nullptr;
    if (entries_->Get(key, entry) < 0 || entry == nullptr) {
        return -1;
    }
    count = reinterpret_cast<KeyEntry*>(entry)->count_.load(std::memory_order_relaxed);
    return 0;
}

int Segment::GetCount(const Slice& key, uint32_t idx, uint64_t& count) {
    auto pos = ts_idx_map_.find(idx);
    if (pos == ts_idx_map_.end()) {
        return -1;
    }
    if (ts_cnt_ == 1) {
        return GetCount(key, count);
    }
    void* entry_arr = nullptr;
    if (entries_->Get(key, entry_arr) < 0 || entry_arr == nullptr) {
        return -1;
    }
    count = reinterpret_cast<KeyEntry**>(entry_arr)[pos->second]->count_.load(std::memory_order_relaxed);
    return 0;
}

MemTableIterator* Segment::NewIterator(const Slice& key, Ticket& ticket, type::CompressType compress_type) {
    if (entries_ == nullptr || ts_cnt_ > 1) {
        return new MemTableIterator(nullptr, compress_type);
    }
    void* entry = nullptr;
    if (entries_->Get(key, entry) < 0 || entry == nullptr) {
        return new MemTableIterator(nullptr, compress_type);
    }
    ticket.Push(reinterpret_cast<KeyEntry*>(entry));
    return new MemTableIterator(reinterpret_cast<KeyEntry*>(entry)->entries.NewIterator(), compress_type);
}

MemTableIterator* Segment::NewIterator(const Slice& key, uint32_t idx,
        Ticket& ticket, type::CompressType compress_type) {
    auto pos = ts_idx_map_.find(idx);
    if (pos == ts_idx_map_.end()) {
        return new MemTableIterator(nullptr, compress_type);
    }
    if (ts_cnt_ == 1) {
        return NewIterator(key, ticket, compress_type);
    }
    void* entry_arr = nullptr;
    if (entries_->Get(key, entry_arr) < 0 || entry_arr == nullptr) {
        return new MemTableIterator(nullptr, compress_type);
    }
    auto entry = reinterpret_cast<KeyEntry**>(entry_arr)[pos->second];
    ticket.Push(entry);
    return new MemTableIterator(entry->entries.NewIterator(), compress_type);
}

MemTableIterator::MemTableIterator(TimeEntries::Iterator* it, type::CompressType compress_type)
    : it_(it), compress_type_(compress_type) {}

MemTableIterator::~MemTableIterator() {
    if (it_ != nullptr) {
        delete it_;
    }
}

void MemTableIterator::Seek(const uint64_t time) {
    if (it_) {
        it_->Seek(time);
    }
}

bool MemTableIterator::Valid() {
    if (it_ == nullptr) {
        return false;
    }
    return it_->Valid();
}

void MemTableIterator::Next() {
    if (it_) it_->Next();
}

::openmldb::base::Slice MemTableIterator::GetValue() const {
    if (compress_type_ == type::CompressType::kSnappy) {
        tmp_buf_.clear();
        snappy::Uncompress(it_->GetValue()->data, it_->GetValue()->size, &tmp_buf_);
        return openmldb::base::Slice(tmp_buf_);
    }
    return ::openmldb::base::Slice(it_->GetValue()->data, it_->GetValue()->size);
}

uint64_t MemTableIterator::GetKey() const { return it_->GetKey(); }

void MemTableIterator::SeekToFirst() {
    if (it_) it_->SeekToFirst();
}

void MemTableIterator::SeekToLast() {
    if (it_) it_->SeekToLast();
}

}  // namespace storage
}  // namespace openmldb
