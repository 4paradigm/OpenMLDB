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

#include "storage/iot_segment.h"

#include "iot_segment.h"

namespace openmldb::storage {
base::Slice RowToSlice(const ::hybridse::codec::Row& row) {
    butil::IOBuf buf;
    size_t size;
    if (codec::EncodeRpcRow(row, &buf, &size)) {
        auto r = new char[buf.size()];
        buf.copy_to(r);  // TODO(hw): don't copy, move it to slice
        // slice own the new r
        return {r, size, true};
    }
    LOG(WARNING) << "convert row to slice failed";
    return {};
}

std::string PackPkeysAndPts(const std::string& pkeys, uint64_t pts) {
    std::string buf;
    uint32_t pkeys_size = pkeys.size();
    buf.append(reinterpret_cast<const char*>(&pkeys_size), sizeof(uint32_t));
    buf.append(pkeys);
    buf.append(reinterpret_cast<const char*>(&pts), sizeof(uint64_t));
    return buf;
}

bool UnpackPkeysAndPts(const std::string& block, std::string* pkeys, uint64_t* pts) {
    DLOG_ASSERT(block.size() >= sizeof(uint32_t) + sizeof(uint64_t)) << "block size is " << block.size();
    uint32_t offset = 0;
    uint32_t pkeys_size = *reinterpret_cast<const uint32_t*>(block.data() + offset);
    offset += sizeof(uint32_t);
    pkeys->assign(block.data() + offset, pkeys_size);
    offset += pkeys_size;
    *pts = *reinterpret_cast<const uint64_t*>(block.data() + offset);
    DLOG_ASSERT(offset + sizeof(uint64_t) == block.size())
        << "offset is " << offset << " block size is " << block.size();
    return true;
}

// put_if_absent unsupported, iot table will reject put, no need to check here, just ignore
bool IOTSegment::PutUnlock(const Slice& key, uint64_t time, DataBlock* row, bool put_if_absent, bool auto_gen_ts) {
    void* entry = nullptr;
    uint32_t byte_size = 0;
    // one key just one entry
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
        // no need to check if absent when first put
    } else if (IsClusteredTs(ts_idx_map_.begin()->first)) {
        // if cidx and key match, check ts -> insert or update
        if (auto_gen_ts) {
            // cidx(keys) has just one entry for one keys, so if keys exists, needs delete
            LOG_IF(ERROR, reinterpret_cast<KeyEntry*>(entry)->entries.GetSize() > 1)
                << "cidx keys has more than one entry, " << reinterpret_cast<KeyEntry*>(entry)->entries.GetSize();
            // TODO(hw): client will delete old row, so if pkeys exists when auto ts, fail it
            return false;
        } else {
            // cidx(keys+ts) check if ts match
            if (ListContains(reinterpret_cast<KeyEntry*>(entry), time, row, false)) {
                LOG(WARNING) << "key " << key.ToString() << " ts " << time << " exists in cidx";
                return false;
            }
        }
    }

    idx_cnt_vec_[0]->fetch_add(1, std::memory_order_relaxed);
    uint8_t height = reinterpret_cast<KeyEntry*>(entry)->entries.Insert(time, row);
    reinterpret_cast<KeyEntry*>(entry)->count_.fetch_add(1, std::memory_order_relaxed);
    byte_size += GetRecordTsIdxSize(height);
    idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
    DLOG(INFO) << "idx_byte_size_ " << idx_byte_size_ << " after add " << byte_size;
    return true;
}

bool IOTSegment::Put(const Slice& key, const std::map<int32_t, uint64_t>& ts_map, DataBlock* cblock, DataBlock* sblock,
                     bool put_if_absent) {
    if (ts_map.empty()) {
        return false;
    }
    if (ts_cnt_ == 1) {
        bool ret = false;
        if (auto pos = ts_map.find(ts_idx_map_.begin()->first); pos != ts_map.end()) {
            // TODO(hw): why ts_map key is int32_t, default ts is uint32_t?
            ret = Segment::Put(key, pos->second,
                               (index_types_[ts_idx_map_.begin()->second] == common::kSecondary ? sblock : cblock),
                               false, pos->first == DEFAULT_TS_COL_ID);
        }
        return ret;
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
        auto auto_gen_ts = (pos->first == DEFAULT_TS_COL_ID);
        auto pblock = (index_types_[pos->second] == common::kSecondary ? sblock : cblock);
        if (IsClusteredTs(pos->first)) {
            // if cidx and key match, check ts -> insert or update
            if (auto_gen_ts) {
                // cidx(keys) has just one entry for one keys, so if keys exists, needs delete
                LOG_IF(ERROR, reinterpret_cast<KeyEntry*>(entry)->entries.GetSize() > 1)
                    << "cidx keys has more than one entry, " << reinterpret_cast<KeyEntry*>(entry)->entries.GetSize();
                // TODO(hw): client will delete old row, so if pkeys exists when auto ts, fail it
                if (reinterpret_cast<KeyEntry*>(entry)->entries.GetSize() > 0) {
                    LOG(WARNING) << "key " << key.ToString() << " exists in cidx";
                    return false;
                }
            } else {
                // cidx(keys+ts) check if ts match
                if (ListContains(reinterpret_cast<KeyEntry*>(entry), kv.second, pblock, false)) {
                    LOG(WARNING) << "key " << key.ToString() << " ts " << kv.second << " exists in cidx";
                    return false;
                }
            }
        }
        uint8_t height = entry->entries.Insert(kv.second, pblock);
        entry->count_.fetch_add(1, std::memory_order_relaxed);
        byte_size += GetRecordTsIdxSize(height);
        idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
        DLOG(INFO) << "idx_byte_size_ " << idx_byte_size_;
        idx_cnt_vec_[pos->second]->fetch_add(1, std::memory_order_relaxed);
    }
    return true;
}

absl::Status IOTSegment::CheckKeyExists(const Slice& key, const std::map<int32_t, uint64_t>& ts_map) {
    // check lock
    void* entry_arr = nullptr;
    std::lock_guard<std::mutex> lock(mu_);  // need shrink?
    int ret = entries_->Get(key, entry_arr);
    if (ret < 0 || entry_arr == nullptr) {
        return absl::NotFoundError("key not found");
    }
    if (ts_map.size() != 1) {
        return absl::InvalidArgumentError("ts map size is not 1");
    }
    auto idx_ts = ts_map.begin();
    auto pos = ts_idx_map_.find(idx_ts->first);
    if (pos == ts_idx_map_.end()) {
        return absl::InvalidArgumentError("ts not found");
    }
    // be careful, ts id in arg maybe negative cuz it's int32, but id in member is uint32
    if (!IsClusteredTs(idx_ts->first)) {
        LOG(WARNING) << "idx_ts->first " << idx_ts->first << " is not clustered ts "
                     << (clustered_ts_id_.has_value() ? std::to_string(clustered_ts_id_.value()) : "no");
        return absl::InvalidArgumentError("ts is not clustered");
    }
    KeyEntry* entry = nullptr;
    if (ts_cnt_ == 1) {
        LOG_IF(ERROR, pos->second != 0) << "when ts cnt == 1, pos second is " << pos->second;
        entry = reinterpret_cast<KeyEntry*>(entry_arr);
    } else {
        entry = reinterpret_cast<KeyEntry**>(entry_arr)[pos->second];
    }

    if (entry == nullptr) {
        return absl::NotFoundError("ts entry not found");
    }
    auto auto_gen_ts = (idx_ts->first == DEFAULT_TS_COL_ID);
    if (auto_gen_ts) {
        // cidx(keys) has just one entry for one keys, so if keys exists, needs delete
        DLOG_ASSERT(reinterpret_cast<KeyEntry*>(entry)->entries.GetSize() == 1) << "cidx keys has more than one entry";
        if (reinterpret_cast<KeyEntry*>(entry)->entries.GetSize() > 0) {
            return absl::AlreadyExistsError("key exists: " + key.ToString());
        }
    } else {
        // don't use listcontains, we don't need to check value, just check if time exists
        storage::DataBlock* v = nullptr;
        if (entry->entries.Get(idx_ts->second, v) == 0) {
            return absl::AlreadyExistsError(absl::StrCat("key+ts exists: ", key.ToString(), ", ts ", idx_ts->second));
        }
    }

    return absl::NotFoundError("ts not found");
}
// TODO(hw): when add lock? ref segment, don't lock iter
void IOTSegment::GrepGCEntry(const std::map<uint32_t, TTLSt>& ttl_st_map, GCEntryInfo* gc_entry_info) {
    if (ttl_st_map.empty()) {
        DLOG(INFO) << "ttl map is empty, skip gc";
        return;
    }

    bool need_gc = false;
    for (const auto& kv : ttl_st_map) {
        if (ts_idx_map_.find(kv.first) == ts_idx_map_.end()) {
            LOG(WARNING) << "ts idx " << kv.first << " not found";
            return;
        }
        if (kv.second.NeedGc()) {
            need_gc = true;
        }
    }
    if (!need_gc) {
        DLOG(INFO) << "no need gc, skip gc";
        return;
    }
    GrepGCAllType(ttl_st_map, gc_entry_info);
}

void GrepGC4Abs(KeyEntry* entry, const Slice& key, const TTLSt& ttl, uint64_t cur_time, uint64_t ttl_offset,
                GCEntryInfo* gc_entry_info) {
    if (ttl.abs_ttl == 0) {
        return;  // never expire
    }
    uint64_t expire_time = cur_time - ttl_offset - ttl.abs_ttl;
    std::unique_ptr<TimeEntries::Iterator> iter(entry->entries.NewIterator());
    iter->Seek(expire_time);
    // delete (expire, last]
    while (iter->Valid()) {
        if (iter->GetKey() > expire_time) {
            break;
        }
        // expire_time has offset, so we don't need to check if equal
        // if (iter->GetKey() == expire_time) {
        //     continue;  // save ==, don't gc
        // }
        gc_entry_info->AddEntry(key, iter->GetKey(), iter->GetValue());
        if (gc_entry_info->Full()) {
            LOG(INFO) << "gc entry info full, stop gc grep";
            return;
        }
        iter->Next();
    }
}

void GrepGC4Lat(KeyEntry* entry, const Slice& key, const TTLSt& ttl, GCEntryInfo* gc_entry_info) {
    auto keep_cnt = ttl.lat_ttl;
    if (keep_cnt == 0) {
        return;  // never exipre
    }

    std::unique_ptr<TimeEntries::Iterator> iter(entry->entries.NewIterator());
    iter->SeekToFirst();
    while (iter->Valid()) {
        if (keep_cnt > 0) {
            keep_cnt--;
        } else {
            gc_entry_info->AddEntry(key, iter->GetKey(), iter->GetValue());
        }
        if (gc_entry_info->Full()) {
            LOG(INFO) << "gc entry info full, stop gc grep";
            return;
        }
        iter->Next();
    }
}

void GrepGC4AbsAndLat(KeyEntry* entry, const Slice& key, const TTLSt& ttl, uint64_t cur_time, uint64_t ttl_offset,
                      GCEntryInfo* gc_entry_info) {
    if (ttl.abs_ttl == 0 || ttl.lat_ttl == 0) {
        return;  // never exipre
    }
    // keep both
    uint64_t expire_time = cur_time - ttl_offset - ttl.abs_ttl;
    auto keep_cnt = ttl.lat_ttl;
    std::unique_ptr<TimeEntries::Iterator> iter(entry->entries.NewIterator());
    iter->SeekToFirst();
    // if > lat cnt and < expire, delete
    while (iter->Valid()) {
        if (keep_cnt > 0) {
            keep_cnt--;
        } else if (iter->GetKey() < expire_time) {
            gc_entry_info->AddEntry(key, iter->GetKey(), iter->GetValue());
        }
        if (gc_entry_info->Full()) {
            LOG(INFO) << "gc entry info full, stop gc grep";
            return;
        }
        iter->Next();
    }
}
void GrepGC4AbsOrLat(KeyEntry* entry, const Slice& key, const TTLSt& ttl, uint64_t cur_time, uint64_t ttl_offset,
                     GCEntryInfo* gc_entry_info) {
    if (ttl.abs_ttl == 0 && ttl.lat_ttl == 0) {
        return;
    }
    if (ttl.abs_ttl == 0) {
        // == lat ttl
        GrepGC4Lat(entry, key, ttl, gc_entry_info);
        return;
    }
    if (ttl.lat_ttl == 0) {
        GrepGC4Abs(entry, key, ttl, cur_time, ttl_offset, gc_entry_info);
        return;
    }
    uint64_t expire_time = cur_time - ttl_offset - ttl.abs_ttl;
    auto keep_cnt = ttl.lat_ttl;
    std::unique_ptr<TimeEntries::Iterator> iter(entry->entries.NewIterator());
    iter->SeekToFirst();
    // if  > keep cnt or < expire time, delete
    while (iter->Valid()) {
        if (keep_cnt > 0) {
            keep_cnt--;  // safe
        } else {
            gc_entry_info->AddEntry(key, iter->GetKey(), iter->GetValue());
            iter->Next();
            continue;
        }
        if (iter->GetKey() < expire_time) {
            gc_entry_info->AddEntry(key, iter->GetKey(), iter->GetValue());
        }
        if (gc_entry_info->Full()) {
            LOG(INFO) << "gc entry info full, stop gc grep";
            return;
        }
        iter->Next();
    }
}

// actually only one ttl for cidx, clean up later
void IOTSegment::GrepGCAllType(const std::map<uint32_t, TTLSt>& ttl_st_map, GCEntryInfo* gc_entry_info) {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t cur_time = consumed / 1000;
    std::unique_ptr<KeyEntries::Iterator> it(entries_->NewIterator());
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry** entry_arr = reinterpret_cast<KeyEntry**>(it->GetValue());
        Slice key = it->GetKey();
        it->Next();
        for (const auto& kv : ttl_st_map) {
            DLOG(INFO) << "key " << key.ToString() << ", ts idx " << kv.first << ", ttl " << kv.second.ToString()
                       << ", ts_cnt_ " << ts_cnt_;
            if (!kv.second.NeedGc()) {
                continue;
            }
            auto pos = ts_idx_map_.find(kv.first);
            if (pos == ts_idx_map_.end() || pos->second >= ts_cnt_) {
                LOG(WARNING) << "gc ts idx " << kv.first << " not found";
                continue;
            }
            KeyEntry* entry = nullptr;
            // time series :[(ts, row), ...], so get key means get ts
            if (ts_cnt_ == 1) {
                LOG_IF(DFATAL, pos->second != 0) << "when ts cnt == 1, pos second is " << pos->second;
                entry = reinterpret_cast<KeyEntry*>(entry_arr);
            } else {
                entry = entry_arr[pos->second];
            }
            if (entry == nullptr) {
                DLOG(DFATAL) << "entry is null, impossible";
                continue;
            }
            switch (kv.second.ttl_type) {
                case ::openmldb::storage::TTLType::kAbsoluteTime: {
                    GrepGC4Abs(entry, key, kv.second, cur_time, ttl_offset_, gc_entry_info);
                    break;
                }
                case ::openmldb::storage::TTLType::kLatestTime: {
                    GrepGC4Lat(entry, key, kv.second, gc_entry_info);
                    break;
                }
                case ::openmldb::storage::TTLType::kAbsAndLat: {
                    GrepGC4AbsAndLat(entry, key, kv.second, cur_time, ttl_offset_, gc_entry_info);
                    break;
                }
                case ::openmldb::storage::TTLType::kAbsOrLat: {
                    GrepGC4AbsOrLat(entry, key, kv.second, cur_time, ttl_offset_, gc_entry_info);
                    break;
                }
                default:
                    return;
            }
        }
    }
    DLOG(INFO) << "[GC ts map] iot segment gc consumed " << (::baidu::common::timer::get_micros() - consumed) / 1000
               << "ms, gc entry size " << gc_entry_info->Size();
}
}  // namespace openmldb::storage
