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

#ifndef SRC_STORAGE_IOT_SEGMENT_H_
#define SRC_STORAGE_IOT_SEGMENT_H_

#include "catalog/tablet_catalog.h"
#include "codec/row_codec.h"
#include "codec/row_iterator.h"
#include "codec/sql_rpc_row_codec.h"
#include "storage/mem_table_iterator.h"
#include "storage/segment.h"
#include "storage/table.h"  // for storage::Schema

DECLARE_uint32(cidx_gc_max_size);

namespace openmldb::storage {

base::Slice RowToSlice(const ::hybridse::codec::Row& row);

// [pkeys_size, pkeys, pts_size, ts_id, tsv, ...]
std::string PackPkeysAndPts(const std::string& pkeys, uint64_t pts);
bool UnpackPkeysAndPts(const std::string& block, std::string* pkeys, uint64_t* pts);

// secondary index iterator
// GetValue will lookup, and it may trigger rpc
class IOTIterator : public MemTableIterator {
 public:
    IOTIterator(TimeEntries::Iterator* it, type::CompressType compress_type,
                std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter)
        : MemTableIterator(it, compress_type), cidx_iter_(std::move(cidx_iter)) {}
    virtual ~IOTIterator() {}

    openmldb::base::Slice GetValue() const override {
        auto pkeys_pts = MemTableIterator::GetValue();
        std::string pkeys;
        uint64_t ts;
        if (!UnpackPkeysAndPts(pkeys_pts.ToString(), &pkeys, &ts)) {
            LOG(WARNING) << "unpack pkeys and pts failed";
            return "";
        }
        cidx_iter_->Seek(pkeys);
        if (cidx_iter_->Valid()) {
            // seek to ts
            auto ts_iter = cidx_iter_->GetValue();
            ts_iter->Seek(ts);
            if (ts_iter->Valid()) {
                return RowToSlice(ts_iter->GetValue());
            }
        }
        // TODO(hw): Valid() to check row data? what if only one entry invalid?
        return "";
    }

 private:
    std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter_;
};

class IOTTraverseIterator : public MemTableTraverseIterator {
 public:
    IOTTraverseIterator(Segment** segments, uint32_t seg_cnt, ::openmldb::storage::TTLType ttl_type,
                        uint64_t expire_time, uint64_t expire_cnt, uint32_t ts_index, type::CompressType compress_type,
                        std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter)
        : MemTableTraverseIterator(segments, seg_cnt, ttl_type, expire_time, expire_cnt, ts_index, compress_type),
          cidx_iter_(std::move(cidx_iter)) {}
    ~IOTTraverseIterator() override {}

    openmldb::base::Slice GetValue() const override {
        auto pkeys_pts = MemTableTraverseIterator::GetValue();
        std::string pkeys;
        uint64_t ts;
        if (!UnpackPkeysAndPts(pkeys_pts.ToString(), &pkeys, &ts)) {
            LOG(WARNING) << "unpack pkeys and pts failed";
            return "";
        }
        // distribute cidx iter should seek to (key, ts)
        DLOG(INFO) << "seek to " << pkeys << ", " << ts;
        cidx_iter_->Seek(pkeys);
        if (cidx_iter_->Valid()) {
            // seek to ts
            auto ts_iter_ = cidx_iter_->GetValue();
            ts_iter_->Seek(ts);
            if (ts_iter_->Valid()) {
                // TODO(hw): hard copy, or hold ts_iter to store value? IOTIterator should be the same.
                DLOG(INFO) << "valid, " << ts_iter_->GetValue().ToString();
                return RowToSlice(ts_iter_->GetValue());
            }
        }
        LOG(WARNING) << "no suitable iter";
        return "";  // won't core, just no row for select?
    }

 private:
    std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter_;
    std::unique_ptr<hybridse::codec::RowIterator> ts_iter_;
};

class IOTWindowIterator : public MemTableWindowIterator {
 public:
    IOTWindowIterator(TimeEntries::Iterator* it, ::openmldb::storage::TTLType ttl_type, uint64_t expire_time,
                      uint64_t expire_cnt, type::CompressType compress_type,
                      std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter)
        : MemTableWindowIterator(it, ttl_type, expire_time, expire_cnt, compress_type),
          cidx_iter_(std::move(cidx_iter)) {
        DLOG(INFO) << "create IOTWindowIterator";
    }
    // for debug
    void SetSchema(const codec::Schema& schema, const std::vector<int>& pkeys_idx) {
        pkeys_idx_ = pkeys_idx;
        row_view_.reset(new codec::RowView(schema));
    }
    const ::hybridse::codec::Row& GetValue() override {
        auto pkeys_pts = MemTableWindowIterator::GetValue();
        if (pkeys_pts.empty()) {
            LOG(WARNING) << "empty pkeys_pts for key " << GetKey();
            return dummy;
        }

        // unpack the row and get pkeys+pts
        // Row -> cols
        std::string pkeys;
        uint64_t ts;
        if (!UnpackPkeysAndPts(pkeys_pts.ToString(), &pkeys, &ts)) {
            LOG(WARNING) << "unpack pkeys and pts failed";
            return dummy;
        }
        // TODO(hw): what if no ts? it'll be 0 for temp
        DLOG(INFO) << "pkeys=" << pkeys << ", ts=" << ts;
        cidx_iter_->Seek(pkeys);
        if (cidx_iter_->Valid()) {
            // seek to ts
            DLOG(INFO) << "seek to ts " << ts;
            // hold the row iterator to avoid invalidation
            cidx_ts_iter_ = std::move(cidx_iter_->GetValue());
            cidx_ts_iter_->Seek(ts);
            // must be the same keys+ts
            if (cidx_ts_iter_->Valid()) {
                // DLOG(INFO) << "valid, is the same value? " << GetKeys(cidx_ts_iter_->GetValue());
                return cidx_ts_iter_->GetValue();
            }
        }
        // Valid() to check row data? what if only one entry invalid?
        return dummy;
    }

 private:
    std::string GetKeys(const hybridse::codec::Row& pkeys_pts) {
        std::string pkeys, key;  // RowView Get will assign output, no need to clear
        for (auto pkey_idx : pkeys_idx_) {
            if (!pkeys.empty()) {
                pkeys += "|";
            }
            // TODO(hw): if null, append to key?
            auto ret = row_view_->GetStrValue(pkeys_pts.buf(), pkey_idx, &key);
            if (ret == -1) {
                LOG(WARNING) << "get pkey failed";
                return {};
            }
            pkeys += key.empty() ? hybridse::codec::EMPTY_STRING : key;
            DLOG(INFO) << pkey_idx << "=" << key;
        }
        return pkeys;
    }

 private:
    std::unique_ptr<::hybridse::codec::WindowIterator> cidx_iter_;
    std::unique_ptr<hybridse::codec::RowIterator> cidx_ts_iter_;
    // for debug
    std::unique_ptr<codec::RowView> row_view_;
    std::vector<int> pkeys_idx_;

    ::hybridse::codec::Row dummy;
};

class IOTKeyIterator : public MemTableKeyIterator {
 public:
    IOTKeyIterator(Segment** segments, uint32_t seg_cnt, ::openmldb::storage::TTLType ttl_type, uint64_t expire_time,
                   uint64_t expire_cnt, uint32_t ts_index, type::CompressType compress_type,
                   std::shared_ptr<catalog::TabletTableHandler> cidx_handler, const std::string& cidx_name)
        : MemTableKeyIterator(segments, seg_cnt, ttl_type, expire_time, expire_cnt, ts_index, compress_type) {
        // cidx_iter will be used by RowIterator but it's unique, so create it when get RowIterator
        cidx_handler_ = cidx_handler;
        cidx_name_ = cidx_name;
    }

    ~IOTKeyIterator() override {}
    void SetSchema(const std::shared_ptr<openmldb::storage::Schema>& schema,
                   const std::shared_ptr<openmldb::storage::IndexDef>& cidx) {
        schema_ = *schema;  // copy
        // pkeys idx
        std::map<std::string, int> col_idx_map;
        for (int i = 0; i < schema_.size(); i++) {
            col_idx_map[schema_[i].name()] = i;
        }
        pkeys_idx_.clear();
        for (auto pkey : cidx->GetColumns()) {
            pkeys_idx_.emplace_back(col_idx_map[pkey.GetName()]);
        }
    }
    ::hybridse::vm::RowIterator* GetRawValue() override {
        DLOG(INFO) << "GetRawValue for key " << GetKey().ToString() << ", bind cidx " << cidx_name_;
        TimeEntries::Iterator* it = GetTimeIter();
        auto cidx_iter = cidx_handler_->GetWindowIterator(cidx_name_);
        auto iter =
            new IOTWindowIterator(it, ttl_type_, expire_time_, expire_cnt_, compress_type_, std::move(cidx_iter));
        // iter->SetSchema(schema_, pkeys_idx_);
        return iter;
    }

 private:
    std::shared_ptr<catalog::TabletTableHandler> cidx_handler_;
    std::string cidx_name_;
    // test
    codec::Schema schema_;
    std::vector<int> pkeys_idx_;
};

class GCEntryInfo {
 public:
    typedef std::pair<uint64_t, DataBlock*> Entry;
    ~GCEntryInfo() {
        for (auto& entry : entries_) {
            entry.second->dim_cnt_down--;
            // TODO delete?
        }
    }
    void AddEntry(const Slice& keys, uint64_t ts, storage::DataBlock* ptr) {
        // to avoid Block deleted before gc, add ref
        ptr->dim_cnt_down++;  // TODO(hw): no concurrency? or make sure under lock
        entries_.emplace_back(ts, ptr);
    }
    std::size_t Size() { return entries_.size(); }
    std::vector<Entry>& GetEntries() { return entries_; }
    bool Full() { return entries_.size() >= FLAGS_cidx_gc_max_size; }

 private:
    // std::vector<std::pair<Slice, uint64_t>> entries_;
    std::vector<Entry> entries_;
};

class IOTSegment : public Segment {
 public:
    explicit IOTSegment(uint8_t height) : Segment(height) {}
    IOTSegment(uint8_t height, const std::vector<uint32_t>& ts_idx_vec,
               const std::vector<common::IndexType>& index_types)
        : Segment(height, ts_idx_vec), index_types_(index_types) {
        // find clustered ts id
        for (uint32_t i = 0; i < ts_idx_vec.size(); i++) {
            if (index_types_[i] == common::kClustered) {
                clustered_ts_id_ = ts_idx_vec[i];
                break;
            }
        }
    }
    ~IOTSegment() override {}

    bool PutUnlock(const Slice& key, uint64_t time, DataBlock* row, bool put_if_absent, bool check_all_time);
    bool Put(const Slice& key, const std::map<int32_t, uint64_t>& ts_map, DataBlock* cblock, DataBlock* sblock,
             bool put_if_absent = false);
    // use ts map to get idx in entry_arr
    // no ok status, exists or not found
    absl::Status CheckKeyExists(const Slice& key, const std::map<int32_t, uint64_t>& ts_map);
    // DEFAULT_TS_COL_ID is uint32_t max, so clsutered_ts_id_ can't have a init value, use std::optional
    bool IsClusteredTs(uint32_t ts_id) {
        return clustered_ts_id_.has_value() ? (ts_id == clustered_ts_id_.value()) : false;
    }

    std::optional<uint32_t> ClusteredTs() const { return clustered_ts_id_; }

    void GrepGCEntry(const std::map<uint32_t, TTLSt>& ttl_st_map, GCEntryInfo* gc_entry_info);

    MemTableIterator* NewIterator(const Slice& key, Ticket& ticket, type::CompressType compress_type) {  // NOLINT
        DLOG_ASSERT(false) << "unsupported, let iot table create it";
        return nullptr;
    }
    MemTableIterator* NewIterator(const Slice& key, uint32_t idx, Ticket& ticket,  // NOLINT
                                  type::CompressType compress_type) {
        DLOG_ASSERT(false) << "unsupported, let iot table create it";
        return nullptr;
    }

 private:
    void GrepGCAllType(const std::map<uint32_t, TTLSt>& ttl_st_map, GCEntryInfo* gc_entry_info);

 private:
    std::vector<common::IndexType> index_types_;
    std::optional<uint32_t> clustered_ts_id_;
};

}  // namespace openmldb::storage
#endif  // SRC_STORAGE_IOT_SEGMENT_H_
