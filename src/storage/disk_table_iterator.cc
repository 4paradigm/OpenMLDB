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

#include "storage/disk_table_iterator.h"

#include <string>
#include "gflags/gflags.h"
#include "storage/key_transform.h"

DECLARE_uint32(max_traverse_cnt);

namespace openmldb {
namespace storage {

DiskTableIterator::DiskTableIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
                                     const std::string& pk)
    : db_(db), it_(it), snapshot_(snapshot), pk_(pk), ts_(0) {}

DiskTableIterator::DiskTableIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
                                     const std::string& pk, uint32_t ts_idx)
    : db_(db), it_(it), snapshot_(snapshot), pk_(pk), ts_(0), ts_idx_(ts_idx) {
    has_ts_idx_ = true;
}

DiskTableIterator::~DiskTableIterator() {
    delete it_;
    db_->ReleaseSnapshot(snapshot_);
}

bool DiskTableIterator::Valid() {
    if (it_ == nullptr || !it_->Valid()) {
        return false;
    }
    rocksdb::Slice cur_pk;
    uint32_t cur_ts_idx = UINT32_MAX;
    ParseKeyAndTs(has_ts_idx_, it_->key(), &cur_pk, &ts_, &cur_ts_idx);
    int ret = cur_pk.compare(rocksdb::Slice(pk_));
    return has_ts_idx_ ? ret == 0 && cur_ts_idx == ts_idx_ : ret == 0;
}

void DiskTableIterator::Next() { return it_->Next(); }

openmldb::base::Slice DiskTableIterator::GetValue() const {
    rocksdb::Slice value = it_->value();
    return openmldb::base::Slice(value.data(), value.size());
}

std::string DiskTableIterator::GetPK() const { return pk_; }

uint64_t DiskTableIterator::GetKey() const { return ts_; }

void DiskTableIterator::SeekToFirst() {
    if (has_ts_idx_) {
        std::string combine_key = CombineKeyTs(pk_, UINT64_MAX, ts_idx_);
        it_->Seek(rocksdb::Slice(combine_key));
    } else {
        std::string combine_key = CombineKeyTs(pk_, UINT64_MAX);
        it_->Seek(rocksdb::Slice(combine_key));
    }
}

void DiskTableIterator::Seek(const uint64_t ts) {
    if (has_ts_idx_) {
        std::string combine_key = CombineKeyTs(pk_, ts, ts_idx_);
        it_->Seek(rocksdb::Slice(combine_key));
    } else {
        std::string combine_key = CombineKeyTs(pk_, ts);
        it_->Seek(rocksdb::Slice(combine_key));
    }
}

DiskTableTraverseIterator::DiskTableTraverseIterator(rocksdb::DB* db, rocksdb::Iterator* it,
                                                     const rocksdb::Snapshot* snapshot,
                                                     ::openmldb::storage::TTLType ttl_type, const uint64_t& expire_time,
                                                     const uint64_t& expire_cnt)
    : db_(db),
      it_(it),
      snapshot_(snapshot),
      record_idx_(0),
      expire_value_(expire_time, expire_cnt, ttl_type),
      has_ts_idx_(false),
      ts_idx_(0),
      traverse_cnt_(0) {}

DiskTableTraverseIterator::DiskTableTraverseIterator(rocksdb::DB* db, rocksdb::Iterator* it,
                                                     const rocksdb::Snapshot* snapshot,
                                                     ::openmldb::storage::TTLType ttl_type, const uint64_t& expire_time,
                                                     const uint64_t& expire_cnt, int32_t ts_idx)
    : db_(db),
      it_(it),
      snapshot_(snapshot),
      record_idx_(0),
      expire_value_(expire_time, expire_cnt, ttl_type),
      has_ts_idx_(true),
      ts_idx_(ts_idx),
      traverse_cnt_(0) {}

DiskTableTraverseIterator::~DiskTableTraverseIterator() {
    delete it_;
    db_->ReleaseSnapshot(snapshot_);
}

uint64_t DiskTableTraverseIterator::GetCount() const { return traverse_cnt_; }

bool DiskTableTraverseIterator::Valid() {
    if (FLAGS_max_traverse_cnt > 0 && traverse_cnt_ >= FLAGS_max_traverse_cnt) {
        return false;
    }
    return it_->Valid();
}

void DiskTableTraverseIterator::Next() {
    for (it_->Next(); it_->Valid(); it_->Next()) {
        std::string last_pk = pk_;
        uint32_t cur_ts_idx = UINT32_MAX;
        traverse_cnt_++;
        ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
        if (last_pk == pk_) {
            if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                traverse_cnt_--;
                continue;
            }
            record_idx_++;
        } else {
            record_idx_ = 0;
            if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                traverse_cnt_--;
                continue;
            }
            record_idx_ = 1;
        }
        if (FLAGS_max_traverse_cnt > 0 && traverse_cnt_ >= FLAGS_max_traverse_cnt) {
            break;
        }
        if (IsExpired()) {
            NextPK();
        }
        break;
    }
}

openmldb::base::Slice DiskTableTraverseIterator::GetValue() const {
    rocksdb::Slice value = it_->value();
    return openmldb::base::Slice(value.data(), value.size());
}

std::string DiskTableTraverseIterator::GetPK() const { return pk_; }

uint64_t DiskTableTraverseIterator::GetKey() const { return ts_; }

void DiskTableTraverseIterator::SeekToFirst() {
    it_->SeekToFirst();
    record_idx_ = 1;
    for (; it_->Valid(); it_->Next()) {
        uint32_t cur_ts_idx = UINT32_MAX;
        traverse_cnt_++;
        if (FLAGS_max_traverse_cnt > 0 && traverse_cnt_ >= FLAGS_max_traverse_cnt) {
            break;
        }
        ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
        if (has_ts_idx_ && cur_ts_idx != ts_idx_) {
            continue;
        }
        if (IsExpired()) {
            NextPK();
        }
        break;
    }
}

void DiskTableTraverseIterator::Seek(const std::string& pk, uint64_t time) {
    std::string combine;
    if (has_ts_idx_) {
        combine = CombineKeyTs(rocksdb::Slice(pk), time, ts_idx_);
    } else {
        combine = CombineKeyTs(rocksdb::Slice(pk), time);
    }
    it_->Seek(rocksdb::Slice(combine));
    if (expire_value_.ttl_type == ::openmldb::storage::TTLType::kLatestTime) {
        record_idx_ = 0;
        for (; it_->Valid(); it_->Next()) {
            uint32_t cur_ts_idx = UINT32_MAX;
            traverse_cnt_++;
            if (FLAGS_max_traverse_cnt > 0 && traverse_cnt_ >= FLAGS_max_traverse_cnt) {
                break;
            }
            ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
            if (pk_ == pk) {
                if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                    continue;
                }
                record_idx_++;
                if (IsExpired()) {
                    NextPK();
                    break;
                }
                if (ts_ > time) {
                    continue;
                }
            } else {
                record_idx_ = 1;
                if (IsExpired()) {
                    NextPK();
                }
            }
            break;
        }
    } else {
        for (; it_->Valid(); it_->Next()) {
            uint32_t cur_ts_idx = UINT32_MAX;
            traverse_cnt_++;
            if (FLAGS_max_traverse_cnt > 0 && traverse_cnt_ >= FLAGS_max_traverse_cnt) {
                break;
            }
            ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
            if (pk_ == pk) {
                if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                    continue;
                }
                if (ts_ > time) {
                    continue;
                }
                if (IsExpired()) {
                    NextPK();
                }
            } else {
                if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                    continue;
                }
                if (IsExpired()) {
                    NextPK();
                }
            }
            break;
        }
    }
}

bool DiskTableTraverseIterator::IsExpired() { return expire_value_.IsExpired(ts_, record_idx_); }

void DiskTableTraverseIterator::NextPK() {
    std::string last_pk = pk_;
    std::string combine;
    if (has_ts_idx_) {
        std::string combine_key = CombineKeyTs(rocksdb::Slice(last_pk), 0, ts_idx_);
        it_->Seek(rocksdb::Slice(combine_key));
    } else {
        std::string combine_key = CombineKeyTs(rocksdb::Slice(last_pk), 0);
        it_->Seek(rocksdb::Slice(combine_key));
    }
    record_idx_ = 1;
    while (it_->Valid()) {
        uint32_t cur_ts_idx = UINT32_MAX;
        traverse_cnt_++;
        if (FLAGS_max_traverse_cnt > 0 && traverse_cnt_ >= FLAGS_max_traverse_cnt) {
            break;
        }
        ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
        if (pk_ != last_pk) {
            if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                it_->Next();
                continue;
            }
            if (!IsExpired()) {
                return;
            } else {
                last_pk = pk_;
                std::string combine;
                if (has_ts_idx_) {
                    std::string combine_key = CombineKeyTs(rocksdb::Slice(last_pk), 0, ts_idx_);
                    it_->Seek(rocksdb::Slice(combine_key));
                } else {
                    std::string combine_key = CombineKeyTs(rocksdb::Slice(last_pk), 0);
                    it_->Seek(rocksdb::Slice(combine_key));
                }
                record_idx_ = 1;
            }
        } else {
            it_->Next();
        }
    }
}

DiskTableKeyIterator::DiskTableKeyIterator(rocksdb::DB* db, rocksdb::Iterator* it,
                                           const rocksdb::Snapshot* snapshot, ::openmldb::storage::TTLType ttl_type,
                                           const uint64_t& expire_time, const uint64_t& expire_cnt,
                                           rocksdb::ColumnFamilyHandle* column_handle)
    : db_(db),
      it_(it),
      snapshot_(snapshot),
      ttl_type_(ttl_type),
      expire_time_(expire_time),
      expire_cnt_(expire_cnt),
      has_ts_idx_(false),
      ts_idx_(0),
      column_handle_(column_handle) {}

DiskTableKeyIterator::DiskTableKeyIterator(rocksdb::DB* db, rocksdb::Iterator* it,
                                           const rocksdb::Snapshot* snapshot, ::openmldb::storage::TTLType ttl_type,
                                           const uint64_t& expire_time, const uint64_t& expire_cnt, int32_t ts_idx,
                                           rocksdb::ColumnFamilyHandle* column_handle)
    : db_(db),
      it_(it),
      snapshot_(snapshot),
      ttl_type_(ttl_type),
      expire_time_(expire_time),
      expire_cnt_(expire_cnt),
      has_ts_idx_(true),
      ts_idx_(ts_idx),
      column_handle_(column_handle) {}

DiskTableKeyIterator::~DiskTableKeyIterator() {
    delete it_;
    db_->ReleaseSnapshot(snapshot_);
}

void DiskTableKeyIterator::SeekToFirst() {
    it_->SeekToFirst();
    uint32_t cur_ts_idx = UINT32_MAX;
    ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
}

void DiskTableKeyIterator::NextPK() {
    std::string last_pk = pk_;
    std::string combine_key;
    if (has_ts_idx_) {
        combine_key = CombineKeyTs(rocksdb::Slice(last_pk), 0, ts_idx_);
    } else {
        combine_key = CombineKeyTs(rocksdb::Slice(last_pk), 0);
    }
    it_->Seek(rocksdb::Slice(combine_key));
    while (it_->Valid()) {
        uint32_t cur_ts_idx = UINT32_MAX;
        ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
        if (pk_ != last_pk) {
            if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                it_->Next();
                continue;
            }
            break;
        } else {
            it_->Next();
        }
    }
}

void DiskTableKeyIterator::Next() { NextPK(); }

void DiskTableKeyIterator::Seek(const std::string& pk) {
    std::string combine;
    uint64_t tmp_ts = UINT64_MAX;
    if (has_ts_idx_) {
        combine = CombineKeyTs(rocksdb::Slice(pk), tmp_ts, ts_idx_);
    } else {
        combine = CombineKeyTs(rocksdb::Slice(pk), tmp_ts);
    }
    it_->Seek(rocksdb::Slice(combine));
    for (; it_->Valid(); it_->Next()) {
        uint32_t cur_ts_idx = UINT32_MAX;
        ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
        if (pk_ == pk) {
            if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                continue;
            }
        }
        break;
    }
}

bool DiskTableKeyIterator::Valid() {
    return it_->Valid();
}

const hybridse::codec::Row DiskTableKeyIterator::GetKey() {
    hybridse::codec::Row row(
        ::hybridse::base::RefCountedSlice::Create(pk_.c_str(), pk_.size()));
    return row;
}

std::unique_ptr<::hybridse::vm::RowIterator> DiskTableKeyIterator::GetValue() {
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    // ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, column_handle_);
    return std::make_unique<DiskTableRowIterator>(db_, it, snapshot, ttl_type_, expire_time_,
                                                  expire_cnt_, pk_, ts_, has_ts_idx_, ts_idx_);
}

::hybridse::vm::RowIterator* DiskTableKeyIterator::GetRawValue() {
    rocksdb::ReadOptions ro = rocksdb::ReadOptions();
    const rocksdb::Snapshot* snapshot = db_->GetSnapshot();
    ro.snapshot = snapshot;
    // ro.prefix_same_as_start = true;
    ro.pin_data = true;
    rocksdb::Iterator* it = db_->NewIterator(ro, column_handle_);
    return new DiskTableRowIterator(db_, it, snapshot, ttl_type_, expire_time_, expire_cnt_, pk_, ts_, has_ts_idx_,
                                    ts_idx_);
}

DiskTableRowIterator::DiskTableRowIterator(rocksdb::DB* db, rocksdb::Iterator* it, const rocksdb::Snapshot* snapshot,
                                           ::openmldb::storage::TTLType ttl_type, uint64_t expire_time,
                                           uint64_t expire_cnt, std::string pk, uint64_t ts, bool has_ts_idx,
                                           uint32_t ts_idx)
    : db_(db),
      it_(it),
      snapshot_(snapshot),
      record_idx_(1),
      expire_value_(expire_time, expire_cnt, ttl_type),
      pk_(pk),
      row_pk_(pk),
      ts_(ts),
      has_ts_idx_(has_ts_idx),
      ts_idx_(ts_idx),
      row_() {}

DiskTableRowIterator::~DiskTableRowIterator() {
    delete it_;
    db_->ReleaseSnapshot(snapshot_);
}

bool DiskTableRowIterator::Valid() const {
    if (!pk_valid_) return false;
    if (!it_->Valid() || expire_value_.IsExpired(ts_, record_idx_)) {
        return false;
    }
    return true;
}

void DiskTableRowIterator::Next() {
    ResetValue();
    for (it_->Next(); it_->Valid(); it_->Next()) {
        uint32_t cur_ts_idx = UINT32_MAX;
        ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
        if (row_pk_ == pk_) {
            if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                // combineKey is (pk, ts_col, ts). So if cur_ts_idx != ts_idx,
                // iterator will never get to (pk, ts_idx_) again. Can break here.
                pk_valid_ = false;
                break;
            }
            record_idx_++;
            pk_valid_ = true;
        } else {
            pk_valid_ = false;
        }
        break;
    }
}

inline const uint64_t& DiskTableRowIterator::GetKey() const { return ts_; }

const ::hybridse::codec::Row& DiskTableRowIterator::GetValue() {
    if (ValidValue()) {
        return row_;
    }
    valid_value_ = true;
    size_t size = it_->value().size();
    int8_t* copyed_row_data = reinterpret_cast<int8_t*>(malloc(size));
    memcpy(copyed_row_data, it_->value().data(), size);
    row_.Reset(::hybridse::base::RefCountedSlice::CreateManaged(copyed_row_data, size));
    return row_;
}

void DiskTableRowIterator::Seek(const uint64_t& key) {
    ResetValue();
    if (expire_value_.ttl_type == TTLType::kAbsoluteTime) {
        std::string combine;
        uint64_t tmp_ts = key;
        if (has_ts_idx_) {
            combine = CombineKeyTs(rocksdb::Slice(row_pk_), tmp_ts, ts_idx_);
        } else {
            combine = CombineKeyTs(rocksdb::Slice(row_pk_), tmp_ts);
        }
        it_->Seek(rocksdb::Slice(combine));
        for (; it_->Valid(); it_->Next()) {
            uint32_t cur_ts_idx = UINT32_MAX;
            ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
            if (pk_ == row_pk_) {
                if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                    // combineKey is (pk, ts_col, ts). So if cur_ts_idx != ts_idx,
                    // iterator will never get to (pk, ts_idx_) again. Can break here.
                    pk_valid_ = false;
                    break;
                }
                pk_valid_ = true;
            } else {
                pk_valid_ = false;
            }
            break;
        }
    } else {
        SeekToFirst();
        while (Valid() && GetKey() > key) {
            Next();
        }
    }
}

void DiskTableRowIterator::SeekToFirst() {
    ResetValue();
    record_idx_ = 1;
    std::string combine;
    uint64_t tmp_ts = UINT64_MAX;
    if (has_ts_idx_) {
        combine = CombineKeyTs(rocksdb::Slice(row_pk_), tmp_ts, ts_idx_);
    } else {
        combine = CombineKeyTs(rocksdb::Slice(row_pk_), tmp_ts);
    }
    it_->Seek(rocksdb::Slice(combine));
    for (; it_->Valid(); it_->Next()) {
        uint32_t cur_ts_idx = UINT32_MAX;
        ParseKeyAndTs(has_ts_idx_, it_->key(), &pk_, &ts_, &cur_ts_idx);
        if (pk_ == row_pk_) {
            if (has_ts_idx_ && (cur_ts_idx != ts_idx_)) {
                // combineKey is (pk, ts_col, ts). So if cur_ts_idx != ts_idx,
                // iterator will never get to (pk, ts_idx_) again. Can break here.
                pk_valid_ = false;
                break;
            }
            pk_valid_ = true;
        } else {
            pk_valid_ = false;
        }
        break;
    }
}

bool DiskTableRowIterator::IsSeekable() const { return true; }

}  // namespace storage
}  // namespace openmldb
