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

#include "storage/window_iterator.h"

#include <string>
#include "base/hash.h"

namespace openmldb {
namespace storage {

constexpr uint32_t SEED = 0xe17a1465;

MemTableWindowIterator::~MemTableWindowIterator() {
    delete it_;
}

bool MemTableWindowIterator::Valid() const {
    if (!it_->Valid() || expire_value_.IsExpired(it_->GetKey(), record_idx_)) {
        return false;
    }
    return true;
}

void MemTableWindowIterator::Next() {
    it_->Next();
    record_idx_++;
}

const uint64_t& MemTableWindowIterator::GetKey() const {
    return it_->GetKey();
}

const ::hybridse::codec::Row& MemTableWindowIterator::GetValue() {
    row_.Reset(reinterpret_cast<const int8_t*>(it_->GetValue()->data), it_->GetValue()->size);
    return row_;
}

void MemTableWindowIterator::Seek(const uint64_t& key) {
    if (expire_value_.ttl_type == TTLType::kAbsoluteTime) {
        it_->Seek(key);
    } else {
        SeekToFirst();
        while (Valid() && GetKey() > key) {
            Next();
        }
    }
}

void MemTableWindowIterator::SeekToFirst() {
    record_idx_ = 1;
    it_->SeekToFirst();
}

MemTableKeyIterator::MemTableKeyIterator(Segment** segments, uint32_t seg_cnt, ::openmldb::storage::TTLType ttl_type,
                                         uint64_t expire_time, uint64_t expire_cnt, uint32_t ts_index)
    : segments_(segments),
      seg_cnt_(seg_cnt),
      seg_idx_(0),
      pk_it_(nullptr),
      it_(nullptr),
      ttl_type_(ttl_type),
      expire_time_(expire_time),
      expire_cnt_(expire_cnt),
      ticket_(),
      ts_idx_(0) {
    uint32_t idx = 0;
    if (segments_[0]->GetTsIdx(ts_index, idx) == 0) {
        ts_idx_ = idx;
    }
}

MemTableKeyIterator::~MemTableKeyIterator() {
    if (pk_it_ != nullptr) delete pk_it_;
}

void MemTableKeyIterator::SeekToFirst() {
    ticket_.Pop();
    if (pk_it_ != nullptr) {
        delete pk_it_;
        pk_it_ = nullptr;
    }
    for (seg_idx_ = 0; seg_idx_ < seg_cnt_; seg_idx_++) {
        pk_it_ = segments_[seg_idx_]->GetKeyEntries()->NewIterator();
        pk_it_->SeekToFirst();
        if (pk_it_->Valid()) return;
        delete pk_it_;
        pk_it_ = nullptr;
    }
}

void MemTableKeyIterator::Seek(const std::string& key) {
    if (pk_it_ != nullptr) {
        delete pk_it_;
        pk_it_ = nullptr;
    }
    ticket_.Pop();
    if (seg_cnt_ > 1) {
        seg_idx_ = ::openmldb::base::hash(key.c_str(), key.length(), SEED) % seg_cnt_;
    }
    Slice spk(key);
    pk_it_ = segments_[seg_idx_]->GetKeyEntries()->NewIterator();
    pk_it_->Seek(spk);
    if (!pk_it_->Valid()) {
        NextPK();
    }
}

bool MemTableKeyIterator::Valid() {
    return pk_it_ != nullptr && pk_it_->Valid();
}

void MemTableKeyIterator::Next() {
    NextPK();
}

::hybridse::vm::RowIterator* MemTableKeyIterator::GetRawValue() {
    TimeEntries::Iterator* it = nullptr;
    if (segments_[seg_idx_]->GetTsCnt() > 1) {
        KeyEntry* entry = ((KeyEntry**)pk_it_->GetValue())[ts_idx_];  // NOLINT
        it = entry->entries.NewIterator();
        ticket_.Push(entry);
    } else {
        it = ((KeyEntry*)pk_it_->GetValue())  // NOLINT
                 ->entries.NewIterator();
        ticket_.Push((KeyEntry*)pk_it_->GetValue());  // NOLINT
    }
    it->SeekToFirst();
    return new MemTableWindowIterator(it, ttl_type_, expire_time_, expire_cnt_);
}

std::unique_ptr<::hybridse::vm::RowIterator> MemTableKeyIterator::GetValue() {
    return std::unique_ptr<::hybridse::vm::RowIterator>(GetRawValue());
}

const hybridse::codec::Row MemTableKeyIterator::GetKey() {
    return hybridse::codec::Row(
            ::hybridse::base::RefCountedSlice::Create(pk_it_->GetKey().data(), pk_it_->GetKey().size()));
}

void MemTableKeyIterator::NextPK() {
    do {
        ticket_.Pop();
        if (pk_it_->Valid()) {
            pk_it_->Next();
        }
        if (!pk_it_->Valid()) {
            delete pk_it_;
            pk_it_ = nullptr;
            seg_idx_++;
            if (seg_idx_ < seg_cnt_) {
                pk_it_ = segments_[seg_idx_]->GetKeyEntries()->NewIterator();
                pk_it_->SeekToFirst();
                if (!pk_it_->Valid()) {
                    continue;
                }
            }
        }
        break;
    } while (true);
}

}  // namespace storage
}  // namespace openmldb
