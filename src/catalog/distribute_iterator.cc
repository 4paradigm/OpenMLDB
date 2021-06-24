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

#include "catalog/distribute_iterator.h"

namespace openmldb {
namespace catalog {

FullTableIterator::FullTableIterator(std::shared_ptr<Tables> tables)
    : tables_(tables), cur_pid_(0), it_(), key_(0), value_() {}

void FullTableIterator::SeekToFirst() {
    it_.reset();
    for (const auto& kv : *tables_) {
        it_.reset(kv.second->NewTraverseIterator(0));
        it_->SeekToFirst();
        if (it_->Valid()) {
            cur_pid_ = kv.first;
            key_ = it_->GetKey();
            break;
        }
    }
}

bool FullTableIterator::Valid() const { return it_ && it_->Valid(); }

void FullTableIterator::Next() {
    it_->Next();
    if (!it_->Valid()) {
        auto iter = tables_->find(cur_pid_);
        if (iter == tables_->end()) {
            return;
        }
        for (iter++; iter != tables_->end(); iter++) {
            it_.reset(iter->second->NewTraverseIterator(0));
            it_->SeekToFirst();
            if (it_->Valid()) {
                cur_pid_ = iter->first;
                break;
            }
        }
    }
    if (it_ && it_->Valid()) {
        key_ = it_->GetKey();
    }
}

const ::hybridse::codec::Row& FullTableIterator::GetValue() {
    value_ = ::hybridse::codec::Row(
        ::hybridse::base::RefCountedSlice::Create(it_->GetValue().data(), it_->GetValue().size()));
    return value_;
}

DistributeWindowIterator::DistributeWindowIterator(std::shared_ptr<Tables> tables, uint32_t index)
    : tables_(tables), index_(index), cur_pid_(0), pid_num_(1), it_() {
    if (tables && !tables->empty()) {
        pid_num_ = tables->begin()->second->GetTableMeta()->table_partition_size();
    }
}

void DistributeWindowIterator::Seek(const std::string& key) {
    // assume all partitions in one tablet
    DLOG(INFO) << "seek to key " << key;
    it_.reset();
    if (!tables_) {
        return;
    }
    if (pid_num_ > 0) {
        cur_pid_ = (uint32_t)(::openmldb::base::hash64(key) % pid_num_);
    }
    auto iter = tables_->find(cur_pid_);
    if (iter != tables_->end()) {
        it_.reset(iter->second->NewWindowIterator(index_));
        it_->Seek(key);
        if (it_->Valid()) {
            return;
        }
    }
    for (const auto& kv : *tables_) {
        if (kv.first <= cur_pid_) {
            continue;
        }
        it_.reset(kv.second->NewWindowIterator(index_));
        it_->SeekToFirst();
        if (it_->Valid()) {
            cur_pid_ = kv.first;
            break;
        }
    }
}

void DistributeWindowIterator::SeekToFirst() {
    DLOG(INFO) << "seek to first";
    it_.reset();
    if (!tables_) {
        return;
    }
    for (const auto& kv : *tables_) {
        it_.reset(kv.second->NewWindowIterator(index_));
        it_->SeekToFirst();
        if (it_->Valid()) {
            cur_pid_ = kv.first;
            break;
        }
    }
}

void DistributeWindowIterator::Next() {
    it_->Next();
    if (!it_->Valid()) {
        auto iter = tables_->find(cur_pid_);
        if (iter == tables_->end()) {
            return;
        }
        for (iter++; iter != tables_->end(); iter++) {
            it_.reset(iter->second->NewWindowIterator(index_));
            it_->SeekToFirst();
            if (it_->Valid()) {
                cur_pid_ = iter->first;
                break;
            }
        }
    }
}

bool DistributeWindowIterator::Valid() { return it_ && it_->Valid(); }

std::unique_ptr<::hybridse::codec::RowIterator> DistributeWindowIterator::GetValue() { return it_->GetValue(); }

::hybridse::codec::RowIterator* DistributeWindowIterator::GetRawValue() { return it_->GetRawValue(); }

const ::hybridse::codec::Row DistributeWindowIterator::GetKey() { return it_->GetKey(); }

}  // namespace catalog
}  // namespace openmldb
