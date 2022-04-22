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
#include "gflags/gflags.h"

DECLARE_uint32(traverse_cnt_limit);

namespace openmldb {
namespace catalog {

constexpr uint32_t INVALID_PID = UINT32_MAX;

FullTableIterator::FullTableIterator(uint32_t tid, std::shared_ptr<Tables> tables,
        const std::map<uint32_t, std::shared_ptr<::openmldb::client::TabletClient>>& tablet_clients)
    : tid_(tid), tables_(tables), tablet_clients_(tablet_clients), in_local_(true), cur_pid_(INVALID_PID),
    it_(), kv_it_(), key_(0), last_pk_(), value_() {
}

void FullTableIterator::SeekToFirst() {
    Reset();
    Next();
}

bool FullTableIterator::Valid() const {
    return (it_ && it_->Valid()) || (kv_it_ && kv_it_->Valid());
}

void FullTableIterator::Next() {
    if (NextFromLocal()) {
        return;
    }
    NextFromRemote();
}

void FullTableIterator::Reset() {
    it_.reset();
    kv_it_.reset();
    cur_pid_ = INVALID_PID;
    in_local_ = true;
}

void FullTableIterator::EndLocal() {
    in_local_ = false;
    cur_pid_ = INVALID_PID;
    it_.reset();
}

bool FullTableIterator::NextFromLocal() {
    if (!in_local_ || !tables_ || tables_->empty()) {
        return false;
    }
    if (it_) {
        it_->Next();
        if (it_->Valid()) {
            key_ = it_->GetKey();
            return true;
        }
    }
    it_.reset();
    auto iter = tables_->begin();
    if (cur_pid_ == INVALID_PID) {
        cur_pid_ = iter->first;
    } else {
        iter = tables_->find(cur_pid_);;
        if (iter == tables_->end()) {
            EndLocal();
            return false;
        }
        iter++;
    }
    do {
        if (iter == tables_->end()) {
            EndLocal();
            return false;
        }
        cur_pid_ = iter->first;
        it_.reset(iter->second->NewTraverseIterator(0));
        it_->SeekToFirst();
        if (it_->Valid()) {
            break;
        }
        iter++;
    } while (true);
    if (it_ && it_->Valid()) {
        key_ = it_->GetKey();
        return true;
    }
    EndLocal();
    return false;
}

bool FullTableIterator::NextFromRemote() {
    if (tablet_clients_.empty()) {
        return false;
    }
    if (kv_it_) {
        kv_it_->Next();
        if (kv_it_->Valid()) {
            key_ = kv_it_->GetKey();
            last_pk_ = kv_it_->GetPK();
            return true;
        }
    }
    auto iter = tablet_clients_.end();;
    if (cur_pid_ == INVALID_PID) {
        cur_pid_ = iter->first;
        iter = tablet_clients_.begin();
    } else {
        iter = tablet_clients_.find(cur_pid_);
    }
    do {
        if (iter == tablet_clients_.end()) {
            return false;
        }
        cur_pid_ = iter->first;
        uint32_t count = 0;
        if (kv_it_) {
            if (!kv_it_->IsFinish()) {
                kv_it_.reset(iter->second->Traverse(tid_, cur_pid_, "", last_pk_, key_,
                            FLAGS_traverse_cnt_limit, count));
                DLOG(INFO) << "last pk " << last_pk_ << " key " << key_ << " count " << count;
            } else {
                iter++;
                kv_it_.reset();
                continue;
            }
        } else {
            kv_it_.reset(iter->second->Traverse(tid_, cur_pid_, "", "", 0, FLAGS_traverse_cnt_limit, count));
            DLOG(INFO) << "count " << count;
        }
        if (kv_it_ && kv_it_->Valid()) {
            last_pk_ = kv_it_->GetPK();
            key_ = kv_it_->GetKey();
            break;
        }
        iter++;
        kv_it_.reset();
    } while (true);
    return true;
}

const ::hybridse::codec::Row& FullTableIterator::GetValue() {
    if (it_) {
        value_ = ::hybridse::codec::Row(
            ::hybridse::base::RefCountedSlice::Create(it_->GetValue().data(), it_->GetValue().size()));
        return value_;
    } else {
        value_ = ::hybridse::codec::Row(
            ::hybridse::base::RefCountedSlice::Create(kv_it_->GetValue().data(), kv_it_->GetValue().size()));
        return value_;
    }
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
