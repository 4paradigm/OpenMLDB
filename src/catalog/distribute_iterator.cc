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
    it_(), kv_it_(), key_(0), last_ts_(0), last_pk_(), value_() {
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
            return true;
        }
    }
    auto iter = tablet_clients_.begin();
    if (cur_pid_ == INVALID_PID) {
        cur_pid_ = iter->first;
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
                DLOG(INFO) << "pid " << cur_pid_ << " last pk " << last_pk_ <<
                    " key " << last_ts_ << " count " << count;
                kv_it_ = iter->second->Traverse(tid_, cur_pid_, "", last_pk_, last_ts_,
                            FLAGS_traverse_cnt_limit, count);
            } else {
                iter++;
                kv_it_.reset();
                continue;
            }
        } else {
            kv_it_ = iter->second->Traverse(tid_, cur_pid_, "", "", 0, FLAGS_traverse_cnt_limit, count);
            DLOG(INFO) << "count " << count;
        }
        if (kv_it_ && kv_it_->Valid()) {
            last_pk_ = kv_it_->GetLastPK();
            last_ts_ = kv_it_->GetLastTS();
            response_vec_.emplace_back(kv_it_->GetResponse());
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

DistributeWindowIterator::DistributeWindowIterator(uint32_t tid, uint32_t pid_num, std::shared_ptr<Tables> tables,
        uint32_t index, const std::string& index_name,
        const std::map<uint32_t, std::shared_ptr<::openmldb::client::TabletClient>>& tablet_clients)
    : tid_(tid), pid_num_(pid_num), tables_(tables), tablet_clients_(tablet_clients),
    index_(index), index_name_(index_name),
    cur_pid_(0), it_(), kv_it_() {}

void DistributeWindowIterator::Reset() {
    it_.reset();
    kv_it_.reset();
    cur_pid_ = INVALID_PID;
}

void DistributeWindowIterator::Seek(const std::string& key) {
    DLOG(INFO) << "seek to key " << key;
    Reset();
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
    DLOG(INFO) << "seek to key " << key << " from remote. " << " cur_pid " << cur_pid_;
    auto client_iter = tablet_clients_.find(cur_pid_);
    if (client_iter != tablet_clients_.end()) {
        uint32_t count = 0;
        kv_it_ = client_iter->second->Traverse(tid_, cur_pid_, index_name_, key, 0,
                    FLAGS_traverse_cnt_limit, count);
        if (kv_it_ && kv_it_->Valid()) {
            response_vec_.emplace_back(kv_it_->GetResponse());
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
    Reset();
    if (!tables_) {
        return;
    }
    for (const auto& kv : *tables_) {
        it_.reset(kv.second->NewWindowIterator(index_));
        it_->SeekToFirst();
        if (it_->Valid()) {
            cur_pid_ = kv.first;
            return;
        }
    }
    for (const auto& kv : tablet_clients_) {
        uint32_t count = 0;
        cur_pid_ = kv.first;
        kv_it_ = kv.second->Traverse(tid_, cur_pid_, index_name_, "", 0, FLAGS_traverse_cnt_limit, count);
        if (kv_it_ && kv_it_->Valid()) {
            response_vec_.emplace_back(kv_it_->GetResponse());
            return;
        }
    }
}

void DistributeWindowIterator::Next() {
    // TODO(dl239) : next from remote
    if (it_) {
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
}

bool DistributeWindowIterator::Valid() {
    return (it_ && it_->Valid()) || (kv_it_ && kv_it_->Valid());
}

std::unique_ptr<::hybridse::codec::RowIterator> DistributeWindowIterator::GetValue() {
    return std::unique_ptr<::hybridse::codec::RowIterator>(GetRawValue());
}

::hybridse::codec::RowIterator* DistributeWindowIterator::GetRawValue() {
    if (it_) {
        return it_->GetRawValue();
    }
    return new RemoteWindowIterator(tid_, cur_pid_, index_name_, kv_it_,
            response_vec_.back(), tablet_clients_[cur_pid_]);
}

const ::hybridse::codec::Row DistributeWindowIterator::GetKey() {
    if (it_) {
        return it_->GetKey();
    }
    return hybridse::codec::Row(hybridse::base::RefCountedSlice::Create(kv_it_->GetPK().data(),
                kv_it_->GetPK().size()));
}

void RemoteWindowIterator::Next() {
    kv_it_->Next();
    /*if (!kv_it_->Valid() && !kv_it_->IsFinish()) {
        uint32_t count = 0;
        kv_it_.reset(tablet_client_->Traverse(tid_, pid_, index_name_, pk_, ts_,
                    FLAGS_traverse_cnt_limit, false, count));
    }*/
}

}  // namespace catalog
}  // namespace openmldb
