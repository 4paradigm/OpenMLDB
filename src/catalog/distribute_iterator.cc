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
DECLARE_uint32(max_traverse_cnt);
DECLARE_uint32(max_traverse_pk_cnt);

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
    return (cnt_ <= FLAGS_max_traverse_cnt) && ((it_ && it_->Valid()) || (kv_it_ && kv_it_->Valid()));
}

void FullTableIterator::Next() {
    // reset the buffered value
    ResetValue();
    cnt_++;
    if (cnt_ > FLAGS_max_traverse_cnt) {
        PDLOG(WARNING, "FullTableIterator exceed the max_traverse_cnt, tid %u, cnt %lld, max_traverse_cnt %u", tid_,
              cnt_, FLAGS_max_traverse_cnt);
        return;
    }

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
    ResetValue();
    cnt_ = 0;
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
                kv_it_ = iter->second->Traverse(tid_, cur_pid_, "", last_pk_, last_ts_,
                            FLAGS_traverse_cnt_limit, false, kv_it_->GetTSPos(), count);
                DLOG(INFO) << "pid " << cur_pid_ << " last pk " << last_pk_ <<
                    " key " << last_ts_ << " ts_pos " << kv_it_->GetTSPos() << " count " << count;
            } else {
                iter++;
                kv_it_.reset();
                continue;
            }
        } else {
            kv_it_ = iter->second->Traverse(tid_, cur_pid_, "", "", 0, FLAGS_traverse_cnt_limit, false, 0, count);
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
    if (ValidValue()) {
        return value_;
    }

    valid_value_ = true;
    if (it_ && it_->Valid()) {
        value_ = ::hybridse::codec::Row(
            ::hybridse::base::RefCountedSlice::Create(it_->GetValue().data(), it_->GetValue().size()));
        return value_;
    } else {
        auto slice_row = kv_it_->GetValue();
        size_t sz = slice_row.size();
        int8_t* copyed_row_data = new int8_t[sz];
        memcpy(copyed_row_data, slice_row.data(), sz);
        auto shared_slice = ::hybridse::base::RefCountedSlice::CreateManaged(copyed_row_data, sz);
        value_.Reset(shared_slice);
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
    pk_cnt_ = 0;
}

// seek to the pos where key = `key` on success
// if the key does not exist, iterator will be invalid
void DistributeWindowIterator::Seek(const std::string& key) {
    Reset();
    DLOG(INFO) << "seek to key " << key;
    const auto& stat = SeekByKey(key);
    if (stat.pid == INVALID_PID) {
        DLOG(INFO) << "no pos found for key " << key;
        return;
    }
    if (stat.it != nullptr) {
        it_.reset(stat.it);
    } else if (stat.kv_it != nullptr) {
        response_vec_.push_back(stat.kv_it->GetResponse());
        kv_it_ = stat.kv_it;
    } else {
        DLOG(INFO) << "no pos found for key " << key;
        return;
    }
    cur_pid_ = stat.pid;
}

DistributeWindowIterator::ItStat DistributeWindowIterator::SeekToFirstRemote() const {
    for (const auto& kv : tablet_clients_) {
        uint32_t count = 0;
        auto it = kv.second->Traverse(tid_, kv.first, index_name_, "", 0, FLAGS_traverse_cnt_limit, false, 0, count);
        if (it && it->Valid()) {
            DLOG(INFO) << "first pos in remote: pid=" << kv.first;
            return {kv.first, nullptr, it};
        }
    }
    return {INVALID_PID, nullptr, {}};
}

void DistributeWindowIterator::SeekToFirst() {
    DLOG(INFO) << "seek to first";
    Reset();
    if (!tables_) {
        return;
    }
    for (const auto& kv : *tables_) {
        auto it = kv.second->NewWindowIterator(index_);
        if (it != nullptr) {
            it->SeekToFirst();
            if (it->Valid()) {
                DLOG(INFO) << "first pos in local: pid=" << kv.first;
                it_.reset(it);
                cur_pid_ = kv.first;
                return;
            }
            delete it;
        }
    }
    const auto& stat = SeekToFirstRemote();
    if (stat.kv_it) {
        response_vec_.push_back(stat.kv_it->GetResponse());
        kv_it_ = stat.kv_it;
        cur_pid_ = stat.pid;
        return;
    }
    DLOG(INFO) << "empty window iterator";
}

DistributeWindowIterator::ItStat DistributeWindowIterator::SeekByKey(const std::string& key) const {
    if (!tables_ || pid_num_ <= 0) {
        return {INVALID_PID, nullptr, {}};
    }

    uint32_t pid = static_cast<uint32_t>(::openmldb::base::hash64(key) % pid_num_);

    DLOG(INFO) << "seeking to key " << key << ". cur_pid " << pid;
    auto iter = tables_->find(pid);
    if (iter != tables_->end()) {
        auto it = iter->second->NewWindowIterator(index_);
        if (it != nullptr) {
            it->Seek(key);
            if (it->Valid()) {
                return {pid, it, {}};
            }
            delete it;
        }
    }
    DLOG(INFO) << "seeking to key " << key << " from remote. cur_pid " << pid;
    auto client_iter = tablet_clients_.find(pid);
    if (client_iter != tablet_clients_.end()) {
        uint32_t count = 0;
        auto it = client_iter->second->Traverse(tid_, pid, index_name_, key, UINT64_MAX,
                FLAGS_traverse_cnt_limit, false, 0, count);
        if (it != nullptr && it->Valid() && key == it->GetPK()) {
            return {pid, {}, it};
        }
    }

    return {INVALID_PID, nullptr, {}};
}

void DistributeWindowIterator::Next() {
    pk_cnt_++;
    if (pk_cnt_ >= FLAGS_max_traverse_pk_cnt) {
        PDLOG(WARNING,
              "DistributeWindowIterator exceeds the max_traverse_pk_cnt, tid %u, cnt %lld, max_traverse_pk_cnt %u",
              tid_, pk_cnt_, FLAGS_max_traverse_pk_cnt);
        return;
    }
    if (it_ && it_->Valid()) {
        it_->Next();
        if (it_->Valid()) {
            return;
        } else {
            auto iter = tables_->find(cur_pid_);
            if (iter == tables_->end()) {
                return;
            }
            for (iter++; iter != tables_->end(); iter++) {
                it_.reset(iter->second->NewWindowIterator(index_));
                it_->SeekToFirst();
                if (it_->Valid()) {
                    cur_pid_ = iter->first;
                    return;
                }
            }
        }
    }
    if (kv_it_ && kv_it_->Valid()) {
        std::string cur_pk = kv_it_->GetPK();
        auto traverse_it = std::dynamic_pointer_cast<openmldb::base::TraverseKvIterator>(kv_it_);
        uint64_t last_ts = 0;
        uint32_t ts_pos = 1;
        if (traverse_it) {
            traverse_it->NextPK();
            if (traverse_it->Valid()) {
                return;
            }
            last_ts = traverse_it->GetLastTS();
            ts_pos = traverse_it->GetTSPos();
        }
        auto iter = tablet_clients_.find(cur_pid_);
        if (iter == tablet_clients_.end()) {
            return;
        }
        uint32_t count = 0;
        kv_it_ = iter->second->Traverse(tid_, cur_pid_, index_name_, cur_pk, last_ts,
                FLAGS_traverse_cnt_limit, true, ts_pos, count);
        DLOG(INFO) << "pid " << cur_pid_ << " last pk " << cur_pk << " key " << last_ts << " count " << count;
        if (kv_it_ && kv_it_->Valid()) {
            response_vec_.emplace_back(kv_it_->GetResponse());
            return;
        }
        do {
            iter++;
            if (iter == tablet_clients_.end()) {
                return;
            }
            cur_pid_ = iter->first;
            uint32_t count = 0;
            kv_it_ =
                iter->second->Traverse(tid_, cur_pid_, index_name_, "", 0, FLAGS_traverse_cnt_limit, false, 0, count);
            DLOG(INFO) << "count " << count;
            if (kv_it_ && kv_it_->Valid()) {
                response_vec_.emplace_back(kv_it_->GetResponse());
                break;
            }
            kv_it_.reset();
        } while (true);
    } else {
        const auto& stat = SeekToFirstRemote();
        if (stat.kv_it) {
            response_vec_.push_back(stat.kv_it->GetResponse());
            kv_it_ = stat.kv_it;
            cur_pid_ = stat.pid;
            return;
        }
    }
}

bool DistributeWindowIterator::Valid() {
    return (pk_cnt_ < FLAGS_max_traverse_pk_cnt) && ((it_ && it_->Valid()) || (kv_it_ && kv_it_->Valid()));
}

std::unique_ptr<::hybridse::codec::RowIterator> DistributeWindowIterator::GetValue() {
    return std::unique_ptr<::hybridse::codec::RowIterator>(GetRawValue());
}

::hybridse::codec::RowIterator* DistributeWindowIterator::GetRawValue() {
    if (it_ && it_->Valid()) {
        return it_->GetRawValue();
    }
    auto traverse_it = std::dynamic_pointer_cast<openmldb::base::TraverseKvIterator>(kv_it_);
    if (traverse_it) {
        auto response = std::dynamic_pointer_cast<::openmldb::api::TraverseResponse>(traverse_it->GetResponse());
        auto new_traverse_it = std::make_shared<openmldb::base::TraverseKvIterator>(response);
        new_traverse_it->Seek(traverse_it->GetPK());
        return new RemoteWindowIterator(tid_, cur_pid_, index_name_, new_traverse_it, tablet_clients_[cur_pid_]);
    } else {
        auto response = std::dynamic_pointer_cast<::openmldb::api::ScanResponse>(kv_it_->GetResponse());
        auto scan_it = std::make_shared<openmldb::base::ScanKvIterator>(kv_it_->GetPK(), response);
        return new RemoteWindowIterator(tid_, cur_pid_, index_name_, scan_it, tablet_clients_[cur_pid_]);
    }
}

const ::hybridse::codec::Row DistributeWindowIterator::GetKey() {
    if (it_ && it_->Valid()) {
        return it_->GetKey();
    }
    return hybridse::codec::Row(hybridse::base::RefCountedSlice::Create(kv_it_->GetPK().data(),
                kv_it_->GetPK().size()));
}

const ::hybridse::codec::Row& RemoteWindowIterator::GetValue() {
    if (ValidValue()) {
        return row_;
    }

    auto slice_row = kv_it_->GetValue();
    size_t sz = slice_row.size();
    // for distributed environment, slice_row's data probably become invalid when the DistributeWindowIterator
    // iterator goes out of scope. so copy action occurred here
    int8_t* copyed_row_data = new int8_t[sz];
    memcpy(copyed_row_data, slice_row.data(), sz);
    auto shared_slice = ::hybridse::base::RefCountedSlice::CreateManaged(copyed_row_data, sz);
    row_.Reset(shared_slice);
    DLOG(INFO) << "get value  pk " << pk_ << " ts_key " << kv_it_->GetKey() << " ts " << ts_;
    valid_value_ = true;
    return row_;
}

RemoteWindowIterator::RemoteWindowIterator(uint32_t tid, uint32_t pid, const std::string& index_name,
        const std::shared_ptr<::openmldb::base::KvIterator>& kv_it,
        const std::shared_ptr<openmldb::client::TabletClient>& client)
    : tid_(tid), pid_(pid), index_name_(index_name), kv_it_(kv_it), tablet_client_(client),
        is_traverse_data_(false), ts_(0) {
    if (kv_it_ && kv_it_->Valid()) {
        pk_ = kv_it_->GetPK();
        ts_ = kv_it_->GetKey();
        response_vec_.emplace_back(kv_it_->GetResponse());
        auto traverse_it = std::dynamic_pointer_cast<openmldb::base::TraverseKvIterator>(kv_it);
        if (traverse_it) {
            is_traverse_data_ = true;
        }
    }
}

bool RemoteWindowIterator::Valid() const {
    if (!kv_it_ || !kv_it_->Valid()) {
        return false;
    }
    if (is_traverse_data_ && kv_it_->GetPK() != pk_) {
        return false;
    }
    DLOG(INFO) << "RemoteWindowIterator Valid pk " << pk_ << " ts " << kv_it_->GetKey();
    return true;
}

void RemoteWindowIterator::ScanRemote(uint64_t key, uint32_t ts_pos) {
    uint32_t count = 0;
    kv_it_ = tablet_client_->Traverse(tid_, pid_, index_name_, pk_, key,
                FLAGS_traverse_cnt_limit, false, ts_pos, count);
    DLOG(INFO) << "traverse key " << pk_ << " ts " << key << " from remote. tid "
        << tid_ << " pid " << pid_ << " ts_pos " << ts_pos;
    if (kv_it_ && kv_it_->Valid()) {
        ts_ = kv_it_->GetKey();
        response_vec_.emplace_back(kv_it_->GetResponse());
    }
}

void RemoteWindowIterator::Seek(const uint64_t& key) {
    ResetValue();

    DLOG(INFO) << "RemoteWindowIterator seek " << key;
    if (!kv_it_) {
        return;
    }
    while (kv_it_->Valid() && kv_it_->GetKey() > key) {
        if (is_traverse_data_ && kv_it_->GetPK() != pk_) {
            break;
        }
        kv_it_->Next();
    }
    if (kv_it_->Valid()) {
        ts_ = kv_it_->GetKey();
    } else {
        ScanRemote(key, 0);
    }
}

void RemoteWindowIterator::Next() {
    ResetValue();

    kv_it_->Next();
    if (kv_it_->Valid()) {
        if (is_traverse_data_ && kv_it_->GetPK() != pk_) {
            kv_it_.reset();
            return;
        }
        ts_ = kv_it_->GetKey();
    } else {
        auto traverse_it = std::dynamic_pointer_cast<openmldb::base::TraverseKvIterator>(kv_it_);
        ScanRemote(traverse_it->GetLastTS(), traverse_it->GetTSPos());
    }
}

}  // namespace catalog
}  // namespace openmldb
