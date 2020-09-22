/*
 * distribute_iterator.cc
 * Copyright (C) 4paradigm.com 2020
 * Author denglong
 * Date 2020-09-21
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "catalog/distribute_iterator.h"

namespace rtidb {
namespace catalog {

DistributeWindowIterator::DistributeWindowIterator(std::shared_ptr<Tables> tables, uint32_t index)
    : tables_(tables), index_(index), it_() {}

void DistributeWindowIterator::Seek(const std::string& key) {
    uint32_t pid = 0;
    // assume all partitions in one tablet
    uint32_t pid_num = tables_->size();
    if (pid_num > 0) {
        pid = (uint32_t)(::rtidb::base::hash64(key) % pid_num);
    }
    auto iter = tables_->find(pid);
    if (iter != tables_->end()) {
        it_.reset(iter->second->NewWindowIterator(index_));
        it_->Seek(key);
    }
}

void DistributeWindowIterator::SeekToFirst() {
    for (const auto& kv : *tables_) {
        it_.reset(kv.second->NewWindowIterator(index_));
        it_->SeekToFirst();
        if (it_->Valid()) {
            break;
        }
        it_.reset();
    }
}

void DistributeWindowIterator::Next() {
    if (it_) {
        it_->Next();
    }
}

bool DistributeWindowIterator::Valid() { return it_ && it_->Valid(); }

std::unique_ptr<::fesql::codec::RowIterator> DistributeWindowIterator::GetValue() { return it_->GetValue(); }

::fesql::codec::RowIterator* DistributeWindowIterator::GetValue(int8_t* addr) { return it_->GetValue(addr); }

const ::fesql::codec::Row DistributeWindowIterator::GetKey() { return it_->GetKey(); }

DistributeRowIterator::DistributeRowIterator() {}

bool DistributeRowIterator::Valid() const { return true; }

void DistributeRowIterator::Next() {}

const uint64_t& DistributeRowIterator::GetKey() const { return cur_ts_; }

const ::fesql::codec::Row& DistributeRowIterator::GetValue() { return value_; }

void DistributeRowIterator::Seek(const uint64_t& k) {}

void DistributeRowIterator::SeekToFirst() {}

bool DistributeRowIterator::IsSeekable() const { return true; }

}  // namespace catalog
}  // namespace rtidb
