/*
 *  Copyright 2021 4Paradigm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef STREAM_SRC_STORAGE_TABLE_H_
#define STREAM_SRC_STORAGE_TABLE_H_

#include <atomic>
#include <limits>
#include <memory>
#include <string>
#include <vector>
#include "storage/segment.h"
#include "storage/iterator.h"
#include "base/slice.h"

namespace streaming {
namespace interval_join {

using Segment = openmldb::storage::Segment;
using Slice = openmldb::storage::Slice;
using DataBlock = openmldb::storage::DataBlock;
using KeyEntry = openmldb::storage::KeyEntry;
using MemTableIterator = openmldb::storage::MemTableIterator;
using TableIterator = openmldb::storage::TableIterator;
using Ticket = openmldb::storage::Ticket;

class CombinedIterator : public TableIterator {
 public:
    explicit CombinedIterator(const std::vector<MemTableIterator*>& iters) : iters_(iters) {}
    ~CombinedIterator() {}
    bool Valid() {
        for (auto& iter : iters_) {
            if (iter && iter->Valid()) {
                return true;
            }
        }

        return false;
    }

    bool Null() const {
        for (const auto& iter : iters_) {
            if (iter == nullptr) {
                return false;
            }
        }
        return true;
    }

    void Next() {
        if (idx_ == -1) {
            idx_ = FindMaxIdx();
        }
        if (idx_ == -1) {
            return;
        }
        iters_[idx_]->Next();
        idx_ = FindMaxIdx();
    }

    openmldb::base::Slice GetValue() const override {
        return iters_[idx_]->GetValue();
    }

    const void* GetRawValue() {
        return iters_[idx_]->GetValue().data();
    }

    std::string GetPK() const {
        return iters_[idx_]->GetPK();
    }

    uint64_t GetKey() const {
        return iters_[idx_]->GetKey();
    }

    void SeekToFirst() {
        for (auto& iter : iters_) {
            if (iter) {
                iter->SeekToFirst();
            }
        }
        idx_ = FindMaxIdx();
    }

    void SeekToLast() {}

    void Seek(uint64_t time) {
        for (auto& iter : iters_) {
            if (iter) {
                iter->Seek(time);
            }
        }
        idx_ = FindMaxIdx();
    }
    uint64_t GetCount() const { return 0; }

 private:
    std::vector<MemTableIterator*> iters_;
    int idx_ = -1;

    int FindMaxIdx() {
        uint64_t max = std::numeric_limits<uint64_t>::min();
        int idx = -1;
        for (int i = 0; i < iters_.size(); i++) {
            auto iter = iters_[i];
            if (iter->Valid()) {
                if (iter->GetKey() >= max) {
                    idx = i;
                    max = iter->GetKey();
                }
            }
        }
        return idx;
    }
};

class SkipListTable : public Segment {
 public:
    FRIEND_TEST(IntervalJoinTest, SnapshotTest);
    inline __attribute__((always_inline)) void Put(const Slice& key, uint64_t time, Element* ele,
                                                   bool gen_id = true) {
        DataBlock* block = new DataBlock(1, reinterpret_cast<const char*>(ele), ele->size());
        Segment::Put(key, time, block);
        if (gen_id) {
            ele->set_id(std::atomic_fetch_add_explicit(&SkipListTable::id_, 1L, std::memory_order_acq_rel));
        }
    }

    ::openmldb::base::Node<uint64_t, DataBlock*>* Expire(const Slice& key, uint64_t time) volatile {
        void* entry = NULL;
        if (entries_->Get(key, entry) < 0 || entry == NULL) {
            return nullptr;
        }
        return ((KeyEntry*)entry)->entries.Split(time);  // NOLINT
    }

    std::vector<::openmldb::base::Node<uint64_t, DataBlock*>*> Expire(uint64_t time) volatile {
        std::vector<::openmldb::base::Node<uint64_t, DataBlock*>*> to_del;
        to_del.reserve(entries_->GetSize());

        auto it = entries_->NewIterator();
        it->SeekToFirst();
        while (it->Valid()) {
            KeyEntry* entry = reinterpret_cast<KeyEntry*>(it->GetValue());
            to_del.emplace_back(entry->entries.Split(time));
            it->Next();
        }

        return to_del;
    }

 private:
    inline static std::atomic<int64_t> id_ = 0;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_STORAGE_TABLE_H_
