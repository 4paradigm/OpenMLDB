/*
 * table_iterator.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#include "storage/table_iterator.h"

#include <memory>
#include <string>
#include <utility>
#include "base/hash.h"
#include "storage/codec.h"

namespace fesql {
namespace storage {

static constexpr uint32_t SEED = 0xe17a1465;

WindowInternalIterator::WindowInternalIterator(
    std::unique_ptr<base::Iterator<uint64_t, DataBlock*>> ts_it)
    : ts_it_(std::move(ts_it)) {}
WindowInternalIterator::~WindowInternalIterator() {}

void WindowInternalIterator::Seek(uint64_t ts) { ts_it_->Seek(ts); }

void WindowInternalIterator::SeekToFirst() { ts_it_->SeekToFirst(); }

bool WindowInternalIterator::Valid() { return ts_it_->Valid(); }

void WindowInternalIterator::Next() { ts_it_->Next(); }

const base::Slice WindowInternalIterator::GetValue() {
    return base::Slice(
        ts_it_->GetValue()->data,
        RowView::GetSize(reinterpret_cast<int8_t*>(ts_it_->GetValue()->data)));
}

const uint64_t WindowInternalIterator::GetKey() { return ts_it_->GetKey(); }

WindowTableIterator::WindowTableIterator(Segment*** segments, uint32_t seg_cnt,
                                         uint32_t index,
                                         std::shared_ptr<Table> table)
    : segments_(segments),
      seg_cnt_(seg_cnt),
      index_(index),
      seg_idx_(0),
      pk_it_(),
      table_(table) {
    GoToStart();
}

WindowTableIterator::~WindowTableIterator() {}

void WindowTableIterator::Seek(const std::string& key) {
    uint32_t seg_idx =
        ::fesql::base::hash(key.c_str(), key.length(), SEED) % seg_cnt_;
    base::Slice pk(key);
    key_ = pk;
    Segment* segment = segments_[index_][seg_idx];
    if (segment->GetEntries() == NULL) {
        return;
    }
    pk_it_ = std::move(std::unique_ptr<base::Iterator<base::Slice, void*>>(
        segments_[index_][seg_idx]->GetEntries()->NewIterator()));
    pk_it_->Seek(pk);
}

void WindowTableIterator::SeekToFirst() {}

std::unique_ptr<vm::Iterator> WindowTableIterator::GetValue() {
    if (!pk_it_)
        return std::move(
            std::unique_ptr<EmptyWindowIterator>(new EmptyWindowIterator()));
    std::unique_ptr<base::Iterator<uint64_t, DataBlock*>> it(
        (reinterpret_cast<TimeEntry*>(pk_it_->GetValue()))->NewIterator());
    std::unique_ptr<WindowInternalIterator> wit(
        new WindowInternalIterator(std::move(it)));
    return std::move(wit);
}

void WindowTableIterator::GoToStart() {
    while (seg_idx_ < seg_cnt_) {
        if (!pk_it_) {
            pk_it_ =
                std::move(std::unique_ptr<base::Iterator<base::Slice, void*>>(
                    segments_[index_][seg_idx_]->GetEntries()->NewIterator()));
            pk_it_->SeekToFirst();
        }
        if (pk_it_->Valid()) {
            return;
        } else {
            seg_idx_++;
            pk_it_ = std::move(
                std::unique_ptr<base::Iterator<base::Slice, void*>>());
        }
    }
}

void WindowTableIterator::GoToNext() {
    if (pk_it_) {
        pk_it_->Next();
        if (pk_it_->Valid()) return;
    }
    seg_idx_++;
    while (seg_idx_ < seg_cnt_) {
        pk_it_ = std::move(std::unique_ptr<base::Iterator<base::Slice, void*>>(
            segments_[index_][seg_idx_]->GetEntries()->NewIterator()));
        pk_it_->SeekToFirst();
        if (pk_it_->Valid()) return;
        seg_idx_++;
    }
}

void WindowTableIterator::Next() { GoToNext(); }

const base::Slice WindowTableIterator::GetKey() {
    if (pk_it_) {
        return pk_it_->GetKey();
    }
    return base::Slice();
}

bool WindowTableIterator::Valid() {
    if (pk_it_ && pk_it_->Valid()) return true;
    return false;
}

FullTableIterator::FullTableIterator(Segment*** segments, uint32_t seg_cnt,
                                     std::shared_ptr<Table> table)
    : seg_cnt_(seg_cnt),
      seg_idx_(0),
      segments_(segments),
      ts_it_(),
      pk_it_(),
      table_(table) {
    GoToStart();
}

void FullTableIterator::GoToNext() {
    if (ts_it_) {
        ts_it_->Next();
        if (ts_it_->Valid()) return;
    }
    if (pk_it_) {
        pk_it_->Next();
        while (pk_it_->Valid()) {
            auto it = (reinterpret_cast<TimeEntry*>(pk_it_->GetValue()))
                          ->NewIterator();
            it->SeekToFirst();
            if (it->Valid()) {
                ts_it_ = std::move(
                    std::unique_ptr<base::Iterator<uint64_t, DataBlock*>>(it));
                return;
            } else {
                pk_it_->Next();
            }
        }
        // try to incr seg_idx
        seg_idx_++;
        while (seg_idx_ < seg_cnt_) {
            pk_it_ =
                std::move(std::unique_ptr<base::Iterator<base::Slice, void*>>(
                    segments_[0][seg_idx_]->GetEntries()->NewIterator()));
            pk_it_->SeekToFirst();
            while (pk_it_->Valid()) {
                auto it = (reinterpret_cast<TimeEntry*>(pk_it_->GetValue()))
                              ->NewIterator();
                it->SeekToFirst();
                if (it->Valid()) {
                    ts_it_ = std::move(
                        std::unique_ptr<base::Iterator<uint64_t, DataBlock*>>(
                            it));
                    return;
                } else {
                    pk_it_->Next();
                }
            }
            seg_idx_++;
        }
    }
}

void FullTableIterator::GoToStart() {
    while (seg_idx_ < seg_cnt_) {
        if (!pk_it_) {
            pk_it_ =
                std::move(std::unique_ptr<base::Iterator<base::Slice, void*>>(
                    segments_[0][seg_idx_]->GetEntries()->NewIterator()));
            pk_it_->SeekToFirst();
        }
        if (pk_it_->Valid()) {
            if (!ts_it_) {
                auto it = (reinterpret_cast<TimeEntry*>(pk_it_->GetValue()))
                              ->NewIterator();
                ts_it_ = std::move(
                    std::unique_ptr<base::Iterator<uint64_t, DataBlock*>>(it));
                ts_it_->SeekToFirst();
            }
            if (ts_it_->Valid()) {
                break;
            } else {
                seg_idx_++;
                pk_it_ = std::move(
                    std::unique_ptr<base::Iterator<base::Slice, void*>>());
                ts_it_ = std::move(
                    std::unique_ptr<base::Iterator<uint64_t, DataBlock*>>());
            }
        } else {
            seg_idx_++;
            pk_it_ = std::move(
                std::unique_ptr<base::Iterator<base::Slice, void*>>());
        }
    }
}

bool FullTableIterator::Valid() {
    if (ts_it_ && ts_it_->Valid()) return true;
    return false;
}

void FullTableIterator::Next() { GoToNext(); }

const base::Slice FullTableIterator::GetValue() {
    return base::Slice(
        ts_it_->GetValue()->data,
        RowView::GetSize(reinterpret_cast<int8_t*>(ts_it_->GetValue()->data)));
}

}  // namespace storage
}  // namespace fesql
