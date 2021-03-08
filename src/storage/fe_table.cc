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


#include "storage/fe_table.h"
#include <sys/time.h>
#include <algorithm>
#include <string>
#include "base/fe_hash.h"
#include "base/fe_slice.h"
#include "glog/logging.h"

namespace fesql {
namespace storage {

static constexpr uint32_t SEED = 0xe17a1465;
static constexpr uint32_t COMBINE_KEY_RESERVE_SIZE = 128;

Table::Table(uint32_t id, uint32_t pid, const TableDef& table_def)
    : id_(id),
      pid_(pid),
      table_def_(table_def),
      row_view_(table_def_.columns()) {}

Table::~Table() {
    if (segments_ != NULL) {
        for (uint32_t i = 0; i < index_map_.size(); i++) {
            for (uint32_t j = 0; j < seg_cnt_; j++) {
                delete segments_[i][j];
            }
            delete[] segments_[i];
        }
        delete[] segments_;
    }
}

bool Table::Init() {
    std::map<std::string, uint32_t> col_map;
    for (int idx = 0; idx < table_def_.columns_size(); idx++) {
        col_map.insert(std::make_pair(table_def_.columns(idx).name(), idx));
    }
    for (int idx = 0; idx < table_def_.indexes_size(); idx++) {
        if (index_map_.find(table_def_.indexes(idx).name()) !=
            index_map_.end()) {
            return false;
        }
        IndexSt st;
        st.name = table_def_.indexes(idx).name();
        st.ts_pos = fesql::vm::INVALID_POS;
        if (!table_def_.indexes(idx).second_key().empty()) {
            if (col_map.find(table_def_.indexes(idx).second_key()) ==
                col_map.end()) {
                return false;
            }
            st.ts_pos = col_map[table_def_.indexes(idx).second_key()];
            switch (table_def_.columns(st.ts_pos).type()) {
                case type::kInt64:
                case type::kTimestamp: {
                    break;
                }
                default: {
                    LOG(WARNING) << "Invalid index ts type: "
                                 << fesql::type::Type_Name(
                                        table_def_.columns(st.ts_pos).type());
                    return false;
                }
            }
        }

        st.index = idx;
        std::vector<std::pair<fesql::type::Type, size_t>> col_vec;
        for (int i = 0; i < table_def_.indexes(idx).first_keys_size(); i++) {
            std::string name = table_def_.indexes(idx).first_keys(i);
            auto iter = col_map.find(name);
            if (iter == col_map.end()) return false;
            if (st.ts_pos == iter->second) {
                LOG(WARNING)
                    << "Invalid index: ts column can't be used as key column";
                return false;
            }
            switch (table_def_.columns(iter->second).type()) {
                case type::kBool:
                case type::kInt16:
                case type::kInt32:
                case type::kInt64:
                case type::kDate:
                case type::kTimestamp:
                case type::kVarchar: {
                    break;
                }
                default: {
                    LOG(WARNING)
                        << "Invalid index key type: "
                        << fesql::type::Type_Name(
                               table_def_.columns(iter->second).type());
                    return false;
                }
            }

            col_vec.push_back(std::make_pair(
                table_def_.columns(iter->second).type(), iter->second));
        }
        if (col_vec.empty()) return false;
        st.keys = col_vec;
        index_map_.insert(
            std::make_pair(table_def_.indexes(idx).name(), std::move(st)));
    }
    if (index_map_.empty()) {
        LOG(WARNING) << "no index in table" << table_def_.name();
        return false;
    }
    segments_ = new Segment**[index_map_.size()];
    for (uint32_t i = 0; i < index_map_.size(); i++) {
        segments_[i] = new Segment*[seg_cnt_];
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            segments_[i][j] = new Segment();
        }
    }
    DLOG(INFO) << "table " << table_def_.name() << " init ok";
    return true;
}  // namespace storage
bool Table::DecodeKeysAndTs(const IndexSt& index, const char* row,
                            uint32_t size, std::string& key,
                            int64_t* time_ptr) {
    if (index.keys.size() > 1) {
        key.reserve(COMBINE_KEY_RESERVE_SIZE);
        for (const auto& col : index.keys) {
            if (!key.empty()) {
                key.append("|");
            }
            if (row_view_.IsNULL(reinterpret_cast<const int8_t*>(row),
                                 col.second)) {
                key.append(codec::NONETOKEN);
            } else if (col.first == ::fesql::type::kVarchar) {
                const char* val = NULL;
                uint32_t length = 0;
                row_view_.GetValue(reinterpret_cast<const int8_t*>(row),
                                   col.second, &val, &length);
                if (length != 0) {
                    key.append(val, length);
                } else {
                    key.append(codec::EMPTY_STRING);
                }
            } else {
                int64_t value = 0;
                row_view_.GetInteger(reinterpret_cast<const int8_t*>(row),
                                     col.second, col.first, &value);
                key.append(std::to_string(value));
            }
        }
    } else {
        if (row_view_.IsNULL(reinterpret_cast<const int8_t*>(row),
                             index.keys[0].second)) {
            key = codec::NONETOKEN;
        } else if (index.keys[0].first == ::fesql::type::kVarchar) {
            const char* buf = nullptr;
            uint32_t size = 0;
            key = row_view_.GetValue(reinterpret_cast<const int8_t*>(row),
                                     index.keys[0].second, &buf, &size);
            key = std::string(buf, size);
            if (key == "") {
                key = codec::EMPTY_STRING;
            }
        } else {
            int64_t value = 0;
            row_view_.GetInteger(reinterpret_cast<const int8_t*>(row),
                                 index.keys[0].second, index.keys[0].first,
                                 &value);
            key = std::to_string(value);
        }
    }
    if (fesql::vm::INVALID_POS == index.ts_pos ||
        row_view_.IsNULL(reinterpret_cast<const int8_t*>(row), index.ts_pos)) {
        struct timeval cur_time;
        gettimeofday(&cur_time, NULL);
        *time_ptr = cur_time.tv_sec * 1000 + cur_time.tv_usec / 1000;
        return true;
    }
    row_view_.GetInteger(reinterpret_cast<const int8_t*>(row), index.ts_pos,
                         table_def_.columns(index.ts_pos).type(), time_ptr);
    return true;
}
bool Table::Put(const char* row, uint32_t size) {
    if (row_view_.GetSize(reinterpret_cast<const int8_t*>(row)) != size) {
        return false;
    }
    DataBlock* block =
        reinterpret_cast<DataBlock*>(malloc(sizeof(DataBlock) + size));
    block->ref_cnt = table_def_.indexes_size();
    memcpy(block->data, row, size);
    for (const auto& kv : index_map_) {
        std::string key;
        uint32_t seg_index = 0;
        int64_t time = 1;
        if (!DecodeKeysAndTs(kv.second, row, size, key, &time)) {
            return false;
        }
        if (seg_cnt_ > 1) {
            seg_index =
                ::fesql::base::hash(key.c_str(), key.length(), SEED) % seg_cnt_;
        }
        Segment* segment = segments_[kv.second.index][seg_index];
        Slice spk(key);
        segment->Put(spk, (uint64_t)time, block);
    }
    return true;
}

std::unique_ptr<TableIterator> Table::NewIndexIterator(const std::string& pk,
                                                       const uint32_t index) {
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::fesql::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    base::Slice spk(pk);
    Segment* segment = segments_[index][seg_idx];
    if (segment->GetEntries() == NULL) {
        return std::unique_ptr<TableIterator>(new TableIterator());
    }
    void* entry = NULL;
    if (segment->GetEntries()->Get(spk, entry) < 0 || entry == NULL) {
        return std::unique_ptr<TableIterator>(new TableIterator());
    }
    return std::unique_ptr<TableIterator>(new TableIterator(
        (reinterpret_cast<TimeEntry*>(entry))->NewIterator()));
}

std::unique_ptr<TableIterator> Table::NewIterator(
    const std::string& pk, const std::string& index_name) {
    auto iter = index_map_.find(index_name);
    if (iter == index_map_.end()) {
        LOG(WARNING) << "index name \"" << index_name << "\" not exist";
        return nullptr;
    }
    return NewIndexIterator(pk, iter->second.index);
}

std::unique_ptr<TableIterator> Table::NewIterator(const std::string& pk,
                                                  const uint64_t ts) {
    auto iter = NewIndexIterator(pk, 0);
    iter->Seek(ts);
    base::Slice spk(pk);
    return iter;
}

std::unique_ptr<TableIterator> Table::NewIterator(const std::string& pk) {
    return NewIndexIterator(pk, 0);
}

std::unique_ptr<TableIterator> Table::NewTraverseIterator(
    const std::string& index_name) {
    auto iter = index_map_.find(index_name);
    if (iter == index_map_.end()) {
        LOG(WARNING) << "index name \"" << index_name << "\" not exist";
        return nullptr;
    }
    return std::unique_ptr<TableIterator>(
        new TableIterator(segments_[iter->second.index], seg_cnt_));
}

std::unique_ptr<TableIterator> Table::NewTraverseIterator() {
    return std::unique_ptr<TableIterator>(
        new TableIterator(segments_[0], seg_cnt_));
}

// Iterator

TableIterator::TableIterator(base::Iterator<uint64_t, DataBlock*>* ts_it)
    : ts_it_(ts_it) {}

TableIterator::TableIterator(Segment** segments, uint32_t seg_cnt)
    : segments_(segments), seg_cnt_(seg_cnt) {}

TableIterator::~TableIterator() {
    delete ts_it_;
    delete pk_it_;
}

void TableIterator::Seek(const uint64_t& time) {
    if (ts_it_ == NULL) {
        return;
    }
    ts_it_->Seek(time);
}

void TableIterator::Seek(const std::string& key, uint64_t ts) {
    if (pk_it_ == NULL && seg_cnt_ == 0) {
        return;
    }
    if (seg_cnt_ > 1) {
        seg_idx_ =
            ::fesql::base::hash(key.c_str(), key.length(), SEED) % seg_cnt_;
        delete pk_it_;
        pk_it_ = segments_[seg_idx_]->GetEntries()->NewIterator();
    }
    base::Slice spk(key);
    pk_it_->Seek(spk);

    if (pk_it_->Valid()) {
        delete ts_it_;
        ts_it_ = NULL;
        while (pk_it_->Valid()) {
            ts_it_ = (reinterpret_cast<TimeEntry*>(pk_it_->GetValue()))
                         ->NewIterator();
            ts_it_->SeekToFirst();
            if (ts_it_->Valid()) break;
            delete ts_it_;
            ts_it_ = NULL;
            pk_it_->Next();
        }
    }
}

bool TableIterator::CurrentTsValid() {
    if (ts_it_ == NULL) {
        return false;
    }
    return ts_it_->Valid();
}

bool TableIterator::Valid() const {
    if (ts_it_ == NULL) {
        return false;
    }
    if (pk_it_ == NULL) {
        return ts_it_->Valid();
    }
    return pk_it_->Valid() && ts_it_->Valid();
}

bool TableIterator::SeekToNextTsInPks() {
    while (pk_it_->Valid()) {
        ts_it_ =
            (reinterpret_cast<TimeEntry*>(pk_it_->GetValue()))->NewIterator();
        ts_it_->SeekToFirst();
        if (ts_it_->Valid()) return true;
        delete ts_it_;
        ts_it_ = NULL;
        pk_it_->Next();
    }
    return false;
}
void TableIterator::NextTs() {
    if (ts_it_ == NULL) {
        return;
    }
    ts_it_->Next();
}

void TableIterator::NextTsInPks() {
    if (!ts_it_->Valid() && pk_it_ != NULL) {
        pk_it_->Next();
        if (SeekToNextTsInPks()) return;
    }
    if (pk_it_ != NULL && !pk_it_->Valid()) {
        while (seg_idx_ + 1 < seg_cnt_) {
            delete pk_it_;
            pk_it_ = segments_[++seg_idx_]->GetEntries()->NewIterator();
            pk_it_->SeekToFirst();
            if (SeekToNextTsInPks()) return;
        }
    }
    return;
}

void TableIterator::Next() {
    if (ts_it_ == NULL) {
        return;
    }
    ts_it_->Next();
    if (!ts_it_->Valid() && pk_it_ != NULL) {
        pk_it_->Next();
        if (SeekToNextTsInPks()) return;
    }
    if (pk_it_ != NULL && !pk_it_->Valid()) {
        while (seg_idx_ + 1 < seg_cnt_) {
            delete pk_it_;
            pk_it_ = segments_[++seg_idx_]->GetEntries()->NewIterator();
            pk_it_->SeekToFirst();
            if (SeekToNextTsInPks()) return;
        }
    }
}

const base::Slice& TableIterator::GetValue() {
    value_ = base::Slice(ts_it_->GetValue()->data,
                         codec::RowView::GetSize(reinterpret_cast<int8_t*>(
                             ts_it_->GetValue()->data)));
    return value_;
}

const uint64_t& TableIterator::GetKey() const { return ts_it_->GetKey(); }

const base::Slice TableIterator::GetPK() {
    if (pk_it_ == NULL) {
        return base::Slice();
    }
    return pk_it_->GetKey();
}

void TableIterator::SeekToFirst() {
    if (seg_cnt_ > 0) {
        seg_idx_ = 0;
        delete ts_it_;
        ts_it_ = NULL;
        while (seg_idx_ < seg_cnt_) {
            delete pk_it_;
            pk_it_ = segments_[seg_idx_]->GetEntries()->NewIterator();
            pk_it_->SeekToFirst();
            if (SeekToNextTsInPks()) return;
            ++seg_idx_;
        }
    }
    if (pk_it_ != NULL) {
        delete ts_it_;
        ts_it_ = NULL;
        pk_it_->SeekToFirst();
        if (SeekToNextTsInPks()) {
            return;
        } else {
            ts_it_ = NULL;
        }
    }
    if (ts_it_ == NULL) {
        return;
    }
    ts_it_->SeekToFirst();
}
bool TableIterator::IsSeekable() const { return true; }

}  // namespace storage
}  // namespace fesql
