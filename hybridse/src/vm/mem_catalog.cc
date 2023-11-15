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

#include "vm/mem_catalog.h"

#include <algorithm>

namespace hybridse {
namespace vm {
MemTimeTableIterator::MemTimeTableIterator(const MemTimeTable* table,
                                           const vm::Schema* schema)
    : table_(table),
      schema_(schema),
      start_iter_(table->cbegin()),
      end_iter_(table->cend()),
      iter_(table->cbegin()) {}
MemTimeTableIterator::MemTimeTableIterator(const MemTimeTable* table,
                                           const vm::Schema* schema,
                                           int32_t start, int32_t end)
    : table_(table),
      schema_(schema),
      start_iter_(table_->begin() + start),
      end_iter_(table_->begin() + end),
      iter_(start_iter_) {}
MemTimeTableIterator::~MemTimeTableIterator() {}

// TODO(chenjing): speed up seek for memory iterator
void MemTimeTableIterator::Seek(const uint64_t& ts) {
    iter_ = start_iter_;
    while (iter_ != end_iter_) {
        if (iter_->first <= ts) {
            return;
        }
        iter_++;
    }
}
void MemTimeTableIterator::SeekToFirst() { iter_ = start_iter_; }
const uint64_t& MemTimeTableIterator::GetKey() const { return iter_->first; }
const Row& hybridse::vm::MemTimeTableIterator::GetValue() {
    return iter_->second;
}
void MemTimeTableIterator::Next() { iter_++; }
bool MemTimeTableIterator::Valid() const { return end_iter_ > iter_; }
bool MemTimeTableIterator::IsSeekable() const { return true; }
MemWindowIterator::MemWindowIterator(const MemSegmentMap* partitions,
                                     const Schema* schema)
    : WindowIterator(),
      partitions_(partitions),
      schema_(schema),
      iter_(partitions->cbegin()),
      start_iter_(partitions->cbegin()),
      end_iter_(partitions->cend()) {}

MemWindowIterator::~MemWindowIterator() {}

void MemWindowIterator::Seek(const std::string& key) {
    iter_ = partitions_->find(key);
}
void MemWindowIterator::SeekToFirst() { iter_ = start_iter_; }
void MemWindowIterator::Next() { iter_++; }
bool MemWindowIterator::Valid() { return end_iter_ != iter_; }

RowIterator* MemWindowIterator::GetRawValue() {
    return new MemTimeTableIterator(&(iter_->second), schema_);
}

const Row MemWindowIterator::GetKey() { return Row(iter_->first); }

MemTimeTableHandler::MemTimeTableHandler()
    : TableHandler(),
      table_name_(""),
      db_(""),
      schema_(nullptr),
      types_(),
      index_hint_(),
      table_(),
      order_type_(kNoneOrder) {}
MemTimeTableHandler::MemTimeTableHandler(const Schema* schema)
    : TableHandler(),
      table_name_(""),
      db_(""),
      schema_(schema),
      types_(),
      index_hint_(),
      table_(),
      order_type_(kNoneOrder) {}
MemTimeTableHandler::MemTimeTableHandler(const std::string& table_name,
                                         const std::string& db,
                                         const Schema* schema)
    : TableHandler(),
      table_name_(table_name),
      db_(db),
      schema_(schema),
      types_(),
      index_hint_(),
      table_(),
      order_type_(kNoneOrder) {}

MemTimeTableHandler::~MemTimeTableHandler() {}

RowIterator* MemTimeTableHandler::GetRawIterator() {
    return new MemTimeTableIterator(&table_, schema_);
}

void MemTimeTableHandler::AddRow(const uint64_t key, const Row& row) {
    table_.emplace_back(key, row);
}

void MemTimeTableHandler::AddFrontRow(const uint64_t key, const Row& row) {
    table_.emplace_front(key, row);
}
void MemTimeTableHandler::PopBackRow() { table_.pop_back(); }

void MemTimeTableHandler::PopFrontRow() { table_.pop_front(); }

const Types& MemTimeTableHandler::GetTypes() { return types_; }

void MemTimeTableHandler::Sort(const bool is_asc) {
    if (is_asc) {
        AscComparor comparor;
        std::sort(table_.begin(), table_.end(), comparor);
        order_type_ = kAscOrder;
    } else {
        DescComparor comparor;
        std::sort(table_.begin(), table_.end(), comparor);
        order_type_ = kDescOrder;
    }
}
void MemTimeTableHandler::Reverse() {
    std::reverse(table_.begin(), table_.end());
    order_type_ = kAscOrder == order_type_
                      ? kDescOrder
                      : kDescOrder == order_type_ ? kAscOrder : kNoneOrder;
}

MemPartitionHandler::MemPartitionHandler()
    : PartitionHandler(),
      table_name_(""),
      db_(""),
      schema_(nullptr),
      order_type_(kNoneOrder) {}

MemPartitionHandler::MemPartitionHandler(const Schema* schema)
    : PartitionHandler(),
      table_name_(""),
      db_(""),
      schema_(schema),
      order_type_(kNoneOrder) {}
MemPartitionHandler::MemPartitionHandler(const std::string& table_name,
                                         const std::string& db,
                                         const Schema* schema)
    : PartitionHandler(),
      table_name_(table_name),
      db_(db),
      schema_(schema),
      order_type_(kNoneOrder) {}
MemPartitionHandler::~MemPartitionHandler() {}
const Schema* MemPartitionHandler::GetSchema() { return schema_; }
const std::string& MemPartitionHandler::GetName() { return table_name_; }
const std::string& MemPartitionHandler::GetDatabase() { return db_; }
const Types& MemPartitionHandler::GetTypes() { return types_; }
const IndexHint& MemPartitionHandler::GetIndex() { return index_hint_; }
bool MemPartitionHandler::AddRow(const std::string& key, uint64_t ts,
                                 const Row& row) {
    auto iter = partitions_.find(key);
    if (iter == partitions_.cend()) {
        partitions_.insert(std::pair<std::string, MemTimeTable>(
            key, {std::make_pair(ts, row)}));
    } else {
        iter->second.push_back(std::make_pair(ts, row));
    }
    return true;
}
std::unique_ptr<WindowIterator> MemPartitionHandler::GetWindowIterator() {
    return std::unique_ptr<WindowIterator>(
        new MemWindowIterator(&partitions_, schema_));
}
void MemPartitionHandler::Sort(const bool is_asc) {
    if (is_asc) {
        AscComparor comparor;
        for (auto& segment : partitions_) {
            std::sort(segment.second.begin(), segment.second.end(), comparor);
        }
        order_type_ = kAscOrder;
    } else {
        DescComparor comparor;
        for (auto& segment : partitions_) {
            std::sort(segment.second.begin(), segment.second.end(), comparor);
        }
        order_type_ = kDescOrder;
    }
}
void MemPartitionHandler::Reverse() {
    for (auto& segment : partitions_) {
        std::reverse(segment.second.begin(), segment.second.end());
    }
    order_type_ = kAscOrder == order_type_
                      ? kDescOrder
                      : kDescOrder == order_type_ ? kAscOrder : kNoneOrder;
}
void MemPartitionHandler::Print() {
    for (auto iter = partitions_.cbegin(); iter != partitions_.cend(); iter++) {
        std::cout << iter->first << ":";
        for (auto segment_iter = iter->second.cbegin();
             segment_iter != iter->second.cend(); segment_iter++) {
            std::cout << segment_iter->first << ",";
        }
        std::cout << std::endl;
    }
}

RowIterator* MemTableHandler::GetRawIterator() {
    return new MemTableIterator(&table_, schema_);
}

MemTableHandler::MemTableHandler()
    : TableHandler(),
      table_name_(""),
      db_(""),
      schema_(nullptr),
      types_(),
      index_hint_(),
      table_(),
      order_type_(kNoneOrder) {}
MemTableHandler::MemTableHandler(const Schema* schema)
    : TableHandler(),
      table_name_(""),
      db_(""),
      schema_(schema),
      types_(),
      index_hint_(),
      table_(),
      order_type_(kNoneOrder) {}
MemTableHandler::MemTableHandler(const std::string& table_name,
                                 const std::string& db, const Schema* schema)
    : TableHandler(),
      table_name_(table_name),
      db_(db),
      schema_(schema),
      types_(),
      index_hint_(),
      table_(),
      order_type_(kNoneOrder) {}
void MemTableHandler::AddRow(const Row& row) { table_.push_back(row); }
void MemTableHandler::Resize(const size_t size) { table_.resize(size); }
bool MemTableHandler::SetRow(const size_t idx, const Row& row) {
    if (idx >= table_.size()) {
        return false;
    }
    table_[idx] = row;
    return true;
}
void MemTableHandler::Reverse() {
    std::reverse(table_.begin(), table_.end());
    order_type_ = kAscOrder == order_type_
                      ? kDescOrder
                      : kDescOrder == order_type_ ? kAscOrder : kNoneOrder;
}
MemTableHandler::~MemTableHandler() {}
MemTableIterator::MemTableIterator(const MemTable* table,
                                   const vm::Schema* schema)
    : table_(table),
      schema_(schema),
      start_iter_(table->cbegin()),
      end_iter_(table->cend()),
      iter_(table->cbegin()),
      key_(0) {}
MemTableIterator::MemTableIterator(const MemTable* table,
                                   const vm::Schema* schema, int32_t start,
                                   int32_t end)
    : table_(table),
      schema_(schema),
      start_iter_(table_->begin() + start),
      end_iter_(table_->begin() + end),
      iter_(start_iter_),
      key_(0) {}
MemTableIterator::~MemTableIterator() {}
void MemTableIterator::Seek(const uint64_t& ts) {
    iter_ = start_iter_ + ts;
    key_ = ts;
}
void MemTableIterator::SeekToFirst() {
    iter_ = start_iter_;
    key_ = 0;
}
const uint64_t& MemTableIterator::GetKey() const { return key_; }

bool MemTableIterator::Valid() const { return end_iter_ > iter_; }
void MemTableIterator::Next() {
    iter_++;
    key_++;
}
const Row& MemTableIterator::GetValue() { return *iter_; }
bool MemTableIterator::IsSeekable() const { return true; }

/**
 * Iterator implementation for request union table
 */
class RequestUnionIterator : public RowIterator {
 public:
    RequestUnionIterator(uint64_t request_ts, const Row* request_row,
                         RowIterator* window_iter)
        : request_ts_(request_ts),
          request_row_(request_row),
          window_iter_(window_iter) {}
    ~RequestUnionIterator() { delete window_iter_; }
    bool Valid() const override {
        return window_iter_start_ ? window_iter_->Valid() : true;
    }
    void Next() override {
        if (window_iter_start_) {
            window_iter_->Next();
        } else {
            window_iter_start_ = true;
        }
    }
    const uint64_t& GetKey() const override {
        return window_iter_start_ ? window_iter_->GetKey() : request_ts_;
    }
    const Row& GetValue() override {
        return window_iter_start_ ? window_iter_->GetValue() : *request_row_;
    }
    void Seek(const uint64_t& key) override {
        if (request_ts_ <= key) {
            SeekToFirst();
        } else {
            window_iter_start_ = true;
            window_iter_->Seek(key);
        }
    }
    void SeekToFirst() override {
        window_iter_->SeekToFirst();
        window_iter_start_ = false;
    }
    bool IsSeekable() const override { return window_iter_->IsSeekable(); }

 private:
    uint64_t request_ts_;
    const Row* request_row_;
    RowIterator* window_iter_;
    bool window_iter_start_ = false;
};

RowIterator* RequestUnionTableHandler::GetRawIterator() {
    auto window_iter = window_->GetRawIterator();
    if (window_iter == nullptr) {
        LOG(WARNING) << "Illegal window iterator";
        return nullptr;
    }
    return new RequestUnionIterator(request_ts_, &request_row_, window_iter);
}

// row iter interfaces for llvm
void GetRowIter(int8_t* input, int8_t* iter_addr) {
    auto list_ref = reinterpret_cast<codec::ListRef<Row>*>(input);
    auto handler = reinterpret_cast<codec::ListV<Row>*>(list_ref->list);
    auto local_iter =
        new (iter_addr) std::unique_ptr<RowIterator>(handler->GetIterator());
    (*local_iter)->SeekToFirst();
}
bool RowIterHasNext(int8_t* iter_ptr) {
    auto& local_iter =
        *reinterpret_cast<std::unique_ptr<RowIterator>*>(iter_ptr);
    return local_iter->Valid();
}
void RowIterNext(int8_t* iter_ptr) {
    auto& local_iter =
        *reinterpret_cast<std::unique_ptr<RowIterator>*>(iter_ptr);
    local_iter->Next();
}

// FIXME(ace): `GetValue` and `row.buf` both returns a reference
//  which make this function dangerous. When calls theis function
//  multiple times or together with `RowIterGetCurSlice`, returned references
//  got invalided except the last one.
//
//  It is better deprecated `RowIterGetCurSlice`, `RowIterGetCurSliceSize`, and
//  use `RowGetSlice`, `RowIterGetCurSlice` instead.
int8_t* RowIterGetCurSlice(int8_t* iter_ptr, size_t idx) {
    auto& local_iter =
        *reinterpret_cast<std::unique_ptr<RowIterator>*>(iter_ptr);
    const Row& row = local_iter->GetValue();
    return row.buf(idx);
}
size_t RowIterGetCurSliceSize(int8_t* iter_ptr, size_t idx) {
    auto& local_iter =
        *reinterpret_cast<std::unique_ptr<RowIterator>*>(iter_ptr);
    const Row& row = local_iter->GetValue();
    return row.size(idx);
}
void RowIterDelete(int8_t* iter_ptr) {
    auto& local_iter =
        *reinterpret_cast<std::unique_ptr<RowIterator>*>(iter_ptr);
    local_iter = nullptr;
}
int8_t* RowGetSlice(int8_t* row_ptr, size_t idx) {
    auto row = reinterpret_cast<Row*>(row_ptr);
    return row->buf(idx);
}
size_t RowGetSliceSize(int8_t* row_ptr, size_t idx) {
    auto row = reinterpret_cast<Row*>(row_ptr);
    return row->size(idx);
}
}  // namespace vm
}  // namespace hybridse
