/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * mem_catalog.cc
 *
 * Author: chenjing
 * Date: 2020/3/25
 *--------------------------------------------------------------------------
 **/
#include "vm/mem_catalog.h"
#include <algorithm>
namespace fesql {
namespace vm {
using storage::Row;
MemTableIterator::MemTableIterator(const MemSegment* table,
                                   const vm::Schema& schema)
    : table_(table), schema_(schema), iter_(table->cbegin()) {}
MemTableIterator::~MemTableIterator() {}

// TODO(chenjing): speed up seek for memory iterator
void MemTableIterator::Seek(uint64_t ts) {
    iter_ = table_->cbegin();
    while (iter_ != table_->cend()) {
        if (iter_->first <= ts) {
            return;
        }
        iter_++;
    }
}
void MemTableIterator::SeekToFirst() { iter_ = table_->cbegin(); }
const uint64_t MemTableIterator::GetKey() { return iter_->first; }
const base::Slice fesql::vm::MemTableIterator::GetValue() {
    return base::Slice(reinterpret_cast<char*>(iter_->second.buf),
                       iter_->second.size);
}
void MemTableIterator::Next() { iter_++; }

bool MemTableIterator::Valid() { return iter_ != table_->cend(); }

MemWindowIterator::MemWindowIterator(const MemSegmentMap* partitions,
                                     const Schema& schema)
    : WindowIterator(),
      partitions_(partitions),
      schema_(schema),
      iter_(partitions->cbegin()) {}

MemWindowIterator::~MemWindowIterator() {}

void MemWindowIterator::Seek(const std::string& key) {
    iter_ = partitions_->find(key);
}
void MemWindowIterator::SeekToFirst() { iter_ = partitions_->cbegin(); }
void MemWindowIterator::Next() { iter_++; }
bool MemWindowIterator::Valid() { return partitions_->cend() != iter_; }
std::unique_ptr<Iterator> MemWindowIterator::GetValue() {
    std::unique_ptr<Iterator> it = std::unique_ptr<Iterator>(
        new MemTableIterator(&(iter_->second), schema_));
    return std::move(it);
}
const base::Slice MemWindowIterator::GetKey() {
    return base::Slice(iter_->first);
}

MemTableHandler::MemTableHandler(const Schema& schema)
    : TableHandler(), table_name_(""), db_(""), schema_(schema) {}
MemTableHandler::MemTableHandler(const std::string& table_name,
                                 const std::string& db, const Schema& schema)
    : TableHandler(), table_name_(table_name), db_(db), schema_(schema) {}

MemTableHandler::~MemTableHandler() {}
std::unique_ptr<Iterator> MemTableHandler::GetIterator() {
    std::unique_ptr<MemTableIterator> it(
        new MemTableIterator(&table_, schema_));
    return std::move(it);
}
std::unique_ptr<WindowIterator> MemTableHandler::GetWindowIterator(
    const std::string& idx_name) {
    return std::unique_ptr<WindowIterator>();
}
void MemTableHandler::AddRow(const Row& row) {
    table_.push_back(std::make_pair(0, row));
}

void MemTableHandler::AddRow(const uint64_t key, const Row& row) {
    table_.push_back(std::make_pair(key, row));
}
const Types& MemTableHandler::GetTypes() { return types_; }

void MemTableHandler::Sort(const bool is_asc) {
    if (is_asc) {
        AscComparor comparor;
        std::sort(table_.begin(), table_.end(), comparor);
    } else {
        DescComparor comparor;
        std::sort(table_.begin(), table_.end(), comparor);
    }
}

const base::Slice MemTableHandler::Get(int32_t pos) {
    if (pos < 0 || table_.size() >= pos) {
        return base::Slice();
    }
    auto slice = table_.at(pos).second;
    return base::Slice(reinterpret_cast<char*>(slice.buf), slice.size);
}

MemPartitionHandler::MemPartitionHandler(const Schema& schema)
    : PartitionHandler(),
      table_name_(""),
      db_(""),
      schema_(schema),
      is_asc_(true) {}
MemPartitionHandler::MemPartitionHandler(const std::string& table_name,
                                         const std::string& db,
                                         const Schema& schema)
    : PartitionHandler(),
      table_name_(table_name),
      db_(db),
      schema_(schema),
      is_asc_(true) {}
MemPartitionHandler::~MemPartitionHandler() {}
const Schema& MemPartitionHandler::GetSchema() { return schema_; }
const std::string& MemPartitionHandler::GetName() { return table_name_; }
const std::string& MemPartitionHandler::GetDatabase() { return db_; }
const Types& MemPartitionHandler::GetTypes() { return types_; }
const IndexHint& MemPartitionHandler::GetIndex() { return index_hint_; }
bool MemPartitionHandler::AddRow(const std::string& key, uint64_t ts,
                                 const Row& row) {
    auto iter = partitions_.find(key);
    if (iter == partitions_.cend()) {
        partitions_.insert(
            std::pair<std::string, MemSegment>(key, {std::make_pair(ts, row)}));
    } else {
        iter->second.push_back(std::make_pair(ts, row));
    }
    return false;
}
std::unique_ptr<WindowIterator> MemPartitionHandler::GetWindowIterator() {
    std::unique_ptr<WindowIterator> it = std::unique_ptr<WindowIterator>(
        new MemWindowIterator(&partitions_, GetSchema()));
    return std::move(it);
}
void MemPartitionHandler::Sort(const bool is_asc) {
    if (is_asc) {
        AscComparor comparor;
        for (auto& segment : partitions_) {
            std::sort(segment.second.begin(), segment.second.end(), comparor);
        }
    } else {
        DescComparor comparor;
        for (auto& segment : partitions_) {
            std::sort(segment.second.begin(), segment.second.end(), comparor);
        }
    }

    for (auto iter = partitions_.cbegin(); iter != partitions_.cend(); iter++) {
        for (auto segment_iter = iter->second.cbegin();
             segment_iter != iter->second.cend(); segment_iter++) {
            std::cout << segment_iter->first << ",";
        }
        std::cout << std::endl;
    }
}
const bool MemPartitionHandler::IsAsc() { return is_asc_; }
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
}  // namespace vm
}  // namespace fesql
