/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * mem_table_iterator.cc
 *
 * Author: chenjing
 * Date: 2020/3/25
 *--------------------------------------------------------------------------
 **/
#include "mem_table_iterator.h"
#include "glog/logging.h"
namespace fesql {
namespace vm {
using fesql::storage::Row;
MemTableIterator::MemTableIterator(const MemSegment *table,
                                   const vm::Schema &schema)
    : table_(table), schema_(schema), iter_(table->cbegin()) {}
MemTableIterator::~MemTableIterator() {}

//TODO(chenjing): speed up seek for memory iterator
void MemTableIterator::Seek(uint64_t ts) {
    iter_ = table_->cbegin();
    while(iter_ != table_->cend()) {
        if (iter_->first <= ts) {
            return;
        }
        iter_++;
    }
}
void MemTableIterator::SeekToFirst() {}
const uint64_t MemTableIterator::GetKey() { return iter_->first; }
const base::Slice fesql::vm::MemTableIterator::GetValue() {
    return base::Slice(reinterpret_cast<char *>(iter_->second.buf),
                       iter_->second.size);
}
void MemTableIterator::Next() { iter_++; }

bool MemTableIterator::Valid() { return iter_ != table_->end(); }

MemWindowIterator::MemWindowIterator(
    const std::map<std::string, MemSegment> *partitions, const Schema &schema)
    : WindowIterator(), partitions_(partitions), schema_(schema) {}

MemWindowIterator::~MemWindowIterator() {}

void MemWindowIterator::Seek(const std::string &key) {
    iter_ = partitions_->find(key);
}
void MemWindowIterator::SeekToFirst() { iter_ = partitions_->cbegin(); }
void MemWindowIterator::Next() { iter_++; }
bool MemWindowIterator::Valid() {
    partitions_->cend() != iter_;
    return false;
}
std::unique_ptr<Iterator> MemWindowIterator::GetValue() {
    std::unique_ptr<Iterator> it = std::unique_ptr<Iterator>(
        new MemTableIterator(&(iter_->second), schema_));
    return std::move(it);
}
const base::Slice MemWindowIterator::GetKey() {
    return base::Slice(iter_->first);
}

}  // namespace vm
}  // namespace fesql
