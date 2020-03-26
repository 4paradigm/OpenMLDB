/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * mem_catalog.cc
 *
 * Author: chenjing
 * Date: 2020/3/25
 *--------------------------------------------------------------------------
 **/
#include "mem_catalog.h"
#include "vm/mem_table_iterator.h"
namespace fesql {
namespace vm {
using storage::Row;

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
    uint64_t key = table_.size();
    table_.push_back(std::make_pair(key, row));
}

void MemTableHandler::AddRow(const uint64_t key, const Row& row) {
    table_.push_back(std::make_pair(key, row));
}
const Types& MemTableHandler::GetTypes() { return types_; }

void MemTableHandler::Sort() {
    Comparor comparor;
    std::sort(table_.begin(), table_.end(), comparor);
}

bool MemPartitionHandler::AddRow(const std::string& key, const Row& row) {
    auto iter = partitions_.find(key);
    if (iter == partitions_.cend()) {
        partitions_.insert(
            std::pair<std::string, MemSegment>(key, {std::make_pair(0, row)}));
    } else {
        iter->second.push_back(std::make_pair(iter->second.size(), row));
    }
    return false;
}

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

void MemPartitionHandler::Sort() {
    Comparor comparor;
    for (auto segment : partitions_) {
        std::sort(segment.second.begin(), segment.second.end(), comparor);
    }
}

}  // namespace vm
}  // namespace fesql
