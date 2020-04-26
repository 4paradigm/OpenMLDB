//
// schema.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2020-03-11
//

#include "storage/schema.h"
#include <utility>

namespace rtidb {
namespace storage {

ColumnDef::ColumnDef(const std::string& name, uint32_t id,
                     ::rtidb::type::DataType type)
    : name_(name), id_(id), type_(type) {}

ColumnDef::~ColumnDef() {}

TableColumn::TableColumn() {
}

TableColumn::~TableColumn() {
}

std::shared_ptr<ColumnDef> TableColumn::GetColumn(uint32_t idx) {
    if (idx < columns_.size()) {
        return columns_.at(idx);
    }
    return std::shared_ptr<ColumnDef>();
}

std::shared_ptr<ColumnDef> TableColumn::GetColumn(const std::string& name) {
    auto it = column_map_.find(name);
    if (it != column_map_.end()) {
        return it->second;
    } else {
        return std::shared_ptr<ColumnDef>();
    }
}

std::vector<std::shared_ptr<ColumnDef>> TableColumn::GetAllColumn() {
    return columns_;
}

void TableColumn::AddColumn(std::shared_ptr<ColumnDef> column_def) {
    columns_.push_back(column_def);
    column_map_.insert(std::make_pair(column_def->GetName(), column_def));
}

IndexDef::IndexDef(const std::string& name, uint32_t id)
    : name_(name), index_id_(id), status_(IndexStatus::kReady) {}

IndexDef::IndexDef(const std::string& name, uint32_t id, IndexStatus status)
    : name_(name), index_id_(id), status_(status) {}

IndexDef::IndexDef(const std::string& name, uint32_t id,
                   const IndexStatus& status, ::rtidb::type::IndexType type,
                   const std::vector<ColumnDef>& columns)
    : name_(name),
      index_id_(id),
      status_(status),
      type_(type),
      columns_(columns) {}

IndexDef::~IndexDef() {}

bool ColumnDefSortFunc(const ColumnDef& cd_a, const ColumnDef& cd_b) {
    return (cd_a.GetId() < cd_b.GetId());
}

TableIndex::TableIndex() {
    indexs_ = std::make_shared<std::vector<std::shared_ptr<IndexDef>>>();
    pk_index_ = std::shared_ptr<IndexDef>();
    combine_col_name_map_ =
        std::make_shared<
        std::unordered_map<std::string, std::shared_ptr<IndexDef>>>();
}

TableIndex::~TableIndex() {
    indexs_->clear();
    combine_col_name_map_->clear();
}

void TableIndex::ReSet() {
    auto new_indexs =
        std::make_shared<std::vector<std::shared_ptr<IndexDef>>>();
    std::atomic_store_explicit(&indexs_, new_indexs, std::memory_order_relaxed);
    pk_index_ = std::shared_ptr<IndexDef>();
    auto new_map =
        std::make_shared<
        std::unordered_map<std::string, std::shared_ptr<IndexDef>>>();
    std::atomic_store_explicit(&combine_col_name_map_, new_map,
                               std::memory_order_relaxed);
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(uint32_t idx) {
    auto indexs =
        std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    if (idx < indexs->size()) {
        return indexs->at(idx);
    }
    return std::shared_ptr<IndexDef>();
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(const std::string& name) {
    auto indexs =
        std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    for (const auto& index : *indexs) {
        if (index->GetName() == name) {
            return index;
        }
    }
    return std::shared_ptr<IndexDef>();
}

std::vector<std::shared_ptr<IndexDef>> TableIndex::GetAllIndex() {
    return *std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
}

int TableIndex::AddIndex(std::shared_ptr<IndexDef> index_def) {
    auto old_indexs =
        std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    if (old_indexs->size() >= MAX_INDEX_NUM) {
        return -1;
    }
    auto new_indexs =
        std::make_shared<std::vector<std::shared_ptr<IndexDef>>>(*old_indexs);
    new_indexs->push_back(index_def);
    std::atomic_store_explicit(&indexs_, new_indexs, std::memory_order_relaxed);
    if (index_def->GetType() == ::rtidb::type::kPrimaryKey ||
        index_def->GetType() == ::rtidb::type::kAutoGen) {
        pk_index_ = index_def;
    }
    std::string combine_name = "";
    int count = 0;
    for (auto& col_def : index_def->GetColumns()) {
        if (count++ > 0) {
            combine_name.append("_");
        }
        combine_name.append(col_def.GetName());
    }
    auto old_map = std::atomic_load_explicit(&combine_col_name_map_,
                                             std::memory_order_relaxed);
    auto new_map =
        std::make_shared<std::unordered_map<std::string,
        std::shared_ptr<IndexDef>>>(*old_map);
    new_map->insert(std::make_pair(combine_name, index_def));
    std::atomic_store_explicit(&combine_col_name_map_, new_map,
                               std::memory_order_relaxed);
    return 0;
}

bool TableIndex::HasAutoGen() {
    if (pk_index_->GetType() == ::rtidb::type::kAutoGen) {
        return true;
    }
    return false;
}

std::shared_ptr<IndexDef> TableIndex::GetPkIndex() { return pk_index_; }

const std::shared_ptr<IndexDef> TableIndex::GetIndexByCombineStr(
    const std::string& combine_str) {
    auto map = std::atomic_load_explicit(&combine_col_name_map_,
                                         std::memory_order_relaxed);
    auto it = map->find(combine_str);
    if (it != map->end()) {
        return it->second;
    } else {
        return std::shared_ptr<IndexDef>();
    }
}

}  // namespace storage
}  // namespace rtidb
