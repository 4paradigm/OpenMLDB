//
// schema.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2020-03-11
//

#include "schema.h"

namespace rtidb {
namespace storage {

IndexDef::IndexDef(const std::string& name, uint32_t id) : 
    name_(name), index_id_(id), status_(IndexStatus::kReady) {
}

IndexDef::IndexDef(const std::string& name, uint32_t id, IndexStatus status) : 
    name_(name), index_id_(id), status_(status) {
}

IndexDef::IndexDef(const std::string& name, uint32_t id,  
        const IndexStatus& status, ::rtidb::type::IndexType type, 
        const std::map<uint32_t, ::rtidb::common::ColumnDesc>& column_idx_map) :
    name_(name), index_id_(id), status_(status), type_(type), column_idx_map_(column_idx_map) {
}

IndexDef::~IndexDef() {
}

TableIndex::TableIndex() {
    indexs_ = std::make_shared<std::vector<std::shared_ptr<IndexDef>>>();
    pk_index_ = std::shared_ptr<IndexDef>();
}

TableIndex::~TableIndex() {
    indexs_->clear();
}

void TableIndex::ReSet() {
    std::atomic_load_explicit(&indexs_, std::memory_order_relaxed)->clear();
}

void TableIndex::SetAllIndex(const std::vector<std::shared_ptr<IndexDef>>& index_vec) {
    auto new_indexs = std::make_shared<std::vector<std::shared_ptr<IndexDef>>>(index_vec);
    std::atomic_store_explicit(&indexs_, new_indexs, std::memory_order_relaxed);
    for (auto index_def : *new_indexs) {
        if (index_def->GetType() == ::rtidb::type::kPrimaryKey || 
                index_def->GetType() == ::rtidb::type::kAutoGen) {
            pk_index_ = index_def; 
            break;
        } 
    }
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(uint32_t idx) {
    auto indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    if (idx < indexs->size()) {
        return indexs->at(idx);
    }
    return std::shared_ptr<IndexDef>();
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(const std::string& name) {
    auto indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
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

void TableIndex::AddIndex(std::shared_ptr<IndexDef> index_def) {
    auto old_indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    auto new_indexs = std::make_shared<std::vector<std::shared_ptr<IndexDef>>>(*old_indexs);
    new_indexs->push_back(index_def);
    std::atomic_store_explicit(&indexs_, new_indexs, std::memory_order_relaxed);
    if (index_def->GetType() == ::rtidb::type::kPrimaryKey || 
            index_def->GetType() == ::rtidb::type::kAutoGen) {
        pk_index_ = index_def; 
    } 
}

bool TableIndex::HasAutoGen() {
    if (pk_index_->GetType() == ::rtidb::type::kAutoGen) {
        return true;
    }
    return false;
}

std::shared_ptr<IndexDef> TableIndex::GetPkIndex() {
    return pk_index_;    
}

ColumnDef::ColumnDef(const std::string& name, uint32_t id, ::rtidb::type::DataType type) :
    name_(name), id_(id), type_(type) {
}

ColumnDef::~ColumnDef() {
}

TableColumn::TableColumn() {
    columns_ = std::make_shared<std::vector<std::shared_ptr<ColumnDef>>>();
    column_map_ = std::make_shared<std::map<std::string, std::shared_ptr<ColumnDef>>>();
}

TableColumn::~TableColumn() {
    columns_->clear();
    column_map_->clear();
}

void TableColumn::SetAllColumn(const std::vector<std::shared_ptr<ColumnDef>>& column_def) {
    auto new_columns = std::make_shared<std::vector<std::shared_ptr<ColumnDef>>>(column_def);
    std::atomic_store_explicit(&columns_, new_columns, std::memory_order_relaxed);
}

std::shared_ptr<ColumnDef> TableColumn::GetColumn(uint32_t idx) {
    auto columns = std::atomic_load_explicit(&columns_, std::memory_order_relaxed);
    if (idx < columns->size()) {
        return columns->at(idx);
    }
    return std::shared_ptr<ColumnDef>();
}

std::shared_ptr<ColumnDef> TableColumn::GetColumn(const std::string& name) {
    auto map = std::atomic_load_explicit(&column_map_, std::memory_order_relaxed);
    auto it = (*map).find(name);
    if (it != (*map).end()) {
        return it->second;
    } else {
        return std::shared_ptr<ColumnDef>();
    }
}

std::vector<std::shared_ptr<ColumnDef>> TableColumn::GetAllColumn() {
    return *std::atomic_load_explicit(&columns_, std::memory_order_relaxed);
}

void TableColumn::AddColumn(std::shared_ptr<ColumnDef> column_def) {
        auto old_columns = std::atomic_load_explicit(&columns_, std::memory_order_relaxed);
        auto new_columns = std::make_shared<std::vector<std::shared_ptr<ColumnDef>>>(*old_columns);
        new_columns->push_back(column_def);
        std::atomic_store_explicit(&columns_, new_columns, std::memory_order_relaxed);

        auto old_map = std::atomic_load_explicit(&column_map_, std::memory_order_relaxed);
        auto new_map = std::make_shared<std::map<std::string, std::shared_ptr<ColumnDef>>>(*old_map);
        new_map->insert(std::make_pair(column_def->GetName(), column_def));
        std::atomic_store_explicit(&column_map_, new_map, std::memory_order_relaxed);
}

}
}
