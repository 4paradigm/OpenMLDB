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

TableIndex::TableIndex() : pk_idx_id_(0) {
    indexs_ = std::make_shared<std::vector<std::shared_ptr<IndexDef>>>();
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
    for (auto index_def : index_vec) {
        if (index_def->GetType() == ::rtidb::type::kPrimaryKey || 
                index_def->GetType() == ::rtidb::type::kAutoGen) {
            pk_idx_id_ = index_def->GetId();
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

bool TableIndex::HasAutoGen() {
    std::shared_ptr<IndexDef> index_def = GetIndex(pk_idx_id_);
    if (index_def && index_def->GetType() == ::rtidb::type::kAutoGen) {
        return true;
    }
    return false;
}

void TableIndex::AddIndex(std::shared_ptr<IndexDef> index_def) {
    auto old_indexs = std::atomic_load_explicit(&indexs_, std::memory_order_relaxed);
    auto new_indexs = std::make_shared<std::vector<std::shared_ptr<IndexDef>>>(*old_indexs);
    new_indexs->push_back(index_def);
    std::atomic_store_explicit(&indexs_, new_indexs, std::memory_order_relaxed);
    if (index_def->GetType() == ::rtidb::type::kPrimaryKey || 
            index_def->GetType() == ::rtidb::type::kAutoGen) {
        pk_idx_id_ = index_def->GetId();
    } 
}

}
}
