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

IndexDef::~IndexDef() {
}

TableIndex::TableIndex() : indexs_() {
}

TableIndex::~TableIndex() {
    indexs_.clear();
}

void TableIndex::ReSet() {
    indexs_.clear();
}

void TableIndex::SetAllIndex(const std::vector<std::shared_ptr<IndexDef>>& index_vec) {
    indexs_ = index_vec; 
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(uint32_t idx) {
    if (idx < indexs_.size()) {
        return indexs_[idx];
    }
    return std::shared_ptr<IndexDef>();
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(const std::string& name) {
    for (const auto& index : indexs_) {
        if (index->GetName() == name) {
            return index;
        }
    }
    return std::shared_ptr<IndexDef>();
}

void TableIndex::AddIndex(std::shared_ptr<IndexDef> index_def) {
    indexs_.push_back(index_def);
}

}
}
