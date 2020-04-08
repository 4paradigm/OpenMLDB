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

IndexDef::~IndexDef() {
}

TableIndex::TableIndex() : indexs_(MAX_INDEX_NUM, std::shared_ptr<IndexDef>()), size_(0) {
}

TableIndex::~TableIndex() {
    indexs_.clear();
}

void TableIndex::ReSet() {
    indexs_ = std::vector<std::shared_ptr<IndexDef>>(MAX_INDEX_NUM, std::shared_ptr<IndexDef>());
    size_ = 0;
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(uint32_t idx) {
    if (idx < size_) {
        return indexs_.at(idx);
    }
    return std::shared_ptr<IndexDef>();
}

std::shared_ptr<IndexDef> TableIndex::GetIndex(const std::string& name) {
    for (uint32_t idx = 0; idx < size_; idx++) {
        if (indexs_[idx]->GetName() == name) {
            return indexs_[idx];
        }
    }
    return std::shared_ptr<IndexDef>();
}

std::vector<std::shared_ptr<IndexDef>> TableIndex::GetAllIndex() {
    return std::vector<std::shared_ptr<IndexDef>>(indexs_.begin(), indexs_.begin() + size_);
}

int TableIndex::AddIndex(std::shared_ptr<IndexDef> index_def) {
    if (size_ >= MAX_INDEX_NUM) {
        return -1;
    }
    indexs_[size_] = index_def;
    size_++;
    return 0;
}

}
}
