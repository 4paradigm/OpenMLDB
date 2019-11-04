//
// table.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong 
// Date 2019-11-01
//
//
#include "table.h"

#include "util/hash.h"
#include "util/slice.h"
#include <algorithm>


namespace fesql {
namespace storage {

const static uint32_t SEED = 0xe17a1465;

Table::Table(const std::string& name, uint32_t id, uint32_t pid, uint32_t seg_cnt) : 
    name_(name), id_(id), pid_(pid), seg_cnt_(seg_cnt) {}

Table::~Table() {
    if (segments_ != NULL) {
        for (uint32_t i = 0; i < seg_cnt_; i++) {
            delete segments_[i];
        }
        delete[] segments_;
    }
}

bool Table::Init() {
    segments_ = new Segment*[seg_cnt_];
    for (uint32_t i = 0; i < seg_cnt_; i++) {
        segments_[i] = new Segment();
    }
    return true;
}

bool Table::Put(const std::string& pk, 
                uint64_t time,
                const char* data, 
                uint32_t size) {
    uint32_t index = 0;
    if (seg_cnt_ > 1) {
        index = hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Segment* segment = segments_[index];
    Slice spk(pk);
    segment->Put(spk, time, data, size);
    return true;
}


TableIterator* Table::NewIterator(const std::string& pk) {
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Slice spk(pk);
    Segment* segment = segments_[seg_idx];
    return segment->NewIterator(spk);
}

TableIterator* Table::NewIterator() {
    Segment* segment = segments_[0];
    return segment->NewIterator();
}

}
}
