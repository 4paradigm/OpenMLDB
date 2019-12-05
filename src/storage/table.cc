//
// table.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//
//
#include "storage/table.h"

#include <string>
#include <algorithm>
#include "base/hash.h"
#include "base/slice.h"


namespace fesql {
namespace storage {

const static uint32_t SEED = 0xe17a1465;  // NOLINT

Table::Table(uint32_t id, uint32_t pid, const TableDef& table_def) 
    : id_(id), pid_(pid), table_def_(table_def), row_view_(table_def.columns()) {
}

Table::~Table() {
    if (segments_ != NULL) {
        for (uint32_t i = 0; i < index_map_.size(); i++) {
            for (uint32_t j = 0; j < seg_cnt_; j++) {
                delete segments_[i][j];
            }
            delete[] segments_[i];
        }
        delete[] segments_;
    }
}

bool Table::Init() {
    std::map<std::string, uint32_t> col_map;
    for (int idx = 0; idx < table_def_.columns_size(); idx++) {
        col_map.insert(std::make_pair(table_def_.columns(idx).name(), idx));
    }
    for (int idx = 0; idx < table_def_.indexes_size(); idx++) {
        if (index_map_.find(table_def_.indexes(idx).name()) != index_map_.end()) {
            return false;
        }
        IndexSt st;
        st.name = table_def_.indexes(idx).first_keys(0);
        st.type = table_def_.columns(col_map[st.name]).type();
        st.schema_pos = col_map[st.name];
        st.index_pos = idx;
        st.ts_pos = col_map[table_def_.indexes(idx).second_key()];
        index_map_.insert(std::make_pair(table_def_.indexes(idx).name(), st));
    }
    segments_ = new Segment**[index_map_.size()];
    for (uint32_t i = 0; i < index_map_.size(); i++) {
        segments_[i] = new Segment*[seg_cnt_];
        for (int j = 0; j < seg_cnt_; j++) {
            segments_[i][j] = new Segment();
        }
    }
    return true;
}

bool Table::Put(const char* row, uint32_t size) {
    row_view_.Reset(reinterpret_cast<const int8_t*>(row), size);
    if (row_view_.GetSize() != size) {
        return false;
    }
    /*DataBlock* block = reinterpret_cast<DataBlock*>(malloc(sizeof(DataBlock) + size));
    block->ref_cnt = table_def_.indexes_size();
    block->ref_cnt = 1;
    memcpy(block->data, row, size);*/
    uint32_t index_cnt = 0;
    for (int idx = 0; idx < table_def_.indexes_size(); idx++) {
        //char* val = NULL;
        char* val = "key1";
        uint32_t length = 4;
        //auto iter = index_map_.find(table_def_.indexes(idx).name());
        //row_view_.GetString(iter->second.schema_pos, &val, &length);
        row_view_.GetString(0, &val, &length);
        uint32_t index = 0;
        if (seg_cnt_ > 1) {
            index = ::fesql::base::hash(val, length, SEED) % seg_cnt_;
        }
        int64_t time = 1; 
        //row_view_.GetInt64(iter->second.ts_pos, &time);
        // row_view_.GetInt64(1, &time);
        //Segment* segment = segments_[iter->second.index_pos][index];
        Segment* segment = segments_[0][index];
        Slice spk(val, length);
        segment->Put(spk, (uint64_t)time, row, size);
    }
    return true;
}

bool Table::Put(const std::string& pk, uint64_t time, const char* data,
                uint32_t size) {
    uint32_t index = 0;
    if (seg_cnt_ > 1) {
        index = ::fesql::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Segment* segment = segments_[0][index];
    Slice spk(pk);
    segment->Put(spk, time, data, size);
    return true;
}

TableIterator* Table::NewIterator(const std::string& pk) {
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::fesql::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Slice spk(pk);
    Segment* segment = segments_[0][seg_idx];
    return segment->NewIterator(spk);
}

TableIterator* Table::NewIterator() {
    Segment* segment = segments_[0][0];
    return segment->NewIterator();
}

}  // namespace storage
}  // namespace fesql
