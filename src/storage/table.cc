//
// table.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//
//

#include <algorithm>
#include <string>
#include "glog/logging.h"
#include "base/hash.h"
#include "base/slice.h"
#include "storage/table.h"

namespace fesql {
namespace storage {

static constexpr uint32_t SEED = 0xe17a1465;
static constexpr uint32_t COMBINE_KEY_RESERVE_SIZE = 128;

Table::Table(uint32_t id, uint32_t pid, const TableDef& table_def)
    : id_(id),
      pid_(pid),
      table_def_(table_def),
      row_view_(table_def_.columns()) {}

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
        if (index_map_.find(table_def_.indexes(idx).name()) !=
            index_map_.end()) {
            return false;
        }
        IndexSt st;
        st.name = table_def_.indexes(idx).name();
        if (col_map.find(table_def_.indexes(idx).second_key()) ==
            col_map.end()) {
            return false;
        }
        st.ts_pos = col_map[table_def_.indexes(idx).second_key()];
        st.index = idx;
        std::vector<ColInfo> col_vec;
        for (int i = 0; i < table_def_.indexes(idx).first_keys_size(); i++) {
            std::string name = table_def_.indexes(idx).first_keys(i);
            ColInfo col;
            auto iter = col_map.find(name);
            if (iter == col_map.end()) return false;
            col.type = table_def_.columns(iter->second).type();
            col.pos = iter->second;
            col_vec.push_back(std::move(col));
        }
        if (col_vec.empty()) return false;
        st.keys = col_vec;
        index_map_.insert(
            std::make_pair(table_def_.indexes(idx).name(), std::move(st)));
    }
    if (index_map_.empty()) {
        LOG(WARNING) << "no index in table" << table_def_.name();
        return false;
    }
    segments_ = new Segment**[index_map_.size()];
    for (uint32_t i = 0; i < index_map_.size(); i++) {
        segments_[i] = new Segment*[seg_cnt_];
        for (uint32_t j = 0; j < seg_cnt_; j++) {
            segments_[i][j] = new Segment();
        }
    }
    DLOG(INFO) << "table " << table_def_.name() << " init ok";
    return true;
}

bool Table::Put(const char* row, uint32_t size) {
    if (row_view_.GetSize(reinterpret_cast<const int8_t*>(row)) != size) {
        return false;
    }
    DataBlock* block =
        reinterpret_cast<DataBlock*>(malloc(sizeof(DataBlock) + size));
    block->ref_cnt = table_def_.indexes_size();
    memcpy(block->data, row, size);
    for (const auto& kv : index_map_) {
        std::string key;
        Slice spk;
        if (kv.second.keys.size() > 1) {
            key.reserve(COMBINE_KEY_RESERVE_SIZE);
            for (const auto& col : kv.second.keys) {
                if (!key.empty()) {
                    key.append("|");
                }
                if (col.type == ::fesql::type::kVarchar) {
                    char* val = NULL;
                    uint32_t length = 0;
                    row_view_.GetValue(reinterpret_cast<const int8_t*>(row),
                                       col.pos, &val, &length);
                    if (length != 0) {
                        key.append(val, length);
                    }
                } else {
                    int64_t value = 0;
                    row_view_.GetInteger(reinterpret_cast<const int8_t*>(row),
                                         col.pos, col.type, &value);
                    key.append(std::to_string(value));
                }
            }
            spk.reset(key.c_str(), key.length());
        } else {
            if (kv.second.keys[0].type == ::fesql::type::kVarchar) {
                char* val = NULL;
                uint32_t length = 0;
                row_view_.GetValue(reinterpret_cast<const int8_t*>(row),
                                   kv.second.keys[0].pos, &val, &length);
                spk.reset(val, length);
            } else {
                int64_t value = 0;
                row_view_.GetInteger(reinterpret_cast<const int8_t*>(row),
                                     kv.second.keys[0].pos,
                                     kv.second.keys[0].type, &value);
                key = std::to_string(value);
                spk.reset(key.c_str(), key.length());
            }
        }
        uint32_t seg_index = 0;
        if (seg_cnt_ > 1) {
            seg_index =
                ::fesql::base::hash(spk.data(), spk.size(), SEED) % seg_cnt_;
        }
        int64_t time = 1;
        row_view_.GetInteger(
            reinterpret_cast<const int8_t*>(row), kv.second.ts_pos,
            table_def_.columns(kv.second.ts_pos).type(), &time);
        Segment* segment = segments_[kv.second.index][seg_index];
        segment->Put(spk, (uint64_t)time, block);
    }
    return true;
}

std::unique_ptr<TableIterator> Table::NewIterator(const std::string& pk) {
    uint32_t seg_idx = 0;
    if (seg_cnt_ > 1) {
        seg_idx = ::fesql::base::hash(pk.c_str(), pk.length(), SEED) % seg_cnt_;
    }
    Slice spk(pk);
    Segment* segment = segments_[0][seg_idx];
    return std::move(segment->NewIterator(spk));
}

std::unique_ptr<TableIterator> Table::NewIterator() {
    Segment* segment = segments_[0][0];
    return std::move(segment->NewIterator());
}

}  // namespace storage
}  // namespace fesql
