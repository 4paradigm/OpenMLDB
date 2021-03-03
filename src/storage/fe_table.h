/*
 * src/storage/fe_table.h
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//
// table.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//

#pragma once

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/iterator.h"
#include "codec/fe_row_codec.h"
#include "storage/fe_segment.h"
#include "vm/catalog.h"

namespace fesql {
namespace storage {

using ::fesql::base::ConstIterator;
using ::fesql::type::IndexDef;
using ::fesql::type::TableDef;
static constexpr uint32_t SEG_CNT = 8;

class TableIterator : public ConstIterator<uint64_t, base::Slice> {
 public:
    TableIterator() = default;
    explicit TableIterator(base::Iterator<uint64_t, DataBlock*>* ts_it);
    TableIterator(Segment** segments, uint32_t seg_cnt);
    ~TableIterator();
    void Seek(const uint64_t& time);
    void Seek(const std::string& key, uint64_t ts);
    bool Valid() const;
    bool CurrentTsValid();
    void Next();
    void NextTs();
    void NextTsInPks();
    const base::Slice& GetValue();
    const uint64_t& GetKey() const;
    const base::Slice GetPK();
    void SeekToFirst();
    bool IsSeekable() const override;

 private:
    bool SeekToNextTsInPks();

 private:
    Segment** segments_ = NULL;
    uint32_t seg_cnt_ = 0;
    uint32_t seg_idx_ = 0;
    base::Iterator<base::Slice, void*>* pk_it_ = NULL;
    base::Iterator<uint64_t, DataBlock*>* ts_it_ = NULL;
    base::Slice value_;
};

class Table {
 public:
    Table() = default;

    Table(uint32_t id, uint32_t pid, const TableDef& table_def);

    ~Table();

    bool Init();

    bool Put(const char* row, uint32_t size);

    std::unique_ptr<TableIterator> NewIterator(const std::string& pk,
                                               const uint64_t ts);

    std::unique_ptr<TableIterator> NewIterator(const std::string& pk,
                                               const std::string& index_name);

    std::unique_ptr<TableIterator> NewIterator(const std::string& pk);

    std::unique_ptr<TableIterator> NewTraverseIterator(
        const std::string& index_name);
    std::unique_ptr<TableIterator> NewTraverseIterator();

    inline uint32_t GetId() const { return id_; }

    inline uint32_t GetPid() const { return pid_; }

    struct IndexSt {
        std::string name;
        uint32_t index;
        uint32_t ts_pos;
        std::vector<std::pair<::fesql::type::Type, size_t>> keys;
    };

    const std::map<std::string, IndexSt>& GetIndexMap() const {
        return index_map_;
    }

    inline const TableDef& GetTableDef() { return table_def_; }

    inline Segment*** GetSegments() { return segments_; }

    inline uint32_t GetSegCnt() { return seg_cnt_; }
    bool DecodeKeysAndTs(const IndexSt& index, const char* row, uint32_t size,
                         std::string& key,  // NOLINT
                         int64_t* time_ptr);

 private:
    std::unique_ptr<TableIterator> NewIndexIterator(const std::string& pk,
                                                    const uint32_t index);

 private:
    std::string name_;
    uint32_t id_;
    uint32_t pid_;
    uint32_t seg_cnt_ = SEG_CNT;
    Segment*** segments_ = NULL;
    TableDef table_def_;
    codec::RowView row_view_;
    std::map<std::string, IndexSt> index_map_;
};

}  // namespace storage
}  // namespace fesql
