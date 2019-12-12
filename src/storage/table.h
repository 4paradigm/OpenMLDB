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
#include "storage/codec.h"
#include "storage/segment.h"

namespace fesql {
namespace storage {

using ::fesql::base::Iterator;
using ::fesql::type::IndexDef;
using ::fesql::type::TableDef;

static constexpr uint32_t SEG_CNT = 8;

class TableIterator {
 public:
    TableIterator() = default;
    TableIterator(Iterator<Slice, void*>* pk_it,
                  Iterator<uint64_t, DataBlock*>* ts_it);
    TableIterator(Segment** segments, uint32_t seg_cnt);
    ~TableIterator();
    void Seek(uint64_t time);
    void Seek(const std::string& key, uint64_t ts);
    bool Valid();
    void Next();
    Slice GetValue() const;
    uint64_t GetKey() const;
    Slice GetPK() const;
    void SeekToFirst();

 private:
    bool SeekToNextTsInPks();

 private:
    Segment** segments_ = NULL;
    uint32_t seg_cnt_ = 0;
    uint32_t seg_idx_ = 0;
    Iterator<Slice, void*>* pk_it_ = NULL;
    Iterator<uint64_t, DataBlock*>* ts_it_ = NULL;
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

    struct ColInfo {
        ::fesql::type::Type type;
        uint32_t pos;
    };

    struct IndexSt {
        std::string name;
        uint32_t index;
        uint32_t ts_pos;
        std::vector<ColInfo> keys;
    };

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
    RowView row_view_;
    std::map<std::string, IndexSt> index_map_;
};

}  // namespace storage
}  // namespace fesql
