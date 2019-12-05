//
// table.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//

#pragma once

#include <string>
#include <atomic>
#include <memory>
#include "base/iterator.h"
#include "storage/segment.h"
#include "storage/codec.h"

namespace fesql {
namespace storage {

using ::fesql::base::Iterator;
using ::fesql::type::TableDef;
using ::fesql::type::IndexDef;

static const uint32_t SEG_CNT = 8;

class Table {
 public:
    Table() = default;

    Table(uint32_t id, uint32_t pid, const TableDef& table_def);

    ~Table();

    bool Init();

    bool Put(const std::string& pk, uint64_t time, const char* data,
             uint32_t size);

    bool Put(const char* row, uint32_t size);

    TableIterator* NewIterator(const std::string& pk);
    TableIterator* NewIterator();

    inline uint32_t GetId() const { return id_; }

    inline uint32_t GetPid() const { return pid_; }

    struct IndexSt {
        std::string name;
        ::fesql::type::Type type;
        uint32_t schema_pos;
        uint32_t index_pos;
        uint32_t ts_pos;
    };

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
