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
#include <vector>
#include <utility>
#include "base/iterator.h"
#include "storage/codec.h"
#include "storage/segment.h"

namespace fesql {
namespace storage {

using ::fesql::base::Iterator;
using ::fesql::type::IndexDef;
using ::fesql::type::TableDef;

static constexpr uint32_t SEG_CNT = 1;

class Table {
 public:
    Table() = default;

    Table(uint32_t id, uint32_t pid, const TableDef& table_def);

    ~Table();

    bool Init();

    bool Put(const char* row, uint32_t size);

    std::unique_ptr<TableIterator> NewIterator(const std::string& pk);
    std::unique_ptr<TableIterator> NewIterator();

    inline uint32_t GetId() const { return id_; }

    inline uint32_t GetPid() const { return pid_; }

    int32_t GetInteger(const char* row, uint32_t idx, ::fesql::type::Type type,
                       uint64_t* value);

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

    const std::map<std::string, IndexSt>& GetIndexMap() const {
        return index_map_;
    }

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
