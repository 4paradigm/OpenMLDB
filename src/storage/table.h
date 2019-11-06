//
// table.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//

#pragma once

#include <atomic>
#include <memory>
#include "iterator.h"
#include "segment.h"

namespace fesql {
namespace storage {

class Table {

public:

    Table() = default;
    Table(const std::string& name, uint32_t id, uint32_t pid, uint32_t seg_cnt);
    ~Table();

    bool Init();

    bool Put(const std::string& pk, uint64_t time, const char* data, uint32_t size);

    TableIterator* NewIterator(const std::string& pk);
    TableIterator* NewIterator();

    inline std::string GetName() const {
        return name_;
    }

    inline uint32_t GetId() const {
        return id_;
    }

    inline uint32_t GetPid() const {
        return pid_;
    }

private:
    std::string name_;
    uint32_t id_;
    uint32_t pid_;
    uint32_t seg_cnt_;
    Segment** segments_ = NULL;
};

}
}
