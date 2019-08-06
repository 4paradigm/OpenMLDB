//
//// snapshot.h
//// Copyright (C) 2017 4paradigm.com
//// Author denglong
//// Date 2019-08-06
////
//
#pragma once

#include <memory>
#include "storage/table.h"

namespace rtidb {
namespace storage {

class Snapshot {

public:
    Snapshot() : offset_(0), making_snapshot_(false) {}
    virtual int MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset) = 0;
    virtual bool Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) = 0;
    uint64_t GetOffset() { return offset_; }

protected:
    uint64_t offset_;
    std::atomic<bool> making_snapshot_;
    std::string snapshot_path_;
};

}
}
