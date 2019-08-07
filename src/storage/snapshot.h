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
#include "proto/tablet.pb.h"

namespace rtidb {
namespace storage {

class Snapshot {

public:
    Snapshot(uint32_t tid, uint32_t pid) : tid_(tid), pid_(pid), offset_(0), making_snapshot_(false) {}
    virtual bool Init() = 0;
    virtual int MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset) = 0;
    virtual bool Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) = 0;
    uint64_t GetOffset() { return offset_; }
    int GenManifest(const std::string& snapshot_name, uint64_t key_count, uint64_t offset, uint64_t term);
    int GetManifest(::rtidb::api::Manifest& manifest);

protected:
    uint32_t tid_;
    uint32_t pid_;
    uint64_t offset_;
    std::atomic<bool> making_snapshot_;
    std::string snapshot_path_;
};

}
}
