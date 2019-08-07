//
// disk_table_snapshot.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-08-06
//

#pragma once

#include "storage/snapshot.h"
#include "storage/table.h"
#include "storage/disk_table.h"
#include "proto/common.pb.h"

namespace rtidb {
namespace storage {

class DiskTableSnapshot : public Snapshot {
public:
    DiskTableSnapshot(uint32_t tid, uint32_t pid, ::rtidb::common::StorageMode storage_mode);
    virtual ~DiskTableSnapshot() = default;
    virtual bool Init() override;
    virtual int MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset) override;
    virtual bool Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) override;

private:
    ::rtidb::common::StorageMode storage_mode_;

};

}
}
