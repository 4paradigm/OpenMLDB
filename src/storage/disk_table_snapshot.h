//
// disk_table_snapshot.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-08-06
//

#pragma once

#include <memory>
#include <string>
#include "proto/common.pb.h"
#include "storage/disk_table.h"
#include "storage/snapshot.h"
#include "storage/table.h"

namespace openmldb {
namespace storage {

class DiskTableSnapshot : public Snapshot {
 public:
    DiskTableSnapshot(uint32_t tid, uint32_t pid,
                      ::openmldb::common::StorageMode storage_mode,
                      const std::string& db_root_path);
    virtual ~DiskTableSnapshot() = default;
    bool Init() override;
    int MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset,
                             uint64_t end_offset) override;
    bool Recover(std::shared_ptr<Table> table,
                         uint64_t& latest_offset) override;
    void SetTerm(uint64_t term) { term_ = term; }

 private:
    ::openmldb::common::StorageMode storage_mode_;
    uint64_t term_;
    std::string db_root_path_;
};

}  // namespace storage
}  // namespace openmldb
