//
// disk_table_snapshot.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-08-06
//

#include <gflags/gflags.h>
#include "storage/disk_table_snapshot.h"
#include "base/file_util.h"
#include "logging.h"

namespace rtidb {
namespace storage {

DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);

DiskTableSnapshot::DiskTableSnapshot(uint32_t tid, uint32_t pid, 
		::rtidb::common::StorageMode storage_mode) : Snapshot(tid, pid), 
		storage_mode_(storage_mode) {
}

bool DiskTableSnapshot::Init() {
	std::string db_root_path = storage_mode_ == ::rtidb::common::StorageMode::kSSD ?  FLAGS_ssd_root_path : FLAGS_hdd_root_path;
	snapshot_path_ = db_root_path + "/" + std::to_string(tid_) + "_" + std::to_string(pid_) + "/snapshot/";
    if (!::rtidb::base::MkdirRecur(snapshot_path_)) {
        PDLOG(WARNING, "fail to create db meta path %s", snapshot_path_.c_str());
        return false;
    }
	return true;
}

int DiskTableSnapshot::MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset) {
    DiskTable* disk_table = dynamic_cast<DiskTable*>(table.get());
    if (disk_table == NULL) {
        return -1;
    }
    disk_table->CreateCheckPoint(snapshot_path_);
    return 0;
}

}
}
