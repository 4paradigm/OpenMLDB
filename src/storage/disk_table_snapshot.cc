//
// disk_table_snapshot.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-08-06
//

#include <gflags/gflags.h>
#include "storage/disk_table_snapshot.h"
#include "base/file_util.h"
#include "base/strings.h"
#include "logging.h"
#include <limits.h>
#include <stdlib.h>
#include <boost/algorithm/string/predicate.hpp>

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

DECLARE_string(ssd_root_path);
DECLARE_string(hdd_root_path);

namespace rtidb {
namespace storage {

DiskTableSnapshot::DiskTableSnapshot(uint32_t tid, uint32_t pid, 
        ::rtidb::common::StorageMode storage_mode) : Snapshot(tid, pid), 
        storage_mode_(storage_mode), term_(0) {
}

bool DiskTableSnapshot::Init() {
    std::string db_root_path = storage_mode_ == ::rtidb::common::StorageMode::kSSD ?  FLAGS_ssd_root_path : FLAGS_hdd_root_path;
    snapshot_path_ = db_root_path + "/" + std::to_string(tid_) + "_" + std::to_string(pid_) + "/snapshot/";
    if (!::rtidb::base::MkdirRecur(snapshot_path_)) {
        PDLOG(WARNING, "fail to create db meta path %s", snapshot_path_.c_str());
        return false;
    }
    char abs_path_buff[PATH_MAX];
    if (realpath(snapshot_path_.c_str(), abs_path_buff) == NULL) {
        PDLOG(WARNING, "fail to get realpath. source path %s", snapshot_path_.c_str());
        return false;
    }
    snapshot_path_.assign(abs_path_buff);
    if (!boost::ends_with(snapshot_path_, "/")) {
        snapshot_path_.append("/");
    }
    return true;
}

int DiskTableSnapshot::MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset) {
    if (making_snapshot_.load(std::memory_order_acquire)) {
        PDLOG(INFO, "snapshot is doing now!");
        return 0;
    }
    making_snapshot_.store(true, std::memory_order_release);
    std::string now_time = ::rtidb::base::GetNowTime();
    std::string snapshot_dir = snapshot_path_ + now_time.substr(0, now_time.length() - 2);
    if (::rtidb::base::IsExists(snapshot_dir)) {
        PDLOG(WARNING, "checkpoint dir[%s] is exist", snapshot_dir.c_str());
        making_snapshot_.store(false, std::memory_order_release);
        return -1;
    }
    DiskTable* disk_table = dynamic_cast<DiskTable*>(table.get());
    if (disk_table == NULL) {
        making_snapshot_.store(false, std::memory_order_release);
        return -1;
    }
    uint64_t record_count = disk_table->GetRecordCnt();
    uint64_t cur_offset = disk_table->GetOffset();
    disk_table->CreateCheckPoint(snapshot_dir);
    ::rtidb::api::Manifest manifest;
    GetLocalManifest(manifest);
    int ret = 0;
    if (GenManifest(snapshot_dir, record_count, cur_offset, term_) == 0) {
        if (manifest.has_name() && manifest.name() != snapshot_dir) {
            PDLOG(DEBUG, "delete old checkpoint[%s]", manifest.name().c_str());
            if (!::rtidb::base::RemoveDir(manifest.name())) {
                PDLOG(WARNING, "delete checkpoint failed. checkpoint dir[%s]", snapshot_dir.c_str());
            }
        }
        offset_ = cur_offset;
    } else {
        PDLOG(WARNING, "GenManifest failed. delete checkpoint[%s]", snapshot_dir.c_str());
        ret = -1;
    }
    making_snapshot_.store(false, std::memory_order_release);
    return ret;
}

bool DiskTableSnapshot::Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) {
    // TODO
    return true;
}

}
}
