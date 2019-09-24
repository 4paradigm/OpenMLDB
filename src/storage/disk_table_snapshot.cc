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

namespace rtidb {
namespace storage {

const std::string MANIFEST = "MANIFEST";

DiskTableSnapshot::DiskTableSnapshot(uint32_t tid, uint32_t pid, 
        ::rtidb::common::StorageMode storage_mode,
        const std::string& db_root_path) : Snapshot(tid, pid), 
        storage_mode_(storage_mode), term_(0),
        db_root_path_(db_root_path){
}

bool DiskTableSnapshot::Init() {
    snapshot_path_ = db_root_path_ + "/" + std::to_string(tid_) + "_" + std::to_string(pid_) + "/snapshot/";
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
        PDLOG(INFO, "snapshot is doing now! tid %u pid %u", tid_, pid_);
        return 0;
    }
    making_snapshot_.store(true, std::memory_order_release);
    int ret = 0;
    do {
        std::string now_time = ::rtidb::base::GetNowTime();
        std::string snapshot_dir_name = now_time.substr(0, now_time.length() - 2);
        std::string snapshot_dir = snapshot_path_ + snapshot_dir_name;
        std::string snapshot_dir_tmp = snapshot_dir + ".tmp";
        if (::rtidb::base::IsExists(snapshot_dir_tmp)) {
            PDLOG(WARNING, "checkpoint dir[%s] is exist", snapshot_dir_tmp.c_str());
            if (!::rtidb::base::RemoveDir(snapshot_dir_tmp)) {
                PDLOG(WARNING, "delete checkpoint failed. checkpoint dir[%s]", snapshot_dir_tmp.c_str());
                break;
            }
        }
        DiskTable* disk_table = dynamic_cast<DiskTable*>(table.get());
        if (disk_table == NULL) {
            break;
        }
        uint64_t record_count = disk_table->GetRecordCnt();
        uint64_t cur_offset = disk_table->GetOffset();
        if (disk_table->CreateCheckPoint(snapshot_dir_tmp) < 0) {
            PDLOG(WARNING, "create checkpoint failed. checkpoint dir[%s]", snapshot_dir_tmp.c_str());
            break;
        }
        if (::rtidb::base::IsExists(snapshot_dir)) {
            std::string snapshot_dir_bak = snapshot_dir + ".bak";
            if (::rtidb::base::IsExists(snapshot_dir_bak)) {
                if (!::rtidb::base::RemoveDir(snapshot_dir_bak)) {
                    PDLOG(WARNING, "delete checkpoint bak failed. checkpoint bak dir[%s]", snapshot_dir_bak.c_str());
                    break;
                }
            }
            if (!::rtidb::base::Rename(snapshot_dir, snapshot_dir_bak)) {
                PDLOG(WARNING, "rename checkpoint failed. checkpoint bak dir[%s]", snapshot_dir_bak.c_str());
                break;
            }
        }
        if (!::rtidb::base::Rename(snapshot_dir_tmp, snapshot_dir)) {
            PDLOG(WARNING, "rename checkpoint failed. checkpoint dir[%s]", snapshot_dir.c_str());
            break;
        }
        ::rtidb::api::Manifest manifest;
        GetLocalManifest(snapshot_path_ + MANIFEST, manifest);
        if (GenManifest(snapshot_dir_name, record_count, cur_offset, term_) == 0) {
            if (manifest.has_name() && manifest.name() != snapshot_dir) {
                PDLOG(DEBUG, "delete old checkpoint[%s]", manifest.name().c_str());
                if (!::rtidb::base::RemoveDir(snapshot_path_ + manifest.name())) {
                    PDLOG(WARNING, "delete checkpoint failed. checkpoint dir[%s]", snapshot_dir.c_str());
                }
            }
            offset_ = cur_offset;
            out_offset = cur_offset;
            ret = 0;
        } else {
            PDLOG(WARNING, "GenManifest failed. delete checkpoint[%s]", snapshot_dir.c_str());
            ::rtidb::base::RemoveDir(snapshot_dir);
        }
    } while (false);
    making_snapshot_.store(false, std::memory_order_release);
    return ret;
}

bool DiskTableSnapshot::Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) {
    // TODO
    return true;
}

}
}
